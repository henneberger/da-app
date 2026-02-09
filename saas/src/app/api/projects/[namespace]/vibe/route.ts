import { NextResponse } from "next/server";
import crypto from "node:crypto";
import { assertNamespace, ensureNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { getCodexModel, getOpenAIClient } from "@/lib/openai";

export const runtime = "nodejs";

type Body = { message: string };

function sseEvent(event: string, data: unknown): Uint8Array {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  return new TextEncoder().encode(payload);
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

async function getStoredManifest(ns: string): Promise<string> {
  const cm = await kubectl([
    "get",
    "configmap",
    "da-app-project-manifest",
    "-n",
    ns,
    "-o",
    "json",
  ]);
  if (!cm.ok) {
    throw new Error(
      "Missing ConfigMap da-app-project-manifest in this namespace. Create the project from a sample first.",
    );
  }
  const obj: unknown = JSON.parse(cm.stdout);
  const data =
    isRecord(obj) && isRecord(obj.data) ? (obj.data as Record<string, unknown>) : null;
  const manifest = data?.["manifest.yaml"];
  if (typeof manifest !== "string" || !manifest.trim()) {
    throw new Error("ConfigMap da-app-project-manifest had empty data.manifest.yaml");
  }
  return manifest;
}

async function discoverNamespacedCustomResources(): Promise<
  { resource: string; kind: string; group: string }[]
> {
  const crdRes = await kubectl(["get", "crd", "-o", "json"]);
  if (!crdRes.ok) return [];
  const crdsObj: unknown = JSON.parse(crdRes.stdout);
  const crdItems: unknown[] =
    isRecord(crdsObj) && Array.isArray(crdsObj.items) ? (crdsObj.items as unknown[]) : [];

  const resources: { resource: string; kind: string; group: string }[] = [];
  for (const crd of crdItems) {
    if (!isRecord(crd) || !isRecord(crd.spec)) continue;
    const spec = crd.spec as Record<string, unknown>;
    if (spec.scope !== "Namespaced") continue;
    const group = typeof spec.group === "string" ? spec.group : null;
    const names = isRecord(spec.names) ? (spec.names as Record<string, unknown>) : null;
    const plural = names && typeof names.plural === "string" ? names.plural : null;
    const kind = names && typeof names.kind === "string" ? names.kind : null;
    if (!group || !plural || !kind) continue;
    resources.push({ resource: `${plural}.${group}`, kind, group });
  }
  resources.sort((a, b) => a.resource.localeCompare(b.resource));
  return resources;
}

async function exportCustomResourcesYaml(ns: string): Promise<string> {
  const resources = await discoverNamespacedCustomResources();
  const parts: string[] = [];
  for (const r of resources) {
    const res = await kubectl(["get", r.resource, "-n", ns, "-o", "yaml"]);
    if (!res.ok) continue;
    // Avoid huge empty lists.
    if (!res.stdout.includes("\nitems:\n") && !res.stdout.includes("\nitems:")) continue;
    if (res.stdout.includes("items: []")) continue;
    parts.push(`# Resource: ${r.resource}\n${res.stdout.trimEnd()}`);
  }
  return parts.join("\n---\n");
}

async function summarizePodIssues(ns: string): Promise<string[]> {
  const pods = await kubectl(["get", "pods", "-n", ns, "-o", "json"]);
  if (!pods.ok) return [];
  const obj: unknown = JSON.parse(pods.stdout);
  const items: unknown[] =
    isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];

  const issues: string[] = [];
  for (const p of items) {
    if (!isRecord(p)) continue;
    const md = isRecord(p.metadata) ? (p.metadata as Record<string, unknown>) : {};
    const st = isRecord(p.status) ? (p.status as Record<string, unknown>) : {};
    const name = typeof md.name === "string" ? md.name : "unknown";
    const phase = typeof st.phase === "string" ? st.phase : "Unknown";
    const cs = Array.isArray(st.containerStatuses) ? (st.containerStatuses as unknown[]) : [];
    for (const c of cs) {
      if (!isRecord(c)) continue;
      const cname = typeof c.name === "string" ? c.name : "container";
      const ready = c.ready === true;
      const state = isRecord(c.state) ? (c.state as Record<string, unknown>) : {};
      const waiting = isRecord(state.waiting) ? (state.waiting as Record<string, unknown>) : null;
      const reason = waiting && typeof waiting.reason === "string" ? waiting.reason : null;
      const message = waiting && typeof waiting.message === "string" ? waiting.message : null;
      if (!ready || phase !== "Running") {
        issues.push(
          `${name}/${cname}: phase=${phase}, ready=${ready}${
            reason ? `, reason=${reason}` : ""
          }${message ? `, message=${message}` : ""}`,
        );
      }
    }
  }
  return issues.slice(0, 50);
}

export async function POST(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();

  let body: Body;
  try {
    body = (await req.json()) as Body;
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const message = String(body.message ?? "").trim();
  if (!message) {
    return NextResponse.json({ error: "message was empty" }, { status: 400 });
  }

  try {
    assertNamespace(ns);
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 400 });
  }

  const stream = new ReadableStream<Uint8Array>({
    start: async (controller) => {
      const runId = crypto.randomUUID();
      const send = (event: string, data: unknown) => controller.enqueue(sseEvent(event, data));

      try {
        send("run", { runId, namespace: ns });
        send("step", { id: "export", status: "running", label: "Exporting current state" });

        await ensureNamespace(ns);
        const manifest = await getStoredManifest(ns);
        const crExport = await exportCustomResourcesYaml(ns);

        send("step", { id: "export", status: "done" });
        send("step", { id: "llm", status: "running", label: "Generating patch with Codex" });

        const client = getOpenAIClient();
        const model = getCodexModel();

        const system = [
          "You are an expert Kubernetes + data-pipeline engineer.",
          "Task: modify the data product by editing its Kubernetes YAML manifest.",
          "Rules:",
          "- Output ONLY valid JSON with keys: summary (string) and manifest (string). No markdown fences.",
          "- The manifest value MUST be a single Kubernetes multi-document YAML manifest that is apply-ready.",
          "- Keep object names stable unless asked; avoid deleting resources unless explicitly asked.",
          "- Prefer minimal diffs: edit only what is needed to satisfy the user request.",
          "- Do not add a namespace field; we apply with kubectl -n.",
          "- Ensure YAML is valid and apply-ready.",
        ].join("\n");

        const user = [
          `User request:\n${message}`,
          "\nCurrent stored manifest (authoritative source-of-truth for cloning):",
          manifest,
          "\nExported current Custom Resources in namespace (may include runtime drift):",
          crExport || "(none)",
        ].join("\n\n");

        let outText = "";

        const resp = await client.responses.create({
          model,
          stream: true,
          input: [
            { role: "developer", content: system },
            { role: "user", content: user },
          ],
        });

        for await (const ev of resp) {
          if (ev.type === "response.output_text.delta") {
            outText += ev.delta;
          }
          if (ev.type === "response.completed") {
            // handled after loop
          }
          if (ev.type === "response.failed") {
            send("error", { message: "OpenAI response failed" });
          }
        }

        send("step", { id: "llm", status: "done" });

        const raw = outText.trim();
        let summary = "";
        let newManifest = "";
        try {
          const parsed = JSON.parse(raw) as { summary?: unknown; manifest?: unknown };
          summary = typeof parsed.summary === "string" ? parsed.summary : "";
          newManifest = typeof parsed.manifest === "string" ? parsed.manifest : "";
        } catch {
          // Fallback: treat raw output as the manifest if JSON parsing fails.
          newManifest = raw;
        }
        if (!newManifest) {
          throw new Error("Model returned empty manifest");
        }

        send("step", { id: "apply", status: "running", label: "Applying to Kubernetes" });
        const apply = await kubectl(["apply", "-n", ns, "-f", "-"], { input: newManifest });
        if (!apply.ok) {
          throw new Error(`kubectl apply failed: ${apply.stderr || apply.stdout}`);
        }

        const cmGen = await kubectl(
          [
            "create",
            "configmap",
            "da-app-project-manifest",
            "-n",
            ns,
            `--from-file=manifest.yaml=/dev/stdin`,
            "--dry-run=client",
            "-o",
            "yaml",
          ],
          { input: newManifest },
        );
        if (cmGen.ok) {
          await kubectl(["apply", "-f", "-"], { input: cmGen.stdout });
        }

        send("step", { id: "apply", status: "done", output: apply.stdout.trim() });

        send("step", { id: "check", status: "running", label: "Checking k8s for issues" });
        const issues = await summarizePodIssues(ns);
        send("step", { id: "check", status: "done", issues });

        send("summary", { text: summary || "Applied changes.", issuesCount: issues.length });
        send("done", { ok: true, issuesCount: issues.length });
      } catch (e: unknown) {
        send("error", { message: errorMessage(e) });
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}
