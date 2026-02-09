import { NextResponse } from "next/server";
import crypto from "node:crypto";
import YAML from "yaml";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function stableStringify(v: unknown): string {
  const seen = new WeakSet<object>();
  const norm = (x: unknown): unknown => {
    if (x === null) return null;
    if (typeof x !== "object") return x;
    if (Array.isArray(x)) return x.map(norm);
    if (seen.has(x as object)) return "[Circular]";
    seen.add(x as object);
    const o = x as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(o).sort()) {
      out[k] = norm(o[k]);
    }
    return out;
  };
  return JSON.stringify(norm(v));
}

function groupsFromManifest(manifestText: string | null): Set<string> {
  const groups = new Set<string>();
  if (!manifestText?.trim()) return groups;
  try {
    const docs = YAML.parseAllDocuments(manifestText);
    for (const d of docs) {
      const js = d.toJS({}) as unknown;
      if (!js || typeof js !== "object") continue;
      const r = js as Record<string, unknown>;
      const apiVersion = typeof r.apiVersion === "string" ? r.apiVersion : "";
      if (!apiVersion.includes("/")) continue;
      const group = apiVersion.split("/", 1)[0] ?? "";
      if (group) groups.add(group);
    }
  } catch {
    // ignore malformed YAML; we'll fall back to a small default allowlist.
  }
  return groups;
}

async function listCustomResourceSpecs(
  ns: string,
  allowedGroups: Set<string>,
): Promise<
  { apiVersion: string; kind: string; name: string; spec?: unknown }[]
> {
  // Discover CRDs, then list instances; hash only desired-state (spec) to avoid status/resourceVersion churn.
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
    // Only fingerprint CRs that belong to the data product's API group(s).
    // Otherwise, unrelated controllers (service mesh, cert-manager, etc) can churn specs and cause false "changes".
    if (group && !allowedGroups.has(group)) continue;
    const names = isRecord(spec.names) ? (spec.names as Record<string, unknown>) : null;
    const plural = names && typeof names.plural === "string" ? names.plural : null;
    const kind = names && typeof names.kind === "string" ? names.kind : null;
    if (!group || !plural || !kind) continue;
    resources.push({ resource: `${plural}.${group}`, kind, group });
  }

  const out: { apiVersion: string; kind: string; name: string; spec?: unknown }[] = [];
  for (const r of resources) {
    const list = await kubectl(["get", r.resource, "-n", ns, "-o", "json"]);
    if (!list.ok) continue;
    let obj: unknown;
    try {
      obj = JSON.parse(list.stdout);
    } catch {
      continue;
    }
    const its: unknown[] =
      isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];
    for (const it of its) {
      if (!isRecord(it)) continue;
      const md = isRecord(it.metadata) ? (it.metadata as Record<string, unknown>) : {};
      out.push({
        apiVersion: typeof it.apiVersion === "string" ? it.apiVersion : `${r.group}/unknown`,
        kind: typeof it.kind === "string" ? it.kind : r.kind,
        name: typeof md.name === "string" ? md.name : "unknown",
        spec: "spec" in it ? (it as Record<string, unknown>).spec : undefined,
      });
    }
  }
  out.sort((a, b) => (a.kind + "/" + a.name).localeCompare(b.kind + "/" + b.name));
  return out;
}

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const cm = await kubectl([
      "get",
      "configmap",
      "da-app-project-manifest",
      "-n",
      ns,
      "-o",
      "json",
    ]);
    let manifestText: string | null = null;
    if (cm.ok) {
      const cmObj: unknown = JSON.parse(cm.stdout);
      if (isRecord(cmObj) && isRecord(cmObj.data)) {
        const m = cmObj.data["manifest.yaml"];
        if (typeof m === "string") manifestText = m;
      }
    }

    const allowedGroups = groupsFromManifest(manifestText);
    // Fall back to the primary group used by our sample/operator if we can't infer groups.
    if (allowedGroups.size === 0) allowedGroups.add("dev.henneberger");

    const crs = await listCustomResourceSpecs(ns, allowedGroups);
    const payload = stableStringify({ manifest: manifestText, crs });
    const fp = crypto.createHash("sha256").update(payload).digest("hex");
    return NextResponse.json({ fingerprint: fp });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}
