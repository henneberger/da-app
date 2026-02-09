import { NextResponse } from "next/server";
import { assertNamespace, ensureNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

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
      "Missing ConfigMap da-app-project-manifest in this namespace. Create the project from a sample first, or store a manifest configmap.",
    );
  }
  const obj: unknown = JSON.parse(cm.stdout);
  if (!isRecord(obj) || !isRecord(obj.data)) {
    throw new Error("Invalid manifest configmap shape");
  }
  const manifest = obj.data["manifest.yaml"];
  if (typeof manifest !== "string" || !manifest.trim()) {
    throw new Error("ConfigMap da-app-project-manifest had empty data.manifest.yaml");
  }
  return manifest;
}

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const manifest = await getStoredManifest(ns);
    return new NextResponse(manifest, {
      status: 200,
      headers: { "content-type": "text/yaml; charset=utf-8" },
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

export async function PUT(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    await ensureNamespace(ns);

    const manifest = await req.text();
    if (!manifest.trim()) {
      return NextResponse.json(
        { error: "Manifest body was empty" },
        { status: 400 },
      );
    }

    const apply = await kubectl(["apply", "-n", ns, "-f", "-"], { input: manifest });
    if (!apply.ok) {
      return NextResponse.json(
        {
          error: "kubectl apply failed",
          cmd: apply.cmd,
          stderr: apply.stderr,
          stdout: apply.stdout,
        },
        { status: 500 },
      );
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
      { input: manifest },
    );
    if (!cmGen.ok) {
      return NextResponse.json(
        {
          error: "Failed to generate manifest configmap yaml",
          cmd: cmGen.cmd,
          stderr: cmGen.stderr,
          stdout: cmGen.stdout,
        },
        { status: 500 },
      );
    }

    const cmApply = await kubectl(["apply", "-f", "-"], { input: cmGen.stdout });
    if (!cmApply.ok) {
      return NextResponse.json(
        {
          error: "Failed to apply manifest configmap",
          cmd: cmApply.cmd,
          stderr: cmApply.stderr,
          stdout: cmApply.stdout,
        },
        { status: 500 },
      );
    }

    return NextResponse.json({ ok: true, applied: apply.stdout.trim() });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

