import { NextResponse } from "next/server";
import path from "node:path";
import { ensureNamespace, kubectl, assertNamespace } from "@/lib/k8s";
import { readSample, samplePath } from "@/lib/repo";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

type Body = {
  namespace: string;
  sample?: string;
};

export async function POST(req: Request) {
  let body: Body;
  try {
    body = (await req.json()) as Body;
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const ns = String(body.namespace ?? "").trim();
  const sample = path.basename(String(body.sample ?? "web-forum.yaml"));

  try {
    assertNamespace(ns);
    await ensureNamespace(ns);

    // Apply sample manifest into the namespace.
    const apply = await kubectl(["apply", "-n", ns, "-f", samplePath(sample)]);
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

    // Store the source manifest so we can support clone without having to reconstruct yaml.
    const manifestText = await readSample(sample);
    const cm = await kubectl(
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
      { input: manifestText },
    );
    if (!cm.ok) {
      return NextResponse.json(
        {
          error: "Failed to generate manifest configmap",
          cmd: cm.cmd,
          stderr: cm.stderr,
          stdout: cm.stdout,
        },
        { status: 500 },
      );
    }

    const cmApply = await kubectl(["apply", "-f", "-"], { input: cm.stdout });
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

    return NextResponse.json({
      ok: true,
      namespace: ns,
      applied: apply.stdout.trim(),
    });
  } catch (e: unknown) {
    return NextResponse.json(
      { error: errorMessage(e) },
      { status: 500 },
    );
  }
}
