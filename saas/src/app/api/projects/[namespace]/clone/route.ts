import { NextResponse } from "next/server";
import { assertNamespace, ensureNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

type Body = { targetNamespace: string };

export async function POST(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const source = String(namespace ?? "").trim();

  let body: Body;
  try {
    body = (await req.json()) as Body;
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const target = String(body.targetNamespace ?? "").trim();

  try {
    assertNamespace(source);
    assertNamespace(target);

    const cm = await kubectl([
      "get",
      "configmap",
      "da-app-project-manifest",
      "-n",
      source,
      "-o",
      "json",
    ]);
    if (!cm.ok) {
      return NextResponse.json(
        {
          error:
            "Source project is missing da-app-project-manifest configmap. Re-apply the sample or create the manifest configmap first.",
          cmd: cm.cmd,
          stderr: cm.stderr,
          stdout: cm.stdout,
        },
        { status: 400 },
      );
    }

    const cmObj = JSON.parse(cm.stdout);
    const manifest = cmObj?.data?.["manifest.yaml"];
    if (!manifest || typeof manifest !== "string") {
      return NextResponse.json(
        { error: "Manifest configmap did not contain data.manifest.yaml" },
        { status: 400 },
      );
    }

    await ensureNamespace(target);

    const apply = await kubectl(["apply", "-n", target, "-f", "-"], {
      input: manifest,
    });
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

    // Install/update manifest configmap in target namespace.
    const cmGen = await kubectl(
      [
        "create",
        "configmap",
        "da-app-project-manifest",
        "-n",
        target,
        `--from-file=manifest.yaml=/dev/stdin`,
        "--dry-run=client",
        "-o",
        "yaml",
      ],
      { input: manifest },
    );
    if (cmGen.ok) {
      await kubectl(["apply", "-f", "-"], { input: cmGen.stdout });
    }

    return NextResponse.json({
      ok: true,
      sourceNamespace: source,
      targetNamespace: target,
      applied: apply.stdout.trim(),
    });
  } catch (e: unknown) {
    return NextResponse.json(
      { error: errorMessage(e) },
      { status: 500 },
    );
  }
}
