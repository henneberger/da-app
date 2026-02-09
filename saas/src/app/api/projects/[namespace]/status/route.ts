import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function parseItems(stdout: string): unknown[] {
  try {
    const obj: unknown = JSON.parse(stdout);
    if (isRecord(obj) && Array.isArray(obj.items)) return obj.items as unknown[];
  } catch {
    // ignore
  }
  return [];
}

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();

  try {
    assertNamespace(ns);

    const deploy = await kubectl(["get", "deploy", "-n", ns, "-o", "json"]);
    const pods = await kubectl(["get", "pods", "-n", ns, "-o", "json"]);
    const svcs = await kubectl(["get", "svc", "-n", ns, "-o", "json"]);

    if (!deploy.ok && !pods.ok && !svcs.ok) {
      return NextResponse.json(
        { error: "Failed to query namespace status" },
        { status: 500 },
      );
    }

    const deployments = deploy.ok ? parseItems(deploy.stdout) : [];
    const podsItems = pods.ok ? parseItems(pods.stdout) : [];
    const services = svcs.ok ? parseItems(svcs.stdout) : [];

    const podPhaseCounts: Record<string, number> = {};
    for (const p of podsItems) {
      const ph =
        isRecord(p) && isRecord(p.status) && typeof p.status.phase === "string"
          ? p.status.phase
          : "Unknown";
      podPhaseCounts[ph] = (podPhaseCounts[ph] ?? 0) + 1;
    }

    const deploymentSummaries = deployments.map((d: unknown) => {
      if (!isRecord(d)) return { name: "unknown", ready: "0/0", updated: 0 };
      const md = isRecord(d.metadata) ? d.metadata : {};
      const st = isRecord(d.status) ? d.status : {};
      const name = typeof md.name === "string" ? md.name : "unknown";
      const readyReplicas =
        typeof st.readyReplicas === "number" ? st.readyReplicas : 0;
      const replicas = typeof st.replicas === "number" ? st.replicas : 0;
      const updated = typeof st.updatedReplicas === "number" ? st.updatedReplicas : 0;
      return { name, ready: `${readyReplicas}/${replicas}`, updated };
    });

    return NextResponse.json({
      namespace: ns,
      deployments: deploymentSummaries,
      podPhaseCounts,
      services: services.map((s: unknown) => {
        if (!isRecord(s)) return { name: "unknown", type: "ClusterIP" };
        const md = isRecord(s.metadata) ? s.metadata : {};
        const spec = isRecord(s.spec) ? s.spec : {};
        return {
          name: typeof md.name === "string" ? md.name : "unknown",
          type: typeof spec.type === "string" ? spec.type : "ClusterIP",
        };
      }),
    });
  } catch (e: unknown) {
    return NextResponse.json(
      { error: errorMessage(e) },
      { status: 500 },
    );
  }
}
