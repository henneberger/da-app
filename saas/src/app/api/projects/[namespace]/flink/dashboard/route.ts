import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { startPortForward, stopPortForward, type PortForwardHandle } from "@/lib/portforward";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function parseServiceCandidates(obj: unknown): { name: string; ports: number[] }[] {
  const items: unknown[] =
    isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];
  const out: { name: string; ports: number[] }[] = [];
  for (const it of items) {
    if (!isRecord(it)) continue;
    const md = isRecord(it.metadata) ? (it.metadata as Record<string, unknown>) : {};
    const spec = isRecord(it.spec) ? (it.spec as Record<string, unknown>) : {};
    const name = typeof md.name === "string" ? md.name : null;
    if (!name) continue;
    const portsArr = Array.isArray(spec.ports) ? (spec.ports as unknown[]) : [];
    const ports: number[] = [];
    for (const p of portsArr) {
      if (!isRecord(p)) continue;
      if (typeof p.port === "number") ports.push(p.port);
    }
    out.push({ name, ports });
  }
  return out;
}

async function listFlinkCandidates(
  ns: string,
): Promise<{ name: string; remotePort: number; score: number }[]> {
  const svcs = await kubectl(["get", "svc", "-n", ns, "-o", "json"]);
  if (!svcs.ok) return [];
  const obj: unknown = JSON.parse(svcs.stdout);
  const candidates = parseServiceCandidates(obj);

  // Prefer common REST/UI ports (8081) and service names containing flink/jobmanager.
  const scored = candidates
    .map((c) => {
      let score = 0;
      if (c.ports.includes(8081)) score += 10;
      const n = c.name.toLowerCase();
      if (n.includes("flink")) score += 3;
      if (n.includes("jobmanager") || n.includes("jm")) score += 2;
      if (c.ports.includes(8080)) score += 1;
      const remotePort = c.ports.includes(8081) ? 8081 : c.ports[0];
      return remotePort ? { name: c.name, remotePort, score } : null;
    })
    .filter((x): x is NonNullable<typeof x> => Boolean(x))
    .sort((a, b) => b.score - a.score);

  return scored;
}

async function probeHttp(url: string): Promise<boolean> {
  // Port-forwards can take a moment to come up; Flink can also briefly return 503 while booting.
  // We only need to know the TCP/HTTP path is reachable.
  for (let i = 0; i < 12; i++) {
    try {
      await fetch(url, { method: "GET" });
      return true;
    } catch {
      // wait a bit and retry
      await new Promise((r) => setTimeout(r, 200));
    }
  }
  return false;
}

export async function POST(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const candidates = await listFlinkCandidates(ns);
    if (!candidates.length) {
      return NextResponse.json(
        { error: "Could not auto-detect a Flink service (need a Service with port 8081)." },
        { status: 404 },
      );
    }

    const tried: { svc: string; port: number; error: string }[] = [];
    for (const c of candidates.slice(0, 10)) {
      try {
        const pf = await startPortForward({
          namespace: ns,
          kind: "svc",
          name: c.name,
          remotePort: c.remotePort,
        });
        const ok = await probeHttp(pf.url);
        if (!ok) {
          stopPortForward(pf.id);
          tried.push({ svc: c.name, port: c.remotePort, error: "HTTP probe failed" });
          continue;
        }

        return NextResponse.json({
          ok: true,
          handle: {
            id: pf.id,
            target: pf.target,
            localPort: pf.localPort,
            startedAt: pf.startedAt,
          } satisfies PortForwardHandle,
          url: pf.url,
        });
      } catch (e: unknown) {
        tried.push({ svc: c.name, port: c.remotePort, error: errorMessage(e) });
      }
    }

    return NextResponse.json(
      { error: "Failed to connect to Flink dashboard via port-forward.", tried },
      { status: 500 },
    );
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

export async function DELETE(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const body: unknown = await req.json().catch(() => ({}));
    const id =
      body && typeof body === "object" && "id" in body
        ? String((body as Record<string, unknown>).id ?? "")
        : "";
    if (!id) return NextResponse.json({ error: "id is required" }, { status: 400 });
    const ok = stopPortForward(id);
    return NextResponse.json({ ok });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}
