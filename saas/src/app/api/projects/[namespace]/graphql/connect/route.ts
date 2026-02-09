import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { startPortForward, stopPortForward } from "@/lib/portforward";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

type Candidate = { name: string; port: number; score: number };

async function listServiceCandidates(ns: string): Promise<Candidate[]> {
  const svcs = await kubectl(["get", "svc", "-n", ns, "-o", "json"]);
  if (!svcs.ok) return [];
  const obj: unknown = JSON.parse(svcs.stdout);
  const items: unknown[] =
    isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];

  const out: Candidate[] = [];
  for (const it of items) {
    if (!isRecord(it)) continue;
    const md = isRecord(it.metadata) ? it.metadata : {};
    const spec = isRecord(it.spec) ? it.spec : {};
    const name = typeof md.name === "string" ? md.name : null;
    if (!name) continue;
    const portsArr = Array.isArray(spec.ports) ? (spec.ports as unknown[]) : [];
    for (const p of portsArr) {
      if (!isRecord(p) || typeof p.port !== "number") continue;
      const port = p.port;
      let score = 0;
      const n = name.toLowerCase();
      if (n.includes("graphql")) score += 10;
      if (n.includes("vertx")) score += 6;
      if (n.includes("forum")) score += 2;
      if (port === 8080 || port === 4000 || port === 3000 || port === 80) score += 5;
      out.push({ name, port, score });
    }
  }
  out.sort((a, b) => b.score - a.score);
  return out;
}

async function probeGraphQL(baseUrl: string): Promise<string | null> {
  const paths = ["/graphql", "/"];
  for (const path of paths) {
    const url = baseUrl.replace(/\/$/, "") + path;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "query { __typename }" }),
      });
      if (!res.ok) continue;
      const json: unknown = await res.json().catch(() => null);
      if (isRecord(json) && isRecord(json.data) && "__typename" in json.data) {
        return url;
      }
    } catch {
      // ignore
    }
  }
  return null;
}

export async function POST(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const candidates = await listServiceCandidates(ns);
    if (!candidates.length) {
      return NextResponse.json(
        { error: "No Services found in namespace to port-forward to." },
        { status: 404 },
      );
    }

    const tried: { svc: string; port: number; error: string }[] = [];
    for (const c of candidates.slice(0, 15)) {
      try {
        const pf = await startPortForward({
          namespace: ns,
          kind: "svc",
          name: c.name,
          remotePort: c.port,
        });
        const endpoint = await probeGraphQL(pf.url);
        if (endpoint) {
          return NextResponse.json({
            ok: true,
            service: { name: c.name, port: c.port },
            handle: {
              id: pf.id,
              target: pf.target,
              localPort: pf.localPort,
              startedAt: pf.startedAt,
            },
            baseUrl: pf.url,
            endpointUrl: endpoint,
          });
        }
        stopPortForward(pf.id);
        tried.push({ svc: c.name, port: c.port, error: "Probe failed" });
      } catch (e: unknown) {
        tried.push({ svc: c.name, port: c.port, error: errorMessage(e) });
      }
    }

    return NextResponse.json(
      {
        error:
          "Could not connect to a GraphQL endpoint via port-forward. Try ensuring the operator created a service for the GraphQL server.",
        tried,
      },
      { status: 500 },
    );
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}
