import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { startPortForward, stopPortForward } from "@/lib/portforward";
import { kubectl } from "@/lib/k8s";
import { mcpInitialize, mcpToolsList, type McpHttpClient } from "@/lib/mcp-http";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

type Body = {
  path?: string; // default /mcp
  auth?: { type: "bearer"; token: string } | { type: "basic"; username: string; password: string } | null;
};

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
      if (n.includes("graphql")) score += 8;
      if (n.includes("vertx")) score += 6;
      if (n.includes("forum")) score += 2;
      if (port === 8080 || port === 4000 || port === 3000 || port === 80) score += 5;
      out.push({ name, port, score });
    }
  }
  out.sort((a, b) => b.score - a.score);
  return out;
}

function authHeaders(auth: Body["auth"]): Record<string, string> {
  if (!auth) return {};
  if (auth.type === "bearer") return { authorization: `Bearer ${auth.token}` };
  const token = Buffer.from(`${auth.username}:${auth.password}`).toString("base64");
  return { authorization: `Basic ${token}` };
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
    body = {};
  }

  const path = String(body.path ?? "/mcp").trim() || "/mcp";

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

        const baseUrl = pf.url.replace(/\/$/, "");
        const endpointUrl = baseUrl + path;

        const client: McpHttpClient = {
          endpointUrl,
          headers: authHeaders(body.auth),
        };

        try {
          await mcpInitialize(client);
          const tools = await mcpToolsList(client);
          return NextResponse.json({
            ok: true,
            service: { name: c.name, port: c.port },
            handle: {
              id: pf.id,
              target: pf.target,
              localPort: pf.localPort,
              startedAt: pf.startedAt,
            },
            endpointUrl,
            toolsCount: tools.length,
            authRequired: false,
          });
        } catch (e: unknown) {
          // If unauthorized, treat as connectable but needs auth.
          const msg = errorMessage(e);
          const status =
            e &&
            typeof e === "object" &&
            "status" in (e as Record<string, unknown>) &&
            typeof (e as Record<string, unknown>).status === "number"
              ? ((e as Record<string, unknown>).status as number)
              : null;
          if (status === 401 || status === 403 || msg.includes("returned HTML")) {
            return NextResponse.json({
              ok: true,
              service: { name: c.name, port: c.port },
              handle: {
                id: pf.id,
                target: pf.target,
                localPort: pf.localPort,
                startedAt: pf.startedAt,
              },
              endpointUrl,
              toolsCount: 0,
              authRequired: true,
            });
          }
          stopPortForward(pf.id);
          tried.push({ svc: c.name, port: c.port, error: msg });
        }
      } catch (e: unknown) {
        tried.push({ svc: c.name, port: c.port, error: errorMessage(e) });
      }
    }

    return NextResponse.json(
      { error: "Could not connect to an MCP endpoint via port-forward.", tried },
      { status: 500 },
    );
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}
