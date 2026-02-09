import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { getPortForward } from "@/lib/portforward";

export const runtime = "nodejs";

export async function GET(
  req: Request,
  { params }: { params: Promise<{ namespace: string; id: string; path?: string[] }> },
) {
  const { namespace, id, path } = await params;
  const ns = String(namespace ?? "").trim();
  const handleId = String(id ?? "").trim();

  try {
    assertNamespace(ns);
    const h = getPortForward(handleId);
    if (!h || h.target.namespace !== ns) {
      return NextResponse.json(
        { error: "port-forward handle not found" },
        { status: 404 },
      );
    }

    const u = new URL(req.url);
    const rel = Array.isArray(path) && path.length ? `/${path.join("/")}` : "/";
    const targetUrl = `http://127.0.0.1:${h.localPort}${rel}${u.search}`;

    const upstream = await fetch(targetUrl, {
      method: "GET",
      headers: {
        // avoid leaking host and such; keep minimal headers
        "user-agent": req.headers.get("user-agent") ?? "da-app-saas",
        accept: req.headers.get("accept") ?? "*/*",
      },
    });

    const headers = new Headers(upstream.headers);
    headers.delete("content-security-policy");
    headers.delete("x-frame-options");

    return new Response(upstream.body, {
      status: upstream.status,
      headers,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

