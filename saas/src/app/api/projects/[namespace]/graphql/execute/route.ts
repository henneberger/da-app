import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

type Body = {
  endpointUrl: string;
  query: string;
  variables?: Record<string, unknown> | null;
  operationName?: string | null;
};

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

  const endpointUrl = String(body.endpointUrl ?? "").trim();
  const query = String(body.query ?? "");
  if (!endpointUrl || !query.trim()) {
    return NextResponse.json(
      { error: "endpointUrl and query are required" },
      { status: 400 },
    );
  }

  try {
    assertNamespace(ns);
    const res = await fetch(endpointUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query,
        variables: body.variables ?? null,
        operationName: body.operationName ?? null,
      }),
    });
    const text = await res.text();
    // Try to preserve JSON response if possible.
    try {
      const json = JSON.parse(text);
      return NextResponse.json({ ok: res.ok, status: res.status, body: json });
    } catch {
      return NextResponse.json({ ok: res.ok, status: res.status, bodyText: text });
    }
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

