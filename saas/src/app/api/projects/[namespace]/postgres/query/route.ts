import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

type Body = { sql: string };

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

  const sql = String(body.sql ?? "").trim();
  if (!sql) {
    return NextResponse.json({ error: "SQL was empty" }, { status: 400 });
  }

  try {
    assertNamespace(ns);
    const { searchParams } = new URL(req.url);
    const db = String(searchParams.get("db") ?? "forum").trim() || "forum";
    const exec = await kubectl([
      "exec",
      "-n",
      ns,
      "deploy/postgres",
      "--",
      "psql",
      "-U",
      "app",
      "-d",
      db,
      "-v",
      "ON_ERROR_STOP=1",
      "-P",
      "pager=off",
      "-c",
      sql,
    ]);
    if (!exec.ok) {
      return NextResponse.json(
        { error: "psql failed", cmd: exec.cmd, stderr: exec.stderr, stdout: exec.stdout },
        { status: 500 },
      );
    }
    return NextResponse.json({ ok: true, stdout: exec.stdout, stderr: exec.stderr });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}
