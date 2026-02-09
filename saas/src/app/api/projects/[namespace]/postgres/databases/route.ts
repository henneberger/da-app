import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { psql } from "../_psql";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const q = await psql(
      ns,
      "postgres",
      "select datname from pg_database where datistemplate = false order by datname;",
    );
    if (!q.ok) {
      return NextResponse.json(
        { error: "psql failed", cmd: q.cmd, stderr: q.stderr, stdout: q.stdout },
        { status: 500 },
      );
    }
    const databases = q.stdout
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);
    return NextResponse.json({ databases });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

