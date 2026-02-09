import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { psql } from "../_psql";

export const runtime = "nodejs";

export async function GET(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  const { searchParams } = new URL(req.url);
  const db = String(searchParams.get("db") ?? "").trim();
  const schema = String(searchParams.get("schema") ?? "public").trim();

  if (!db) {
    return NextResponse.json({ error: "db is required" }, { status: 400 });
  }

  try {
    assertNamespace(ns);
    // table_schema, table_name, table_type
    const sql = `
      select table_schema, table_name, table_type
      from information_schema.tables
      where table_schema = '${schema.replace(/'/g, "''")}'
      order by table_type, table_name;
    `;
    const q = await psql(ns, db, sql);
    if (!q.ok) {
      return NextResponse.json(
        { error: "psql failed", cmd: q.cmd, stderr: q.stderr, stdout: q.stdout },
        { status: 500 },
      );
    }
    const tables = q.stdout
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const [s, name, type] = line.split("\t");
        return { schema: s, name, type };
      });
    return NextResponse.json({ tables });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

