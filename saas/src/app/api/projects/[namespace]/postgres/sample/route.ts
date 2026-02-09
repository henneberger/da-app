import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { psql } from "../_psql";

export const runtime = "nodejs";

function ident(x: string): string {
  // Quote identifiers to avoid injection; double-quote and escape internal quotes.
  return `"${x.replace(/"/g, '""')}"`;
}

export async function GET(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  const { searchParams } = new URL(req.url);
  const db = String(searchParams.get("db") ?? "").trim();
  const schema = String(searchParams.get("schema") ?? "public").trim();
  const table = String(searchParams.get("table") ?? "").trim();
  const limit = Math.max(1, Math.min(200, Number(searchParams.get("limit") ?? "50")));

  if (!db || !schema || !table) {
    return NextResponse.json(
      { error: "db, schema, and table are required" },
      { status: 400 },
    );
  }

  try {
    assertNamespace(ns);

    const colsQ = await psql(
      ns,
      db,
      `
        select column_name
        from information_schema.columns
        where table_schema = '${schema.replace(/'/g, "''")}'
          and table_name = '${table.replace(/'/g, "''")}'
        order by ordinal_position;
      `,
    );
    if (!colsQ.ok) {
      return NextResponse.json(
        { error: "psql failed", cmd: colsQ.cmd, stderr: colsQ.stderr, stdout: colsQ.stdout },
        { status: 500 },
      );
    }
    const columns = colsQ.stdout
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);

    const sel =
      columns.length > 0
        ? columns.map((c) => ident(c)).join(", ")
        : "*";

    const dataQ = await psql(
      ns,
      db,
      `select ${sel} from ${ident(schema)}.${ident(table)} limit ${limit};`,
    );
    if (!dataQ.ok) {
      return NextResponse.json(
        { error: "psql failed", cmd: dataQ.cmd, stderr: dataQ.stderr, stdout: dataQ.stdout },
        { status: 500 },
      );
    }

    const rows = dataQ.stdout
      .split("\n")
      .map((s) => s.replace(/\r$/, ""))
      .filter((s) => s.length > 0)
      .map((line) => line.split("\t"));

    return NextResponse.json({ columns, rows });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

