import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { findNamespacedResourceByKind } from "../../crd-utils";

export const runtime = "nodejs";

type Body = {
  mutationCrName: string;
  args?: Record<string, unknown>;
  jwt?: Record<string, unknown>;
  argTypes?: Record<string, unknown>;
  analyze?: boolean;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function sqlEscapeLiteral(v: string): string {
  return v.replace(/\\/g, "\\\\").replace(/'/g, "''");
}

function toParamType(t: unknown): "text" | "int" | "bigint" | "bool" | "jsonb" {
  const s = String(t ?? "text").toLowerCase();
  if (s === "int" || s === "integer") return "int";
  if (s === "bigint") return "bigint";
  if (s === "bool" || s === "boolean") return "bool";
  if (s === "json" || s === "jsonb") return "jsonb";
  return "text";
}

function renderSqlLiteral(value: unknown, type: ReturnType<typeof toParamType>): string {
  const raw = value == null ? "" : String(value);
  if (type === "int" || type === "bigint") {
    if (!/^-?\\d+$/.test(raw.trim())) {
      throw new Error(`Expected ${type} but got "${raw}"`);
    }
    return raw.trim();
  }
  if (type === "bool") {
    const s = raw.trim().toLowerCase();
    if (s === "true" || s === "t" || s === "1") return "true";
    if (s === "false" || s === "f" || s === "0") return "false";
    throw new Error(`Expected bool but got "${raw}"`);
  }
  if (type === "jsonb") {
    return `'${sqlEscapeLiteral(raw)}'::jsonb`;
  }
  return `'${sqlEscapeLiteral(raw)}'`;
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
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  try {
    assertNamespace(ns);
    const crName = String(body.mutationCrName ?? "").trim();
    if (!crName) {
      return NextResponse.json(
        { error: "mutationCrName is required" },
        { status: 400 },
      );
    }

    const r = await findNamespacedResourceByKind("PostgresMutation");
    if (!r) {
      return NextResponse.json(
        { error: "CRD kind=PostgresMutation not found in cluster" },
        { status: 404 },
      );
    }

    const mRes = await kubectl(["get", r.resource, crName, "-n", ns, "-o", "json"]);
    if (!mRes.ok) {
      return NextResponse.json(
        { error: mRes.stderr || mRes.stdout, cmd: mRes.cmd },
        { status: 500 },
      );
    }

    const mObj: unknown = JSON.parse(mRes.stdout);
    const spec =
      isRecord(mObj) && isRecord(mObj.spec) ? (mObj.spec as Record<string, unknown>) : {};
    const sql = typeof spec.sql === "string" ? spec.sql : "";
    const paramsArr = Array.isArray(spec.params) ? (spec.params as unknown[]) : [];
    if (!sql.trim()) {
      return NextResponse.json({ error: "Mutation spec.sql was empty" }, { status: 400 });
    }

    const args = isRecord(body.args) ? body.args : {};
    const jwt = isRecord(body.jwt) ? body.jwt : {};
    const argTypes = isRecord(body.argTypes) ? body.argTypes : {};

    const idxToLiteral = new Map<number, string>();
    for (const p of paramsArr) {
      if (!isRecord(p)) continue;
      const index = typeof p.index === "number" ? p.index : null;
      const source = isRecord(p.source) ? (p.source as Record<string, unknown>) : {};
      if (!index) continue;
      const kind = String(source.kind ?? "");
      const name = String(source.name ?? "");
      if (!name) continue;

      let value: unknown;
      if (kind === "ARG") {
        if (!(name in args)) {
          return NextResponse.json(
            { error: `Missing argument "${name}" (index ${index})` },
            { status: 400 },
          );
        }
        value = args[name];
      } else if (kind === "JWT") {
        if (!(name in jwt)) {
          return NextResponse.json(
            { error: `Missing jwt claim "${name}" (index ${index})` },
            { status: 400 },
          );
        }
        value = jwt[name];
      } else {
        return NextResponse.json(
          { error: `Param source kind "${kind}" is not supported in this UI yet.` },
          { status: 400 },
        );
      }

      const type = toParamType(argTypes[name]);
      const lit = renderSqlLiteral(value, type);
      idxToLiteral.set(index, lit);
    }

    const maxIndex = Math.max(0, ...Array.from(idxToLiteral.keys()));
    let rendered = sql;
    for (let i = maxIndex; i >= 1; i--) {
      const lit = idxToLiteral.get(i);
      if (!lit) continue;
      rendered = rendered.replaceAll(`$${i}`, lit);
    }

    const analyze = body.analyze === true;
    const explainStmt = analyze
      ? `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT)\n${rendered}`
      : `EXPLAIN (VERBOSE, FORMAT TEXT)\n${rendered}`;

    const plan = await kubectl([
      "exec",
      "-n",
      ns,
      "deploy/postgres",
      "--",
      "psql",
      "-U",
      "app",
      "-d",
      "forum",
      "-v",
      "ON_ERROR_STOP=1",
      "-P",
      "pager=off",
      "-c",
      explainStmt,
    ]);
    if (!plan.ok) {
      return NextResponse.json(
        { error: "EXPLAIN failed", cmd: plan.cmd, stderr: plan.stderr, stdout: plan.stdout },
        { status: 500 },
      );
    }

    const run = await kubectl([
      "exec",
      "-n",
      ns,
      "deploy/postgres",
      "--",
      "psql",
      "-U",
      "app",
      "-d",
      "forum",
      "-v",
      "ON_ERROR_STOP=1",
      "-P",
      "pager=off",
      "-c",
      rendered,
    ]);
    if (!run.ok) {
      return NextResponse.json(
        { error: "Mutation execution failed", cmd: run.cmd, stderr: run.stderr, stdout: run.stdout, renderedSql: rendered },
        { status: 500 },
      );
    }

    return NextResponse.json({
      ok: true,
      renderedSql: rendered,
      explain: plan.stdout,
      stdout: run.stdout,
      stderr: run.stderr,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

