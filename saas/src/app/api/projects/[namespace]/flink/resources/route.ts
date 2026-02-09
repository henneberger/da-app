import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

type FlinkCatalog = { name: string; sql: string };
type FlinkSqlJob = {
  name: string;
  image: string | null;
  serviceAccount: string | null;
  catalogRefs: string[];
  sql: string;
};

async function resourceForKinds(
  group: string,
  kinds: string[],
): Promise<Record<string, { resource: string; kind: string }>> {
  const crdRes = await kubectl(["get", "crd", "-o", "json"]);
  if (!crdRes.ok) return {};
  const crdsObj: unknown = JSON.parse(crdRes.stdout);
  const items: unknown[] =
    isRecord(crdsObj) && Array.isArray(crdsObj.items)
      ? (crdsObj.items as unknown[])
      : [];

  const want = new Set(kinds);
  const out: Record<string, { resource: string; kind: string }> = {};
  for (const crd of items) {
    if (!isRecord(crd) || !isRecord(crd.spec)) continue;
    const spec = crd.spec as Record<string, unknown>;
    if (spec.scope !== "Namespaced") continue;
    const g = typeof spec.group === "string" ? spec.group : null;
    if (!g || g !== group) continue;
    const names = isRecord(spec.names) ? (spec.names as Record<string, unknown>) : null;
    const plural = names && typeof names.plural === "string" ? names.plural : null;
    const kind = names && typeof names.kind === "string" ? names.kind : null;
    if (!plural || !kind) continue;
    if (!want.has(kind)) continue;
    out[kind] = { kind, resource: `${plural}.${g}` };
  }
  return out;
}

function parseItems(obj: unknown): unknown[] {
  return isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];
}

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();

  try {
    assertNamespace(ns);

    const resources = await resourceForKinds("dev.henneberger", [
      "FlinkCatalog",
      "FlinkSqlJob",
    ]);

    const catalogs: FlinkCatalog[] = [];
    const jobs: FlinkSqlJob[] = [];

    if (resources.FlinkCatalog) {
      const res = await kubectl(["get", resources.FlinkCatalog.resource, "-n", ns, "-o", "json"]);
      if (res.ok) {
        const obj: unknown = JSON.parse(res.stdout);
        for (const it of parseItems(obj)) {
          if (!isRecord(it)) continue;
          const md = isRecord(it.metadata) ? (it.metadata as Record<string, unknown>) : {};
          const spec = isRecord(it.spec) ? (it.spec as Record<string, unknown>) : {};
          catalogs.push({
            name: typeof md.name === "string" ? md.name : "unknown",
            sql: typeof spec.sql === "string" ? spec.sql : "",
          });
        }
      }
    }

    if (resources.FlinkSqlJob) {
      const res = await kubectl(["get", resources.FlinkSqlJob.resource, "-n", ns, "-o", "json"]);
      if (res.ok) {
        const obj: unknown = JSON.parse(res.stdout);
        for (const it of parseItems(obj)) {
          if (!isRecord(it)) continue;
          const md = isRecord(it.metadata) ? (it.metadata as Record<string, unknown>) : {};
          const spec = isRecord(it.spec) ? (it.spec as Record<string, unknown>) : {};
          const refs = Array.isArray(spec.catalogRefs) ? (spec.catalogRefs as unknown[]) : [];
          jobs.push({
            name: typeof md.name === "string" ? md.name : "unknown",
            image: typeof spec.image === "string" ? spec.image : null,
            serviceAccount: typeof spec.serviceAccount === "string" ? spec.serviceAccount : null,
            catalogRefs: refs.filter((x) => typeof x === "string") as string[],
            sql: typeof spec.sql === "string" ? spec.sql : "",
          });
        }
      }
    }

    catalogs.sort((a, b) => a.name.localeCompare(b.name));
    jobs.sort((a, b) => a.name.localeCompare(b.name));

    return NextResponse.json({ namespace: ns, catalogs, jobs });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

