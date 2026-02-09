import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

type CrInstance = {
  apiVersion: string;
  kind: string;
  name: string;
};

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();

  try {
    assertNamespace(ns);

    const crdRes = await kubectl(["get", "crd", "-o", "json"]);
    if (!crdRes.ok) {
      return NextResponse.json(
        { error: crdRes.stderr || crdRes.stdout, cmd: crdRes.cmd },
        { status: 500 },
      );
    }

    const crdsObj: unknown = JSON.parse(crdRes.stdout);
    const items: unknown[] =
      isRecord(crdsObj) && Array.isArray(crdsObj.items)
        ? (crdsObj.items as unknown[])
        : [];

    const resources: { resource: string; kind: string; group: string }[] = [];
    for (const crd of items) {
      if (!isRecord(crd) || !isRecord(crd.spec)) continue;
      const spec = crd.spec;
      if (spec.scope !== "Namespaced") continue;
      const group = typeof spec.group === "string" ? spec.group : null;
      const names = isRecord(spec.names) ? spec.names : null;
      const plural = names && typeof names.plural === "string" ? names.plural : null;
      const kind = names && typeof names.kind === "string" ? names.kind : null;
      if (!group || !plural || !kind) continue;
      resources.push({ resource: `${plural}.${group}`, kind, group });
    }

    const found: CrInstance[] = [];
    for (const r of resources) {
      const list = await kubectl(["get", r.resource, "-n", ns, "-o", "json"]);
      if (!list.ok) continue;
      let obj: unknown;
      try {
        obj = JSON.parse(list.stdout);
      } catch {
        continue;
      }
      const its: unknown[] =
        isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];
      for (const it of its) {
        if (!isRecord(it)) continue;
        const md = isRecord(it.metadata) ? it.metadata : {};
        found.push({
          apiVersion: typeof it.apiVersion === "string" ? it.apiVersion : `${r.group}/unknown`,
          kind: typeof it.kind === "string" ? it.kind : r.kind,
          name: typeof md.name === "string" ? md.name : "unknown",
        });
      }
    }

    found.sort((a, b) =>
      (a.kind + "/" + a.name).localeCompare(b.kind + "/" + b.name),
    );

    return NextResponse.json({ namespace: ns, items: found });
  } catch (e: unknown) {
    return NextResponse.json(
      { error: errorMessage(e) },
      { status: 500 },
    );
  }
}
