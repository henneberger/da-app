import { NextResponse } from "next/server";
import { assertNamespace, kubectl } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { findNamespacedResourceByKind } from "../crd-utils";

export const runtime = "nodejs";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();
  try {
    assertNamespace(ns);
    const r = await findNamespacedResourceByKind("GraphQLOperation");
    if (!r) {
      return NextResponse.json(
        { error: "CRD kind=GraphQLOperation not found in cluster" },
        { status: 404 },
      );
    }

    const res = await kubectl(["get", r.resource, "-n", ns, "-o", "json"]);
    if (!res.ok) {
      return NextResponse.json(
        { error: res.stderr || res.stdout, cmd: res.cmd },
        { status: 500 },
      );
    }

    const obj: unknown = JSON.parse(res.stdout);
    const items: unknown[] =
      isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];

    const out = items
      .map((it) => {
        if (!isRecord(it)) return null;
        const md = isRecord(it.metadata) ? it.metadata : {};
        const spec = isRecord(it.spec) ? it.spec : {};
        return {
          name: typeof md.name === "string" ? md.name : "unknown",
          operationName:
            typeof spec.operationName === "string" ? spec.operationName : null,
          description:
            typeof spec.description === "string" ? spec.description : null,
          document: typeof spec.document === "string" ? spec.document : "",
        };
      })
      .filter((x): x is NonNullable<typeof x> => Boolean(x));

    out.sort((a, b) => (a.operationName ?? "").localeCompare(b.operationName ?? "") || a.name.localeCompare(b.name));
    return NextResponse.json({ items: out });
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 500 });
  }
}

