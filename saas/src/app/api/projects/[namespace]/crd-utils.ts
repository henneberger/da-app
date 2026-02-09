import { kubectl } from "@/lib/k8s";

type CrdInfo = {
  kind: string;
  group: string;
  plural: string;
  scope: string;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

export async function findNamespacedResourceByKind(
  kind: string,
): Promise<{ resource: string; group: string; plural: string } | null> {
  const crdRes = await kubectl(["get", "crd", "-o", "json"]);
  if (!crdRes.ok) return null;
  const obj: unknown = JSON.parse(crdRes.stdout);
  const items: unknown[] =
    isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];

  const crds: CrdInfo[] = [];
  for (const it of items) {
    if (!isRecord(it) || !isRecord(it.spec)) continue;
    const spec = it.spec;
    const names = isRecord(spec.names) ? spec.names : null;
    const k = names && typeof names.kind === "string" ? names.kind : null;
    const plural = names && typeof names.plural === "string" ? names.plural : null;
    const group = typeof spec.group === "string" ? spec.group : null;
    const scope = typeof spec.scope === "string" ? spec.scope : null;
    if (!k || !plural || !group || !scope) continue;
    crds.push({ kind: k, plural, group, scope });
  }

  const match = crds.find((c) => c.kind === kind && c.scope === "Namespaced");
  if (!match) return null;
  return { resource: `${match.plural}.${match.group}`, group: match.group, plural: match.plural };
}

