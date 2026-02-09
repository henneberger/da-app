"use client";

import YAML from "yaml";

export type K8sDoc = {
  doc: YAML.Document.Parsed;
  apiVersion?: string;
  kind?: string;
  name?: string;
};

function asRecord(v: unknown): Record<string, unknown> | null {
  return typeof v === "object" && v !== null ? (v as Record<string, unknown>) : null;
}

export function parseManifest(manifest: string): K8sDoc[] {
  const docs = YAML.parseAllDocuments(manifest, { prettyErrors: true });
  return docs.map((doc) => {
    const js = doc.toJS({}) as unknown;
    const r = asRecord(js);
    const md = r && asRecord(r.metadata);
    return {
      doc,
      apiVersion: r && typeof r.apiVersion === "string" ? r.apiVersion : undefined,
      kind: r && typeof r.kind === "string" ? r.kind : undefined,
      name: md && typeof md.name === "string" ? md.name : undefined,
    };
  });
}

export function serializeManifest(docs: K8sDoc[]): string {
  return docs
    .map((d) => d.doc.toString().trimEnd())
    .filter((s) => s.trim().length > 0)
    .join("\n---\n");
}

