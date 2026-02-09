import { NextResponse } from "next/server";
import path from "node:path";
import fs from "node:fs/promises";
import YAML from "yaml";
import { repoRoot } from "@/lib/repo";

export const runtime = "nodejs";

type SampleItem = {
  file: string;
  title: string;
  kindGuess: string;
  description: string;
  features: string[];
};

function titleFromFile(file: string): string {
  const base = file.replace(/\.(ya?ml)$/i, "");
  return base
    .split(/[-_]+/g)
    .filter(Boolean)
    .map((w) => w.slice(0, 1).toUpperCase() + w.slice(1))
    .join(" ");
}

function kindGuessFromFile(file: string): string {
  const base = file.toLowerCase();
  if (base.includes("forum")) return "web-forum";
  if (base.includes("chat")) return "chat";
  if (base.includes("shop") || base.includes("store")) return "store";
  return "data-product";
}

function descriptionFromFile(file: string, features: string[]): string {
  const base = file.toLowerCase();
  if (base.includes("forum")) {
    const parts: string[] = [];
    if (features.includes("GraphQLSchema")) parts.push("GraphQL");
    if (features.includes("Connection")) parts.push("Postgres");
    if (features.includes("McpServer")) parts.push("MCP");
    if (features.includes("FlinkCatalog")) parts.push("Flink");
    return parts.length
      ? `Web forum sample (${parts.join(" + ")}).`
      : "Web forum sample data product.";
  }
  return "Sample data product.";
}

async function featuresFromYaml(absPath: string): Promise<string[]> {
  try {
    const text = await fs.readFile(absPath, "utf-8");
    const docs = YAML.parseAllDocuments(text);
    const kinds = new Set<string>();
    for (const d of docs) {
      const js = d.toJS({}) as unknown;
      if (!js || typeof js !== "object") continue;
      const r = js as Record<string, unknown>;
      if (typeof r.kind === "string") kinds.add(r.kind);
    }
    return Array.from(kinds).sort();
  } catch {
    return [];
  }
}

export async function GET() {
  try {
    const samplesDir = path.join(repoRoot(), "samples");
    const names = await fs.readdir(samplesDir).catch(() => []);
    const files = names
      .filter((n) => /\.(ya?ml)$/i.test(n))
      .sort((a, b) => a.localeCompare(b));

    const items: SampleItem[] = [];
    for (const file of files) {
      const abs = path.join(samplesDir, file);
      const features = await featuresFromYaml(abs);
      items.push({
        file,
        title: titleFromFile(file),
        kindGuess: kindGuessFromFile(file),
        description: descriptionFromFile(file, features),
        features,
      });
    }

    return NextResponse.json({ items });
  } catch {
    return NextResponse.json({ items: [] }, { status: 200 });
  }
}
