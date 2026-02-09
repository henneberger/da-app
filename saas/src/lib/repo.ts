import path from "node:path";
import fs from "node:fs/promises";

export function repoRoot(): string {
  // Allow override for container deployments.
  if (process.env.DA_APP_REPO_ROOT) return process.env.DA_APP_REPO_ROOT;
  // Next.js server runs from /.../da-app/saas in this repo.
  return path.resolve(process.cwd(), "..");
}

export function samplePath(sampleFile: string): string {
  return path.join(repoRoot(), "samples", sampleFile);
}

export async function readSample(sampleFile: string): Promise<string> {
  const p = samplePath(sampleFile);
  return await fs.readFile(p, "utf-8");
}

