import { kubectl } from "@/lib/k8s";

export async function psql(
  ns: string,
  db: string,
  sql: string,
): Promise<{ ok: boolean; stdout: string; stderr: string; cmd: string }> {
  const exec = await kubectl([
    "exec",
    "-n",
    ns,
    "deploy/postgres",
    "--",
    "psql",
    "-U",
    "app",
    "-d",
    db,
    "-v",
    "ON_ERROR_STOP=1",
    "-P",
    "pager=off",
    // Unaligned, tuples-only, tab-separated
    "-A",
    "-t",
    "-F",
    "\t",
    "-c",
    sql,
  ]);
  return { ok: exec.ok, stdout: exec.stdout, stderr: exec.stderr, cmd: exec.cmd };
}

