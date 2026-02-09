import { execFile, type ExecFileException } from "node:child_process";

export type KubectlResult = {
  ok: boolean;
  stdout: string;
  stderr: string;
  exitCode: number;
  cmd: string;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function isValidK8sNamespaceName(ns: string): boolean {
  // DNS label: lowercase alphanumerics or '-', start/end alphanumeric, max 63
  return /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(ns) && ns.length <= 63;
}

export function assertNamespace(ns: string) {
  if (!isValidK8sNamespaceName(ns)) {
    throw new Error(
      `Invalid namespace "${ns}". Must match DNS label (lowercase, digits, '-') and be <= 63 chars.`,
    );
  }
}

export async function kubectl(
  args: string[],
  opts?: { input?: string; timeoutMs?: number },
): Promise<KubectlResult> {
  const cmd = ["kubectl", ...args].join(" ");
  const timeoutMs = opts?.timeoutMs ?? 120_000;

  return await new Promise((resolve) => {
    const child = execFile(
      "kubectl",
      args,
      {
        env: process.env,
        encoding: "utf8",
        timeout: timeoutMs,
        maxBuffer: 20 * 1024 * 1024,
      },
      (error: ExecFileException | null, stdout, stderr) => {
        const exitCode =
          typeof error?.code === "number"
            ? error.code
            : error
              ? 1
              : 0;
        resolve({
          ok: !error,
          stdout: String(stdout ?? ""),
          stderr: String(stderr ?? ""),
          exitCode,
          cmd,
        });
      },
    );

    if (opts?.input != null) {
      child.stdin?.write(opts.input);
      child.stdin?.end();
    }
  });
}

export async function ensureNamespace(ns: string) {
  assertNamespace(ns);
  const get = await kubectl(["get", "namespace", ns, "-o", "name"]);
  if (!get.ok) {
    const create = await kubectl(["create", "namespace", ns]);
    if (!create.ok) {
      throw new Error(
        `Failed to create namespace ${ns}: ${create.stderr || create.stdout}`,
      );
    }
  }
  const label = await kubectl([
    "label",
    "namespace",
    ns,
    "da-app-project=true",
    "--overwrite",
  ]);
  if (!label.ok) {
    throw new Error(`Failed to label namespace ${ns}: ${label.stderr}`);
  }
}

export async function getNamespacesLabeledAsProjects(): Promise<
  { name: string; status?: string; labels?: Record<string, string> }[]
> {
  const res = await kubectl(["get", "ns", "-l", "da-app-project=true", "-o", "json"]);
  if (!res.ok) throw new Error(res.stderr || res.stdout);
  const obj: unknown = JSON.parse(res.stdout);
  const items: unknown[] =
    isRecord(obj) && Array.isArray(obj.items) ? (obj.items as unknown[]) : [];

  return items.map((n: unknown) => {
    if (!isRecord(n)) return { name: "unknown" };
    const md = isRecord(n.metadata) ? n.metadata : {};
    const st = isRecord(n.status) ? n.status : {};
    const labels =
      isRecord(md.labels) && !Array.isArray(md.labels)
        ? (md.labels as Record<string, string>)
        : {};
    return {
      name: typeof md.name === "string" ? md.name : "unknown",
      status: typeof st.phase === "string" ? st.phase : undefined,
      labels,
    };
  });
}
