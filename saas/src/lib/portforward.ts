import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import net from "node:net";

export type PortForwardTarget = {
  namespace: string;
  kind: "svc" | "pod";
  name: string;
  remotePort: number;
};

export type PortForwardHandle = {
  id: string;
  target: PortForwardTarget;
  localPort: number;
  startedAt: number;
};

type InternalHandle = PortForwardHandle & {
  proc: ChildProcessWithoutNullStreams;
};

function store(): Map<string, InternalHandle> {
  const g = globalThis as unknown as {
    __DA_APP_PORTFW__?: Map<string, InternalHandle>;
  };
  if (!g.__DA_APP_PORTFW__) g.__DA_APP_PORTFW__ = new Map();
  return g.__DA_APP_PORTFW__;
}

async function pickFreePort(): Promise<number> {
  return await new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (!addr || typeof addr === "string") {
        srv.close();
        reject(new Error("Failed to allocate port"));
        return;
      }
      const p = addr.port;
      srv.close(() => resolve(p));
    });
  });
}

export function listPortForwards(ns?: string): PortForwardHandle[] {
  const items = Array.from(store().values()).map((h) => ({
    id: h.id,
    target: h.target,
    localPort: h.localPort,
    startedAt: h.startedAt,
  }));
  return ns ? items.filter((i) => i.target.namespace === ns) : items;
}

export function getPortForward(id: string): PortForwardHandle | null {
  const h = store().get(id);
  if (!h) return null;
  return {
    id: h.id,
    target: h.target,
    localPort: h.localPort,
    startedAt: h.startedAt,
  };
}

export async function startPortForward(
  target: PortForwardTarget,
): Promise<PortForwardHandle & { url: string }> {
  // Reuse if already running for same target.
  for (const h of store().values()) {
    if (
      h.target.namespace === target.namespace &&
      h.target.kind === target.kind &&
      h.target.name === target.name &&
      h.target.remotePort === target.remotePort
    ) {
      return { ...h, url: `http://127.0.0.1:${h.localPort}/` };
    }
  }

  const localPort = await pickFreePort();
  const id = `${target.namespace}-${target.kind}-${target.name}-${target.remotePort}-${localPort}`;

  const args = [
    "port-forward",
    "-n",
    target.namespace,
    `${target.kind}/${target.name}`,
    `${localPort}:${target.remotePort}`,
  ];

  const proc = spawn("kubectl", args, { env: process.env });
  proc.on("exit", () => {
    store().delete(id);
  });

  // Wait until forwarding is established or the process errors.
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(
        new Error(
          `Timed out starting kubectl port-forward (${args.join(" ")}).`,
        ),
      );
    }, 10_000);

    let stderrBuf = "";
    let stdoutBuf = "";

    const onData = (buf: Buffer) => {
      const s = buf.toString("utf8");
      stdoutBuf += s;
      if (s.includes("Forwarding from")) {
        cleanup();
        resolve();
      }
    };

    const onErr = (buf: Buffer) => {
      const s = buf.toString("utf8");
      stderrBuf += s;
      // Don't treat normal port-forward chatter as fatal.
      const lower = s.toLowerCase();
      const fatal =
        lower.includes("error:") ||
        lower.includes("unable to") ||
        lower.includes("forbidden") ||
        lower.includes("denied") ||
        lower.includes("not found") ||
        lower.includes("no endpoints available") ||
        lower.includes("address already in use");
      if (fatal) {
        cleanup();
        reject(
          new Error(
            (s.trim() ||
              stderrBuf.trim() ||
              stdoutBuf.trim() ||
              "kubectl port-forward failed") as string,
          ),
        );
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      proc.stdout.off("data", onData);
      proc.stderr.off("data", onErr);
      proc.off("error", onProcError);
      proc.off("exit", onExit);
    };

    const onProcError = (e: Error) => {
      cleanup();
      reject(e);
    };

    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(
        new Error(
          `kubectl port-forward exited (code=${code}, signal=${signal}). ${stderrBuf.trim() || stdoutBuf.trim()}`,
        ),
      );
    };

    proc.stdout.on("data", onData);
    proc.stderr.on("data", onErr);
    proc.on("error", onProcError);
    proc.on("exit", onExit);
  });

  const handle: InternalHandle = {
    id,
    target,
    localPort,
    startedAt: Date.now(),
    proc,
  };

  store().set(id, handle);
  return { ...handle, url: `http://127.0.0.1:${localPort}/` };
}

export function stopPortForward(id: string): boolean {
  const h = store().get(id);
  if (!h) return false;
  try {
    h.proc.kill();
  } catch {
    // ignore
  }
  store().delete(id);
  return true;
}
