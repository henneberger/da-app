"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import { Loader2Icon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { HighlightEditor } from "@/components/code-editor";
import { errorMessage } from "@/lib/errors";
import { LoadingInline } from "@/components/loading-inline";

type McpServer = {
  name: string;
  path: string;
  schemaRefs: unknown[];
  operationRefs: unknown[];
  toolNameStrategy: string | null;
};

type Auth =
  | { type: "none" }
  | { type: "bearer"; token: string }
  | { type: "basic"; username: string; password: string };

type LogItem =
  | { type: "info"; text: string }
  | { type: "tool"; name: string; status: "calling" | "result"; payload: unknown }
  | { type: "assistant"; text: string };

export function McpClientView({ namespace }: { namespace: string }) {
  const [servers, setServers] = useState<McpServer[]>([]);
  const [loading, setLoading] = useState(false);

  const [selectedServer, setSelectedServer] = useState<string | null>(null);
  const server = useMemo(
    () => servers.find((s) => s.name === selectedServer) ?? servers[0] ?? null,
    [servers, selectedServer],
  );

  const [auth, setAuth] = useState<Auth>({ type: "none" });
  const [endpointUrl, setEndpointUrl] = useState<string | null>(null);
  const [authRequired, setAuthRequired] = useState(false);
  const [toolsCount, setToolsCount] = useState<number | null>(null);

  const [message, setMessage] = useState(
    'Create 3 posts and then query them',
  );
  const [running, setRunning] = useState(false);
  const [log, setLog] = useState<LogItem[]>([]);
  const [statusText, setStatusText] = useState<string>("");

  const endRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    endRef.current?.scrollIntoView({ block: "end" });
  }, [log, running]);

  async function loadServers() {
    setLoading(true);
    try {
      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/mcp/servers`,
        { cache: "no-store" },
      );
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "failed to load servers");
      const items: McpServer[] = Array.isArray(json?.items) ? json.items : [];
      setServers(items);
      if (!selectedServer && items.length) setSelectedServer(items[0].name);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadServers();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [namespace]);

  async function connect() {
    if (!server) {
      toast.error("No McpServer resources found in this namespace");
      return;
    }
    setRunning(true);
    try {
      const body: Record<string, unknown> = { path: server.path };
      if (auth.type === "bearer") body.auth = { type: "bearer", token: auth.token };
      if (auth.type === "basic") body.auth = { type: "basic", username: auth.username, password: auth.password };

      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/mcp/connect`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "connect failed");
      setEndpointUrl(String(json?.endpointUrl ?? ""));
      setAuthRequired(Boolean(json?.authRequired));
      setToolsCount(typeof json?.toolsCount === "number" ? json.toolsCount : null);
      setLog((prev) => [
        ...prev,
        { type: "info", text: `Connected: ${String(json?.endpointUrl ?? "")}` },
      ]);
      if (json?.authRequired) {
        setLog((prev) => [
          ...prev,
          { type: "info", text: "Auth required. This endpoint likely redirects to login or returns 401/403. Enter credentials and connect again." },
        ]);
      } else {
        toast.success("MCP connected");
      }
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setRunning(false);
    }
  }

  async function runAgent() {
    if (!endpointUrl) {
      toast.error("Connect first");
      return;
    }
    const msg = message.trim();
    if (!msg) return;

    setRunning(true);
    setStatusText("Starting...");
    setLog((prev) => [...prev, { type: "info", text: `You: ${msg}` }]);

    try {
      const body: Record<string, unknown> = { endpointUrl, message: msg };
      if (auth.type === "bearer") body.auth = { type: "bearer", token: auth.token };
      if (auth.type === "basic") body.auth = { type: "basic", username: auth.username, password: auth.password };

      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/mcp/agent`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      if (!res.ok || !res.body) {
        const j = await res.json().catch(() => ({}));
        throw new Error(j?.error ?? "agent failed");
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        while (true) {
          const idx = buf.indexOf("\n\n");
          if (idx === -1) break;
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);

          const lines = raw.split("\n");
          let event = "message";
          let data = "";
          for (const line of lines) {
            if (line.startsWith("event:")) event = line.slice(6).trim();
            if (line.startsWith("data:")) data += line.slice(5).trim();
          }
          if (!data) continue;
          let obj: unknown = null;
          try {
            obj = JSON.parse(data);
          } catch {
            obj = { text: data };
          }

          if (event === "tool") {
            const o = obj as Record<string, unknown>;
            setLog((prev) => [
              ...prev,
              {
                type: "tool",
                name: String(o?.name ?? "tool"),
                status: (String(o?.status ?? "calling") as "calling" | "result"),
                payload: (o?.result ?? o?.args ?? o),
              },
            ]);
          }
          if (event === "assistant") {
            const o = obj as Record<string, unknown>;
            setLog((prev) => [
              ...prev,
              { type: "assistant", text: String(o?.text ?? "") },
            ]);
          }
          if (event === "status") {
            const o = obj as Record<string, unknown>;
            setStatusText(String(o?.text ?? ""));
          }
          if (event === "error") {
            const o = obj as Record<string, unknown>;
            throw new Error(String(o?.message ?? "unknown error"));
          }
        }
      }
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setRunning(false);
      setStatusText("");
    }
  }

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">MCP</h1>
        <p className="text-sm text-muted-foreground">
          MCP (Model Context Protocol) exposes your GraphQL operations as tool calls over HTTP.
          In this project, the MCP server is deployed alongside your GraphQL server and typically
          lives at <code className="font-mono text-xs">/mcp</code>. Once connected, the agent can
          call MCP tools to create data and query it (for example: create posts, then list them).
        </p>
      </div>

      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold tracking-tight">Server</div>
          <div className="flex items-center gap-2">
            <LoadingInline show={loading} text="Loading..." className="mr-2" />
            <Button variant="outline" onClick={() => void loadServers()} disabled={loading}>
              {loading ? "Loading..." : "Reload"}
            </Button>
            <Button onClick={() => void connect()} disabled={running || !server}>
              {running ? "Working..." : endpointUrl ? "Reconnect" : "Connect"}
            </Button>
          </div>
        </div>
        <Separator className="my-3" />
        <LoadingInline
          show={loading && servers.length === 0}
          text="Detecting MCP servers..."
          className="mb-3"
        />

        {servers.length === 0 ? (
          <div className="text-sm text-muted-foreground">
            No <code className="font-mono text-xs">McpServer</code> resources detected in this namespace.
          </div>
        ) : (
          <div className="space-y-2">
            <div className="flex flex-wrap gap-2">
              {servers.map((s) => (
                <Button
                  key={s.name}
                  size="sm"
                  variant={s.name === (server?.name ?? "") ? "default" : "outline"}
                  onClick={() => setSelectedServer(s.name)}
                >
                  {s.name}
                </Button>
              ))}
            </div>
            {server ? (
              <div className="text-sm text-muted-foreground">
                path: <span className="font-mono text-xs">{server.path}</span>, tools:{" "}
                <span className="font-mono text-xs">{toolsCount ?? "-"}</span>{" "}
                {authRequired ? <Badge variant="destructive">auth required</Badge> : null}
              </div>
            ) : null}
          </div>
        )}

        <Separator className="my-3" />
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-[260px_1fr]">
          <div className="text-sm font-medium">Auth</div>
          <div className="space-y-2">
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                variant={auth.type === "none" ? "default" : "outline"}
                onClick={() => setAuth({ type: "none" })}
              >
                None
              </Button>
              <Button
                size="sm"
                variant={auth.type === "basic" ? "default" : "outline"}
                onClick={() => setAuth({ type: "basic", username: "", password: "" })}
              >
                Basic
              </Button>
              <Button
                size="sm"
                variant={auth.type === "bearer" ? "default" : "outline"}
                onClick={() => setAuth({ type: "bearer", token: "" })}
              >
                Bearer
              </Button>
            </div>
            {auth.type === "basic" ? (
              <div className="grid grid-cols-1 gap-2 lg:grid-cols-2">
                <Input
                  value={auth.username}
                  onChange={(e) => setAuth({ ...auth, username: e.target.value })}
                  placeholder="username"
                />
                <Input
                  value={auth.password}
                  onChange={(e) => setAuth({ ...auth, password: e.target.value })}
                  placeholder="password"
                  type="password"
                />
              </div>
            ) : null}
            {auth.type === "bearer" ? (
              <Input
                value={auth.token}
                onChange={(e) => setAuth({ ...auth, token: e.target.value })}
                placeholder="token"
                type="password"
              />
            ) : null}
          </div>
        </div>
      </Card>

      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold tracking-tight">Agent</div>
          <Button onClick={() => void runAgent()} disabled={running || !endpointUrl}>
            {running ? "Running..." : "Run"}
          </Button>
        </div>
        <Separator className="my-3" />
        <div className="mb-3 flex items-center justify-between rounded-md border bg-muted/30 px-3 py-2">
          <div className="flex items-center gap-2 text-xs text-muted-foreground" aria-live="polite">
            {running ? <Loader2Icon className="size-3 animate-spin" /> : null}
            <span>{running ? (statusText || "Working...") : "Idle"}</span>
          </div>
          <Badge variant={running ? "secondary" : "outline"}>
            {running ? "working" : "ready"}
          </Badge>
        </div>
        <HighlightEditor
          value={message}
          onChange={setMessage}
          language="text"
          minHeight={90}
        />
        <Separator className="my-3" />
        <ScrollArea className="h-[420px]">
          <div className="space-y-3 pr-2">
            {log.map((it, idx) => {
              if (it.type === "info") {
                return (
                  <div key={idx} className="text-sm text-muted-foreground">
                    {it.text}
                  </div>
                );
              }
              if (it.type === "assistant") {
                return (
                  <div key={idx} className="text-sm">
                    <div className="font-semibold">Assistant</div>
                    <div className="whitespace-pre-wrap text-muted-foreground">
                      {it.text}
                    </div>
                  </div>
                );
              }
              return (
                <div key={idx} className="text-sm">
                  <div className="flex items-center justify-between">
                    <div className="font-semibold">
                      Tool: <span className="font-mono text-xs">{it.name}</span>
                    </div>
                    <Badge variant={it.status === "result" ? "secondary" : "outline"}>
                      {it.status}
                    </Badge>
                  </div>
                  <pre className="mt-2 whitespace-pre-wrap rounded-md border bg-muted/30 p-3 text-xs leading-5">
                    {JSON.stringify(it.payload, null, 2)}
                  </pre>
                </div>
              );
            })}
            <div ref={endRef} />
          </div>
        </ScrollArea>
      </Card>
    </div>
  );
}
