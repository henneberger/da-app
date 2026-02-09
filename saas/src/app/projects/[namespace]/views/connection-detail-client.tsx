"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Input } from "@/components/ui/input";
import { HighlightEditor } from "@/components/code-editor";
import { errorMessage } from "@/lib/errors";
import { LoadingInline } from "@/components/loading-inline";

type Conn = {
  name: string;
  host: string | null;
  port: number | null;
  database: string | null;
  user: string | null;
  ssl: boolean | null;
};

type DbMeta = {
  databases: string[];
};

type TableMeta = {
  tables: { schema: string; name: string; type: string }[];
};

type Sample = {
  columns: string[];
  rows: string[][];
};

function asErrorMessage(json: unknown): string | null {
  if (!json || typeof json !== "object") return null;
  const r = json as Record<string, unknown>;
  return typeof r.error === "string" ? r.error : null;
}

function isPseudoFlink(name: string): boolean {
  return name === "_flink";
}

export function ConnectionDetailClient({
  namespace,
  name,
}: {
  namespace: string;
  name: string;
}) {
  const [connections, setConnections] = useState<Conn[]>([]);
  const [loading, setLoading] = useState(false);

  // Flink
  const [dashUrl, setDashUrl] = useState<string | null>(null);
  const [dashId, setDashId] = useState<string | null>(null);
  const [dashLoading, setDashLoading] = useState(false);

  // Postgres explorer
  const [dbs, setDbs] = useState<string[]>([]);
  const [db, setDb] = useState<string>("");
  const [tables, setTables] = useState<TableMeta["tables"]>([]);
  const [tableFilter, setTableFilter] = useState("");
  const [schema, setSchema] = useState("public");
  const [selectedTable, setSelectedTable] = useState<{ schema: string; name: string } | null>(null);
  const [sample, setSample] = useState<Sample | null>(null);
  const [sql, setSql] = useState("select now();");
  const [running, setRunning] = useState(false);
  const [stdout, setStdout] = useState<string>("");
  const [stderr, setStderr] = useState<string>("");

  const conn = useMemo(() => connections.find((c) => c.name === name) ?? null, [connections, name]);

  async function loadConnections() {
    setLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/connections`, { cache: "no-store" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "connections fetch failed");
      setConnections(Array.isArray(json?.items) ? json.items : []);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadConnections();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [namespace]);

  async function connectFlink() {
    setDashLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/flink/dashboard`, { method: "POST" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "dashboard connect failed");
      setDashUrl(String(json?.url ?? ""));
      setDashId(String(json?.handle?.id ?? ""));
      toast.success("Flink dashboard connected");
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setDashLoading(false);
    }
  }

  async function disconnectFlink() {
    if (!dashId) return;
    setDashLoading(true);
    try {
      await fetch(`/api/projects/${encodeURIComponent(namespace)}/flink/dashboard`, {
        method: "DELETE",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: dashId }),
      });
      setDashUrl(null);
      setDashId(null);
      toast.success("Flink dashboard disconnected");
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setDashLoading(false);
    }
  }

  async function loadDatabases() {
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/postgres/databases`, { cache: "no-store" });
      const json: DbMeta = await res.json();
      if (!res.ok) throw new Error(asErrorMessage(json) ?? "databases failed");
      setDbs(Array.isArray(json.databases) ? json.databases : []);
      const initial = (conn?.database ?? json.databases?.[0] ?? "").toString();
      setDb((prev) => prev || initial);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    }
  }

  async function loadTables(nextDb: string, nextSchema: string) {
    if (!nextDb) return;
    try {
      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/postgres/tables?db=${encodeURIComponent(nextDb)}&schema=${encodeURIComponent(nextSchema)}`,
        { cache: "no-store" },
      );
      const json: TableMeta = await res.json();
      if (!res.ok) throw new Error(asErrorMessage(json) ?? "tables failed");
      setTables(Array.isArray(json.tables) ? json.tables : []);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    }
  }

  async function loadSample(nextDb: string, nextSchema: string, nextTable: string) {
    try {
      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/postgres/sample?db=${encodeURIComponent(nextDb)}&schema=${encodeURIComponent(nextSchema)}&table=${encodeURIComponent(nextTable)}&limit=50`,
        { cache: "no-store" },
      );
      const json: Sample = await res.json();
      if (!res.ok) throw new Error(asErrorMessage(json) ?? "sample failed");
      setSample(json);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    }
  }

  useEffect(() => {
    if (!conn) return;
    void loadDatabases();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conn?.name]);

  useEffect(() => {
    if (!conn) return;
    if (!db) return;
    void loadTables(db, schema);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [db, schema, conn?.name]);

  const filteredTables = useMemo(() => {
    const q = tableFilter.trim().toLowerCase();
    if (!q) return tables;
    return tables.filter((t) => `${t.schema}.${t.name}`.toLowerCase().includes(q));
  }, [tables, tableFilter]);

  async function runSql() {
    setRunning(true);
    setStdout("");
    setStderr("");
    try {
      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/postgres/query?db=${encodeURIComponent(db || conn?.database || "forum")}`,
        {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql }),
        },
      );
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "query failed");
      setStdout(String(json?.stdout ?? ""));
      setStderr(String(json?.stderr ?? ""));
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">
            Connection: <span className="font-mono text-xl">{name}</span>
          </h1>
          <p className="text-sm text-muted-foreground">
            {isPseudoFlink(name)
              ? "Flink dashboard access (port-forward + iframe)."
              : "Connection details and interactive tooling."}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <LoadingInline show={loading} text="Loading..." className="mr-2" />
          <LoadingInline
            show={dashLoading}
            text={dashUrl ? "Disconnecting..." : "Connecting..."}
            className="mr-2"
          />
          <Link
            href={`/projects/${encodeURIComponent(namespace)}/connections`}
            className="rounded-md border px-3 py-2 text-sm hover:bg-muted/60"
          >
            Back
          </Link>
          <Button variant="outline" onClick={() => void loadConnections()} disabled={loading}>
            {loading ? "Loading..." : "Reload"}
          </Button>
        </div>
      </div>

      {isPseudoFlink(name) ? (
        <Card className="p-4">
          <div className="flex items-center justify-between">
            <div className="space-y-1">
              <div className="text-sm font-semibold tracking-tight">Flink dashboard</div>
              <div className="text-sm text-muted-foreground">
                Starts a <code className="font-mono text-xs">kubectl port-forward</code> and embeds the UI.
              </div>
            </div>
            <div className="flex items-center gap-2">
              {dashUrl ? (
                <Button variant="outline" onClick={() => void disconnectFlink()} disabled={dashLoading}>
                  {dashLoading ? "Disconnecting..." : "Disconnect"}
                </Button>
              ) : (
                <Button onClick={() => void connectFlink()} disabled={dashLoading}>
                  {dashLoading ? "Connecting..." : "Connect"}
                </Button>
              )}
            </div>
          </div>
          {dashUrl ? (
            <>
              <Separator className="my-3" />
            <div className="text-xs text-muted-foreground">
              Connected via server proxy.
            </div>
            <div className="mt-3 overflow-hidden rounded-md border bg-background">
              <iframe
                src={`/api/projects/${encodeURIComponent(namespace)}/portforward/${encodeURIComponent(dashId ?? "")}/`}
                className="h-[680px] w-full"
                title="Flink Dashboard"
              />
            </div>
          </>
        ) : null}
      </Card>
      ) : !conn ? (
        <Card className="p-4">
          <div className="text-sm text-muted-foreground">
            Connection <span className="font-mono">{name}</span> not found in this namespace.
          </div>
        </Card>
      ) : (
        <div className="space-y-4">
          <Card className="p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold tracking-tight">Postgres</div>
              <Badge variant="secondary">
                {conn.host ?? "?"}:{conn.port ?? "?"}/{conn.database ?? "?"}
              </Badge>
            </div>
            <Separator className="my-3" />
            <div className="grid grid-cols-1 gap-4 lg:grid-cols-[320px_1fr]">
              <Card className="p-3">
                <div className="text-sm font-semibold tracking-tight">Explorer</div>
                <Separator className="my-3" />
                <div className="grid gap-2">
                  <div className="text-xs font-medium text-muted-foreground">Database</div>
                  <div className="flex flex-wrap gap-2">
                    {dbs.length ? (
                      dbs.map((d) => (
                        <button
                          key={d}
                          className={[
                            "rounded-md border px-2 py-1 text-xs font-mono",
                            d === db ? "bg-muted" : "hover:bg-muted/60",
                          ].join(" ")}
                          onClick={() => setDb(d)}
                        >
                          {d}
                        </button>
                      ))
                    ) : (
                      <div className="text-sm text-muted-foreground">-</div>
                    )}
                  </div>

                  <div className="text-xs font-medium text-muted-foreground">Schema</div>
                  <Input value={schema} onChange={(e) => setSchema(e.target.value)} className="font-mono text-xs" />

                  <div className="text-xs font-medium text-muted-foreground">Tables</div>
                  <Input value={tableFilter} onChange={(e) => setTableFilter(e.target.value)} placeholder="Filter tables" />
                  <ScrollArea className="h-[420px]">
                    <div className="space-y-1 pr-2">
                      {filteredTables.map((t) => {
                        const selected = selectedTable?.schema === t.schema && selectedTable?.name === t.name;
                        return (
                          <button
                            key={`${t.schema}.${t.name}`}
                            className={[
                              "w-full rounded-md border px-2 py-2 text-left text-sm",
                              selected ? "bg-muted" : "hover:bg-muted/60",
                            ].join(" ")}
                            onClick={() => {
                              setSelectedTable({ schema: t.schema, name: t.name });
                              setSample(null);
                              if (db) void loadSample(db, t.schema, t.name);
                            }}
                          >
                            <div className="font-mono text-xs">{t.schema}.{t.name}</div>
                            <div className="text-[11px] text-muted-foreground">{t.type}</div>
                          </button>
                        );
                      })}
                      {filteredTables.length === 0 ? (
                        <div className="text-sm text-muted-foreground">No tables found.</div>
                      ) : null}
                    </div>
                  </ScrollArea>
                </div>
              </Card>

              <div className="space-y-4">
                <Card className="p-4">
                  <div className="text-sm font-semibold tracking-tight">Table preview</div>
                  <Separator className="my-3" />
                  {!selectedTable ? (
                    <div className="text-sm text-muted-foreground">Pick a table.</div>
                  ) : !sample ? (
                    <div className="text-sm text-muted-foreground">Loading sample...</div>
                  ) : (
                    <div className="rounded-md border bg-muted/30">
                      <ScrollArea className="h-[260px] p-3">
                        <pre className="text-xs leading-5 whitespace-pre">
                          {[
                            sample.columns.join("\t"),
                            ...sample.rows.map((r) => r.join("\t")),
                          ].join("\n")}
                        </pre>
                      </ScrollArea>
                    </div>
                  )}
                </Card>

                <Card className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold tracking-tight">SQL</div>
                    <Button onClick={() => void runSql()} disabled={running}>
                      {running ? "Running..." : "Run"}
                    </Button>
                  </div>
                  <Separator className="my-3" />
                  <HighlightEditor value={sql} onChange={setSql} language="sql" minHeight={220} />
                </Card>

                {(stdout || stderr) ? (
                  <Card className="p-4">
                    <div className="text-sm font-semibold tracking-tight">Result</div>
                    <Separator className="my-3" />
                    <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                      <div>
                        <div className="text-xs font-medium text-muted-foreground">stdout</div>
                        <div className="mt-2 rounded-md border bg-muted/40">
                          <ScrollArea className="h-[240px] p-3">
                            <pre className="text-xs leading-5 whitespace-pre-wrap">{stdout || "-"}</pre>
                          </ScrollArea>
                        </div>
                      </div>
                      <div>
                        <div className="text-xs font-medium text-muted-foreground">stderr</div>
                        <div className="mt-2 rounded-md border bg-muted/40">
                          <ScrollArea className="h-[240px] p-3">
                            <pre className="text-xs leading-5 whitespace-pre-wrap">{stderr || "-"}</pre>
                          </ScrollArea>
                        </div>
                      </div>
                    </div>
                  </Card>
                ) : null}
              </div>
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}
