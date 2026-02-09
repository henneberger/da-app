"use client";

import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { HighlightEditor } from "@/components/code-editor";
import { errorMessage } from "@/lib/errors";

type QueryItem = {
  name: string;
  typeName: string | null;
  fieldName: string | null;
  connectionRef: string | null;
  sql: string;
  params: unknown[];
};

type ParamSpec = { index: number; argName: string };

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function parseParams(params: unknown[]): ParamSpec[] {
  const out: ParamSpec[] = [];
  for (const p of params) {
    if (!isRecord(p)) continue;
    const idx = typeof p.index === "number" ? p.index : null;
    const src = isRecord(p.source) ? (p.source as Record<string, unknown>) : null;
    const argName =
      src && String(src.kind ?? "") === "ARG" ? String(src.name ?? "") : "";
    if (idx && argName) out.push({ index: idx, argName });
  }
  out.sort((a, b) => a.index - b.index);
  return out;
}

export function QueriesClientView({ namespace }: { namespace: string }) {
  const [items, setItems] = useState<QueryItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [active, setActive] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const [argValues, setArgValues] = useState<Record<string, string>>({});
  const [argTypes, setArgTypes] = useState<Record<string, string>>({});

  const [running, setRunning] = useState(false);
  const [explain, setExplain] = useState("");
  const [stdout, setStdout] = useState("");
  const [renderedSql, setRenderedSql] = useState("");
  const [analyze, setAnalyze] = useState(false);

  async function load() {
    setLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/queries`, { cache: "no-store" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "queries fetch failed");
      setItems(Array.isArray(json?.items) ? json.items : []);
      if (!active && Array.isArray(json?.items) && json.items.length) {
        setActive(String(json.items[0].name));
      }
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [namespace]);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    const base = items.slice();
    if (!q) return base;
    return base.filter((it) => {
      const key = `${it.typeName ?? ""}.${it.fieldName ?? ""} ${it.name}`.toLowerCase();
      return key.includes(q);
    });
  }, [items, filter]);

  const activeItem = useMemo(() => {
    return items.find((i) => i.name === active) ?? null;
  }, [items, active]);

  const params = useMemo(() => parseParams(activeItem?.params ?? []), [activeItem?.params]);

  useEffect(() => {
    // reset outputs when switching queries
    setExplain("");
    setStdout("");
    setRenderedSql("");
  }, [active]);

  async function run() {
    if (!activeItem) return;
    setRunning(true);
    setExplain("");
    setStdout("");
    setRenderedSql("");
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/queries/run`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          queryName: activeItem.name,
          args: argValues,
          argTypes,
          analyze,
        }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "run failed");
      setRenderedSql(String(json?.renderedSql ?? ""));
      setExplain(String(json?.explain ?? ""));
      setStdout(String(json?.stdout ?? ""));
      toast.success("Executed");
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
          <h1 className="text-2xl font-semibold tracking-tight">Queries</h1>
          <p className="text-sm text-muted-foreground">
            These are the SQL queries that back your GraphQL fields (kind=Query). Run them with parameters and see the plan.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={() => void load()} disabled={loading}>
            {loading ? "Loading..." : "Reload"}
          </Button>
          <Button onClick={() => void run()} disabled={running || !activeItem}>
            {running ? "Running..." : analyze ? "Run + Analyze" : "Run"}
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[360px_1fr]">
        <Card className="p-3">
          <div className="flex items-center justify-between">
            <div className="text-sm font-semibold tracking-tight">Catalog</div>
            <Badge variant="secondary">{filtered.length}</Badge>
          </div>
          <Separator className="my-3" />
          <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter (e.g. Query.recentPosts)" />
          <Separator className="my-3" />
          <ScrollArea className="h-[520px]">
            <div className="space-y-1 pr-2">
              {filtered.map((it) => {
                const label = `${it.typeName ?? "?"}.${it.fieldName ?? "?"}`;
                const selected = it.name === active;
                return (
                  <button
                    key={it.name}
                    className={[
                      "w-full rounded-md border px-2 py-2 text-left text-sm",
                      selected ? "bg-muted" : "hover:bg-muted/60",
                    ].join(" ")}
                    onClick={() => setActive(it.name)}
                  >
                    <div className="flex items-center justify-between">
                      <div className="font-medium">{label}</div>
                      <Badge variant="secondary" className="font-mono text-[10px]">
                        {it.name}
                      </Badge>
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      conn: <span className="font-mono">{it.connectionRef ?? "-"}</span>
                    </div>
                  </button>
                );
              })}
              {filtered.length === 0 ? (
                <div className="text-sm text-muted-foreground">No queries found.</div>
              ) : null}
            </div>
          </ScrollArea>
        </Card>

        <div className="space-y-4">
          <Card className="p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold tracking-tight">Selected</div>
              {activeItem ? (
                <Badge variant="secondary">
                  {(activeItem.typeName ?? "?") + "." + (activeItem.fieldName ?? "?")}
                </Badge>
              ) : null}
            </div>
            <Separator className="my-3" />
            {!activeItem ? (
              <div className="text-sm text-muted-foreground">Pick a query on the left.</div>
            ) : (
              <div className="grid gap-3">
                <div className="flex items-center gap-2">
                  <button
                    className={[
                      "rounded-md border px-2 py-1 text-xs",
                      analyze ? "bg-muted" : "hover:bg-muted/60",
                    ].join(" ")}
                    onClick={() => setAnalyze((v) => !v)}
                    type="button"
                  >
                    {analyze ? "Analyze: ON" : "Analyze: OFF"}
                  </button>
                  <div className="text-xs text-muted-foreground">
                    Explain is always run; analyze runs <code className="font-mono text-xs">EXPLAIN ANALYZE</code>.
                  </div>
                </div>

                {params.length ? (
                  <div className="grid gap-2">
                    <div className="text-xs font-medium text-muted-foreground">Parameters</div>
                    {params.map((p) => (
                      <div key={p.index} className="grid grid-cols-1 gap-2 lg:grid-cols-[220px_1fr]">
                        <div className="text-sm">
                          <span className="font-mono">{"$" + p.index}</span>{" "}
                          <span className="text-muted-foreground">{p.argName}</span>
                        </div>
                        <div className="flex gap-2">
                          <Input
                            value={argValues[p.argName] ?? ""}
                            onChange={(e) =>
                              setArgValues((prev) => ({ ...prev, [p.argName]: e.target.value }))
                            }
                            placeholder="value"
                          />
                          <Input
                            value={argTypes[p.argName] ?? "text"}
                            onChange={(e) =>
                              setArgTypes((prev) => ({ ...prev, [p.argName]: e.target.value }))
                            }
                            placeholder="type (text|int|bigint|bool|jsonb)"
                            className="max-w-[220px] font-mono"
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No parameters.</div>
                )}

                <div className="grid gap-2">
                  <div className="text-xs font-medium text-muted-foreground">SQL</div>
                  <HighlightEditor value={activeItem.sql} language="sql" readOnly minHeight={220} />
                </div>
              </div>
            )}
          </Card>

          {renderedSql ? (
            <Card className="p-4">
              <div className="text-sm font-semibold tracking-tight">Rendered SQL</div>
              <Separator className="my-3" />
              <HighlightEditor value={renderedSql} language="sql" readOnly minHeight={160} />
            </Card>
          ) : null}

          {explain ? (
            <Card className="p-4">
              <div className="text-sm font-semibold tracking-tight">Query plan</div>
              <Separator className="my-3" />
              <pre className="whitespace-pre-wrap rounded-md border bg-muted/30 p-3 text-xs leading-5">
                {explain}
              </pre>
            </Card>
          ) : null}

          {stdout ? (
            <Card className="p-4">
              <div className="text-sm font-semibold tracking-tight">Result</div>
              <Separator className="my-3" />
              <pre className="whitespace-pre-wrap rounded-md border bg-muted/30 p-3 text-xs leading-5">
                {stdout}
              </pre>
            </Card>
          ) : null}
        </div>
      </div>
    </div>
  );
}
