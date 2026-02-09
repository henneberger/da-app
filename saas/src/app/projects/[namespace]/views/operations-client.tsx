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
import { LoadingInline } from "@/components/loading-inline";

type Op = {
  name: string;
  operationName: string | null;
  description: string | null;
  document: string;
};

export function OperationsClientView({ namespace }: { namespace: string }) {
  const [items, setItems] = useState<Op[]>([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState("");
  const [active, setActive] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/operations`, { cache: "no-store" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "operations fetch failed");
      const its = Array.isArray(json?.items) ? (json.items as Op[]) : [];
      setItems(its);
      if (!active && its.length) setActive(its[0].name);
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
    if (!q) return items;
    return items.filter((it) => {
      const key = `${it.operationName ?? ""} ${it.name} ${it.description ?? ""}`.toLowerCase();
      return key.includes(q);
    });
  }, [items, filter]);

  const activeItem = useMemo(() => items.find((i) => i.name === active) ?? null, [items, active]);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">Operations</h1>
          <p className="text-sm text-muted-foreground">
            Persisted GraphQL payloads (kind=GraphQLOperation). These map cleanly into MCP tools (see the MCP section) and are meant to be called by agents.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <LoadingInline show={loading} text="Loading..." className="mr-2" />
          <Button variant="outline" onClick={() => void load()} disabled={loading}>
            {loading ? "Loading..." : "Reload"}
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
          <LoadingInline
            show={loading && items.length === 0}
            text="Fetching operations..."
            className="mb-3"
          />
          <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter (e.g. latest_posts)" />
          <Separator className="my-3" />
          <ScrollArea className="h-[560px]">
            <div className="space-y-1 pr-2">
              {filtered.map((it) => {
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
                      <div className="font-medium">{it.operationName ?? it.name}</div>
                      <Badge variant="secondary" className="font-mono text-[10px]">
                        {it.name}
                      </Badge>
                    </div>
                    {it.description ? (
                      <div className="mt-1 text-xs text-muted-foreground">{it.description}</div>
                    ) : null}
                  </button>
                );
              })}
              {filtered.length === 0 ? (
                <div className="text-sm text-muted-foreground">No operations found.</div>
              ) : null}
            </div>
          </ScrollArea>
        </Card>

        <div className="space-y-4">
          <Card className="p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold tracking-tight">Document</div>
              <Badge variant="secondary">read-only</Badge>
            </div>
            <Separator className="my-3" />
            {activeItem ? (
              <HighlightEditor value={activeItem.document} language="graphql" readOnly minHeight={320} />
            ) : (
              <div className="text-sm text-muted-foreground">Pick an operation.</div>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}
