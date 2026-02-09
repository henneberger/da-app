"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
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

export function ConnectionsClientView({ namespace }: { namespace: string }) {
  const [items, setItems] = useState<Conn[]>([]);
  const [loading, setLoading] = useState(false);

  async function load() {
    setLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/connections`, { cache: "no-store" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "connections fetch failed");
      setItems(Array.isArray(json?.items) ? json.items : []);
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

  const postgresConnections = useMemo(() => {
    // Heuristic: if it has host/port/database, treat as postgres-like.
    return items.filter((c) => !!c.host && !!c.port && !!c.database);
  }, [items]);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">Connections</h1>
          <p className="text-sm text-muted-foreground">
            Click a connection to open its tooling (Postgres explorer or Flink dashboard).
          </p>
        </div>
        <div className="flex items-center gap-2">
          <LoadingInline show={loading} text="Loading..." className="mr-2" />
          <Button variant="outline" onClick={() => void load()} disabled={loading}>
            {loading ? "Loading..." : "Reload"}
          </Button>
        </div>
      </div>

      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold tracking-tight">Flink</div>
          <Badge variant="secondary">dashboard</Badge>
        </div>
        <Separator className="my-3" />
        <LoadingInline
          show={loading && items.length === 0}
          text="Fetching connections..."
          className="mb-3"
        />
        <Link
          href={`/projects/${encodeURIComponent(namespace)}/connections/_flink`}
          className="inline-flex rounded-md border px-3 py-2 text-sm hover:bg-muted/60"
        >
          Open Flink dashboard
        </Link>
      </Card>

      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold tracking-tight">Configured</div>
          <Badge variant="secondary">{items.length}</Badge>
        </div>
        <Separator className="my-3" />
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Host</TableHead>
              <TableHead>Database</TableHead>
              <TableHead>User</TableHead>
              <TableHead>SSL</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-sm text-muted-foreground">
                  No Connection resources detected.
                </TableCell>
              </TableRow>
            ) : (
              items.map((c) => (
                <TableRow key={c.name}>
                  <TableCell className="font-medium">
                    <Link
                      href={`/projects/${encodeURIComponent(namespace)}/connections/${encodeURIComponent(c.name)}`}
                      className="underline underline-offset-4 hover:text-foreground"
                    >
                      {c.name}
                    </Link>
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {c.host ? `${c.host}${c.port ? ":" + c.port : ""}` : "-"}
                  </TableCell>
                  <TableCell className="font-mono text-xs">{c.database ?? "-"}</TableCell>
                  <TableCell className="font-mono text-xs">{c.user ?? "-"}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{c.ssl === null ? "-" : c.ssl ? "true" : "false"}</Badge>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>

      {postgresConnections.length ? (
        <Card className="p-4">
          <div className="text-sm font-semibold tracking-tight">Postgres tooling</div>
          <p className="mt-1 text-sm text-muted-foreground">
            For now, query execution uses <code className="font-mono text-xs">kubectl exec</code> into{" "}
            <code className="font-mono text-xs">deploy/postgres</code> in this namespace.
          </p>
          <Separator className="my-3" />
          <div className="flex flex-wrap gap-2">
            <Link
              href={`/projects/${encodeURIComponent(namespace)}/graphql`}
              className="rounded-md border px-3 py-2 text-sm hover:bg-muted/60"
            >
              Open GraphQL workbench
            </Link>
          </div>
        </Card>
      ) : null}
    </div>
  );
}
