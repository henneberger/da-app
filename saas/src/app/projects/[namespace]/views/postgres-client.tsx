"use client";

import { useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { errorMessage } from "@/lib/errors";
import { HighlightEditor } from "@/components/code-editor";

export function PostgresClientView({ namespace }: { namespace: string }) {
  const [sql, setSql] = useState("select now();");
  const [running, setRunning] = useState(false);
  const [stdout, setStdout] = useState<string>("");
  const [stderr, setStderr] = useState<string>("");

  async function run() {
    setRunning(true);
    setStdout("");
    setStderr("");
    try {
      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/postgres/query`,
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
          <h1 className="text-2xl font-semibold tracking-tight">Postgres</h1>
          <p className="text-sm text-muted-foreground">
            Runs SQL via <code className="font-mono text-xs">kubectl exec</code> into{" "}
            <code className="font-mono text-xs">deploy/postgres</code>.
          </p>
        </div>
        <Button onClick={() => void run()} disabled={running}>
          {running ? "Running..." : "Run"}
        </Button>
      </div>

      <Card className="p-4">
        <div className="text-sm font-semibold tracking-tight">SQL</div>
        <Separator className="my-3" />
        <HighlightEditor value={sql} onChange={setSql} language="sql" minHeight={260} />
      </Card>

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
    </div>
  );
}
