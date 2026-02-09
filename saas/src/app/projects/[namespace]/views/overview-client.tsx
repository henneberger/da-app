"use client";

import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { errorMessage } from "@/lib/errors";
import { useRouter } from "next/navigation";
import { LoadingInline } from "@/components/loading-inline";

type Status = {
  deployments: { name: string; ready: string; updated: number }[];
  podPhaseCounts: Record<string, number>;
  services: { name: string; type: string }[];
};

export function OverviewClient({ namespace }: { namespace: string }) {
  const router = useRouter();
  const [status, setStatus] = useState<Status | null>(null);
  const [loading, setLoading] = useState(false);
  const [targetNs, setTargetNs] = useState(`${namespace}-clone`);
  const [cloning, setCloning] = useState(false);

  async function refresh() {
    setLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/status`, { cache: "no-store" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "status failed");
      setStatus(json as Status);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [namespace]);

  async function cloneProject() {
    setCloning(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/clone`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ targetNamespace: targetNs }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "clone failed");
      toast.success(`Cloned into ${json?.targetNamespace ?? targetNs}`);
      router.push(`/projects/${encodeURIComponent(json?.targetNamespace ?? targetNs)}`);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setCloning(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">Overview</h1>
          <p className="text-sm text-muted-foreground">
            Deployed job status and high-signal health indicators.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <LoadingInline
            show={loading}
            text="Fetching status..."
            className="mr-2"
          />
          <LoadingInline
            show={cloning}
            text="Cloning..."
            className="mr-2"
          />
          <Button variant="outline" onClick={() => void refresh()} disabled={loading}>
            {loading ? "Loading..." : "Reload status"}
          </Button>
          <Dialog>
            <DialogTrigger asChild>
              <Button>Clone</Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Clone project</DialogTitle>
                <DialogDescription>
                  Re-applies the stored manifest into a new namespace.
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-2">
                <label className="text-sm font-medium">Target namespace</label>
                <Input value={targetNs} onChange={(e) => setTargetNs(e.target.value)} />
              </div>
              <DialogFooter>
                <Button onClick={() => void cloneProject()} disabled={cloning}>
                  {cloning ? "Cloning..." : "Clone"}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Card className="p-4">
          <div className="text-sm font-medium">Deployments</div>
          <Separator className="my-3" />
          <div className="space-y-2">
            {status?.deployments?.length ? (
              status.deployments.map((d) => (
                <div key={d.name} className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{d.name}</span>
                  <Badge variant="secondary">{d.ready}</Badge>
                </div>
              ))
            ) : loading ? (
              <div className="space-y-2">
                <div className="h-4 w-3/4 rounded bg-muted/50" />
                <div className="h-4 w-2/3 rounded bg-muted/50" />
                <div className="h-4 w-1/2 rounded bg-muted/50" />
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">-</div>
            )}
          </div>
        </Card>

        <Card className="p-4">
          <div className="text-sm font-medium">Pods</div>
          <Separator className="my-3" />
          <div className="space-y-2">
            {status?.podPhaseCounts ? (
              Object.entries(status.podPhaseCounts).map(([k, v]) => (
                <div key={k} className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{k}</span>
                  <Badge variant="secondary">{v}</Badge>
                </div>
              ))
            ) : loading ? (
              <div className="space-y-2">
                <div className="h-4 w-3/4 rounded bg-muted/50" />
                <div className="h-4 w-2/3 rounded bg-muted/50" />
                <div className="h-4 w-1/2 rounded bg-muted/50" />
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">-</div>
            )}
          </div>
        </Card>

        <Card className="p-4">
          <div className="text-sm font-medium">Services</div>
          <Separator className="my-3" />
          <div className="space-y-2">
            {status?.services?.length ? (
              status.services.map((s) => (
                <div key={s.name} className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{s.name}</span>
                  <Badge variant="secondary">{s.type}</Badge>
                </div>
              ))
            ) : loading ? (
              <div className="space-y-2">
                <div className="h-4 w-3/4 rounded bg-muted/50" />
                <div className="h-4 w-2/3 rounded bg-muted/50" />
                <div className="h-4 w-1/2 rounded bg-muted/50" />
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">-</div>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}
