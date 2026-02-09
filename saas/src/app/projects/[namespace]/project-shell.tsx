"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { errorMessage } from "@/lib/errors";
import { VibeChat } from "./vibe-chat";

type NavItem = { href: string; label: string; badge?: string };

export function ProjectShell({
  namespace,
  children,
}: {
  namespace: string;
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const router = useRouter();

  const [loading, setLoading] = useState(false);
  const [fingerprint, setFingerprint] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);

  const navItems: NavItem[] = useMemo(() => {
    const base = `/projects/${encodeURIComponent(namespace)}`;
    return [
      { href: base, label: "Overview" },
      { href: `${base}/graphql`, label: "Schemas" },
      { href: `${base}/operations`, label: "Operations" },
      { href: `${base}/mcp`, label: "MCP" },
      { href: `${base}/connections`, label: "Connections" },
    ];
  }, [namespace]);

  const refreshFingerprint = useCallback(async () => {
    try {
      const res = await fetch(
        `/api/projects/${encodeURIComponent(namespace)}/fingerprint`,
        { cache: "no-store" },
      );
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "fingerprint failed");
      const fp = String(json?.fingerprint ?? "");
      setFingerprint((prev) => {
        if (prev && fp && prev !== fp) setHasChanges(true);
        return fp || prev;
      });
    } catch {
      // ignore (cluster not reachable, etc)
    }
  }, [namespace]);

  const onHardRefresh = useCallback(async () => {
    setLoading(true);
    try {
      await refreshFingerprint();
      setHasChanges(false);
      router.refresh();
      toast.success("Refreshed");
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }, [refreshFingerprint, router]);

  useEffect(() => {
    void refreshFingerprint();
    const t = setInterval(() => void refreshFingerprint(), 15_000);
    return () => clearInterval(t);
  }, [refreshFingerprint]);

  return (
    <div className="relative">
      {/* Reserve only the collapsed chat bar height; expanded chat floats over content. */}
      <div className="pb-[110px]">
        <div className="sticky top-0 z-20 border-b bg-background/80 backdrop-blur">
          <div className="mx-auto max-w-6xl px-4 py-3">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <div className="truncate text-sm font-semibold tracking-tight">
                    {namespace}
                  </div>
                  <Badge variant={hasChanges ? "destructive" : "secondary"}>
                    {hasChanges ? "changes" : "synced"}
                  </Badge>
                </div>
                <div className="mt-0.5 text-[11px] text-muted-foreground">
                  Polling k8s every 15s. Fingerprint:{" "}
                  <span className="font-mono">
                    {fingerprint?.slice(0, 8) ?? "-"}
                  </span>
                </div>
              </div>

              <div className="flex items-center gap-2">
                <Button
                  className={hasChanges ? "animate-pulse" : ""}
                  variant={hasChanges ? "destructive" : "outline"}
                  size="sm"
                  onClick={() => void onHardRefresh()}
                  disabled={loading}
                >
                  {loading
                    ? "Refreshing..."
                    : hasChanges
                      ? "Refresh (changes)"
                      : "Refresh"}
                </Button>
              </div>
            </div>

            <Separator className="my-3" />

            <Card className="p-2">
              <div className="flex gap-1 overflow-x-auto whitespace-nowrap">
                {navItems.map((it) => {
                  const active = pathname === it.href;
                  return (
                    <Link
                      key={it.href}
                      href={it.href}
                      className={[
                        "rounded-md px-3 py-1.5 text-sm",
                        active
                          ? "bg-muted text-foreground"
                          : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                      ].join(" ")}
                    >
                      {it.label}
                    </Link>
                  );
                })}
              </div>
            </Card>
          </div>
        </div>

        <div className="mx-auto max-w-6xl px-4 py-6">
          <section className="min-w-0">{children}</section>
        </div>
      </div>

      <VibeChat namespace={namespace} />
    </div>
  );
}
