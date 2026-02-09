"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { errorMessage } from "@/lib/errors";
import { LoadingInline } from "@/components/loading-inline";
import { FileTextIcon, LayoutTemplateIcon, MessageSquareTextIcon } from "lucide-react";

type Project = { name: string; status?: string; labels?: Record<string, string> };
type Sample = {
  file: string;
  title: string;
  kindGuess: string;
  description: string;
  features: string[];
};

function defaultNamespaceForSample(file: string): string {
  const base = file.replace(/\.(ya?ml)$/i, "");
  return `${base}-demo`
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63);
}

function SampleIcon({ kindGuess }: { kindGuess: string }) {
  if (kindGuess === "web-forum") return <MessageSquareTextIcon className="size-4" />;
  if (kindGuess === "data-product") return <LayoutTemplateIcon className="size-4" />;
  return <FileTextIcon className="size-4" />;
}

export function ProjectsClient() {
  const router = useRouter();
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(false);
  const [ns, setNs] = useState("web-forum-demo");
  const [creating, setCreating] = useState(false);
  const [lastLog, setLastLog] = useState<string | null>(null);
  const [samples, setSamples] = useState<Sample[]>([]);
  const [samplesLoading, setSamplesLoading] = useState(false);
  const [sampleFile, setSampleFile] = useState<string>("web-forum.yaml");

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/projects", { cache: "no-store" });
      const json = await res.json();
      setProjects(Array.isArray(json?.projects) ? json.projects : []);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const loadSamples = useCallback(async () => {
    setSamplesLoading(true);
    try {
      const res = await fetch("/api/samples", { cache: "no-store" });
      const json = await res.json();
      const items: Sample[] = Array.isArray(json?.items) ? json.items : [];
      setSamples(items);
      if (items.length) {
        const next = items.find((s) => s.file === sampleFile) ?? items[0];
        setSampleFile(next.file);
        setNs((prev) => prev || defaultNamespaceForSample(next.file));
      }
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setSamplesLoading(false);
    }
  }, [sampleFile]);

  useEffect(() => {
    void refresh();
    void loadSamples();
  }, [refresh, loadSamples]);

  const rows = useMemo(() => {
    return projects.slice().sort((a, b) => a.name.localeCompare(b.name));
  }, [projects]);

  const selectedSample = useMemo(
    () => samples.find((s) => s.file === sampleFile) ?? null,
    [samples, sampleFile],
  );

  async function applySample() {
    setCreating(true);
    setLastLog(null);
    try {
      const res = await fetch("/api/projects/apply-sample", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ namespace: ns, sample: sampleFile }),
      });
      const json = await res.json();
      if (!res.ok) {
        setLastLog([json?.error, json?.stderr, json?.stdout].filter(Boolean).join("\n\n"));
        throw new Error(json?.error ?? "Failed to apply sample");
      }
      setLastLog(json?.applied ?? null);
      toast.success(`Applied sample into namespace ${json?.namespace ?? ns}`);
      await refresh();
      router.push(`/projects/${encodeURIComponent(json?.namespace ?? ns)}`);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div className="space-y-1">
          <h1
            className="text-2xl font-semibold tracking-tight"
            style={{ fontFamily: "var(--font-space-grotesk)" }}
          >
            Projects
          </h1>
          <p className="text-sm text-muted-foreground">
            A project maps to a Kubernetes namespace with a data-product manifest installed.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <LoadingInline
            show={creating}
            text="Applying sample (kubectl apply)..."
            className="mr-2"
          />
          <Button variant="outline" onClick={() => void refresh()} disabled={loading}>
            Refresh
          </Button>
          <Dialog>
            <DialogTrigger asChild>
              <Button>Create from sample</Button>
            </DialogTrigger>
            <DialogContent className="max-w-3xl">
              <DialogHeader>
                <DialogTitle>Apply sample data product</DialogTitle>
                <DialogDescription>
                  Pick a starter data product, then choose a namespace. This runs{" "}
                  <code className="font-mono text-xs">kubectl apply -n &lt;ns&gt; -f samples/&lt;sample&gt;</code>{" "}
                  and stores the manifest for cloning.
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-3">
                <div className="grid gap-2">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-medium">Samples</div>
                    <LoadingInline show={samplesLoading} text="Scanning..." />
                  </div>
                  <div className="grid grid-cols-1 gap-2 lg:grid-cols-2">
                    {(samples.length
                      ? samples
                      : [
                          {
                            file: "web-forum.yaml",
                            title: "Web Forum",
                            kindGuess: "web-forum",
                            description: "Web forum sample data product.",
                            features: [],
                          } satisfies Sample,
                        ]
                    ).map((s) => {
                      const selected = s.file === sampleFile;
                      return (
                        <button
                          key={s.file}
                          type="button"
                          className={[
                            "rounded-lg border p-3 text-left transition-colors",
                            selected ? "bg-muted" : "hover:bg-muted/60",
                          ].join(" ")}
                          onClick={() => {
                            setSampleFile(s.file);
                            setNs(defaultNamespaceForSample(s.file));
                            setLastLog(null);
                          }}
                        >
                          <div className="flex items-start gap-3">
                            <div className="mt-0.5 rounded-md border bg-background p-1.5">
                              <SampleIcon kindGuess={s.kindGuess} />
                            </div>
                            <div className="min-w-0">
                              <div className="flex items-center justify-between gap-2">
                                <div className="truncate text-sm font-semibold tracking-tight">
                                  {s.title}
                                </div>
                                <Badge
                                  variant="secondary"
                                  className="font-mono text-[10px]"
                                >
                                  {s.file}
                                </Badge>
                              </div>
                              <div className="mt-1 text-xs text-muted-foreground">
                                {s.description}
                              </div>
                              {s.features?.length ? (
                                <div className="mt-2 flex flex-wrap gap-1">
                                  {s.features.slice(0, 4).map((f) => (
                                    <Badge
                                      key={f}
                                      variant="outline"
                                      className="text-[10px]"
                                    >
                                      {f}
                                    </Badge>
                                  ))}
                                </div>
                              ) : null}
                            </div>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </div>

                <div className="grid gap-2">
                  <label className="text-sm font-medium">Namespace</label>
                  <Input value={ns} onChange={(e) => setNs(e.target.value)} placeholder="e.g. web-forum-demo" />
                  <p className="text-xs text-muted-foreground">
                    Must be lowercase DNS label (a-z, 0-9, -) and {"<= 63"} chars.
                  </p>
                </div>
                <div className="text-xs text-muted-foreground">
                  Selected sample:{" "}
                  <span className="font-mono">{selectedSample?.file ?? sampleFile}</span>
                </div>
                {lastLog ? (
                  <>
                    <Separator />
                    <div className="space-y-2">
                      <div className="text-sm font-medium">Output</div>
                      <Card className="bg-muted/40">
                        <ScrollArea className="h-48 p-3">
                          <pre className="text-xs leading-5 whitespace-pre-wrap">{lastLog}</pre>
                        </ScrollArea>
                      </Card>
                    </div>
                  </>
                ) : null}
              </div>
              <DialogFooter>
                <Button onClick={() => void applySample()} disabled={creating}>
                  {creating ? "Applying..." : "Apply sample"}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <Card className="p-0 overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Namespace</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Label</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={3} className="text-sm text-muted-foreground">
                  {loading ? "Loading..." : "No projects found. Create one from the sample."}
                </TableCell>
              </TableRow>
            ) : (
              rows.map((p) => (
                <TableRow key={p.name}>
                  <TableCell className="font-medium">
                    <Link className="underline underline-offset-4 hover:text-foreground" href={`/projects/${encodeURIComponent(p.name)}`}>
                      {p.name}
                    </Link>
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">{p.status ?? "-"}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">da-app-project=true</Badge>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>
    </div>
  );
}
