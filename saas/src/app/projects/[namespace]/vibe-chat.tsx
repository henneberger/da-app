"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";

type Msg = { role: "user" | "assistant"; text: string };
type Step = {
  id: string;
  status: "queued" | "running" | "done";
  label: string;
  meta?: unknown;
};

export function VibeChat({ namespace }: { namespace: string }) {
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [messages, setMessages] = useState<Msg[]>([]);
  const [assistantDraft, setAssistantDraft] = useState("");
  const [steps, setSteps] = useState<Step[]>([
    { id: "export", status: "queued", label: "Exporting current state" },
    { id: "llm", status: "queued", label: "Generating patch with Codex" },
    { id: "apply", status: "queued", label: "Applying to Kubernetes" },
    { id: "check", status: "queued", label: "Checking k8s for issues" },
  ]);

  const endRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    // Always start collapsed when entering a project.
    setOpen(false);
  }, [namespace]);

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: "end" });
  }, [messages, assistantDraft, open]);

  const hasAnyRunning = useMemo(() => steps.some((s) => s.status === "running"), [steps]);
  const issues = useMemo(() => {
    const check = steps.find((s) => s.id === "check")?.meta;
    if (!check || typeof check !== "object") return [];
    const m = check as Record<string, unknown>;
    if (!Array.isArray(m.issues)) return [];
    return m.issues
      .filter((x) => typeof x === "string")
      .slice(0, 10) as string[];
  }, [steps]);

  function resetSteps() {
    setSteps([
      { id: "export", status: "queued", label: "Exporting current state" },
      { id: "llm", status: "queued", label: "Generating patch with Codex" },
      { id: "apply", status: "queued", label: "Applying to Kubernetes" },
      { id: "check", status: "queued", label: "Checking k8s for issues" },
    ]);
  }

  function updateStep(id: string, status: Step["status"], meta?: unknown) {
    setSteps((prev) =>
      prev.map((s) => (s.id === id ? { ...s, status, meta: meta ?? s.meta } : s)),
    );
  }

  async function send() {
    const msg = input.trim();
    if (!msg || sending) return;
    setInput("");
    setSending(true);
    setAssistantDraft("");
    resetSteps();
    setMessages((prev) => [...prev, { role: "user", text: msg }]);

    let fullAssistant = "";
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/vibe`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: msg }),
      });
      if (!res.ok || !res.body) {
        const j = await res.json().catch(() => ({}));
        throw new Error(j?.error ?? "vibe request failed");
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        // SSE parse (very small custom parser: split on blank line).
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

          if (event === "delta") {
            const o = obj as { text?: unknown };
            const d = String(o?.text ?? "");
            fullAssistant += d;
            setAssistantDraft(fullAssistant);
          }
          if (event === "summary") {
            const o = obj as { text?: unknown; issuesCount?: unknown };
            const t = String(o?.text ?? "Applied changes.");
            const n = typeof o?.issuesCount === "number" ? o.issuesCount : null;
            setMessages((prev) => [
              ...prev,
              {
                role: "assistant",
                text: n != null ? `${t}\n\nk8s issues detected: ${n}` : t,
              },
            ]);
          }
          if (event === "step") {
            const o = obj as { id?: unknown; status?: unknown };
            const id = String(o?.id ?? "");
            const status = String(o?.status ?? "") as Step["status"];
            if (id && status) updateStep(id, status, obj);
          }
          if (event === "error") {
            const o = obj as { message?: unknown };
            throw new Error(String(o?.message ?? "unknown error"));
          }
          if (event === "done") {
            // finalize
          }
        }
      }

      if (fullAssistant.trim()) {
        setMessages((prev) => [...prev, { role: "assistant", text: fullAssistant }]);
        setAssistantDraft("");
      } else {
        toast.success("Applied changes");
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(msg);
    } finally {
      setSending(false);
      setAssistantDraft("");
    }
  }

  return (
    <div className="fixed bottom-0 left-0 right-0 z-30 border-t bg-background/90 backdrop-blur">
      <div className="mx-auto max-w-6xl px-4 py-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="text-sm font-semibold tracking-tight">Vibe code</div>
            <Badge variant={hasAnyRunning ? "secondary" : "outline"}>
              {hasAnyRunning ? "running" : "idle"}
            </Badge>
            <div className="text-xs text-muted-foreground">
              namespace: <span className="font-mono">{namespace}</span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setOpen((v) => !v)}
            >
              {open ? "Collapse" : "Expand"}
            </Button>
          </div>
        </div>

        <div className="mt-3 flex items-center gap-2">
          <Input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Type a change request (Ctrl+Enter to send)..."
            onKeyDown={(e) => {
              if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) void send();
            }}
            disabled={sending}
          />
          <Button onClick={() => void send()} disabled={sending}>
            {sending ? "Running..." : "Send"}
          </Button>
        </div>

        {open ? (
          <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-[1fr_320px]">
            <Card className="overflow-hidden">
              <div className="border-b px-3 py-2 text-xs text-muted-foreground">
                Recent messages (we do not display raw YAML diffs here by default).
              </div>
              <ScrollArea className="h-[120px] px-3 py-2">
                <div className="space-y-2">
                  {messages.slice(-8).map((m, i) => (
                    <div
                      key={i}
                      className={
                        m.role === "user"
                          ? "text-sm"
                          : "text-sm text-muted-foreground"
                      }
                    >
                      <span className="font-semibold">
                        {m.role === "user" ? "You" : "Codex"}
                      </span>
                      <span className="ml-2 whitespace-pre-wrap">{m.text}</span>
                    </div>
                  ))}
                  {assistantDraft ? (
                    <div className="text-sm text-muted-foreground">
                      <span className="font-semibold">Codex</span>
                      <span className="ml-2 whitespace-pre-wrap">
                        {assistantDraft}
                      </span>
                    </div>
                  ) : null}
                  <div ref={endRef} />
                </div>
              </ScrollArea>
            </Card>

            <Card className="p-3">
              <div className="text-sm font-semibold tracking-tight">Progress</div>
              <div className="mt-2 space-y-2">
                {steps.map((s) => (
                  <div
                    key={s.id}
                    className="flex items-center justify-between text-sm"
                  >
                    <span className="text-muted-foreground">{s.label}</span>
                    <Badge
                      variant={
                        s.status === "done"
                          ? "secondary"
                          : s.status === "running"
                            ? "outline"
                            : "outline"
                      }
                    >
                      {s.status}
                    </Badge>
                  </div>
                ))}
              </div>
              {issues.length ? (
                <>
                  <Separator className="my-3" />
                  <div className="text-xs font-medium">Detected issues</div>
                  <div className="mt-2 max-h-[72px] overflow-auto rounded-md border bg-muted/40 p-2 text-[11px] leading-4">
                    {issues.map((x, i) => (
                      <div key={i} className="whitespace-pre-wrap">
                        {x}
                      </div>
                    ))}
                  </div>
                </>
              ) : null}
            </Card>
          </div>
        ) : null}
      </div>
    </div>
  );
}
