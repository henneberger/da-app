"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import {
  buildSchema,
  getNamedType,
  isListType,
  isNonNullType,
  isObjectType,
  parse,
  type DocumentNode,
  type FieldDefinitionNode,
  type ObjectTypeDefinitionNode,
  type GraphQLField,
  type GraphQLInputType,
  type GraphQLObjectType,
  type GraphQLSchema,
} from "graphql";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { errorMessage } from "@/lib/errors";
import { parseManifest, serializeManifest, type K8sDoc } from "./manifest-client";
import { HighlightEditor } from "@/components/code-editor";
import { LoadingInline } from "@/components/loading-inline";

type SchemaTarget = { idx: number; name: string; schema: string };
type QueryItem = {
  name: string;
  typeName: string | null;
  fieldName: string | null;
  connectionRef: string | null;
  sql: string;
  params: unknown[];
};
type MutationItem = {
  name: string;
  mutationName: string | null;
  connectionRef: string | null;
  sql: string;
  params: unknown[];
};
type SubscriptionItem = {
  name: string;
  fieldName: string | null;
  connectionRef: string | null;
  table: string | null;
  operations: unknown[];
  filters: unknown[];
};

type ParamBinding = {
  index: number;
  sourceKind: string;
  sourceName: string;
};

function getPath(obj: unknown, path: string[]): unknown {
  let cur = obj;
  for (const k of path) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[k];
  }
  return cur;
}

function typeToString(t: GraphQLInputType): string {
  if (isNonNullType(t)) return `${typeToString(t.ofType)}!`;
  if (isListType(t)) return `[${typeToString(t.ofType)}]`;
  const n = getNamedType(t);
  return "name" in n ? String((n as { name: string }).name) : "Unknown";
}

function baseTypeName(t: GraphQLInputType): string {
  if (isNonNullType(t)) return baseTypeName(t.ofType);
  if (isListType(t)) return baseTypeName(t.ofType);
  const n = getNamedType(t);
  return "name" in n ? String((n as { name: string }).name) : "Unknown";
}

function gqlToSqlType(typeName: string): string {
  const n = typeName;
  if (n === "Int") return "int";
  if (n === "Boolean") return "bool";
  if (n === "Float") return "text";
  if (n === "ID" || n === "String") return "text";
  // enums -> text, input objects -> jsonb (user can override).
  return "text";
}

function parseParamBindings(params: unknown[]): ParamBinding[] {
  const out: ParamBinding[] = [];
  for (const p of params) {
    if (!p || typeof p !== "object") continue;
    const r = p as Record<string, unknown>;
    const idx = typeof r.index === "number" ? r.index : null;
    const src = r.source && typeof r.source === "object" ? (r.source as Record<string, unknown>) : {};
    if (!idx) continue;
    out.push({
      index: idx,
      sourceKind: String(src.kind ?? ""),
      sourceName: String(src.name ?? ""),
    });
  }
  out.sort((a, b) => a.index - b.index);
  return out;
}

export function GraphQLClientView({ namespace }: { namespace: string }) {
  const [docs, setDocs] = useState<K8sDoc[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [queries, setQueries] = useState<QueryItem[]>([]);
  const [mutations, setMutations] = useState<MutationItem[]>([]);
  const [subscriptions, setSubscriptions] = useState<SubscriptionItem[]>([]);

  const [selected, setSelected] = useState<
    { group: "Query" | "Mutation" | "Subscription" | "Type"; typeName: string; fieldName: string } | null
  >(null);
  const [detailsOpen, setDetailsOpen] = useState(true);
  const [detailsTab, setDetailsTab] = useState<"sql" | "plan" | "output">("sql");

  const [argValues, setArgValues] = useState<Record<string, string>>({});
  const [parentValues, setParentValues] = useState<Record<string, string>>({});
  const [jwtValues, setJwtValues] = useState<Record<string, string>>({});
  const [argTypes, setArgTypes] = useState<Record<string, string>>({});
  const [running, setRunning] = useState(false);
  const [explain, setExplain] = useState("");
  const [stdout, setStdout] = useState("");
  const [renderedSql, setRenderedSql] = useState("");
  const [analyze, setAnalyze] = useState(false);

  const targets: SchemaTarget[] = useMemo(() => {
    const out: SchemaTarget[] = [];
    docs.forEach((d, idx) => {
      if (d.kind !== "GraphQLSchema") return;
      const js = d.doc.toJS({}) as unknown;
      const name = d.name ?? `GraphQLSchema#${idx}`;
      const schema = String(getPath(js, ["spec", "schema"]) ?? "");
      out.push({ idx, name, schema });
    });
    return out;
  }, [docs]);

  const [active, setActive] = useState<number>(0);
  const activeTarget = targets[active];
  const [schemaText, setSchemaText] = useState("");
  const schemaTextareaRef = useRef<HTMLTextAreaElement | null>(null);

  const [mode, setMode] = useState<"graphql" | "flink">("graphql");
  const [flinkLoading, setFlinkLoading] = useState(false);
  const [flinkCatalogs, setFlinkCatalogs] = useState<{ name: string; sql: string }[]>([]);
  const [flinkJobs, setFlinkJobs] = useState<{ name: string; catalogRefs: string[]; image: string | null; serviceAccount: string | null; sql: string }[]>([]);
  const [activeFlink, setActiveFlink] = useState<{ kind: "catalog" | "job"; name: string } | null>(null);

  useEffect(() => {
    setSchemaText(activeTarget?.schema ?? "");
  }, [activeTarget?.schema]);

  async function load() {
    setLoading(true);
    try {
      const [mRes, qRes, muRes, sRes] = await Promise.all([
        fetch(`/api/projects/${encodeURIComponent(namespace)}/manifest`, { cache: "no-store" }),
        fetch(`/api/projects/${encodeURIComponent(namespace)}/queries`, { cache: "no-store" }),
        fetch(`/api/projects/${encodeURIComponent(namespace)}/mutations`, { cache: "no-store" }),
        fetch(`/api/projects/${encodeURIComponent(namespace)}/subscriptions`, { cache: "no-store" }),
      ]);

      const text = await mRes.text();
      if (!mRes.ok) {
        let j: unknown = null;
        try {
          j = JSON.parse(text);
        } catch {
          // ignore
        }
        const err =
          j && typeof j === "object" && "error" in j
            ? String((j as Record<string, unknown>).error)
            : text ?? "manifest fetch failed";
        throw new Error(err);
      }
      setDocs(parseManifest(text));

      const qj = await qRes.json().catch(() => ({}));
      if (qRes.ok) setQueries(Array.isArray(qj?.items) ? qj.items : []);

      const muj = await muRes.json().catch(() => ({}));
      if (muRes.ok) setMutations(Array.isArray(muj?.items) ? muj.items : []);

      const sj = await sRes.json().catch(() => ({}));
      if (sRes.ok) setSubscriptions(Array.isArray(sj?.items) ? sj.items : []);

      toast.success("Loaded GraphQL + resolvers");
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setLoading(false);
    }
  }

  async function loadFlink() {
    setFlinkLoading(true);
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/flink/resources`, { cache: "no-store" });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "flink resources failed");
      setFlinkCatalogs(Array.isArray(json?.catalogs) ? json.catalogs : []);
      setFlinkJobs(Array.isArray(json?.jobs) ? json.jobs : []);
      const first =
        (Array.isArray(json?.jobs) && json.jobs[0]?.name)
          ? ({ kind: "job" as const, name: String(json.jobs[0].name) })
          : (Array.isArray(json?.catalogs) && json.catalogs[0]?.name)
            ? ({ kind: "catalog" as const, name: String(json.catalogs[0].name) })
            : null;
      setActiveFlink((prev) => prev ?? first);
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setFlinkLoading(false);
    }
  }

  useEffect(() => {
    void load();
    void loadFlink();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [namespace]);

  const schemaForExplorer = schemaText;
  const schemaInfo = useMemo(() => {
    if (!schemaForExplorer.trim())
      return {
        error: "Schema is empty" as string | null,
        schema: null as GraphQLSchema | null,
        queryType: null as GraphQLObjectType | null,
        mutationType: null as GraphQLObjectType | null,
        subscriptionType: null as GraphQLObjectType | null,
      };
    try {
      const s = buildSchema(schemaForExplorer);
      const q = s.getQueryType() as GraphQLObjectType | null;
      const m = s.getMutationType() as GraphQLObjectType | null;
      const sub = s.getSubscriptionType() as GraphQLObjectType | null;
      return {
        error: null as string | null,
        schema: s as GraphQLSchema,
        queryType: q,
        mutationType: m,
        subscriptionType: sub,
      };
    } catch (e: unknown) {
      return {
        error: errorMessage(e),
        schema: null as GraphQLSchema | null,
        queryType: null,
        mutationType: null,
        subscriptionType: null,
      };
    }
  }, [schemaForExplorer]);

  const sdlAst: DocumentNode | null = useMemo(() => {
    try {
      if (!schemaText.trim()) return null;
      return parse(schemaText, { noLocation: false });
    } catch {
      return null;
    }
  }, [schemaText]);

  function selectFromCursor(offset: number) {
    if (!sdlAst) return;
    // Find enclosing type + field definition by location.
    for (const def of sdlAst.definitions) {
      if (def.kind !== "ObjectTypeDefinition") continue;
      const obj = def as ObjectTypeDefinitionNode;
      const fields = obj.fields ?? [];
      for (const f of fields) {
        const fd = f as FieldDefinitionNode;
        const loc = fd.loc;
        if (!loc) continue;
        if (offset >= loc.start && offset <= loc.end) {
          const typeName = obj.name.value;
          const fieldName = fd.name.value;
          if (typeName === "Query") setSelected({ group: "Query", typeName, fieldName });
          else if (typeName === "Mutation") setSelected({ group: "Mutation", typeName, fieldName });
          else if (typeName === "Subscription") setSelected({ group: "Subscription", typeName, fieldName });
          else setSelected({ group: "Type", typeName, fieldName });
          setDetailsOpen(true);
          return;
        }
      }
    }
  }

  const selectedResolver = useMemo(() => {
    if (!selected) return null;
    if (selected.group === "Query" || selected.group === "Type") {
      const q = queries.find((it) => it.typeName === selected.typeName && it.fieldName === selected.fieldName) ?? null;
      return q ? { kind: "Query" as const, item: q } : null;
    }
    if (selected.group === "Mutation") {
      const m = mutations.find((it) => it.mutationName === selected.fieldName) ?? null;
      return m ? { kind: "PostgresMutation" as const, item: m } : null;
    }
    if (selected.group === "Subscription") {
      const s = subscriptions.find((it) => it.fieldName === selected.fieldName) ?? null;
      return s ? { kind: "PostgresSubscription" as const, item: s } : null;
    }
    return null;
  }, [selected, queries, mutations, subscriptions]);

  const selectedField: GraphQLField<unknown, unknown> | null = useMemo(() => {
    if (!selected) return null;
    const type =
      selected.group === "Query"
        ? schemaInfo.queryType
        : selected.group === "Mutation"
          ? schemaInfo.mutationType
          : selected.group === "Subscription"
            ? schemaInfo.subscriptionType
            : null;
    if (type && selected.group !== "Type") {
      const fields = type.getFields();
      return fields[selected.fieldName] ?? null;
    }
    // Typed resolvers aren't necessarily on the root types; don't try to resolve full signature right now.
    return null;
  }, [selected, schemaInfo.queryType, schemaInfo.mutationType, schemaInfo.subscriptionType]);

  const selectedBindings = useMemo(() => {
    if (!selectedResolver) return [];
    if (selectedResolver.kind === "Query") return parseParamBindings(selectedResolver.item.params);
    if (selectedResolver.kind === "PostgresMutation") return parseParamBindings(selectedResolver.item.params);
    return [];
  }, [selectedResolver]);

  useEffect(() => {
    // When selection changes, prime argTypes from GraphQL arg types when possible.
    setExplain("");
    setStdout("");
    setRenderedSql("");
    setDetailsTab("sql");

    if (!selectedField) return;
    const next: Record<string, string> = {};
    for (const a of selectedField.args ?? []) {
      const base = baseTypeName(a.type);
      next[a.name] = gqlToSqlType(base);
    }
    setArgTypes((prev) => ({ ...next, ...prev }));
  }, [selected?.group, selected?.typeName, selected?.fieldName, selectedField]);

  async function applySchema() {
    if (!activeTarget) {
      toast.error("No GraphQLSchema found in the stored manifest");
      return;
    }

    setSaving(true);
    try {
      const nextDocs = docs.slice();
      nextDocs[activeTarget.idx].doc.setIn(["spec", "schema"], schemaText);

      const out = serializeManifest(nextDocs);
      const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/manifest`, {
        method: "PUT",
        headers: { "content-type": "text/yaml" },
        body: out,
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error ?? "apply failed");
      setDocs(parseManifest(out));
      toast.success("Applied GraphQL schema changes");
    } catch (e: unknown) {
      toast.error(errorMessage(e));
    } finally {
      setSaving(false);
    }
  }

  async function runSelected() {
    if (!selectedResolver) {
      toast.error("No resolver found for this selection");
      return;
    }
    setRunning(true);
    setExplain("");
    setStdout("");
    setRenderedSql("");
    try {
      if (selectedResolver.kind === "Query") {
        const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/queries/run`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            queryName: selectedResolver.item.name,
            args: argValues,
            parent: parentValues,
            jwt: jwtValues,
            argTypes,
            analyze,
          }),
        });
        const json = await res.json();
        if (!res.ok) throw new Error(json?.error ?? "run failed");
        setRenderedSql(String(json?.renderedSql ?? ""));
        setExplain(String(json?.explain ?? ""));
        setStdout(String(json?.stdout ?? ""));
        setDetailsTab(String(json?.stdout ?? "") ? "output" : String(json?.explain ?? "") ? "plan" : "sql");
        toast.success("Executed");
        return;
      }
      if (selectedResolver.kind === "PostgresMutation") {
        const res = await fetch(`/api/projects/${encodeURIComponent(namespace)}/mutations/run`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            mutationCrName: selectedResolver.item.name,
            args: argValues,
            jwt: jwtValues,
            argTypes,
            analyze,
          }),
        });
        const json = await res.json();
        if (!res.ok) throw new Error(json?.error ?? "run failed");
        setRenderedSql(String(json?.renderedSql ?? ""));
        setExplain(String(json?.explain ?? ""));
        setStdout(String(json?.stdout ?? ""));
        setDetailsTab(String(json?.stdout ?? "") ? "output" : String(json?.explain ?? "") ? "plan" : "sql");
        toast.success("Executed");
        return;
      }
      toast.error("Subscriptions are not executable from this panel yet. Use Operations for live GraphQL execution.");
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
          <h1 className="text-2xl font-semibold tracking-tight">Schemas</h1>
          <p className="text-sm text-muted-foreground">
            API schema workbench. Swap between GraphQL and Flink SQL, and inspect execution details on the right.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <LoadingInline show={loading} text="Loading..." className="mr-2" />
          <LoadingInline show={saving} text="Applying..." className="mr-2" />
          <Button variant="outline" onClick={() => void load()} disabled={loading}>
            {loading ? "Loading..." : "Reload manifest"}
          </Button>
          <Button variant="outline" onClick={() => void loadFlink()} disabled={flinkLoading}>
            {flinkLoading ? "Loading..." : "Reload Flink"}
          </Button>
          <Button onClick={() => void applySchema()} disabled={saving}>
            {saving ? "Applying..." : "Apply schema"}
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[1fr_420px]">
        <div className="space-y-4">
          <Card className="p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold tracking-tight">Schema</div>
              <div className="flex items-center gap-2">
                <Button
                  size="sm"
                  variant={mode === "graphql" ? "default" : "outline"}
                  onClick={() => setMode("graphql")}
                >
                  GraphQL
                </Button>
                <Button
                  size="sm"
                  variant={mode === "flink" ? "default" : "outline"}
                  onClick={() => setMode("flink")}
                >
                  Flink
                </Button>
              </div>
            </div>
            <Separator className="my-3" />
            {mode === "graphql" ? (
              <>
                <LoadingInline
                  show={loading && docs.length === 0}
                  text="Fetching schema and resolvers..."
                  className="mb-3"
                />
                {selected ? (
                  <div className="mb-3 text-sm">
                    Selected:{" "}
                    <span className="font-mono text-xs">
                      {selected.group === "Type"
                        ? `${selected.typeName}.${selected.fieldName}`
                        : `${selected.group}.${selected.fieldName}`}
                    </span>
                  </div>
                ) : (
                  <div className="mb-3 text-sm text-muted-foreground">
                    Click a field in the schema to inspect it.
                  </div>
                )}
                {targets.length === 0 ? (
                  <div className="text-sm text-muted-foreground">
                    No <code className="font-mono text-xs">GraphQLSchema</code> found in the stored manifest.
                  </div>
                ) : (
                  <>
                    <div className="flex flex-wrap gap-2">
                      {targets.map((t, i) => (
                        <Button
                          key={t.name}
                          variant={i === active ? "default" : "outline"}
                          size="sm"
                          onClick={() => setActive(i)}
                        >
                          {t.name}
                        </Button>
                      ))}
                    </div>
                    <Separator className="my-3" />
                    <HighlightEditor
                      value={schemaText}
                      onChange={setSchemaText}
                      language="graphql"
                      placeholder="GraphQL SDL..."
                      minHeight={520}
                      onCursor={({ offset }) => selectFromCursor(offset)}
                      textareaRef={schemaTextareaRef}
                      cursorOnKeyUp={false}
                    />
                  </>
                )}
              </>
            ) : (
              <>
                <LoadingInline
                  show={flinkLoading && flinkCatalogs.length === 0 && flinkJobs.length === 0}
                  text="Fetching Flink catalog + jobs..."
                  className="mb-3"
                />
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-[260px_1fr]">
                  <Card className="p-3">
                    <div className="flex items-center justify-between">
                      <div className="text-sm font-semibold tracking-tight">Flink</div>
                      <Badge variant="secondary">
                        {(flinkCatalogs.length + flinkJobs.length).toString()}
                      </Badge>
                    </div>
                    <Separator className="my-3" />
                    <div className="space-y-1">
                      {flinkCatalogs.map((c) => {
                        const active = activeFlink?.kind === "catalog" && activeFlink.name === c.name;
                        return (
                          <button
                            key={`catalog:${c.name}`}
                            className={[
                              "w-full rounded-md border px-2 py-2 text-left text-sm",
                              active ? "bg-muted" : "hover:bg-muted/60",
                            ].join(" ")}
                            onClick={() => setActiveFlink({ kind: "catalog", name: c.name })}
                            type="button"
                          >
                            <div className="font-medium">Catalog</div>
                            <div className="font-mono text-xs text-muted-foreground">{c.name}</div>
                          </button>
                        );
                      })}
                      {flinkJobs.map((j) => {
                        const active = activeFlink?.kind === "job" && activeFlink.name === j.name;
                        return (
                          <button
                            key={`job:${j.name}`}
                            className={[
                              "w-full rounded-md border px-2 py-2 text-left text-sm",
                              active ? "bg-muted" : "hover:bg-muted/60",
                            ].join(" ")}
                            onClick={() => setActiveFlink({ kind: "job", name: j.name })}
                            type="button"
                          >
                            <div className="font-medium">SQL Job</div>
                            <div className="font-mono text-xs text-muted-foreground">{j.name}</div>
                          </button>
                        );
                      })}
                      {flinkCatalogs.length === 0 && flinkJobs.length === 0 && !flinkLoading ? (
                        <div className="text-sm text-muted-foreground">
                          No <code className="font-mono text-xs">FlinkCatalog</code> or{" "}
                          <code className="font-mono text-xs">FlinkSqlJob</code> detected in this namespace.
                        </div>
                      ) : null}
                    </div>
                  </Card>

                  <div className="space-y-3">
                    {activeFlink ? (
                      <>
                        {activeFlink.kind === "catalog" ? (
                          <>
                            <div className="text-sm font-semibold tracking-tight">Catalog SQL</div>
                            <HighlightEditor
                              value={flinkCatalogs.find((c) => c.name === activeFlink.name)?.sql ?? ""}
                              language="sql"
                              readOnly
                              minHeight={520}
                            />
                          </>
                        ) : (
                          <>
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div className="text-sm font-semibold tracking-tight">Job SQL</div>
                              <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                                <span className="font-mono">
                                  image: {flinkJobs.find((j) => j.name === activeFlink.name)?.image ?? "-"}
                                </span>
                                <span className="font-mono">
                                  catalogs: {(flinkJobs.find((j) => j.name === activeFlink.name)?.catalogRefs ?? []).join(", ") || "-"}
                                </span>
                              </div>
                            </div>
                            <HighlightEditor
                              value={flinkJobs.find((j) => j.name === activeFlink.name)?.sql ?? ""}
                              language="sql"
                              readOnly
                              minHeight={520}
                            />
                          </>
                        )}
                      </>
                    ) : (
                      <div className="text-sm text-muted-foreground">Pick a Flink catalog or job.</div>
                    )}
                  </div>
                </div>
              </>
            )}
          </Card>
        </div>

        <Card className="p-4 lg:sticky lg:top-20 self-start max-h-[calc(100dvh-6rem)] overflow-hidden">
          <div className="flex items-center justify-between">
            <div className="text-sm font-semibold tracking-tight">Details</div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setDetailsOpen((v) => !v)}
              >
                {detailsOpen ? "Collapse" : "Expand"}
              </Button>
              <Button
                size="sm"
                onClick={() => void runSelected()}
                disabled={!detailsOpen || running || !selectedResolver}
              >
                {running ? "Running..." : analyze ? "Run + Analyze" : "Run"}
              </Button>
            </div>
          </div>
          <Separator className="my-3" />
          <ScrollArea className="h-[calc(100dvh-12rem)] pr-3">
            {!detailsOpen ? (
              <div className="text-sm text-muted-foreground">
                Click a field in the schema to inspect it here.
              </div>
            ) : !selected ? (
              <div className="text-sm text-muted-foreground">
                Click a field in the schema.
              </div>
            ) : (
              <div className="space-y-4">
              <div className="text-sm">
                Selected:{" "}
                <span className="font-mono text-xs">
                  {selected.group === "Type"
                    ? `${selected.typeName}.${selected.fieldName}`
                    : `${selected.group}.${selected.fieldName}`}
                </span>
              </div>

              {selectedField ? (
                <div className="text-sm text-muted-foreground">
                  Signature:{" "}
                  <span className="font-mono text-xs">
                    {selected.fieldName}(
                    {(selectedField.args ?? [])
                      .map((a) => `${a.name}: ${typeToString(a.type)}`)
                      .join(", ")}
                    )
                  </span>
                </div>
              ) : null}

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
                  Always runs EXPLAIN; analyze runs EXPLAIN ANALYZE.
                </div>
              </div>

              {!selectedResolver ? (
                <div className="text-sm text-muted-foreground">
                  No resolver found for this field.
                </div>
              ) : selectedResolver.kind === "PostgresSubscription" ? (
                <div className="text-sm text-muted-foreground">
                  This field is backed by a PostgresSubscription. Execute it via <b>Operations</b>.
                </div>
              ) : (
                <>
                  <div className="grid gap-2">
                    <div className="text-xs font-medium text-muted-foreground">
                      Parameters
                    </div>
                    {selectedBindings.length === 0 ? (
                      <div className="text-sm text-muted-foreground">No parameters.</div>
                    ) : (
                      <div className="space-y-2">
                        {selectedBindings.map((b) => {
                          const key = `${b.sourceKind}:${b.sourceName}:${b.index}`;
                          let gqlArgType: GraphQLInputType | null =
                            selectedField?.args?.find((a) => a.name === b.sourceName)?.type ?? null;
                          if (!gqlArgType && b.sourceKind === "PARENT" && selected.group === "Type" && schemaInfo.schema) {
                            const t = schemaInfo.schema.getType(selected.typeName);
                            if (t && isObjectType(t)) {
                              const f = t.getFields?.()[b.sourceName];
                              if (f) {
                                // parent fields are output types; treat as input-ish for default mapping.
                                gqlArgType = f.type as unknown as GraphQLInputType;
                              }
                            }
                          }
                          const gqlBase = gqlArgType ? baseTypeName(gqlArgType) : null;
                          const defaultType =
                            gqlBase ? gqlToSqlType(gqlBase) : "text";
                          const currentType = argTypes[b.sourceName] ?? defaultType;
                          return (
                            <div
                              key={key}
                              className="grid grid-cols-1 gap-2 lg:grid-cols-[180px_1fr]"
                            >
                              <div className="text-sm">
                                <span className="font-mono">
                                  {"$" + b.index}
                                </span>{" "}
                                <span className="text-muted-foreground">
                                  {b.sourceKind}.{b.sourceName}
                                </span>
                                {gqlArgType ? (
                                  <div className="text-xs text-muted-foreground">
                                    gql:{" "}
                                    <span className="font-mono">
                                      {typeToString(gqlArgType)}
                                    </span>
                                  </div>
                                ) : null}
                              </div>
                              <div className="flex gap-2">
                                <Input
                                  value={
                                    b.sourceKind === "PARENT"
                                      ? parentValues[b.sourceName] ?? ""
                                      : b.sourceKind === "JWT"
                                        ? jwtValues[b.sourceName] ?? ""
                                        : argValues[b.sourceName] ?? ""
                                  }
                                  onChange={(e) => {
                                    const v = e.target.value;
                                    if (b.sourceKind === "PARENT") {
                                      setParentValues((p) => ({ ...p, [b.sourceName]: v }));
                                    } else if (b.sourceKind === "JWT") {
                                      setJwtValues((p) => ({ ...p, [b.sourceName]: v }));
                                    } else {
                                      setArgValues((p) => ({ ...p, [b.sourceName]: v }));
                                    }
                                  }}
                                  placeholder="value"
                                />
                                <Input
                                  value={currentType}
                                  onChange={(e) =>
                                    setArgTypes((p) => ({
                                      ...p,
                                      [b.sourceName]: e.target.value,
                                    }))
                                  }
                                  placeholder="type"
                                  className="max-w-[140px] font-mono"
                                />
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>

                  {"sql" in selectedResolver.item ? (
                    <div className="grid gap-2">
                      <div className="text-xs font-medium text-muted-foreground">
                        SQL
                      </div>
                      <HighlightEditor
                        value={String((selectedResolver.item as { sql: string }).sql ?? "")}
                        language="sql"
                        readOnly
                        minHeight={240}
                      />
                    </div>
                  ) : null}
                </>
              )}

              <Tabs value={detailsTab} onValueChange={(v) => setDetailsTab(v as typeof detailsTab)}>
                <TabsList>
                  <TabsTrigger value="sql">Rendered SQL</TabsTrigger>
                  <TabsTrigger value="plan">Plan</TabsTrigger>
                  <TabsTrigger value="output">Output</TabsTrigger>
                </TabsList>
                <TabsContent value="sql" className="mt-3">
                  {renderedSql ? (
                    <HighlightEditor value={renderedSql} language="sql" readOnly minHeight={160} />
                  ) : (
                    <div className="text-sm text-muted-foreground">-</div>
                  )}
                </TabsContent>
                <TabsContent value="plan" className="mt-3">
                  {explain ? (
                    <div className="rounded-md border bg-muted/30">
                      <ScrollArea className="h-[220px] p-3">
                        <pre className="whitespace-pre text-xs leading-5">{explain}</pre>
                      </ScrollArea>
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">-</div>
                  )}
                </TabsContent>
                <TabsContent value="output" className="mt-3">
                  {stdout ? (
                    <div className="rounded-md border bg-muted/30">
                      <ScrollArea className="h-[220px] p-3">
                        <pre className="whitespace-pre text-xs leading-5">{stdout}</pre>
                      </ScrollArea>
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">-</div>
                  )}
                </TabsContent>
              </Tabs>
              </div>
            )}
          </ScrollArea>
        </Card>
      </div>
    </div>
  );
}
