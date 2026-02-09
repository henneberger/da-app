export type McpHttpClient = {
  endpointUrl: string;
  headers: Record<string, string>;
};

export type McpTool = {
  name: string;
  description?: string;
  inputSchema?: unknown;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

export async function mcpJsonRpc(
  client: McpHttpClient,
  method: string,
  params: unknown,
): Promise<unknown> {
  const id = Math.floor(Math.random() * 1e9);
  const res = await fetch(client.endpointUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...client.headers,
    },
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params }),
  });

  const text = await res.text();
  let json: unknown = null;
  try {
    json = JSON.parse(text);
  } catch {
    // non-json response
  }

  if (json == null) {
    const ct = res.headers.get("content-type") ?? "";
    const looksHtml = ct.includes("text/html") || /<html[\s>]/i.test(text);
    if (looksHtml) {
      const err = new Error(
        `MCP endpoint returned HTML (likely an authentication redirect or an upstream error). HTTP ${res.status}.`,
      );
      (err as unknown as Record<string, unknown>).status = res.status;
      (err as unknown as Record<string, unknown>).bodyText = text.slice(0, 2000);
      throw err;
    }
  }

  if (!res.ok) {
    const msg =
      (isRecord(json) && isRecord(json.error) && typeof json.error.message === "string"
        ? json.error.message
        : text) || `HTTP ${res.status}`;
    const err = new Error(msg);
    (err as unknown as Record<string, unknown>).status = res.status;
    (err as unknown as Record<string, unknown>).bodyText = text;
    throw err;
  }

  if (isRecord(json) && "error" in json && json.error) {
    const msg =
      isRecord(json.error) && typeof json.error.message === "string"
        ? json.error.message
        : JSON.stringify(json.error);
    throw new Error(msg);
  }
  if (isRecord(json) && "result" in json) return json.result;
  return json;
}

export async function mcpInitialize(client: McpHttpClient): Promise<unknown> {
  return await mcpJsonRpc(client, "initialize", {
    protocolVersion: "2024-11-05",
    clientInfo: { name: "da-app-saas", version: "0.1.0" },
    capabilities: {},
  });
}

export async function mcpToolsList(client: McpHttpClient): Promise<McpTool[]> {
  const result = await mcpJsonRpc(client, "tools/list", {});
  if (!isRecord(result) || !Array.isArray(result.tools)) return [];
  const out: McpTool[] = [];
  for (const t of result.tools) {
    if (!isRecord(t)) continue;
    const name = typeof t.name === "string" ? t.name : null;
    if (!name) continue;
    out.push({
      name,
      description: typeof t.description === "string" ? t.description : undefined,
      inputSchema:
        "inputSchema" in t ? (t as Record<string, unknown>).inputSchema : undefined,
    });
  }
  return out;
}

export async function mcpToolsCall(
  client: McpHttpClient,
  toolName: string,
  args: Record<string, unknown>,
): Promise<unknown> {
  return await mcpJsonRpc(client, "tools/call", {
    name: toolName,
    arguments: args,
  });
}
