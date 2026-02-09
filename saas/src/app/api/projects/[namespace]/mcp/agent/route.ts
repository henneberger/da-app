import crypto from "node:crypto";
import { NextResponse } from "next/server";
import { assertNamespace } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";
import { getOpenAIClient } from "@/lib/openai";
import type OpenAI from "openai";
import {
  mcpInitialize,
  mcpToolsCall,
  mcpToolsList,
  type McpHttpClient,
} from "@/lib/mcp-http";

export const runtime = "nodejs";

type Body = {
  endpointUrl: string;
  auth?:
    | { type: "bearer"; token: string }
    | { type: "basic"; username: string; password: string }
    | null;
  message: string;
};

function sseEvent(event: string, data: unknown): Uint8Array {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  return new TextEncoder().encode(payload);
}

function authHeaders(auth: Body["auth"]): Record<string, string> {
  if (!auth) return {};
  if (auth.type === "bearer") return { authorization: `Bearer ${auth.token}` };
  const token = Buffer.from(`${auth.username}:${auth.password}`).toString("base64");
  return { authorization: `Basic ${token}` };
}

function toolsToChatTools(
  tools: { name: string; description?: string; inputSchema?: unknown }[],
) {
  return tools.map((t) => ({
    type: "function" as const,
    function: {
      name: t.name,
      description: t.description ?? "",
      parameters:
        t.inputSchema && typeof t.inputSchema === "object"
          ? (t.inputSchema as unknown as Record<string, unknown>)
          : { type: "object", properties: {} },
    },
  }));
}

export async function POST(
  req: Request,
  { params }: { params: Promise<{ namespace: string }> },
) {
  const { namespace } = await params;
  const ns = String(namespace ?? "").trim();

  let body: Body;
  try {
    body = (await req.json()) as Body;
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const endpointUrl = String(body.endpointUrl ?? "").trim();
  const message = String(body.message ?? "").trim();
  if (!endpointUrl || !message) {
    return NextResponse.json(
      { error: "endpointUrl and message are required" },
      { status: 400 },
    );
  }

  try {
    assertNamespace(ns);
  } catch (e: unknown) {
    return NextResponse.json({ error: errorMessage(e) }, { status: 400 });
  }

  const stream = new ReadableStream<Uint8Array>({
    start: async (controller) => {
      const runId = crypto.randomUUID();
      const send = (event: string, data: unknown) =>
        controller.enqueue(sseEvent(event, data));

      try {
        send("run", { runId, namespace: ns });
        send("status", { text: "Connecting to MCP..." });

        const mcp: McpHttpClient = {
          endpointUrl,
          headers: authHeaders(body.auth),
        };

        send("step", { id: "mcp", status: "running", label: "Connecting to MCP" });
        await mcpInitialize(mcp);
        const tools = await mcpToolsList(mcp);
        send("step", { id: "mcp", status: "done", toolsCount: tools.length });
        send("status", { text: `Connected. ${tools.length} tools available.` });

        const client = getOpenAIClient();
        const model = process.env.OPENAI_MCP_MODEL || "gpt-4o-mini";

        const system = [
          "You are an agent that can call tools exposed by an MCP server.",
          "Use tool calls to accomplish the user's request.",
          "When asked to create data then query it, actually do it via tools.",
          "Return a concise summary plus any returned data (IDs, query results).",
        ].join("\n");

        const chatTools = toolsToChatTools(tools);

        const messages: OpenAI.Chat.Completions.ChatCompletionMessageParam[] = [
          { role: "system", content: system },
          { role: "user", content: message },
        ];

        const maxTurns = 8;
        for (let turn = 0; turn < maxTurns; turn++) {
          send("status", { text: "Thinking..." });
          const resp = await client.chat.completions.create({
            model,
            messages,
            tools: chatTools,
            tool_choice: "auto",
          });

          const choice = resp.choices?.[0];
          const msg = choice?.message;
          if (!msg) throw new Error("OpenAI returned no message");

          messages.push({
            role: "assistant",
            content: msg.content ?? null,
            tool_calls: msg.tool_calls,
          });

          const toolCalls = msg.tool_calls as
            | OpenAI.Chat.Completions.ChatCompletionMessageToolCall[]
            | undefined;

          if (!toolCalls || toolCalls.length === 0) {
            const content = typeof msg.content === "string" ? msg.content : "";
            send("assistant", { text: content });
            send("done", { ok: true });
            return;
          }

          for (const tc of toolCalls) {
            if (tc.type !== "function") continue;
            const toolName = tc.function.name;
            let args: Record<string, unknown> = {};
            try {
              args = JSON.parse(tc.function.arguments || "{}");
            } catch {
              args = {};
            }

            send("status", { text: `Calling tool: ${toolName}` });
            send("tool", { status: "calling", name: toolName, args });
            const result = await mcpToolsCall(mcp, toolName, args);
            send("tool", { status: "result", name: toolName, result });
            send("status", { text: `Tool complete: ${toolName}` });

            messages.push({
              role: "tool",
              tool_call_id: tc.id,
              content: JSON.stringify(result),
            });
          }
        }

        send("assistant", {
          text: "Stopped after too many tool turns. Try a smaller request.",
        });
        send("done", { ok: true });
      } catch (e: unknown) {
        send("error", { message: errorMessage(e) });
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}
