import OpenAI from "openai";

export function getOpenAIClient(): OpenAI {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not set on the server");
  }
  return new OpenAI({ apiKey });
}

export function getCodexModel(): string {
  // User asked for a Codex model; keep it configurable since orgs vary.
  return process.env.OPENAI_MODEL || "gpt-5-codex";
}

