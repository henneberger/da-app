import { McpClientView } from "../views/mcp-client";

export const dynamic = "force-dynamic";

export default async function McpPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <McpClientView namespace={namespace} />;
}

