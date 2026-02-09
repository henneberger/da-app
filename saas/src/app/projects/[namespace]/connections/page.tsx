import { ConnectionsClientView } from "../views/connections-client";

export const dynamic = "force-dynamic";

export default async function ConnectionsPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <ConnectionsClientView namespace={namespace} />;
}

