import { PostgresClientView } from "../views/postgres-client";

export const dynamic = "force-dynamic";

export default async function PostgresPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <PostgresClientView namespace={namespace} />;
}

