import { QueriesClientView } from "../views/queries-client";

export const dynamic = "force-dynamic";

export default async function QueriesPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <QueriesClientView namespace={namespace} />;
}

