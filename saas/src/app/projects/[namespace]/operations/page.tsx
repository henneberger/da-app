import { OperationsClientView } from "../views/operations-client";

export const dynamic = "force-dynamic";

export default async function OperationsPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <OperationsClientView namespace={namespace} />;
}

