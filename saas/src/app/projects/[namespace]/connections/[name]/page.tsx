import { ConnectionDetailClient } from "../../views/connection-detail-client";

export const dynamic = "force-dynamic";

export default async function ConnectionDetailPage({
  params,
}: {
  params: Promise<{ namespace: string; name: string }>;
}) {
  const { namespace, name } = await params;
  return <ConnectionDetailClient namespace={namespace} name={name} />;
}

