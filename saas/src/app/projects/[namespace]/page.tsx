import { OverviewClient } from "./views/overview-client";

export const dynamic = "force-dynamic";

export default async function ProjectOverviewPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <OverviewClient namespace={namespace} />;
}
