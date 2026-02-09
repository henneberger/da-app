import { GraphQLClientView } from "../views/graphql-client";

export const dynamic = "force-dynamic";

export default async function GraphQLPage({
  params,
}: {
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return <GraphQLClientView namespace={namespace} />;
}

