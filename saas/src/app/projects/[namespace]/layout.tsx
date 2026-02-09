import { AppShell } from "@/components/app-shell";
import { ProjectShell } from "./project-shell";

export const dynamic = "force-dynamic";

export default async function ProjectLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ namespace: string }>;
}) {
  const { namespace } = await params;
  return (
    <AppShell title={namespace}>
      <ProjectShell namespace={namespace}>{children}</ProjectShell>
    </AppShell>
  );
}

