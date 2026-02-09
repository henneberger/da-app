import { AppShell } from "@/components/app-shell";
import { ProjectsClient } from "./projects-client";

export const dynamic = "force-dynamic";

export default function ProjectsPage() {
  return (
    <AppShell title="projects">
      <ProjectsClient />
    </AppShell>
  );
}

