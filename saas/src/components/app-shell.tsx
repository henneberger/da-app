import Link from "next/link";

export function AppShell({
  title,
  children,
}: {
  title?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-dvh bg-[radial-gradient(1200px_700px_at_10%_-10%,hsl(var(--primary)/0.18),transparent_60%),radial-gradient(900px_600px_at_90%_0%,hsl(var(--ring)/0.16),transparent_55%),linear-gradient(to_bottom,hsl(var(--background)),hsl(var(--background)))]">
      <header className="sticky top-0 z-20 border-b bg-background/80 backdrop-blur">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
          <div className="flex items-baseline gap-3">
            <Link
              href="/projects"
              className="text-sm font-semibold tracking-tight"
              style={{ fontFamily: "var(--font-space-grotesk)" }}
            >
              DA App
            </Link>
            {title ? (
              <span className="text-xs text-muted-foreground">{title}</span>
            ) : null}
          </div>
          <nav className="flex items-center gap-3 text-sm text-muted-foreground">
            <Link href="/projects" className="hover:text-foreground">
              Projects
            </Link>
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-8">{children}</main>
    </div>
  );
}

