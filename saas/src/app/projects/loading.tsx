export default function Loading() {
  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <div className="rounded-lg border bg-card px-5 py-4">
        <div className="text-sm font-semibold tracking-tight">Loading projects</div>
        <div className="mt-0.5 text-xs text-muted-foreground">
          Talking to Kubernetes...
        </div>
      </div>
    </div>
  );
}
