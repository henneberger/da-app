"use client";

import { Loader2Icon } from "lucide-react";

export function LoadingInline({
  show,
  text,
  className,
}: {
  show: boolean;
  text?: string;
  className?: string;
}) {
  if (!show) return null;
  return (
    <div
      className={["flex items-center gap-2 text-xs text-muted-foreground", className]
        .filter(Boolean)
        .join(" ")}
      role="status"
      aria-live="polite"
      aria-busy="true"
    >
      <Loader2Icon className="size-3 animate-spin" />
      <span>{text ?? "Loading..."}</span>
    </div>
  );
}

