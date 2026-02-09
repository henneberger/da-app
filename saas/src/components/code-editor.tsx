"use client";

import CodeEditor from "@uiw/react-textarea-code-editor";
import "@/lib/prism-client";

export function HighlightEditor({
  value,
  onChange,
  language,
  placeholder,
  readOnly,
  minHeight,
  onCursor,
  className,
  textareaRef,
  cursorOnKeyUp = true,
}: {
  value: string;
  onChange?: (next: string) => void;
  language: string;
  placeholder?: string;
  readOnly?: boolean;
  minHeight?: number;
  onCursor?: (info: { offset: number; line: number; column: number }) => void;
  className?: string;
  textareaRef?: React.Ref<HTMLTextAreaElement>;
  cursorOnKeyUp?: boolean;
}) {
  return (
    <CodeEditor
      ref={textareaRef}
      value={value}
      language={language}
      placeholder={placeholder}
      readOnly={readOnly}
      onChange={(e) => onChange?.(e.target.value)}
      onClick={(e) => {
        const el = e.target as HTMLTextAreaElement;
        const offset = el.selectionStart ?? 0;
        const before = el.value.slice(0, offset);
        const lines = before.split("\n");
        const line = lines.length;
        const column = lines[lines.length - 1]?.length ?? 0;
        onCursor?.({ offset, line, column });
      }}
      onKeyUp={(e) => {
        if (!cursorOnKeyUp) return;
        const el = e.target as HTMLTextAreaElement;
        const offset = el.selectionStart ?? 0;
        const before = el.value.slice(0, offset);
        const lines = before.split("\n");
        const line = lines.length;
        const column = lines[lines.length - 1]?.length ?? 0;
        onCursor?.({ offset, line, column });
      }}
      padding={12}
      style={{
        fontSize: 12,
        fontFamily:
          "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
        minHeight: minHeight ? `${minHeight}px` : undefined,
        background: "transparent",
      }}
      className={["rounded-md border bg-muted/30", className].filter(Boolean).join(" ")}
    />
  );
}
