/**
 * JsonPreview - Displays JSON data in a formatted way
 * Extracted from Browser.tsx for reusability
 */

interface JsonPreviewProps {
  data: unknown;
  expanded?: boolean;
}

export function JsonPreview({ data, expanded = false }: JsonPreviewProps) {
  if (data === null) return <span className="json-null">null</span>;
  if (typeof data === "string") {
    const displayValue = expanded
      ? data
      : data.slice(0, 100) + (data.length > 100 ? "..." : "");
    return (
      <span className="json-string whitespace-pre-wrap break-words overflow-wrap-anywhere">"{displayValue}"</span>
    );
  }
  if (typeof data === "number")
    return <span className="json-number">{data}</span>;
  if (typeof data === "boolean")
    return <span className="json-boolean">{String(data)}</span>;
  if (Array.isArray(data)) {
    if (expanded) {
      return (
        <pre className="text-xs text-norse-silver bg-norse-shadow/50 rounded p-2 overflow-x-auto max-h-48 overflow-y-auto break-words whitespace-pre-wrap">
          {JSON.stringify(data, null, 2)}
        </pre>
      );
    }
    return <span className="text-norse-silver">[{data.length} items]</span>;
  }
  if (typeof data === "object") {
    if (expanded) {
      return (
        <pre className="text-xs text-norse-silver bg-norse-shadow/50 rounded p-2 overflow-x-auto max-h-48 overflow-y-auto break-words whitespace-pre-wrap">
          {JSON.stringify(data, null, 2)}
        </pre>
      );
    }
    const keys = Object.keys(data);
    return (
      <span className="text-norse-silver">
        {"{"}...{keys.length} props{"}"}
      </span>
    );
  }
  return <span>{String(data)}</span>;
}

