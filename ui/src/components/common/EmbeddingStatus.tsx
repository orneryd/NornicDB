/**
 * EmbeddingStatus - Displays embedding status nicely
 * Extracted from Browser.tsx for reusability
 */

interface EmbeddingStatusProps {
  embedding: unknown;
}

export function EmbeddingStatus({ embedding }: EmbeddingStatusProps) {
  if (!embedding || typeof embedding !== "object") {
    return <span className="text-norse-silver">No embedding data</span>;
  }

  const emb = embedding as Record<string, unknown>;
  
  // Support both old format (status-based) and new format (has_embedding-based)
  const hasEmbedding = emb.has_embedding === true;
  const dimensions = (emb.embedding_dimensions as number) || (emb.dimensions as number) || 0;
  const model = (emb.embedding_model as string) || (emb.model as string) || undefined;
  
  // Old format: status-based
  const status = (emb.status as string) || (hasEmbedding ? "ready" : "unknown");
  const isReady = status === "ready" || hasEmbedding;
  const isPending = status === "pending";

  return (
    <div className="bg-norse-stone rounded-lg p-3">
      <div className="flex items-center gap-3">
        {/* Status indicator */}
        <div
          className={`flex items-center gap-2 px-3 py-1.5 rounded-full ${
            isReady
              ? "bg-nornic-primary/20"
              : isPending
              ? "bg-valhalla-gold/20"
              : "bg-red-500/20"
          }`}
        >
          <div
            className={`w-2 h-2 rounded-full ${
              isReady
                ? "bg-nornic-primary animate-pulse"
                : isPending
                ? "bg-valhalla-gold animate-pulse"
                : "bg-red-400"
            }`}
          />
          <span
            className={`text-sm font-medium ${
              isReady
                ? "text-nornic-primary"
                : isPending
                ? "text-valhalla-gold"
                : "text-red-400"
            }`}
          >
            {isReady ? "Ready" : isPending ? "Generating..." : status}
          </span>
        </div>

        {/* Dimensions */}
        {dimensions > 0 && (
          <div className="flex items-center gap-1">
            <span className="text-xs text-norse-silver">Dimensions:</span>
            <span className="text-sm text-frost-ice font-mono">
              {dimensions}
            </span>
          </div>
        )}

        {/* Model */}
        {model && (
          <div className="flex items-center gap-1">
            <span className="text-xs text-norse-silver">Model:</span>
            <span className="text-sm text-nornic-accent">{model}</span>
          </div>
        )}
      </div>

      {isPending && (
        <p className="text-xs text-norse-fog mt-2">
          Embedding will be generated automatically by the background queue.
        </p>
      )}
    </div>
  );
}

