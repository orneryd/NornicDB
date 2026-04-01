import type { GraphExplorerRequestMode } from "./graphExplorerTemporal";

function normalizeInput(value: string): string {
  return value.trim();
}

export function isGraphPathRequestDisabled(sourceNodeId: string, targetNodeId: string): boolean {
  const normalizedSourceNodeId = normalizeInput(sourceNodeId);
  const normalizedTargetNodeId = normalizeInput(targetNodeId);

  return (
    normalizedSourceNodeId.length === 0 ||
    normalizedTargetNodeId.length === 0 ||
    normalizedSourceNodeId === normalizedTargetNodeId
  );
}

export function isGraphTemporalRequestDisabled(
  requestMode: GraphExplorerRequestMode,
  asOf: string,
): boolean {
  return (
    (requestMode === "temporal" || requestMode === "diff") &&
    normalizeInput(asOf).length === 0
  );
}
