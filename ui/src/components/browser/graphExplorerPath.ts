import type { BrowserGraphHandoff } from "../../utils/browserUrlState";

function normalizeNodeId(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function isDefined<T>(value: T | undefined): value is T {
  return value !== undefined;
}

export function buildGraphExplorerPathHandoff(
  handoff: BrowserGraphHandoff,
  sourceNodeId: string,
  targetNodeId: string,
): BrowserGraphHandoff {
  const normalizedSourceNodeId = normalizeNodeId(sourceNodeId);
  const normalizedTargetNodeId = normalizeNodeId(targetNodeId);

  return {
    ...handoff,
    mode: "path",
    nodeIds: Array.from(
      new Set([normalizedSourceNodeId, normalizedTargetNodeId].filter(isDefined)),
    ).sort((left, right) => left.localeCompare(right)),
    sourceNodeId: normalizedSourceNodeId,
    targetNodeId: normalizedTargetNodeId,
    asOf: undefined,
    compareTo: undefined,
  };
}

export function getGraphExplorerPathDraft(
  handoff: BrowserGraphHandoff,
  availableNodeIds: string[],
): { sourceNodeId: string; targetNodeId: string } {
  const normalizedAvailableNodeIds = Array.from(
    new Set(
      availableNodeIds
        .map((nodeId) => nodeId.trim())
        .filter(Boolean),
    ),
  ).sort((left, right) => left.localeCompare(right));

  const normalizedSourceNodeId = normalizeNodeId(handoff.sourceNodeId);
  const normalizedTargetNodeId = normalizeNodeId(handoff.targetNodeId);

  const defaultSourceNodeId =
    normalizedSourceNodeId && normalizedAvailableNodeIds.includes(normalizedSourceNodeId)
      ? normalizedSourceNodeId
      : normalizedAvailableNodeIds[0] ?? "";

  const defaultTargetNodeId =
    normalizedTargetNodeId && normalizedAvailableNodeIds.includes(normalizedTargetNodeId)
      ? normalizedTargetNodeId
      : normalizedAvailableNodeIds.find((nodeId) => nodeId !== defaultSourceNodeId) ??
        normalizedAvailableNodeIds[0] ??
        "";

  return {
    sourceNodeId: defaultSourceNodeId,
    targetNodeId: defaultTargetNodeId,
  };
}
