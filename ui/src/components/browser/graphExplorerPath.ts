import type { BrowserGraphHandoff } from "../../utils/browserUrlState";
import { normalizeGraphHandoff } from "../../utils/browserUrlState";

function normalizeNodeId(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeNodeIds(nodeIds: string[]): string[] {
  return Array.from(
    new Set(
      nodeIds
        .map((nodeId) => nodeId.trim())
        .filter(Boolean),
    ),
  );
}

function pickPreferredNodeId(candidates: string[], availableNodeIds: string[]): string | undefined {
  return normalizeNodeIds(candidates).find((nodeId) => availableNodeIds.includes(nodeId));
}

export function buildGraphExplorerPathHandoff(
  handoff: BrowserGraphHandoff,
  sourceNodeId: string,
  targetNodeId: string,
): BrowserGraphHandoff {
  return normalizeGraphHandoff({
    ...handoff,
    mode: "path",
    nodeIds: [sourceNodeId, targetNodeId],
    sourceNodeId: normalizeNodeId(sourceNodeId),
    targetNodeId: normalizeNodeId(targetNodeId),
  });
}

export function getGraphExplorerPathDraft(
  handoff: BrowserGraphHandoff,
  availableNodeIds: string[],
  preferredNodeIds: string[] = [],
): { sourceNodeId: string; targetNodeId: string; inferred: boolean } {
  const normalizedAvailableNodeIds = normalizeNodeIds(availableNodeIds);

  const normalizedSourceNodeId = normalizeNodeId(handoff.sourceNodeId);
  const normalizedTargetNodeId = normalizeNodeId(handoff.targetNodeId);
  const preferredSourceNodeId = pickPreferredNodeId(
    [normalizedSourceNodeId ?? "", ...preferredNodeIds, ...handoff.nodeIds],
    normalizedAvailableNodeIds,
  );

  const defaultSourceNodeId = preferredSourceNodeId ?? normalizedAvailableNodeIds[0] ?? "";

  const preferredTargetNodeId = pickPreferredNodeId(
    [
      normalizedTargetNodeId ?? "",
      ...preferredNodeIds.filter((nodeId) => nodeId !== defaultSourceNodeId),
      ...handoff.nodeIds.filter((nodeId) => nodeId !== defaultSourceNodeId),
    ],
    normalizedAvailableNodeIds.filter((nodeId) => nodeId !== defaultSourceNodeId),
  );

  const defaultTargetNodeId =
    preferredTargetNodeId ??
    normalizedAvailableNodeIds.find((nodeId) => nodeId !== defaultSourceNodeId) ??
    normalizedAvailableNodeIds[0] ??
    "";

  return {
    sourceNodeId: defaultSourceNodeId,
    targetNodeId: defaultTargetNodeId,
    inferred:
      defaultSourceNodeId !== (normalizedSourceNodeId ?? "") ||
      defaultTargetNodeId !== (normalizedTargetNodeId ?? ""),
  };
}
