import type { BrowserGraphHandoff } from "../../utils/browserUrlState";

export type GraphExplorerRequestMode = "standard" | "temporal" | "diff";

function normalizeOptionalTimestamp(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

export function getGraphExplorerRequestMode(
  handoff: BrowserGraphHandoff,
): GraphExplorerRequestMode {
  switch (handoff.mode) {
    case "temporal":
      return "temporal";
    case "diff":
      return "diff";
    default:
      return "standard";
  }
}

export function supportsGraphExplorerTemporalFlow(
  handoff: BrowserGraphHandoff,
): boolean {
  return handoff.nodeIds.length > 0 && !handoff.sourceNodeId && !handoff.targetNodeId;
}

export function buildGraphExplorerTemporalHandoff(
  handoff: BrowserGraphHandoff,
  requestMode: GraphExplorerRequestMode,
  asOf: string,
  compareTo: string,
): BrowserGraphHandoff {
  const nextAsOf = normalizeOptionalTimestamp(asOf);
  const nextCompareTo = normalizeOptionalTimestamp(compareTo);

  switch (requestMode) {
    case "temporal":
      return {
        ...handoff,
        mode: "temporal",
        asOf: nextAsOf,
        compareTo: undefined,
      };
    case "diff":
      return {
        ...handoff,
        mode: "diff",
        asOf: nextAsOf,
        compareTo: nextCompareTo,
      };
    default:
      return {
        ...handoff,
        mode: "neighborhood",
        asOf: undefined,
        compareTo: undefined,
      };
  }
}
