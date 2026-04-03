import type { GraphExplorerViewModel } from "../../graph/viewModel";

export interface GraphPoint {
  x: number;
  y: number;
}

export interface GraphLayoutNode {
  id: string;
  x: number;
  y: number;
}

export interface GraphLayoutResult {
  width: number;
  height: number;
  nodes: Record<string, GraphLayoutNode>;
}

const LAYOUT_WIDTH = 920;
const LAYOUT_HEIGHT = 520;
const NODE_MARGIN = 56;

function getNodeDegrees(viewModel: GraphExplorerViewModel): Map<string, number> {
  const degrees = new Map<string, number>();

  viewModel.renderedNodes.forEach((node) => {
    degrees.set(node.id, 0);
  });

  viewModel.renderedEdges.forEach((edge) => {
    degrees.set(edge.source, (degrees.get(edge.source) ?? 0) + 1);
    degrees.set(edge.target, (degrees.get(edge.target) ?? 0) + 1);
  });

  return degrees;
}

function placeRing(
  ids: string[],
  radius: number,
  center: GraphPoint,
  width: number,
  height: number,
  nodes: Record<string, GraphLayoutNode>,
) {
  if (ids.length === 0) {
    return;
  }

  if (ids.length === 1 && radius === 0) {
    nodes[ids[0]] = {
      id: ids[0],
      x: center.x,
      y: center.y,
    };
    return;
  }

  ids.forEach((id, index) => {
    const angle = (-Math.PI / 2) + (Math.PI * 2 * index) / ids.length;
    const x = center.x + Math.cos(angle) * radius;
    const y = center.y + Math.sin(angle) * radius;

    nodes[id] = {
      id,
      x: Math.min(width - NODE_MARGIN, Math.max(NODE_MARGIN, x)),
      y: Math.min(height - NODE_MARGIN, Math.max(NODE_MARGIN, y)),
    };
  });
}

export function buildGraphExplorerLayout(
  viewModel: GraphExplorerViewModel,
  focusNodeIds: string[] = [],
): GraphLayoutResult {
  const width = LAYOUT_WIDTH;
  const height = LAYOUT_HEIGHT;
  const center = { x: width / 2, y: height / 2 };
  const degrees = getNodeDegrees(viewModel);
  const focusSet = new Set(focusNodeIds);

  const orderedNodes = [...viewModel.renderedNodes].sort((left, right) => {
    const leftFocused = focusSet.has(left.id) ? 1 : 0;
    const rightFocused = focusSet.has(right.id) ? 1 : 0;

    if (leftFocused !== rightFocused) {
      return rightFocused - leftFocused;
    }

    const degreeDiff = (degrees.get(right.id) ?? 0) - (degrees.get(left.id) ?? 0);
    if (degreeDiff !== 0) {
      return degreeDiff;
    }

    return left.id.localeCompare(right.id);
  });

  const orderedIds = orderedNodes.map((node) => node.id);
  const focusedIds = orderedIds.filter((id) => focusSet.has(id));
  const remainingIds = orderedIds.filter((id) => !focusSet.has(id));
  const nodes: Record<string, GraphLayoutNode> = {};

  if (orderedIds.length === 1) {
    placeRing(orderedIds, 0, center, width, height, nodes);
    return { width, height, nodes };
  }

  if (focusedIds.length > 0) {
    const innerRadius = focusedIds.length === 1 ? 0 : Math.min(110, 55 + focusedIds.length * 18);
    placeRing(focusedIds, innerRadius, center, width, height, nodes);
  }

  if (remainingIds.length > 0) {
    const ringBase = Math.min(width, height) / 2 - NODE_MARGIN - 18;
    const outerRadius = focusedIds.length > 0 ? ringBase : Math.min(170, ringBase);
    placeRing(remainingIds, outerRadius, center, width, height, nodes);
  }

  return {
    width,
    height,
    nodes,
  };
}
