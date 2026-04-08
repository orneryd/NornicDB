import type { GraphExplorerViewModel } from "../../graph/viewModel";

export type GraphLayoutMode = "radial" | "grid" | "hierarchy";

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

function buildRadialLayout(
  viewModel: GraphExplorerViewModel,
  focusNodeIds: string[],
  width: number,
  height: number,
): Record<string, GraphLayoutNode> {
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
    return nodes;
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

  return nodes;
}

function buildGridLayout(
  viewModel: GraphExplorerViewModel,
  width: number,
  height: number,
): Record<string, GraphLayoutNode> {
  const nodes: Record<string, GraphLayoutNode> = {};
  const degrees = getNodeDegrees(viewModel);

  const orderedNodes = [...viewModel.renderedNodes].sort((left, right) => {
    const degreeDiff = (degrees.get(right.id) ?? 0) - (degrees.get(left.id) ?? 0);
    return degreeDiff !== 0 ? degreeDiff : left.id.localeCompare(right.id);
  });

  const count = orderedNodes.length;
  if (count === 0) return nodes;

  const cols = Math.max(1, Math.ceil(Math.sqrt(count)));
  const rows = Math.ceil(count / cols);
  const colStep = cols > 1 ? (width - 2 * NODE_MARGIN) / (cols - 1) : 0;
  const rowStep = rows > 1 ? (height - 2 * NODE_MARGIN) / (rows - 1) : 0;

  orderedNodes.forEach((node, index) => {
    const col = index % cols;
    const row = Math.floor(index / cols);
    const x = cols === 1 ? width / 2 : NODE_MARGIN + col * colStep;
    const y = rows === 1 ? height / 2 : NODE_MARGIN + row * rowStep;
    nodes[node.id] = { id: node.id, x, y };
  });

  return nodes;
}

function buildHierarchyLayout(
  viewModel: GraphExplorerViewModel,
  width: number,
  height: number,
): Record<string, GraphLayoutNode> {
  const nodes: Record<string, GraphLayoutNode> = {};
  const degrees = getNodeDegrees(viewModel);

  // Group node ids by degree, sort each group alphabetically for determinism
  const degreeGroups = new Map<number, string[]>();
  viewModel.renderedNodes.forEach((node) => {
    const degree = degrees.get(node.id) ?? 0;
    const group = degreeGroups.get(degree) ?? [];
    group.push(node.id);
    degreeGroups.set(degree, group);
  });

  // Sort groups descending by degree (highest-degree nodes at top)
  const sortedGroups = [...degreeGroups.entries()]
    .sort(([a], [b]) => b - a)
    .map(([, ids]) => ids.sort((a, b) => a.localeCompare(b)));

  const rowCount = sortedGroups.length;
  const rowStep = rowCount > 1 ? (height - 2 * NODE_MARGIN) / (rowCount - 1) : 0;

  sortedGroups.forEach((group, rowIndex) => {
    const y = rowCount === 1 ? height / 2 : NODE_MARGIN + rowIndex * rowStep;
    const colStep = group.length > 1 ? (width - 2 * NODE_MARGIN) / (group.length - 1) : 0;
    group.forEach((id, colIndex) => {
      const x = group.length === 1 ? width / 2 : NODE_MARGIN + colIndex * colStep;
      nodes[id] = { id, x, y };
    });
  });

  return nodes;
}

export function buildGraphExplorerLayout(
  viewModel: GraphExplorerViewModel,
  focusNodeIds: string[] = [],
  mode: GraphLayoutMode = "radial",
): GraphLayoutResult {
  const width = LAYOUT_WIDTH;
  const height = LAYOUT_HEIGHT;

  let nodePositions: Record<string, GraphLayoutNode>;

  switch (mode) {
    case "grid":
      nodePositions = buildGridLayout(viewModel, width, height);
      break;
    case "hierarchy":
      nodePositions = buildHierarchyLayout(viewModel, width, height);
      break;
    default:
      nodePositions = buildRadialLayout(viewModel, focusNodeIds, width, height);
  }

  return { width, height, nodes: nodePositions };
}
