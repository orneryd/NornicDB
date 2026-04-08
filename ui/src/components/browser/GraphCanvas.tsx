import type { GraphExplorerViewModel } from "../../graph/viewModel";
import type { GraphNodeModel } from "../../graph/types";
import { getNodePreview } from "../../utils/nodeUtils";
import { buildGraphExplorerLayout, type GraphLayoutMode } from "./graphExplorerLayout";

interface GraphCanvasProps {
  viewModel: GraphExplorerViewModel;
  focusNodeIds: string[];
  selectedNodeId?: string | null;
  layoutMode?: GraphLayoutMode;
  onNodeSelect: (node: GraphNodeModel) => void;
}

function getDiffStatusSymbol(status: GraphNodeModel["status"]): string {
  switch (status) {
    case "added": return "+";
    case "removed": return "−";
    case "changed": return "~";
    default: return "";
  }
}

function getNodeStatusColors(status?: GraphNodeModel["status"]) {
  switch (status) {
    case "added":
      return {
        fill: "rgba(16, 185, 129, 0.18)",
        stroke: "rgba(52, 211, 153, 0.9)",
        text: "#d1fae5",
      };
    case "removed":
      return {
        fill: "rgba(239, 68, 68, 0.16)",
        stroke: "rgba(248, 113, 113, 0.9)",
        text: "#fee2e2",
      };
    case "changed":
      return {
        fill: "rgba(245, 158, 11, 0.18)",
        stroke: "rgba(251, 191, 36, 0.95)",
        text: "#fef3c7",
      };
    default:
      return {
        fill: "rgba(20, 24, 36, 0.9)",
        stroke: "rgba(74, 158, 255, 0.8)",
        text: "#f3f4f6",
      };
  }
}

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, maxLength - 1)}…`;
}

export function GraphCanvas({
  viewModel,
  focusNodeIds,
  selectedNodeId,
  layoutMode = "radial",
  onNodeSelect,
}: GraphCanvasProps) {
  const layout = buildGraphExplorerLayout(viewModel, focusNodeIds, layoutMode);

  return (
    <section
      aria-labelledby="graph-explorer-visual-heading"
      className="rounded-2xl border border-norse-rune bg-norse-shadow/30 overflow-hidden"
    >
      <div className="border-b border-norse-rune px-4 py-3">
        <div id="graph-explorer-visual-heading" className="text-sm font-medium text-white">
          Visual graph
        </div>
        <p className="mt-1 text-xs text-norse-silver">
          Click any node to inspect it in Node Details. Focused seeds stay highlighted in the layout.
        </p>
      </div>

      <div className="p-3">
        <svg
          viewBox={`0 0 ${layout.width} ${layout.height}`}
          className="h-[32rem] w-full rounded-xl bg-[radial-gradient(circle_at_top,rgba(74,158,255,0.18),transparent_38%),linear-gradient(180deg,rgba(10,14,26,0.92),rgba(20,24,36,0.95))]"
          role="img"
          aria-label={`Graph with ${viewModel.renderedNodeCount} nodes and ${viewModel.renderedEdgeCount} edges`}
        >
          <defs>
            <marker
              id="graph-edge-arrow"
              markerWidth="10"
              markerHeight="10"
              refX="7"
              refY="3"
              orient="auto"
            >
              <path d="M0,0 L0,6 L8,3 z" fill="rgba(156, 163, 175, 0.85)" />
            </marker>
          </defs>

          <rect
            x="12"
            y="12"
            width={layout.width - 24}
            height={layout.height - 24}
            rx="20"
            fill="rgba(20, 24, 36, 0.35)"
            stroke="rgba(42, 50, 71, 0.9)"
            strokeDasharray="8 10"
          />

          {viewModel.renderedEdges.map((edge) => {
            const source = layout.nodes[edge.source];
            const target = layout.nodes[edge.target];

            if (!source || !target) {
              return null;
            }

            const midX = (source.x + target.x) / 2;
            const midY = (source.y + target.y) / 2;
            const semanticOpacity = edge.semantic ? 0.85 : 0.6;

            return (
              <g key={edge.id}>
                <line
                  x1={source.x}
                  y1={source.y}
                  x2={target.x}
                  y2={target.y}
                  stroke={edge.status === "removed" ? "rgba(248, 113, 113, 0.9)" : "rgba(148, 163, 184, 0.72)"}
                  strokeWidth={edge.semantic ? 3 : 2}
                  strokeDasharray={edge.status ? "6 6" : undefined}
                  opacity={semanticOpacity}
                  markerEnd="url(#graph-edge-arrow)"
                />
                <rect
                  x={midX - 36}
                  y={midY - 11}
                  width="72"
                  height="22"
                  rx="11"
                  fill="rgba(10, 14, 26, 0.88)"
                  stroke="rgba(42, 50, 71, 0.95)"
                />
                <text
                  x={midX}
                  y={midY + 4}
                  textAnchor="middle"
                  fill="#9ca3af"
                  fontSize="10"
                  letterSpacing="0.18em"
                >
                  {truncate(edge.type, 10).toUpperCase()}
                </text>
              </g>
            );
          })}

          {viewModel.renderedNodes.map((node) => {
            const position = layout.nodes[node.id];
            if (!position) {
              return null;
            }

            const colors = getNodeStatusColors(node.status);
            const isFocused = focusNodeIds.includes(node.id);
            const isSelected = selectedNodeId === node.id;
            const radius = isFocused ? 28 : 24;
            const label = truncate(node.labels[0] ?? "Node", 16);
            const preview = truncate(getNodePreview(node.properties), 28);

            return (
              <g
                key={node.id}
                transform={`translate(${position.x}, ${position.y})`}
                role="button"
                tabIndex={0}
                onClick={() => onNodeSelect(node)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onNodeSelect(node);
                  }
                }}
                aria-label={`Inspect node ${node.id}${node.status ? ` (${node.status})` : ""}`}
                className="cursor-pointer outline-none"
              >
                {isSelected && <circle r={radius + 13} fill="rgba(74, 158, 255, 0.14)" />}
                {isFocused && (
                  <circle
                    r={radius + 7}
                    fill="none"
                    stroke="rgba(16, 185, 129, 0.65)"
                    strokeWidth="2"
                    strokeDasharray="5 5"
                  />
                )}
                <circle
                  r={radius}
                  fill={colors.fill}
                  stroke={isSelected ? "#4a9eff" : colors.stroke}
                  strokeWidth={isSelected ? 3 : 2}
                />
                {node.status && (
                  <text
                    y={-radius + 12}
                    textAnchor="middle"
                    fill={colors.text}
                    fontSize="11"
                    fontWeight="800"
                    pointerEvents="none"
                    aria-hidden="true"
                  >
                    {getDiffStatusSymbol(node.status)}
                  </text>
                )}
                <text
                  y={node.status ? 6 : 3}
                  textAnchor="middle"
                  fill="#f3f4f6"
                  fontSize="12"
                  fontWeight="600"
                  pointerEvents="none"
                >
                  {truncate(node.id, 12)}
                </text>
                <text
                  y={radius + 18}
                  textAnchor="middle"
                  fill="#9eeaf9"
                  fontSize="11"
                  fontWeight="500"
                  pointerEvents="none"
                >
                  {label}
                </text>
                <text
                  y={radius + 34}
                  textAnchor="middle"
                  fill="#9ca3af"
                  fontSize="10"
                  pointerEvents="none"
                >
                  {preview}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
    </section>
  );
}
