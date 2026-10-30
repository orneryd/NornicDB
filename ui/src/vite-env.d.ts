/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_BASE_PATH: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

declare module "d3-force-3d" {
  interface ForceNode {
    x?: number;
    y?: number;
    z?: number;
    vx?: number;
    vy?: number;
    vz?: number;
  }
  interface PositionForce<Node extends ForceNode> {
    (alpha: number): void;
    initialize(nodes: Node[], random?: () => number, dimensions?: number): void;
    strength(value: number | ((node: Node) => number)): PositionForce<Node>;
  }
  interface CollisionForce<Node extends ForceNode> {
    (alpha: number): void;
    initialize(nodes: Node[], random?: () => number, dimensions?: number): void;
    strength(value: number): CollisionForce<Node>;
    iterations(value: number): CollisionForce<Node>;
  }
  export function forceX<Node extends ForceNode>(target: (node: Node) => number): PositionForce<Node>;
  export function forceY<Node extends ForceNode>(target: (node: Node) => number): PositionForce<Node>;
  export function forceZ<Node extends ForceNode>(target: (node: Node) => number): PositionForce<Node>;
  export function forceCollide<Node extends ForceNode>(radius: (node: Node) => number): CollisionForce<Node>;
  interface LinkForce<Node extends ForceNode> {
    (alpha: number): void;
    initialize(nodes: Node[], random?: () => number, dimensions?: number): void;
    id(accessor: (node: Node) => string): LinkForce<Node>;
    distance(value: number): LinkForce<Node>;
    strength(value: number): LinkForce<Node>;
  }
  export function forceLink<Node extends ForceNode>(links: Array<{
    source: string | Node;
    target: string | Node;
  }>): LinkForce<Node>;
}
