export type GraphNodeID = string;
export type GraphEdgeID = string;
export type GraphGeneratedFrom = "neighborhood" | "expand" | "path" | "temporal" | "diff";
export type GraphDiffStatus = "added" | "removed" | "changed";

export interface GraphNodeModel {
  id: GraphNodeID;
  labels: string[];
  properties: Record<string, unknown>;
  score?: number;
  status?: GraphDiffStatus;
}

export interface GraphEdgeModel {
  id: GraphEdgeID;
  type: string;
  source: GraphNodeID;
  target: GraphNodeID;
  properties?: Record<string, unknown>;
  semantic?: boolean;
  status?: GraphDiffStatus;
}

export interface GraphContractMetadata {
  database: string;
  generated_from: GraphGeneratedFrom;
  depth?: number;
  as_of?: string;
  compare_to?: string;
  node_count: number;
  edge_count: number;
  truncated: boolean;
  warnings?: string[];
}

export interface GraphContractResponse {
  nodes: GraphNodeModel[];
  edges: GraphEdgeModel[];
  meta: GraphContractMetadata;
}

export interface GraphRequestContext {
  database: string;
}

export interface GraphFilterRequest {
  node_ids: GraphNodeID[];
  existing_node_ids?: GraphNodeID[];
  existing_edge_ids?: GraphEdgeID[];
  depth?: number;
  limit?: number;
  labels?: string[];
  relationship_types?: string[];
}

export interface GraphNeighborhoodRequest extends GraphRequestContext, GraphFilterRequest {
  as_of?: string;
}

export interface GraphExpandRequest extends GraphRequestContext, GraphFilterRequest {
  as_of?: string;
}

export interface GraphPathRequest extends GraphRequestContext {
  source_node_id: GraphNodeID;
  target_node_id: GraphNodeID;
  limit?: number;
  labels?: string[];
  relationship_types?: string[];
  as_of?: string;
}

export interface GraphTemporalRequest extends GraphRequestContext {
  node_ids: GraphNodeID[];
  labels?: string[];
  relationship_types?: string[];
  as_of: string;
}

export interface GraphDiffRequest extends GraphRequestContext {
  node_ids: GraphNodeID[];
  labels?: string[];
  relationship_types?: string[];
  as_of: string;
  compare_to?: string;
}

export interface GraphRequestDescriptor<TBody> {
  url: string;
  init: RequestInit & { body: string };
  body: TBody;
}

export type GraphNeighborhoodBody = Omit<GraphNeighborhoodRequest, "database">;
export type GraphExpandBody = Omit<GraphExpandRequest, "database">;
export type GraphPathBody = Omit<GraphPathRequest, "database">;
export type GraphTemporalBody = Omit<GraphTemporalRequest, "database">;
export type GraphDiffBody = Omit<GraphDiffRequest, "database">;
