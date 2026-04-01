import { BASE_PATH, joinBasePath } from "../utils/basePath";
import type {
  GraphDiffBody,
  GraphDiffRequest,
  GraphExpandBody,
  GraphExpandRequest,
  GraphNeighborhoodBody,
  GraphNeighborhoodRequest,
  GraphPathBody,
  GraphPathRequest,
  GraphRequestContext,
  GraphRequestDescriptor,
  GraphTemporalBody,
  GraphTemporalRequest,
} from "./types";

const GRAPH_PREFIX = "/nornicdb/graph";

function normalizeDatabaseName(database: string): string {
  const trimmed = database.trim();
  if (!trimmed) {
    throw new Error("Database name is required for graph requests");
  }
  return trimmed;
}

function assertGraphRequestContext<TBody extends GraphRequestContext>(body: TBody): TBody {
  normalizeDatabaseName(body.database);
  return body;
}

function toGraphBody<TBody extends GraphRequestContext, TPayload extends Omit<TBody, "database">>(
  body: TBody,
): TPayload {
  const { database: _database, ...payload } = assertGraphRequestContext(body);
  return payload as TPayload;
}

function jsonPost<TBody extends GraphRequestContext, TPayload extends Omit<TBody, "database">>(
  url: string,
  request: TBody,
): GraphRequestDescriptor<TPayload> {
  const body = toGraphBody<TBody, TPayload>(request);
  return {
    url,
    body,
    init: {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(body),
    },
  };
}

export function buildGraphRoute(database: string, suffix: string): string {
  const normalizedDatabase = normalizeDatabaseName(database);
  const normalizedSuffix = suffix.startsWith("/") ? suffix : `/${suffix}`;
  return joinBasePath(BASE_PATH, `${GRAPH_PREFIX}/${encodeURIComponent(normalizedDatabase)}${normalizedSuffix}`);
}

export function buildGraphNeighborhoodRequest(
  request: GraphNeighborhoodRequest,
): GraphRequestDescriptor<GraphNeighborhoodBody> {
  return jsonPost(buildGraphRoute(request.database, "/neighborhood"), request);
}

export function buildGraphExpandRequest(
  request: GraphExpandRequest,
): GraphRequestDescriptor<GraphExpandBody> {
  return jsonPost(buildGraphRoute(request.database, "/expand"), request);
}

export function buildGraphPathRequest(
  request: GraphPathRequest,
): GraphRequestDescriptor<GraphPathBody> {
  return jsonPost(buildGraphRoute(request.database, "/path"), request);
}

export function buildGraphTemporalRequest(
  request: GraphTemporalRequest,
): GraphRequestDescriptor<GraphTemporalBody> {
  return jsonPost(buildGraphRoute(request.database, "/temporal"), request);
}

export function buildGraphDiffRequest(
  request: GraphDiffRequest,
): GraphRequestDescriptor<GraphDiffBody> {
  return jsonPost(buildGraphRoute(request.database, "/diff"), request);
}
