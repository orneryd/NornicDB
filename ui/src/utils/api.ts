// NornicDB API Client

// neo4j-driver ships a `browser` field in its package.json that swaps
// the package's node-internal channel for the browser channel at bundle
// time (Rolldown under Vite 8 needs an explicit plugin — see
// vite.config.ts). The driver's URL parser still only accepts the
// canonical Bolt schemes (bolt / bolt+s / neo4j / neo4j+s); the
// browser channel is what turns those into WebSocket frames on the
// wire, so this single import is enough as long as the channel swap
// is wired in vite.config.ts.
import neo4j, {
  isInt,
  type AuthToken,
  type Driver,
  type Integer,
  type Session,
} from "neo4j-driver";
import { BASE_PATH, joinBasePath } from "./basePath";

export interface AuthConfig {
  devLoginEnabled: boolean;
  securityEnabled: boolean;
  oauthProviders: Array<{
    name: string;
    url: string;
    displayName: string;
  }>;
}

export interface DatabaseStats {
  status: string;
  server: {
    uptime_seconds: number;
    requests: number;
    errors: number;
    active: number;
    version: string;
    commit?: string;
    build_time?: string;
  };
  database: {
    nodes: number;
    edges: number;
  };
}

export interface SearchResult {
  node: {
    id: string;
    labels: string[];
    properties: Record<string, unknown>;
    created_at: string;
  };
  score: number;
  rrf_score?: number;
  vector_rank?: number;
  bm25_rank?: number;
}

export interface GraphNodePayload {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
  score?: number;
  status?: string;
}

export interface GraphEdgePayload {
  id: string;
  source: string;
  target: string;
  type: string;
  properties?: Record<string, unknown>;
  semantic?: boolean;
  status?: string;
}

export interface GraphMetaPayload {
  database: string;
  generated_from: string;
  depth?: number;
  as_of?: string;
  compare_to?: string;
  node_count: number;
  edge_count: number;
  truncated: boolean;
}

export interface GraphNeighborhoodResponse {
  nodes: GraphNodePayload[];
  edges: GraphEdgePayload[];
  meta: GraphMetaPayload;
}

export interface CypherResponse {
  results: Array<{
    columns: string[];
    data: Array<{
      row: unknown[];
      meta: unknown[];
    }>;
  }>;
  errors?: Array<{
    code: string;
    message: string;
  }>;
}

export interface ConstituentInfo {
  alias: string;
  databaseName: string;
  type: string; // "local" or "remote"
  accessMode: string; // "read", "write", "read_write"
  uri?: string; // only for remote constituents
}

export interface DatabaseInfo {
  name: string;
  status: string;
  default: boolean;
  type?: string; // "standard", "composite", "system"
  constituents?: ConstituentInfo[]; // only for composite databases
  nodeCount: number;
  edgeCount: number;
  nodeStorageBytes?: number;
  managedEmbeddingBytes?: number;
  searchReady?: boolean;
  searchBuilding?: boolean;
  searchInitialized?: boolean;
  searchStrategy?: string;
  searchPhase?: string;
  searchProcessed?: number;
  searchTotal?: number;
  searchRate?: number;
  searchEtaSeconds?: number;
}

export interface DatabaseRow {
  [key: string]: unknown;
  name: string;
  type?: string;
  access?: string;
  role?: string;
  status?: string;
  default?: boolean;
}

export interface MVCCLifecycleDebtKey {
  logical_key: string;
  namespace?: string;
  debt_bytes: number;
  tombstone_depth: number;
  floor_lag_versions: number;
  versions_to_delete: number;
}

export interface MVCCLifecycleNamespaceMetrics {
  compaction_debt_bytes: number;
  compaction_debt_keys: number;
  prunable_bytes_total: number;
  pruned_bytes_total: number;
}

export interface MVCCLifecycleRollup {
  prune_runs: number;
  keys_processed: number;
  versions_deleted: number;
  bytes_freed: number;
  fence_mismatches: number;
  compaction_debt_bytes_max: number;
  compaction_debt_keys_max: number;
}

export interface MVCCLifecycleLastRun {
  keys_processed: number;
  versions_deleted: number;
  bytes_freed: number;
  fence_mismatches: number;
  hot_contention_keys: number;
}

export interface MVCCSnapshotReaderInfo {
  ReaderID?: string;
  Namespace?: string;
  StartTime?: string;
  SnapshotVersion?: {
    CommitTimestamp?: string;
    CommitSequence?: number;
  };
}

export interface MVCCLifecycleStatus {
  database?: string;
  namespace?: string;
  enabled: boolean;
  running?: boolean;
  paused?: boolean;
  automatic?: boolean;
  cycle_interval?: string;
  pressure_band?: string;
  emergency_mode?: boolean;
  mvcc_active_snapshot_readers?: number;
  mvcc_oldest_reader_age_seconds?: number;
  mvcc_bytes_pinned_by_oldest_reader?: number;
  mvcc_compaction_debt_bytes?: number;
  mvcc_compaction_debt_keys?: number;
  mvcc_snapshot_graceful_expirations_total?: number;
  mvcc_snapshot_hard_expirations_total?: number;
  mvcc_prunable_bytes_total?: number;
  mvcc_pruned_bytes_total?: number;
  mvcc_tombstone_chain_max_depth?: number;
  mvcc_floor_lag_versions?: number;
  mvcc_prune_run_duration_seconds?: number;
  mvcc_prune_run_keys_scanned_total?: number;
  mvcc_prune_stale_plan_skips_total?: number;
  last_run?: MVCCLifecycleLastRun;
  per_namespace?: Record<string, MVCCLifecycleNamespaceMetrics>;
  top_debt_keys?: MVCCLifecycleDebtKey[];
  readers?: MVCCSnapshotReaderInfo[];
  rollups?: Record<string, MVCCLifecycleRollup>;
}

export interface MVCCLifecycleDebtResponse {
  database: string;
  limit?: number;
  keys: MVCCLifecycleDebtKey[];
}

export type RetentionCategory =
  | "SYSTEM"
  | "AUDIT"
  | "USER"
  | "ANALYTICS"
  | "BACKUP"
  | "ARCHIVE"
  | "PHI"
  | "PII"
  | "FINANCIAL"
  | "LEGAL";

export interface RetentionPeriod {
  Duration: number;
  Indefinite: boolean;
}

export interface RetentionPolicy {
  id: string;
  name: string;
  category: RetentionCategory;
  retention_period: RetentionPeriod;
  archive_before_delete: boolean;
  archive_path?: string;
  compliance_frameworks?: string[];
  active: boolean;
  created_at?: string;
  updated_at?: string;
  description?: string;
}

export interface RetentionLegalHold {
  id: string;
  description: string;
  matter?: string;
  placed_by: string;
  placed_at?: string;
  expires_at?: string;
  subject_ids?: string[];
  categories?: RetentionCategory[];
  active: boolean;
}

export interface RetentionErasureRequest {
  id: string;
  subject_id: string;
  subject_email?: string;
  requested_at?: string;
  deadline?: string;
  status: "PENDING" | "IN_PROGRESS" | "COMPLETED" | "FAILED" | "PARTIAL";
  items_found: number;
  items_erased: number;
  items_retained: number;
  retained_reason?: string;
  started_at?: string;
  completed_at?: string;
  error?: string;
  subject_notified: boolean;
}

export interface RetentionStatus {
  enabled: boolean;
  policy_count: number;
  hold_count: number;
  erasure_count: number;
  timestamp?: string;
}

export interface DecayProfileBundle {
  Name: string;
  HalfLifeSeconds: number;
  VisibilityThreshold: number;
  ScoreFloor: number;
  Function: string;
  Scope: string;
  DecayEnabled: boolean;
  ScoreFrom: string;
  ScoreFromProperty?: string;
  Enabled: boolean;
}

export interface DecayProfileBinding {
  Name: string;
  TargetLabels?: string[];
  TargetEdgeType?: string;
  IsWildcard: boolean;
  IsEdge: boolean;
  ProfileRef?: string;
  NoDecay?: boolean;
  VisibilityThreshold?: number;
  Order: number;
}

export interface PromotionProfileDef {
  Name: string;
  Scope: string;
  Multiplier: number;
  ScoreFloor: number;
  ScoreCap: number;
  Enabled: boolean;
}

export interface PromotionPolicyDef {
  Name: string;
  TargetLabels?: string[];
  TargetEdgeType?: string;
  IsWildcard: boolean;
  IsEdge: boolean;
  Enabled: boolean;
}

export interface ScoringResolution {
  TargetID: string;
  TargetScope: string;
  ResolvedDecayProfileID: string;
  ResolvedScoreFrom: string;
  ResolutionSourceChain: string[];
  AppliedDecayProfileNames: string[];
  AppliedPromotionPolicyName: string;
  AppliedPromotionProfileName: string;
  EffectiveRate: number;
  EffectiveThreshold: number;
  EffectiveMultiplier: number;
  BaseScore: number;
  FinalScore: number;
  NoDecay: boolean;
  SuppressionEligible: boolean;
  Explanation: string;
}

export interface KPProfilesResponse {
  bundles: DecayProfileBundle[];
  bindings: DecayProfileBinding[];
  decay_enabled: boolean;
}

export interface KPPoliciesResponse {
  promotion_profiles: PromotionProfileDef[];
  promotion_policies: PromotionPolicyDef[];
}

export interface DeindexWorkItem {
  workItemId: string;
  targetId: string;
  targetScope: string;
  enqueuedAt: number;
  status: string;
}

export interface KPDeindexStatusResponse {
  pending_count: number;
  items: DeindexWorkItem[];
  supported: boolean;
  message?: string;
}

interface DiscoveryResponse {
  bolt_direct: string;
  bolt_routing: string;
  transaction: string;
  neo4j_version: string;
  neo4j_edition: string;
  default_database?: string; // NornicDB extension
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

// neo4jValueToPlain unwraps Bolt-typed values into the same shape the
// HTTP /tx/commit path produced. The neo4j-driver-lite ships its own
// Integer / Node / Relationship / Path classes; the UI's existing
// consumers (parseCypherRows, QueryResultsTable) expect plain JS
// numbers, plain objects, etc. This walks the value tree and substitutes.
function neo4jValueToPlain(v: unknown): unknown {
  if (v === null || v === undefined) {
    return v;
  }
  // neo4j Integer: detected via the driver's isInt() typeguard. Values
  // within Number.MIN_SAFE_INTEGER..MAX_SAFE_INTEGER come back as JS
  // Number; larger ones round-trip as String to avoid precision loss
  // (matching the HTTP /tx/commit path's JSON serialization).
  if (isInt(v)) {
    const intValue = v as Integer;
    if (intValue.inSafeRange()) {
      return intValue.toNumber();
    }
    return intValue.toString();
  }
  // Node / Relationship / PathSegment: serialize properties + identity.
  // Surface elementId at the top level so downstream consumers
  // (extractNodeFromResult, the Browser select-button column) can find
  // it without diving into driver-specific fields. Same shape the HTTP
  // /tx/commit path produced.
  if (
    typeof v === "object" &&
    v !== null &&
    "properties" in (v as Record<string, unknown>) &&
    "identity" in (v as Record<string, unknown>)
  ) {
    const node = v as {
      identity: unknown;
      elementId?: string;
      labels?: string[];
      type?: string;
      properties: Record<string, unknown>;
      start?: unknown;
      end?: unknown;
      startNodeElementId?: string;
      endNodeElementId?: string;
    };
    const identity = neo4jValueToPlain(node.identity);
    const out: Record<string, unknown> = {
      identity,
      properties: neo4jValueToPlain(node.properties),
    };
    // elementId is the canonical node id on driver v5+; fall back to
    // identity (stringified) when the driver doesn't supply one.
    if (typeof node.elementId === "string" && node.elementId !== "") {
      out.elementId = node.elementId;
    } else if (identity !== null && identity !== undefined) {
      out.elementId = String(identity);
    }
    if (Array.isArray(node.labels)) {
      out.labels = node.labels;
    }
    if (typeof node.type === "string") {
      out.type = node.type;
    }
    if (node.start !== undefined) {
      out.start = neo4jValueToPlain(node.start);
    }
    if (node.end !== undefined) {
      out.end = neo4jValueToPlain(node.end);
    }
    if (typeof node.startNodeElementId === "string") {
      out.startNodeElementId = node.startNodeElementId;
    }
    if (typeof node.endNodeElementId === "string") {
      out.endNodeElementId = node.endNodeElementId;
    }
    return out;
  }
  if (Array.isArray(v)) {
    return v.map(neo4jValueToPlain);
  }
  if (typeof v === "object") {
    const obj = v as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(obj)) {
      out[k] = neo4jValueToPlain(val);
    }
    return out;
  }
  return v;
}

function asOptionalString(value: unknown): string | undefined {
  const out = asString(value);
  return out ? out : undefined;
}

function asNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function asOptionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}

function asBoolean(value: unknown): boolean {
  return value === true;
}

function asStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const strings = value.filter(
    (item): item is string => typeof item === "string",
  );
  return strings.length > 0 ? strings : undefined;
}

function escapeCypherStringLiteral(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

class NornicDBClient {
  private defaultDatabase: string | null = null;
  // Bolt-over-WebSocket driver state. The driver is constructed lazily
  // on first executeCypher call (after discovery returns ws_direct).
  // We use auth.none() because the WS upgrade itself carries the
  // same-origin cookie (or an Authorization header from a third-party
  // tab); the server reads either and promotes scheme=none HELLO to
  // those claims. Browser drivers can't read HttpOnly cookies, so we
  // never need to surface the JWT in JS — the UA does it for us.
  private boltDriver: Driver | null = null;
  private boltDriverPromise: Promise<Driver> | null = null;

  // Pre-decoded discovery payload, cached for the same lifetime as
  // defaultDatabase. Used to pick ws_direct vs wss_direct.
  private discovery: DiscoveryResponse | null = null;

  private async parseErrorMessage(
    res: Response,
    fallback: string,
  ): Promise<string> {
    const raw = await res.text().catch(() => "");
    if (raw) {
      try {
        const payload = JSON.parse(raw) as { message?: string; error?: string };
        return payload?.message || payload?.error || raw || fallback;
      } catch {
        return raw;
      }
    }
    return fallback;
  }

  // runCypherOverBolt drives a single Cypher statement through the
  // Bolt-over-WS driver and reshapes the result into the same
  // CypherResponse format the UI's parseCypherRows / display layer
  // already consumes. This replaces the HTTP /tx/commit path.
  //
  // Auth: the WS upgrade carries the same-origin nornicdb_token cookie
  // (browsers attach automatically) or an Authorization: Bearer header
  // if a third-party caller set one; the server promotes the HELLO
  // scheme=none to those claims. See docs/user-guides/connecting-bolt.md.
  //
  // Timeouts: long-running queries are bounded server-side; the driver's
  // connection-acquisition timeout handles connect failures.
  private async runCypherOverBolt(
    dbName: string,
    statement: string,
    parameters?: Record<string, unknown>,
  ): Promise<CypherResponse> {
    const driver = await this.getBoltDriver();
    let session: Session | null = null;
    try {
      session = driver.session({ database: dbName });
      const result = await session.run(
        statement,
        (parameters ?? {}) as Record<string, unknown>,
      );
      // Driver: QueryResult is { records, summary }. keys live on each
      // Record; pull them from the first record (or empty for 0-row results).
      const columns: string[] =
        result.records.length > 0
          ? (result.records[0].keys as string[]).slice()
          : [];
      const data = result.records.map((rec) => {
        const row = columns.map((k) => neo4jValueToPlain(rec.get(k)));
        return { row, meta: [] };
      });
      return {
        results: [{ columns, data }],
      };
    } catch (err) {
      // Surface driver errors in the same shape the UI's
      // assertCypherSuccess / display layer expects.
      const message = err instanceof Error ? err.message : String(err);
      const code =
        err &&
        typeof err === "object" &&
        "code" in err &&
        typeof (err as { code?: unknown }).code === "string"
          ? (err as { code: string }).code
          : "Neo.ClientError.Statement.SyntaxError";
      return {
        results: [],
        errors: [{ code, message }],
      };
    } finally {
      if (session) {
        try {
          await session.close();
        } catch {
          // best-effort
        }
      }
    }
  }

  // Get default database name from discovery endpoint. As a side effect
  // this also caches the full DiscoveryResponse so getBoltDriver can
  // read bolt_direct (and pick its scheme based on the page protocol).
  private async getDefaultDatabase(): Promise<string> {
    if (this.defaultDatabase) {
      return this.defaultDatabase;
    }
    await this.fetchDiscovery();
    return this.defaultDatabase ?? "nornic";
  }

  private async fetchDiscovery(): Promise<DiscoveryResponse | null> {
    if (this.discovery) {
      return this.discovery;
    }
    try {
      const res = await fetch(joinBasePath(BASE_PATH, "/"), {
        credentials: "include",
      });
      if (res.ok) {
        const discovery: DiscoveryResponse = await res.json();
        this.discovery = discovery;
        this.defaultDatabase = discovery.default_database || "nornic";
        return discovery;
      }
    } catch {
      // Fall through to defaults
    }
    this.defaultDatabase = "nornic";
    return null;
  }

  // resolveBoltURL picks the URL the UI hands to neo4j.driver(). The
  // scheme is bolt:// (or bolt+s:// when the page is served over HTTPS
  // so browsers don't refuse a mixed-content upgrade), NOT bolt://.
  //
  // The full neo4j-driver package validates schemes upfront and only
  // accepts bolt / bolt+s / bolt+ssc / neo4j / neo4j+s / neo4j+ssc;
  // bolt:// / bolt+s:// fail with "Unknown scheme: ws". WebSocket transport
  // is selected automatically at runtime when the bundle resolves the
  // browser channel (vite.config.ts wires that), so passing bolt://
  // from the browser still produces a WS upgrade on the wire.
  //
  // Discovery's bolt_direct already carries the correct host:port for
  // this server (port-aware as of the BoltPort config wiring); fall
  // back to window.location.hostname:7687 when discovery is unreachable.
  private resolveBoltURL(): string {
    const usingTLS = window.location.protocol === "https:";
    const discoveryURL = this.discovery?.bolt_direct;
    if (discoveryURL) {
      // Upgrade plain bolt:// to bolt+s:// when the page itself uses
      // HTTPS so the browser permits the connection (mixed content).
      if (usingTLS && discoveryURL.startsWith("bolt://")) {
        return "bolt+s://" + discoveryURL.slice("bolt://".length);
      }
      return discoveryURL;
    }
    const scheme = usingTLS ? "bolt+s" : "bolt";
    return `${scheme}://${window.location.hostname}:7687`;
  }

  // getBoltDriver returns a process-wide singleton Bolt driver. The
  // driver is built lazily so the UI doesn't pay the cost on pages that
  // never run a Cypher query, and so we can defer construction until
  // after discovery has populated this.discovery.
  private async getBoltDriver(): Promise<Driver> {
    if (this.boltDriver) {
      return this.boltDriver;
    }
    if (this.boltDriverPromise) {
      return this.boltDriverPromise;
    }
    this.boltDriverPromise = (async () => {
      await this.fetchDiscovery();
      const url = this.resolveBoltURL();
      // Bolt scheme=none. The AuthToken type insists on a credentials
      // field; the wire-protocol scheme=none is just {scheme:"none"}
      // with no payload. Cast through. The actual auth credential
      // travels in the WS upgrade headers (cookie or Authorization);
      // the server reads either and promotes scheme=none HELLO to
      // those claims.
      const noneAuth = { scheme: "none" } as unknown as AuthToken;
      const driver = neo4j.driver(url, noneAuth, {
        userAgent: "nornicdb-ui/0.1",
      });
      this.boltDriver = driver;
      return driver;
    })();
    try {
      return await this.boltDriverPromise;
    } finally {
      this.boltDriverPromise = null;
    }
  }

  // closeBoltDriver tears down the cached driver. Called on logout so
  // a re-login can pick up a fresh cookie without leaking the old
  // session's connections.
  async closeBoltDriver(): Promise<void> {
    const driver = this.boltDriver;
    this.boltDriver = null;
    this.discovery = null;
    if (driver) {
      try {
        await driver.close();
      } catch {
        // best-effort: a network failure on close is not interesting
      }
    }
  }

  async getAuthConfig(): Promise<AuthConfig> {
    try {
      const res = await fetch(joinBasePath(BASE_PATH, "/auth/config"), {
        credentials: "include",
      });
      if (res.ok) {
        return await res.json();
      }
      // Default config if endpoint doesn't exist
      return {
        devLoginEnabled: true,
        securityEnabled: false,
        oauthProviders: [],
      };
    } catch {
      // Auth disabled by default
      return {
        devLoginEnabled: true,
        securityEnabled: false,
        oauthProviders: [],
      };
    }
  }

  async checkAuth(): Promise<{ authenticated: boolean; user?: string }> {
    try {
      const res = await fetch(joinBasePath(BASE_PATH, "/auth/me"), {
        credentials: "include",
      });
      if (res.ok) {
        const data = await res.json();
        return { authenticated: true, user: data.username };
      }
      return { authenticated: false };
    } catch {
      return { authenticated: false };
    }
  }

  async login(
    username: string,
    password: string,
  ): Promise<{ success: boolean; error?: string }> {
    try {
      const res = await fetch(joinBasePath(BASE_PATH, "/auth/token"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ username, password }),
      });

      if (res.ok) {
        return { success: true };
      }

      const data = await res.json().catch(() => ({ message: "Login failed" }));
      return { success: false, error: data.message || "Invalid credentials" };
    } catch {
      return { success: false, error: "Network error" };
    }
  }

  async logout(): Promise<void> {
    await fetch(joinBasePath(BASE_PATH, "/auth/logout"), {
      method: "POST",
      credentials: "include",
    });
    // Tear down the Bolt driver so the next login uses a fresh cookie.
    await this.closeBoltDriver();
  }

  async getHealth(): Promise<{ status: string; time: string }> {
    const res = await fetch(joinBasePath(BASE_PATH, "/health"));
    return await res.json();
  }

  async getStatus(): Promise<DatabaseStats> {
    const res = await fetch(joinBasePath(BASE_PATH, "/status"));
    return await res.json();
  }

  async search(
    query: string,
    limit: number = 10,
    labels?: string[],
    database?: string,
  ): Promise<SearchResult[]> {
    const body: {
      query: string;
      limit: number;
      labels?: string[];
      database?: string;
    } = {
      query,
      limit,
      labels,
    };
    if (database != null && database !== "") {
      body.database = database;
    }
    const res = await fetch(joinBasePath(BASE_PATH, "/nornicdb/search"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      if (res.status === 503) {
        throw new Error("Search is warming up. Please try again in a moment.");
      }
      const message = await this.parseErrorMessage(
        res,
        `Search failed (${res.status})`,
      );
      throw new Error(message);
    }
    return await res.json();
  }

  async findSimilar(
    nodeId: string,
    limit: number = 10,
    database?: string,
  ): Promise<SearchResult[]> {
    const body: { node_id: string; limit: number; database?: string } = {
      node_id: nodeId,
      limit,
    };
    if (database != null && database !== "") {
      body.database = database;
    }
    const res = await fetch(joinBasePath(BASE_PATH, "/nornicdb/similar"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(body),
    });
    return await res.json();
  }

  async executeCypher(
    statement: string,
    parameters?: Record<string, unknown>,
    database?: string,
  ): Promise<CypherResponse> {
    const dbName =
      database != null && database !== ""
        ? database
        : await this.getDefaultDatabase();
    return this.runCypherOverBolt(dbName, statement, parameters);
  }

  async getResolvedDatabaseName(database?: string): Promise<string> {
    return database != null && database !== ""
      ? database
      : await this.getDefaultDatabase();
  }

  async getGraphNeighborhood(options: {
    nodeIds: string[];
    depth?: number;
    limit?: number;
    labels?: string[];
    relationshipTypes?: string[];
    database?: string;
  }): Promise<GraphNeighborhoodResponse> {
    const dbName = await this.getResolvedDatabaseName(options.database);
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/nornicdb/graph/${encodeURIComponent(dbName)}/neighborhood`,
      ),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          node_ids: options.nodeIds,
          depth: options.depth,
          limit: options.limit,
          labels: options.labels,
          relationship_types: options.relationshipTypes,
        }),
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Graph neighborhood request failed (${res.status})`,
      );
      throw new Error(message);
    }
    return (await res.json()) as GraphNeighborhoodResponse;
  }

  async executeCypherOnDatabase(
    dbName: string,
    statement: string,
    parameters?: Record<string, unknown>,
  ): Promise<CypherResponse> {
    return this.runCypherOverBolt(dbName, statement, parameters);
  }

  async executeSystemCypher(
    statement: string,
    parameters?: Record<string, unknown>,
  ): Promise<CypherResponse> {
    return this.executeCypherOnDatabase("system", statement, parameters);
  }

  async getDatabaseInfo(name: string): Promise<DatabaseInfo> {
    const res = await fetch(
      joinBasePath(BASE_PATH, `/db/${encodeURIComponent(name)}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
      },
    );

    if (!res.ok) {
      const error = await res
        .json()
        .catch(() => ({ message: "Failed to get database info" }));
      throw new Error(error.message || "Failed to get database info");
    }
    return await res.json();
  }

  private parseCypherRows<T extends Record<string, unknown>>(
    resp: CypherResponse,
  ): T[] {
    const result = resp.results?.[0];
    const columns = result?.columns || [];
    const data = result?.data || [];
    return data.map((d) => {
      const row = d.row || [];
      const out: Record<string, unknown> = {};
      for (let i = 0; i < columns.length; i++) {
        out[columns[i]] = row[i];
      }
      return out as T;
    });
  }

  private assertCypherSuccess(resp: CypherResponse, fallback: string): void {
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(
        resp.errors.map((err) => err.message).join("; ") || fallback,
      );
    }
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const resp = await this.executeSystemCypher("SHOW DATABASES");
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(resp.errors.map((e) => e.message).join("; "));
    }

    const rows = this.parseCypherRows<DatabaseRow>(resp);
    const names = rows
      .map((r) => (typeof r.name === "string" ? r.name : ""))
      .filter((n) => n && n !== "system");

    const infos = await Promise.all(
      names.map(async (name) => {
        try {
          return await this.getDatabaseInfo(name);
        } catch {
          return null;
        }
      }),
    );

    return infos.filter((x): x is DatabaseInfo => Boolean(x));
  }

  /** Returns database names for the query dropdown (user-visible DBs, excludes system). */
  async listDatabaseNames(): Promise<string[]> {
    const list = await this.listDatabases();
    return list.map((d) => d.name);
  }

  private quoteCypherIdentifier(identifier: string): string {
    // Cypher uses backticks for identifier quoting; escape embedded backticks by doubling them.
    // Example: db name `a`b` => `a``b`
    return `\`${identifier.split("`").join("``")}\``;
  }

  private validateDatabaseName(name: string): string {
    const trimmed = name.trim();
    if (!trimmed) {
      throw new Error("Database name is required");
    }
    if (trimmed.includes(":")) {
      throw new Error("Database name cannot include ':'");
    }
    if (trimmed.startsWith("_")) {
      throw new Error("Database name cannot start with '_'");
    }
    return trimmed;
  }

  async createDatabase(name: string): Promise<void> {
    const dbName = this.validateDatabaseName(name);
    const resp = await this.executeSystemCypher(
      `CREATE DATABASE ${this.quoteCypherIdentifier(dbName)}`,
    );
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(resp.errors.map((e) => e.message).join("; "));
    }
  }

  async dropDatabase(name: string): Promise<void> {
    const dbName = this.validateDatabaseName(name);
    const resp = await this.executeSystemCypher(
      `DROP DATABASE ${this.quoteCypherIdentifier(dbName)}`,
    );
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(resp.errors.map((e) => e.message).join("; "));
    }
  }

  async deleteNodes(
    nodeIds: string[],
    database?: string,
  ): Promise<{ success: boolean; deleted: number; errors: string[] }> {
    if (nodeIds.length === 0) {
      return { success: true, deleted: 0, errors: [] };
    }

    const dbName =
      database != null && database !== ""
        ? database
        : await this.getDefaultDatabase();

    try {
      // First, verify the nodes exist before deleting (safety check)
      const verifyStatement = `MATCH (n) WHERE id(n) IN $ids RETURN id(n) as nodeId, elementId(n) as elementId`;
      const verifyResult = await this.runCypherOverBolt(
        dbName,
        verifyStatement,
        { ids: nodeIds },
      );
      const foundCount = verifyResult.results[0]?.data?.length || 0;

      if (foundCount === 0) {
        return {
          success: false,
          deleted: 0,
          errors: [
            `None of the requested nodes were found. ` +
              `Requested IDs: ${nodeIds.join(", ")}. ` +
              `This may indicate the nodes were already deleted or the IDs are incorrect.`,
          ],
        };
      }

      if (foundCount !== nodeIds.length) {
        return {
          success: false,
          deleted: 0,
          errors: [
            `Only ${foundCount} of ${nodeIds.length} requested nodes were found. ` +
              `Requested IDs: ${nodeIds.join(", ")}. ` +
              `Some nodes may not exist.`,
          ],
        };
      }

      // Use bulk delete with id(n) IN $ids - verified by unit tests to work correctly
      // This is much more efficient than deleting one by one
      // The UI extracts internal IDs from elementId, which id(n) matches perfectly
      const statement = `MATCH (n) WHERE id(n) IN $ids DETACH DELETE n RETURN count(n) as deleted`;
      const parameters = { ids: nodeIds };

      const result = await this.runCypherOverBolt(
        dbName,
        statement,
        parameters,
      );

      if (result.errors && result.errors.length > 0) {
        return {
          success: false,
          deleted: 0,
          errors: result.errors.map((e) => e.message),
        };
      }

      const deleted = (result.results[0]?.data[0]?.row[0] as number) || 0;

      // CRITICAL: If more nodes were deleted than requested, this is a serious bug
      // The WHERE clause should have filtered correctly - this indicates a query issue
      if (deleted > nodeIds.length) {
        return {
          success: false,
          deleted,
          errors: [
            `CRITICAL: Expected to delete ${nodeIds.length} nodes, but ${deleted} were deleted. ` +
              `This indicates the WHERE clause did not filter correctly. ` +
              `Requested IDs: ${nodeIds.join(", ")}`,
          ],
        };
      }

      // If fewer nodes were deleted, some may not exist
      if (deleted < nodeIds.length) {
        return {
          success: false,
          deleted,
          errors: [
            `Expected to delete ${nodeIds.length} nodes, but only ${deleted} were deleted. ` +
              `Some nodes may not exist. Requested IDs: ${nodeIds.join(", ")}`,
          ],
        };
      }

      return {
        success: true,
        deleted,
        errors: [],
      };
    } catch (err) {
      return {
        success: false,
        deleted: 0,
        errors: [err instanceof Error ? err.message : "Unknown error"],
      };
    }
  }

  async updateNodeProperties(
    nodeId: string,
    properties: Record<string, unknown>,
    database?: string,
  ): Promise<{ success: boolean; error?: string }> {
    const dbName =
      database != null && database !== ""
        ? database
        : await this.getDefaultDatabase();

    // Build SET clause
    const setParts: string[] = [];
    const parameters: Record<string, unknown> = { nodeId };
    let paramIndex = 0;

    for (const [key, value] of Object.entries(properties)) {
      const paramName = `p${paramIndex}`;
      setParts.push(`n.${key} = $${paramName}`);
      parameters[paramName] = value;
      paramIndex++;
    }

    if (setParts.length === 0) {
      return { success: true };
    }

    const statement = `MATCH (n) WHERE id(n) = $nodeId OR n.id = $nodeId SET ${setParts.join(", ")} RETURN n`;

    try {
      const result = await this.runCypherOverBolt(
        dbName,
        statement,
        parameters,
      );

      if (result.errors && result.errors.length > 0) {
        return {
          success: false,
          error: result.errors.map((e) => e.message).join("; "),
        };
      }

      return { success: true };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : "Failed to update node",
      };
    }
  }

  /** Per-database config: overrides and effective (admin only). */
  async getDatabaseConfig(dbName: string): Promise<{
    overrides: Record<string, string>;
    effective: Record<string, string>;
  }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/config`,
      ),
      { credentials: "include" },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message ?? `Failed to load config: ${res.status}`);
    }
    return res.json();
  }

  /** Save per-database config overrides (admin only). */
  async putDatabaseConfig(
    dbName: string,
    overrides: Record<string, string>,
  ): Promise<{
    overrides: Record<string, string>;
    rebuildTriggered?: boolean;
  }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/config`,
      ),
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ overrides }),
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message ?? `Failed to save config: ${res.status}`);
    }
    return res.json();
  }

  /** Allowed per-DB config keys with type and category (admin only). */
  async getDatabaseConfigKeys(): Promise<
    Array<{ key: string; type: string; category: string }>
  > {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/databases/config/keys"),
      { credentials: "include" },
    );
    if (!res.ok) throw new Error("Failed to load config keys");
    return res.json();
  }

  async getDatabaseLifecycleStatus(
    dbName: string,
  ): Promise<MVCCLifecycleStatus> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/mvcc/status`,
      ),
      {
        credentials: "include",
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(
        err.message ?? `Failed to load lifecycle status: ${res.status}`,
      );
    }
    return res.json();
  }

  async getDatabaseLifecycleDebt(
    dbName: string,
    limit: number,
  ): Promise<MVCCLifecycleDebtResponse> {
    const query = new URLSearchParams({ limit: String(limit) });
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/mvcc/debt?${query.toString()}`,
      ),
      {
        credentials: "include",
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(
        err.message ?? `Failed to load lifecycle debt: ${res.status}`,
      );
    }
    return res.json();
  }

  async triggerDatabaseLifecyclePrune(
    dbName: string,
  ): Promise<{ status: string; database: string }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/mvcc/prune`,
      ),
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message ?? `Failed to trigger prune: ${res.status}`);
    }
    return res.json();
  }

  async pauseDatabaseLifecycle(
    dbName: string,
  ): Promise<{ status: string; database: string }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/mvcc/pause`,
      ),
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(
        err.message ?? `Failed to pause lifecycle: ${res.status}`,
      );
    }
    return res.json();
  }

  async resumeDatabaseLifecycle(
    dbName: string,
  ): Promise<{ status: string; database: string }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/mvcc/resume`,
      ),
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(
        err.message ?? `Failed to resume lifecycle: ${res.status}`,
      );
    }
    return res.json();
  }

  async setDatabaseLifecycleSchedule(
    dbName: string,
    interval: string,
  ): Promise<MVCCLifecycleStatus> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/databases/${encodeURIComponent(dbName)}/mvcc/schedule`,
      ),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ interval }),
      },
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(
        err.message ?? `Failed to update lifecycle schedule: ${res.status}`,
      );
    }
    return res.json();
  }

  async getRetentionStatus(): Promise<RetentionStatus> {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/retention/status"),
      {
        credentials: "include",
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to load retention status: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async listRetentionPolicies(): Promise<RetentionPolicy[]> {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/retention/policies"),
      { credentials: "include" },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to load retention policies: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async createRetentionPolicy(
    policy: RetentionPolicy,
  ): Promise<RetentionPolicy> {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/retention/policies"),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(policy),
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to create retention policy: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async updateRetentionPolicy(
    id: string,
    policy: RetentionPolicy,
  ): Promise<RetentionPolicy> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/retention/policies/${encodeURIComponent(id)}`,
      ),
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(policy),
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to update retention policy: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async deleteRetentionPolicy(
    id: string,
  ): Promise<{ status: string; id: string }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/retention/policies/${encodeURIComponent(id)}`,
      ),
      {
        method: "DELETE",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to delete retention policy: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async loadDefaultRetentionPolicies(): Promise<{
    loaded: number;
    skipped: number;
    errors: string[];
    total: number;
  }> {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/retention/policies/defaults"),
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to load default retention policies: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async listRetentionHolds(): Promise<RetentionLegalHold[]> {
    const res = await fetch(joinBasePath(BASE_PATH, "/admin/retention/holds"), {
      credentials: "include",
    });
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to load retention holds: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async createRetentionHold(
    hold: RetentionLegalHold,
  ): Promise<RetentionLegalHold> {
    const res = await fetch(joinBasePath(BASE_PATH, "/admin/retention/holds"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(hold),
    });
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to create retention hold: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async releaseRetentionHold(
    id: string,
  ): Promise<{ status: string; id: string }> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/retention/holds/${encodeURIComponent(id)}`,
      ),
      {
        method: "DELETE",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to release retention hold: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async listRetentionErasures(): Promise<RetentionErasureRequest[]> {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/retention/erasures"),
      { credentials: "include" },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to load retention erasure requests: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async createRetentionErasureRequest(payload: {
    subject_id: string;
    subject_email?: string;
  }): Promise<RetentionErasureRequest> {
    const res = await fetch(
      joinBasePath(BASE_PATH, "/admin/retention/erasures"),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to create retention erasure request: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async processRetentionErasure(id: string): Promise<RetentionErasureRequest> {
    const res = await fetch(
      joinBasePath(
        BASE_PATH,
        `/admin/retention/erasures/${encodeURIComponent(id)}/process`,
      ),
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to process retention erasure request: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async triggerRetentionSweep(): Promise<{ status: string }> {
    const res = await fetch(joinBasePath(BASE_PATH, "/admin/retention/sweep"), {
      method: "POST",
      credentials: "include",
    });
    if (!res.ok) {
      const message = await this.parseErrorMessage(
        res,
        `Failed to trigger retention sweep: ${res.status}`,
      );
      throw new Error(message);
    }
    return res.json();
  }

  async getKnowledgePolicyProfiles(
    database?: string,
  ): Promise<KPProfilesResponse> {
    const dbName = await this.getResolvedDatabaseName(database);
    const [profilesResp, infoResp] = await Promise.all([
      this.executeCypherOnDatabase(
        dbName,
        "CALL nornicdb.knowledgepolicy.profiles()",
      ),
      this.executeCypherOnDatabase(
        dbName,
        "CALL nornicdb.knowledgepolicy.info()",
      ),
    ]);
    this.assertCypherSuccess(
      profilesResp,
      "Failed to load knowledge policy profiles",
    );
    this.assertCypherSuccess(infoResp, "Failed to load knowledge policy info");

    const rows = this.parseCypherRows<Record<string, unknown>>(profilesResp);
    const infoRows = this.parseCypherRows<Record<string, unknown>>(infoResp);
    const bundles: DecayProfileBundle[] = [];
    const bindings: DecayProfileBinding[] = [];

    for (const row of rows) {
      const kind = asString(row.kind);
      if (kind === "bundle") {
        bundles.push({
          Name: asString(row.Name),
          HalfLifeSeconds: asNumber(row.HalfLifeSeconds),
          VisibilityThreshold: asNumber(row.VisibilityThreshold),
          ScoreFloor: asNumber(row.ScoreFloor),
          Function: asString(row.Function),
          Scope: asString(row.Scope),
          DecayEnabled: asBoolean(row.DecayEnabled),
          ScoreFrom: asString(row.ScoreFrom),
          ScoreFromProperty: asOptionalString(row.ScoreFromProperty),
          Enabled: asBoolean(row.Enabled),
        });
        continue;
      }
      if (kind === "binding") {
        bindings.push({
          Name: asString(row.Name),
          TargetLabels: asStringArray(row.TargetLabels),
          TargetEdgeType: asOptionalString(row.TargetEdgeType),
          IsWildcard: asBoolean(row.IsWildcard),
          IsEdge: asBoolean(row.IsEdge),
          ProfileRef: asOptionalString(row.ProfileRef),
          NoDecay: asBoolean(row.NoDecay),
          VisibilityThreshold: asOptionalNumber(row.VisibilityThreshold),
          Order: asNumber(row.Order),
        });
      }
    }

    return {
      bundles,
      bindings,
      decay_enabled: asBoolean(infoRows[0]?.enabled),
    };
  }

  async getKnowledgePolicyPolicies(
    database?: string,
  ): Promise<KPPoliciesResponse> {
    const dbName = await this.getResolvedDatabaseName(database);
    const resp = await this.executeCypherOnDatabase(
      dbName,
      "CALL nornicdb.knowledgepolicy.policies()",
    );
    this.assertCypherSuccess(resp, "Failed to load knowledge policy policies");

    const rows = this.parseCypherRows<Record<string, unknown>>(resp);
    const promotion_profiles: PromotionProfileDef[] = [];
    const promotion_policies: PromotionPolicyDef[] = [];

    for (const row of rows) {
      const kind = asString(row.kind);
      if (kind === "profile") {
        promotion_profiles.push({
          Name: asString(row.Name),
          Scope: asString(row.Scope),
          Multiplier: asNumber(row.Multiplier),
          ScoreFloor: asNumber(row.ScoreFloor),
          ScoreCap: asNumber(row.ScoreCap),
          Enabled: asBoolean(row.Enabled),
        });
        continue;
      }
      if (kind === "policy") {
        promotion_policies.push({
          Name: asString(row.Name),
          TargetLabels: asStringArray(row.TargetLabels),
          TargetEdgeType: asOptionalString(row.TargetEdgeType),
          IsWildcard: asBoolean(row.IsWildcard),
          IsEdge: asBoolean(row.IsEdge),
          Enabled: asBoolean(row.Enabled),
        });
      }
    }

    return {
      promotion_profiles,
      promotion_policies,
    };
  }

  async resolveKnowledgePolicy(params: {
    entityId?: string;
    labels?: string[];
    edgeType?: string;
    database?: string;
  }): Promise<ScoringResolution> {
    const dbName = await this.getResolvedDatabaseName(params.database);
    const statement = `CALL nornicdb.knowledgepolicy.resolve('${escapeCypherStringLiteral(
      params.entityId ?? "",
    )}', '${escapeCypherStringLiteral((params.labels ?? []).join(","))}', '${escapeCypherStringLiteral(
      params.edgeType ?? "",
    )}')`;
    const resp = await this.executeCypherOnDatabase(dbName, statement);
    this.assertCypherSuccess(resp, "Failed to resolve knowledge policy");

    const row = this.parseCypherRows<Record<string, unknown>>(resp)[0] ?? {};
    return {
      TargetID: asString(row.TargetID),
      TargetScope: asString(row.TargetScope),
      ResolvedDecayProfileID: asString(row.ResolvedDecayProfileID),
      ResolvedScoreFrom: asString(row.ResolvedScoreFrom),
      ResolutionSourceChain: asStringArray(row.ResolutionSourceChain) ?? [],
      AppliedDecayProfileNames:
        asStringArray(row.AppliedDecayProfileNames) ?? [],
      AppliedPromotionPolicyName: asString(row.AppliedPromotionPolicyName),
      AppliedPromotionProfileName: asString(row.AppliedPromotionProfileName),
      EffectiveRate: asNumber(row.EffectiveRate),
      EffectiveThreshold: asNumber(row.EffectiveThreshold),
      EffectiveMultiplier: asNumber(row.EffectiveMultiplier),
      BaseScore: asNumber(row.BaseScore),
      FinalScore: asNumber(row.FinalScore),
      NoDecay: asBoolean(row.NoDecay),
      SuppressionEligible: asBoolean(row.SuppressionEligible),
      Explanation: asString(row.Explanation),
    };
  }

  async getDeindexStatus(database?: string): Promise<KPDeindexStatusResponse> {
    const dbName = await this.getResolvedDatabaseName(database);
    const resp = await this.executeCypherOnDatabase(
      dbName,
      "CALL nornicdb.knowledgepolicy.deindexStatus()",
    );
    this.assertCypherSuccess(resp, "Failed to load deindex status");

    const rows = this.parseCypherRows<Record<string, unknown>>(resp);
    const first = rows[0] ?? {};
    const items = rows
      .filter((row) => asString(row.workItemId) !== "")
      .map((row) => ({
        workItemId: asString(row.workItemId),
        targetId: asString(row.targetId),
        targetScope: asString(row.targetScope),
        enqueuedAt: asNumber(row.enqueuedAt),
        status: asString(row.status),
      }));

    return {
      pending_count: asNumber(first.pending_count),
      items,
      supported: first.supported !== false,
      message: asOptionalString(first.message),
    };
  }
}

export const api = new NornicDBClient();
