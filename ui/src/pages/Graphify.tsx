import { useCallback, useEffect, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance } from "3d-force-graph";
import {
  api,
  type CypherResponse,
  type GraphNeighborhoodResponse,
} from "../utils/api";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GNode {
  id: string;
  label: string;
  fileType: string;
  sourceFile?: string;
  sourceLocation?: string;
  nodeKind?: string;
  degree: number;
  highlight: boolean;
  selected: boolean;
  similar?: boolean;
  dimmed?: boolean;
  internalId?: string;
  body?: string;
  x?: number;
  y?: number;
  z?: number;
  fx?: number;
  fy?: number;
  fz?: number;
  vx?: number;
  vy?: number;
  vz?: number;
}

interface GLink {
  source: string | GNode;
  target: string | GNode;
  relation: string;
  highlight: boolean;
  dimmed?: boolean;
}

type GraphifyForceGraph = ForceGraph3DInstance<GNode, GLink>;

interface ArtifactNode {
  id: string;
  label?: string;
  file_type?: string;
  source_file?: string;
  source_location?: string;
  node_kind?: string;
  body?: string;
  [key: string]: unknown;
}

interface ArtifactLink {
  source: string;
  target: string;
  relation?: string;
  [key: string]: unknown;
}

type LoadSource =
  | { kind: "db"; name: string }
  | { kind: "artifact"; name: string };

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Symbol kinds are colored independently so function calls, types and
// variables are visually distinct, and each kind is clustered into its own
// wedge in the band layout.
const KIND_COLORS: Record<string, string> = {
  call: "#38bdf8",
  type: "#fb7185",
  variable: "#34d399",
  file: "#94a3b8",
  document: "#fbbf24",
  other: "#a78bfa",
};

const DEFAULT_NODE_COLOR = KIND_COLORS.other;
const LINK_COLOR = "rgba(125,211,252,0.35)";
const LINK_HIGHLIGHT = "rgba(168,85,247,0.9)";
const HIGHLIGHT_COLOR = "#a855f7";
// Color for semantically similar nodes spawned as a new arm of the graph.
const SIMILAR_COLOR = "#e879f9";

const KIND_LEGEND: Array<[string, string]> = [
  ["call", KIND_COLORS.call],
  ["type", KIND_COLORS.type],
  ["variable", KIND_COLORS.variable],
  ["file", KIND_COLORS.file],
  ["document", KIND_COLORS.document],
  ["other", KIND_COLORS.other],
  ["similar", SIMILAR_COLOR],
];

// The top-level entry point the rooted view starts from: main() in
// cmd/nornicdb/main.go (graphify node id convention: path_segments_symbol).
const MAIN_ENTRY_ID = "cmd_nornicdb_main_main";
const MAIN_ENTRY_QUERY = `MATCH (n)
WHERE n.source_file = 'cmd/nornicdb/main.go' AND n.label = 'main()'
RETURN n.id AS id LIMIT 1`;

const DEFAULT_DEPTH = 3;
const NEIGHBORHOOD_LIMIT = 20000;

// The rooted walk follows the code neighborhood (calls, methods and
// references) in both directions. Directed-out from main() saturates at a
// handful of nodes, and adding the dense import/contains edges blows past
// the node limit at any depth.
const CALL_RELATION_TYPES = ["CALLS", "METHOD", "REFERENCES"];

// three-forcegraph's runtime disables DAG layout on a falsy mode, but its
// typings only accept the DagMode union; route through a null-tolerant cast.
function setDagMode(fg: GraphifyForceGraph, mode: "td" | null): void {
  const setter = fg.dagMode as unknown as (m?: string | null) => unknown;
  setter(mode);
  fg.dagLevelDistance(72);
}

// Absolute y targets for a top-down band layout: the root lands at the
// highest y (top of the scene) and each hop steps one band downward.
function depthTargets(depths: Map<string, number>, spacing: number): Map<string, number> {
  let maxDepth = 0;
  for (const depth of depths.values()) {
    maxDepth = Math.max(maxDepth, depth);
  }
  const targets = new Map<string, number>();
  for (const [id, depth] of depths) {
    targets.set(id, (maxDepth - depth) * spacing);
  }
  return targets;
}

  // Configure the layout orientation: bands are measured as hops from the
  // chosen root along the call/reference links, so only the starting symbol
  // sits at the top level (never an artifact of file structure or of which
  // nodes happen to have no incoming edges). Each band is additionally
  // split into wedges per symbol kind (calls, types, variables, ...) so
  // same-kind symbols form their own neighborhood.
function orientGraph(fg: GraphifyForceGraph, links: GLink[], nodes: GNode[], rootId: string | null): void {
  // dagMode derives levels from link direction (every zero-indegree node
  // lands at the top), so it stays off; this layout owns the bands.
  setDagMode(fg, null);
  fg.d3Force("layers", null);
  const depths = computeDepths(links, rootId ?? undefined);
  // Unreachable clusters (possible when test files are shown) go below the
  // deepest reachable band instead of sharing the root's top level.
  let maxDepth = 0;
  for (const depth of depths.values()) {
    maxDepth = Math.max(maxDepth, depth);
  }
  const effective = new Map<string, number>();
  for (const node of nodes) {
    effective.set(node.id, depths.get(node.id) ?? maxDepth + 1);
  }
  const targets = depthTargets(effective, 72);
  // Seed each band with a radial spread so the tree is readable without a
  // long-running force simulation, then pin every node (fx/fy/fz) so the
  // hot force ticks cannot drift the graph away from the camera frame
  // computed by zoomToFit right after graphData is set.
  const byDepth = new Map<number, GNode[]>();
  for (const node of nodes) {
    const depth = effective.get(node.id) ?? 0;
    const group = byDepth.get(depth) ?? [];
    group.push(node);
    byDepth.set(depth, group);
  }
  for (const [depth, group] of byDepth) {
    // Split the band into one wedge per symbol kind: same-kind symbols
    // cluster into their own angular neighborhood at every depth.
    const byKind = new Map<string, GNode[]>();
    const kinds = new Set<string>();
    for (const node of group) {
      const kind = classifySymbolKind(node);
      kinds.add(kind);
      const kindGroup = byKind.get(kind) ?? [];
      kindGroup.push(node);
      byKind.set(kind, kindGroup);
    }
    const orderedKinds = Array.from(kinds).sort();
    const kindIndex = new Map(orderedKinds.map((kind, index) => [kind, index]));
    const wedgeWidth = (2 * Math.PI) / Math.max(orderedKinds.length, 1);
    for (const [kind, kindNodes] of byKind) {
      const center = -Math.PI / 2 + wedgeWidth * (kindIndex.get(kind) ?? 0);
      const spread = wedgeWidth * 0.85;
      kindNodes.forEach((node, index) => {
        const fraction =
          kindNodes.length > 1 ? index / (kindNodes.length - 1) : 0.5;
        const angle = center - spread / 2 + spread * fraction;
        const radius = 24 + depth * 26;
        node.x = Math.cos(angle) * radius;
        node.z = Math.sin(angle) * radius;
        node.y = targets.get(node.id) ?? 0;
        // Pin the seeded position so the static layout stays exactly where
        // the camera was framed: force ticks must not move nodes after
        // zoomToFit has already computed the frame from these positions.
        node.fx = node.x;
        node.fy = node.y;
        node.fz = node.z;
      });
    }
  }
  if (import.meta.env.DEV) {
    (window as unknown as Record<string, unknown>).__graphifyDebug = {
      rootId,
      maxDepth,
      topBand: nodes
        .filter((node) => (node.y ?? -1) >= (targets.get(rootId ?? "") ?? 0) - 0.5)
        .map((node) => ({ id: node.id, label: node.label, y: node.y })),
    };
  }
  fg.d3AlphaMin(0.9);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const FILE_EXTENSIONS =
  /\.(go|md|py|ts|js|jsx|tsx|yml|yaml|json|sh|c|h|cpp|cc|java|rs|html|css|txt|toml|mod|sum|proto|graphql)$/i;

function rowsFromCypher(resp: CypherResponse): Array<Record<string, unknown>> {
  if (resp.errors && resp.errors.length > 0) {
    throw new Error(resp.errors.map((e) => e.message).join("; "));
  }
  const result = resp.results?.[0];
  const cols = result?.columns ?? [];
  const data = result?.data ?? [];
  return data.map((d) => {
    const out: Record<string, unknown> = {};
    for (let i = 0; i < cols.length; i++) {
      out[cols[i]] = d.row[i];
    }
    return out;
  });
}

function graphifyLabel(fileType: string | undefined): string {
  const base = (fileType ?? "Entity").replace(/[^A-Za-z0-9_]/g, "");
  const label = base.charAt(0).toUpperCase() + base.slice(1);
  return label || "Entity";
}

function isTestSource(sourceFile: string | undefined): boolean {
  const path = String(sourceFile ?? "").toLowerCase();
  if (!path) return false;
  const name = path.split("/").pop() ?? "";
  if (name.endsWith("_test.go") || name.startsWith("test_")) return true;
  if (/\.(test|spec)\.(js|jsx|ts|tsx|mjs|cjs)$/.test(name)) return true;
  if (/(^|\/)(test|tests|testing|spec|specs)(\/|$)/.test(path)) return true;
  return false;
}

function chunk<T>(items: T[], size: number): T[][] {
  const batches: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    batches.push(items.slice(i, i + size));
  }
  return batches;
}

function classifySymbolKind(node: GNode): string {
  if ((node.fileType ?? "").toLowerCase() === "document") {
    return "document";
  }
  const label = (node.label ?? "").trim();
  if (!label) {
    return "other";
  }
  // Function or method call/definition: the label carries a parameter
  // list, e.g. ".GetParser()", "NewManager(...)".
  if (/\(.*\)$/.test(label)) {
    return "call";
  }
  // Imported paths and file references carry slashes or a known file
  // extension. The extension list is explicit so dotted type references
  // like "context.Context" or "language.Tag" classify as variables, not
  // as files.
  if (label.includes("/") || FILE_EXTENSIONS.test(label)) {
    return "file";
  }
  // Go-style capitalized identifiers are types (structs, interfaces).
  if (/^[A-Z]/.test(label)) {
    return "type";
  }
  // Everything else is a variable / field / package name.
  if (/^[a-z_]/.test(label)) {
    return "variable";
  }
  return "other";
}

function sanitizeRelation(relation: string): string {
  const cleaned = relation
    .toUpperCase()
    .replace(/\s/g, "_")
    .replace(/-/g, "_")
    .replace(/[^A-Z0-9_]/g, "_");
  return cleaned || "RELATED_TO";
}

function contentHash(props: Record<string, unknown>): string {
  // FNV-1a 64-bit over the canonical JSON of string-valued properties,
  // matching scripts/graphify_local.py exactly (sorted keys, only string
  // values, updated_at/props_hash excluded) so the upload dialog and the
  // importer agree on whether a node or edge changed.
  const parts: string[] = [];
  for (const key of Object.keys(props).sort()) {
    if (key === "updated_at" || key === "props_hash") continue;
    const value = props[key];
    if (typeof value === "string") {
      parts.push(JSON.stringify(key) + ":" + JSON.stringify(value));
    }
  }
  const canonical = "{" + parts.join(",") + "}";
  let digest = 0xcbf29ce484222325n;
  for (const byte of new TextEncoder().encode(canonical)) {
    digest ^= BigInt(byte);
    digest = (digest * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return digest.toString(16).padStart(16, "0");
}

function validateUploadDatabaseName(name: string, existing: string[], allowExisting = false): string | null {
  const trimmed = name.trim();
  if (!trimmed) return "database name is required";
  if (trimmed.includes(":")) return "database name cannot contain ':'";
  if (trimmed.startsWith("_")) return "database name cannot start with '_'";
  if (existing.includes(trimmed)) {
    if (allowExisting) return null;
    return `database '${trimmed}' already exists — check "update existing" to sync it, or choose a different name`;
  }
  return null;
}

interface Neighbor {
  id: string;
  label: string;
  relation: string;
  direction: "in" | "out";
}

// BFS depth from the root along link direction; unreachable nodes get depth 0.
function computeDepths(links: GLink[], rootId: string | undefined): Map<string, number> {
  const depths = new Map<string, number>();
  const idOf = (end: string | GNode) => (typeof end === "object" ? end.id : end);
  const children = new Map<string, string[]>();
  for (const link of links) {
    const source = idOf(link.source);
    const target = idOf(link.target);
    const list = children.get(source) ?? [];
    list.push(target);
    children.set(source, list);
  }
  if (!rootId) return depths;
  depths.set(rootId, 0);
  const queue: Array<[string, number]> = [[rootId, 0]];
  while (queue.length > 0) {
    const [current, depth] = queue.shift() as [string, number];
    for (const child of children.get(current) ?? []) {
      if (!depths.has(child)) {
        depths.set(child, depth + 1);
        queue.push([child, depth + 1]);
      }
    }
  }
  return depths;
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

function FilterChipList(props: {
  label: string;
  entries: string[];
  onAdd: (entries: string[]) => void;
  onRemove: (entry: string) => void;
  placeholder?: string;
}) {
  const [draft, setDraft] = useState("");
  const add = () => {
    const added = draft
      .split(/[\s,]+/)
      .map((entry) => entry.trim().replace(/\s*:\s*/g, ":"))
      .filter((entry) => entry !== "" && !props.entries.includes(entry));
    if (added.length === 0) {
      setDraft("");
      return;
    }
    props.onAdd([...props.entries, ...added]);
    setDraft("");
  };
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[10px] text-norse-silver/60">{props.label}</span>
      {props.entries.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {props.entries.map((entry) => (
            <span
              key={entry}
              className="inline-flex items-center gap-1 rounded bg-norse-rune/40 border border-norse-rune px-1.5 py-0.5 text-[10px] font-mono text-norse-silver"
            >
              {entry}
              <button
                type="button"
                aria-label={`remove ${entry}`}
                onClick={() => props.onRemove(entry)}
                className="text-norse-silver/60 hover:text-red-300"
              >
                ×
              </button>
            </span>
          ))}
        </div>
      )}
      <div className="flex items-center gap-1">
        <input
          type="text"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              add();
            }
          }}
          placeholder={props.placeholder}
          spellCheck={false}
          className="w-full rounded border border-norse-rune bg-norse-night px-1.5 py-1 text-[11px] text-norse-silver placeholder:text-norse-silver/30 focus:outline-none focus:border-sky-400"
        />
        <button
          type="button"
          onClick={add}
          aria-label={`add ${props.label}`}
          className="rounded border border-norse-rune bg-norse-night px-1.5 py-1 text-[11px] text-norse-silver/70 hover:border-sky-400"
        >
          +
        </button>
      </div>
    </div>
  );
}

export function Graphify() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<GraphifyForceGraph | null>(null);
  const nodeByIdRef = useRef<Map<string, GNode>>(new Map());
  // graphify id -> internal storage id, so findSimilar can seed the vector
  // lookup without an extra query for nodes loaded from the neighborhood.
  const publicToInternalRef = useRef<Map<string, string>>(new Map());
  const searchTimerRef = useRef<number | null>(null);

  const [databases, setDatabases] = useState<string[]>([]);
  const [database, setDatabase] = useState<string>("");
  const [customRoot, setCustomRoot] = useState<{ id: string; label: string } | null>(null);
  // Mirror of customRoot for stale-closure-free reads inside
  // loadFromDatabase: handlers update it synchronously before scheduling the
  // reload, so the fetch always sees the root that was just chosen.
  const customRootRef = useRef<{ id: string; label: string } | null>(null);
  const [depth, setDepth] = useState<number>(DEFAULT_DEPTH);
  const [showTests, setShowTests] = useState(false);
  // Filter entries are managed as add/remove lists in the filter panel;
  // applying them reloads the graph with the lists sent to the
  // neighborhood endpoint.
  const [includeLabels, setIncludeLabels] = useState<string[]>([]);
  const [includeEdgeTypes, setIncludeEdgeTypes] = useState<string[]>([
    ...CALL_RELATION_TYPES,
  ]);
  const [includeProps, setIncludeProps] = useState<string[]>([]);
  const [excludeLabels, setExcludeLabels] = useState<string[]>([]);
  const [excludeEdgeTypes, setExcludeEdgeTypes] = useState<string[]>([]);
  const [excludeProps, setExcludeProps] = useState<string[]>([]);
  // Disconnected subgraphs reported by the endpoint after exclusion
  // filters; selecting one dims the rest of the scene.
  const [components, setComponents] = useState<GraphNeighborhoodResponse[]>([]);
  const [activeComponent, setActiveComponent] = useState<number | null>(null);
  const [source, setSource] = useState<LoadSource | null>(null);
  const [status, setStatus] = useState<string>(
    "select a database and load the graphify tree",
  );
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadedLinks, setLoadedLinks] = useState(0);
  const [selected, setSelected] = useState<GNode | null>(null);
  const [body, setBody] = useState<string | null>(null);
  const [bodyLoading, setBodyLoading] = useState(false);
  const [bodySource, setBodySource] = useState<"artifact" | "db" | null>(null);
  const [neighbors, setNeighbors] = useState<Neighbor[]>([]);
  const [search, setSearch] = useState("");
  const [searchResults, setSearchResults] = useState<Array<{ id: string; label: string; sourceFile?: string; score?: number }>>([]);
  const [similarLoading, setSimilarLoading] = useState(false);
  const [similarCount, setSimilarCount] = useState<number | null>(null);
  const [panelPos, setPanelPos] = useState<{ left: number; top: number } | null>(null);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadDbName, setUploadDbName] = useState("");
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState<{ label: string; percent: number } | null>(null);
  const [updateExisting, setUpdateExisting] = useState(false);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef<{ startX: number; startY: number; left: number; top: number } | null>(null);
  const selectedIdRef = useRef<string | null>(null);
  const rootIdRef = useRef<string | null>(null);

  // --- Database list -------------------------------------------------------

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const names = await api.listDatabaseNames();
        if (cancelled) return;
        setDatabases(names);
        if (names.includes("nornicdbcode")) {
          setDatabase("nornicdbcode");
        } else if (names.includes("graphify")) {
          setDatabase("graphify");
        } else {
          setDatabase(names[0] ?? "");
          if (names.length > 0) {
            setStatus(
              "database 'nornicdbcode' not found — run scripts/graphify_local.py first, then load",
            );
          }
        }
      } catch (err) {
        if (cancelled) return;
        setStatus(
          "could not list databases — sign in, or upload a graphify artifact instead",
        );
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Reload the tree when the test-file filter flips (skip the initial
  // render; the Load tree button drives the first fetch).
  const showTestsFirstRenderRef = useRef(true);
  useEffect(() => {
    if (showTestsFirstRenderRef.current) {
      showTestsFirstRenderRef.current = false;
      return;
    }
    if (database && !loading) {
      void loadFromDatabase();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showTests]);

  // --- Graph init (once) ---------------------------------------------------

  useEffect(() => {
    if (!containerRef.current) return;
    if (graphRef.current) return;

    const el = containerRef.current;
    const fg = new ForceGraph3D(el) as unknown as GraphifyForceGraph;

    fg.backgroundColor("#04060c")
      .showNavInfo(false)
      .width(el.clientWidth)
      .height(el.clientHeight)
      .nodeRelSize(2.4)
      .nodeOpacity(0.95)
      .nodeVal((n) => 1 + Math.sqrt(n.degree) * 0.55)
      .nodeColor((n) => {
        if (n.dimmed) return "#1c2431";
        if (n.selected) return HIGHLIGHT_COLOR;
        if (n.highlight) return "#f0abfc";
        if (n.similar) return SIMILAR_COLOR;
        return KIND_COLORS[classifySymbolKind(n)] ?? DEFAULT_NODE_COLOR;
      })
      .linkOpacity(0.4)
      .linkWidth((l) => (l.dimmed ? 0.25 : l.highlight ? 1.6 : 0.6))
      .linkColor((l) =>
        l.dimmed
          ? "rgba(148,163,184,0.08)"
          : l.highlight
            ? LINK_HIGHLIGHT
            : LINK_COLOR,
      )
      .linkDirectionalArrowLength(3.5)
      .linkDirectionalArrowRelPos(1)
      .linkCurvature(0.2)
      .dagLevelDistance(72)
      .linkDirectionalParticles(0)
      .linkLabel((l) => {
        const s = typeof l.source === "object" ? l.source.label : String(l.source);
        const t = typeof l.target === "object" ? l.target.label : String(l.target);
        return `${s} -[${l.relation}]-> ${t}`;
      })
      .nodeLabel(
        (n) =>
          `${classifySymbolKind(n)} · ${n.label}${n.sourceFile ? ` · ${n.sourceFile}${n.sourceLocation ? ":" + n.sourceLocation : ""}` : ""}`,
      )
      .cooldownTicks(80)
      .warmupTicks(0)
      .onNodeClick((node: GNode | null) => {
        if (node) selectNode(node.id);
      });

    const charge = fg.d3Force("charge") as unknown as
      | { strength: (n: number) => unknown }
      | undefined;
    charge?.strength(-8);

    graphRef.current = fg;
    if (import.meta.env.DEV) {
      (window as unknown as Record<string, unknown>).__graphify = fg;
    }

    const ro = new ResizeObserver(() => {
      fg.width(el.clientWidth);
      fg.height(el.clientHeight);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // --- Node selection -------------------------------------------------------

  const applySelectionVisuals = useCallback((id: string | null) => {
    const fg = graphRef.current;
    if (!fg) return;
    const live = fg.graphData();
    const neighborsOf = new Set<string>();
    if (id) {
      for (const l of live.links) {
        const s = typeof l.source === "object" ? l.source.id : String(l.source);
        const t = typeof l.target === "object" ? l.target.id : String(l.target);
        if (s === id) neighborsOf.add(t);
        if (t === id) neighborsOf.add(s);
      }
    }
    for (const n of live.nodes) {
      n.selected = n.id === id;
      n.highlight = id != null && neighborsOf.has(n.id);
    }
    for (const l of live.links) {
      const s = typeof l.source === "object" ? l.source.id : String(l.source);
      const t = typeof l.target === "object" ? l.target.id : String(l.target);
      l.highlight = s === id || t === id;
    }
    fg.nodeColor(fg.nodeColor());
    fg.linkColor(fg.linkColor());
    fg.linkWidth(fg.linkWidth());
  }, []);

  const selectNode = useCallback(
    async (id: string) => {
      const node = nodeByIdRef.current.get(id);
      if (!node) return;
      selectedIdRef.current = id;
      setSelected(node);
      applySelectionVisuals(id);

      const fg = graphRef.current;
      if (fg) {
        fg.cameraPosition(
          {
            x: node.x ?? 0,
            y: node.y ?? 0,
            z: (node.z ?? 0) + 120,
          },
          { x: node.x ?? 0, y: node.y ?? 0, z: node.z ?? 0 },
          1200,
        );
      }

      // Neighbors from the loaded link set.
      const live = fg?.graphData();
      const seen = new Map<string, Neighbor>();
      for (const l of live?.links ?? []) {
        const s = typeof l.source === "object" ? l.source : null;
        const t = typeof l.target === "object" ? l.target : null;
        if (s && t) {
          if (s.id === id && !seen.has(t.id)) {
            seen.set(t.id, {
              id: t.id,
              label: t.label,
              relation: l.relation,
              direction: "out",
            });
          }
          if (t.id === id && !seen.has(s.id)) {
            seen.set(s.id, {
              id: s.id,
              label: s.label,
              relation: l.relation,
              direction: "in",
            });
          }
        }
      }
      setNeighbors(Array.from(seen.values()).slice(0, 200));

      // Body: artifact-provided bodies win; otherwise pull from the DB.
      if (node.body != null && node.body !== "") {
        setBody(node.body);
        setBodySource("artifact");
        setBodyLoading(false);
        return;
      }
      setBody(null);
      setBodySource(null);
      if (!database) return;
      setBodyLoading(true);
      try {
        const label = graphifyLabel(node.fileType);
        const resp = await api.executeCypherOnDatabase(
          database,
          `MATCH (n:${label} {id: $id}) RETURN n.body AS body LIMIT 1`,
          { id },
        );
        const rows = rowsFromCypher(resp);
        const fetched = rows[0]?.body as string | undefined;
        setBody(fetched ?? null);
        setBodySource(fetched ? "db" : null);
      } catch {
        setBody(null);
        setBodySource(null);
      } finally {
        setBodyLoading(false);
      }
    },
    [applySelectionVisuals, database],
  );

  const focusNode = useCallback(
    (id: string) => {
      void selectNode(id);
    },
    [selectNode],
  );

  // --- Component filtering -------------------------------------------------

  // Dims every node and link outside the selected disconnected component,
  // and fits the camera to that component so disjoint subgraphs are easy to
  // inspect one at a time.
  const applyComponentDim = useCallback(
    (index: number | null) => {
      const fg = graphRef.current;
      if (!fg) return;
      const live = fg.graphData();
      const memberIds =
        index == null
          ? null
          : new Set(
              (components[index]?.nodes ?? []).map((node) => node.id),
            );
      for (const node of live.nodes) {
        node.dimmed = memberIds != null && !memberIds.has(node.id);
      }
      for (const link of live.links) {
        const source =
          typeof link.source === "object"
            ? link.source.id
            : String(link.source);
        const target =
          typeof link.target === "object"
            ? link.target.id
            : String(link.target);
        link.dimmed =
          memberIds != null && (!memberIds.has(source) || !memberIds.has(target));
      }
      fg.nodeColor(fg.nodeColor());
      fg.linkColor(fg.linkColor());
      fg.linkWidth(fg.linkWidth());
      if (memberIds != null) {
        fg.zoomToFit(400, 60, (node) => memberIds.has((node as GNode).id));
      }
    },
    [components],
  );

  useEffect(() => {
    applyComponentDim(activeComponent);
  }, [activeComponent, applyComponentDim]);

  // --- Data loading ---------------------------------------------------------

  const buildGraph = useCallback(
    (rawNodes: ArtifactNode[], rawLinks: ArtifactLink[], rootId: string | null) => {
      const byId = new Map<string, GNode>();
      for (const raw of rawNodes) {
        const id = String(raw.id);
        byId.set(id, {
          id,
          label: String(raw.label ?? raw.id),
          fileType: String(raw.file_type ?? "Entity"),
          sourceFile: raw.source_file,
          sourceLocation: raw.source_location,
          nodeKind: raw.node_kind,
          body: raw.body,
          degree: 0,
          highlight: false,
          selected: false,
        });
      }
      const seen = new Set<string>();
      const links: GLink[] = [];
      for (const raw of rawLinks) {
        const src = byId.get(String(raw.source));
        const tgt = byId.get(String(raw.target));
        if (!src || !tgt || src.id === tgt.id) continue;
        const key = `${src.id}|${tgt.id}|${raw.relation ?? "RELATED_TO"}`;
        if (seen.has(key)) continue;
        seen.add(key);
        links.push({
          source: src,
          target: tgt,
          relation: String(raw.relation ?? "RELATED_TO"),
          highlight: false,
        });
        src.degree += 1;
        tgt.degree += 1;
      }
      nodeByIdRef.current = byId;
      rootIdRef.current = rootId;
      const fg = graphRef.current;
      const nodes = Array.from(byId.values());
      if (fg) {
        // Orient the graph top-down: dagMode when acyclic (the root then
        // lands at the top), otherwise seed positions by depth from the root
        // and hold them with a layered y-force.
        orientGraph(fg, links, nodes, rootId);
      }
      fg?.graphData({ nodes, links });
      if (fg && rootId) {
        // Frame the whole tree once it is laid out; dagMode td keeps the
        // start node pinned at the top of the scene.
        fg.zoomToFit(400, 60);
      }
      setLoadedLinks(links.length);
      setSelected(null);
      setBody(null);
      setBodySource(null);
      setNeighbors([]);
    },
    [],
  );

  const loadFromDatabase = useCallback(async (dbNameOverride?: string) => {
    const dbName = dbNameOverride ?? database;
    if (!dbName) return;
    setLoading(true);
    setError(null);
    try {
      // Rooted neighborhood: resolve the entry node (the main entry of the
      // graphify graph, or the symbol last chosen with "start graph from
      // this symbol"), then walk its neighborhood at the configured depth.
      let rootId = customRootRef.current?.id ?? null;
      let rootLabel = customRootRef.current?.label ?? "main()";
      if (!rootId) {
        setStatus("resolving main entry...");
        try {
          const mainResp = await api.executeCypherOnDatabase(
            dbName,
            MAIN_ENTRY_QUERY,
          );
          const mainRows = rowsFromCypher(mainResp);
          rootId = mainRows[0]?.id != null ? String(mainRows[0].id) : MAIN_ENTRY_ID;
        } catch {
          rootId = MAIN_ENTRY_ID;
        }
      }
      setStatus(
        `walking ${rootLabel} neighborhood at depth ${depth} in '${dbName}'...`,
      );
      // The neighborhood endpoint seeds by internal id(n), not by the
      // graphify id property; resolve the seed first.
      let eidResp = await api.executeCypherOnDatabase(
        dbName,
        `MATCH (n {id: $graphifyId}) RETURN id(n) AS internalId LIMIT 1`,
        { graphifyId: rootId },
      );
      let eidRows = rowsFromCypher(eidResp);
      if (eidRows[0]?.internalId == null) {
        // Artifacts without the canonical main() entry: root at the first
        // node in the database instead.
        const anyResp = await api.executeCypherOnDatabase(
          dbName,
          `MATCH (n) WHERE n.id IS NOT NULL RETURN n.id AS id, n.label AS label LIMIT 1`,
        );
        const anyRows = rowsFromCypher(anyResp);
        if (anyRows[0]?.id != null) {
          rootId = String(anyRows[0].id);
          rootLabel =
            anyRows[0].label != null
              ? String(anyRows[0].label)
              : rootLabel;
          eidResp = await api.executeCypherOnDatabase(
            dbName,
            `MATCH (n {id: $graphifyId}) RETURN id(n) AS internalId LIMIT 1`,
            { graphifyId: rootId },
          );
          eidRows = rowsFromCypher(eidResp);
        }
      }
      const seedInternalId =
        eidRows[0]?.internalId != null
          ? String(eidRows[0].internalId)
          : rootId;
      const hood = await api.getGraphNeighborhood({
        nodeIds: [seedInternalId],
        depth,
        limit: NEIGHBORHOOD_LIMIT,
        labels: includeLabels,
        relationshipTypes: includeEdgeTypes,
        includeProperties: includeProps,
        excludeLabels,
        excludeRelationshipTypes: excludeEdgeTypes,
        excludeProperties: excludeProps,
        direction: "both",
        database: dbName,
      });
      // Payload node ids are internal ids; keep the graphify id property as
      // the public node identity and translate the edge endpoints.
      const internalToPublic = new Map<string, string>();
      const publicToInternal = new Map<string, string>();
      const rawNodes: ArtifactNode[] = hood.nodes.map((payload) => {
        const publicId =
          payload.properties.id != null
            ? String(payload.properties.id)
            : payload.id;
        internalToPublic.set(payload.id, publicId);
        publicToInternal.set(publicId, payload.id);
        return {
          id: publicId,
          label: String(payload.properties.label ?? publicId),
          file_type: String(payload.properties.file_type ?? "Entity"),
          source_file:
            typeof payload.properties.source_file === "string"
              ? payload.properties.source_file
              : undefined,
          source_location:
            typeof payload.properties.source_location === "string"
              ? payload.properties.source_location
              : undefined,
          node_kind:
            typeof payload.properties.node_kind === "string"
              ? payload.properties.node_kind
              : undefined,
          body:
            typeof payload.properties.body === "string"
              ? payload.properties.body
              : undefined,
        };
      });
      const rawLinks: ArtifactLink[] = hood.edges.map((edge) => ({
        source: internalToPublic.get(edge.source) ?? edge.source,
        target: internalToPublic.get(edge.target) ?? edge.target,
        relation: edge.type,
      }));
      // Test-file nodes (and their edges) are filtered out unless "show test
      // files" is checked. The root stays visible so a re-root into test code
      // never empties the graph. Nodes and clusters left unconnected by the
      // removed test links (orphans) are pruned too.
      let visibleNodes = rawNodes;
      let visibleLinks = rawLinks;
      if (!showTests) {
        const visible = new Set<string>([rootId]);
        for (const node of rawNodes) {
          if (!isTestSource(node.source_file)) visible.add(node.id);
        }
        const links = rawLinks.filter(
          (link) => visible.has(link.source) && visible.has(link.target),
        );
        // Keep only the root's connected component of the remaining graph.
        const adjacency = new Map<string, string[]>();
        for (const node of rawNodes) {
          if (visible.has(node.id)) adjacency.set(node.id, []);
        }
        for (const link of links) {
          adjacency.get(link.source)?.push(link.target);
          adjacency.get(link.target)?.push(link.source);
        }
        const reachable = new Set<string>();
        const stack = [rootId];
        while (stack.length > 0) {
          const current = stack.pop();
          if (current === undefined || reachable.has(current)) continue;
          reachable.add(current);
          for (const next of adjacency.get(current) ?? []) stack.push(next);
        }
        visibleNodes = rawNodes.filter((node) => reachable.has(node.id));
        visibleLinks = links.filter(
          (link) => reachable.has(link.source) && reachable.has(link.target),
        );
      }
      buildGraph(visibleNodes, visibleLinks, rootId);
      publicToInternalRef.current = publicToInternal;
      setSource({ kind: "db", name: dbName });
      setComponents(hood.components ?? []);
      setActiveComponent(null);
      // Auto-select the root (or the newly chosen re-root symbol) so its
      // details pane opens immediately after every (re)load.
      const focusId = customRootRef.current?.id ?? rootId;
      const focusNode = nodeByIdRef.current.get(focusId);
      if (focusNode) {
        selectedIdRef.current = focusId;
        void selectNode(focusId);
      } else {
        selectedIdRef.current = null;
        setSelected(null);
      }
      setStatus(
        `rooted at ${rootLabel} · depth ${depth} · ${visibleNodes.length} nodes · ${visibleLinks.length} links` +
          (!showTests ? " · tests hidden" : " · tests shown") +
          (hood.components && hood.components.length > 1
            ? ` · ${hood.components.length} components`
            : "") +
          (hood.meta?.truncated ? " (truncated)" : ""),
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(`Failed to load '${dbName}': ${message}`);
      setStatus("load failed");
    } finally {
      setLoading(false);
    }
  }, [
    database,
    customRoot,
    depth,
    showTests,
    buildGraph,
    selectNode,
    includeLabels,
    includeEdgeTypes,
    includeProps,
    excludeLabels,
    excludeEdgeTypes,
    excludeProps,
  ]);

  const ingestArtifact = useCallback(async () => {
    if (!uploadFile) return;
    const name = uploadDbName.trim();
    const validationError = validateUploadDatabaseName(name, databases, updateExisting);
    if (validationError) {
      setUploadError(validationError);
      return;
    }
    setUploadError(null);
    setError(null);
    setLoading(true);
    const report = (label: string, percent: number) => {
      setStatus(label);
      setUploadProgress({ label, percent });
    };
    try {
      report("parsing artifact...", 2);
      const parsed = JSON.parse(await uploadFile.text()) as {
        nodes?: ArtifactNode[];
        links?: ArtifactLink[];
        edges?: ArtifactLink[];
      };
      const artifactNodes = parsed.nodes ?? [];
      const rawLinks = parsed.links ?? parsed.edges ?? [];
      const idToFileType = new Map<string, string>();
      const known = new Set<string>();
      for (const node of artifactNodes) {
        const id = String(node.id);
        known.add(id);
        idToFileType.set(id, String(node.file_type ?? "Entity"));
      }
      const missing = new Set<string>();
      for (const link of rawLinks) {
        if (!known.has(String(link.source))) missing.add(String(link.source));
        if (!known.has(String(link.target))) missing.add(String(link.target));
      }
      const total = artifactNodes.length + missing.size + rawLinks.length || 1;
      let done = 0;

      report(`ensuring database '${name}'...`, 4);
      const existing = await api.listDatabaseNames();
      if (!existing.includes(name)) {
        await api.createDatabase(name);
      }

      // Group nodes by label; create the id indexes before the first MERGE.
      const byLabel = new Map<string, ArtifactNode[]>();
      for (const node of artifactNodes) {
        const label = graphifyLabel(node.file_type);
        const group = byLabel.get(label) ?? [];
        group.push(node);
        byLabel.set(label, group);
      }
      const labels = new Set(byLabel.keys());
      if (missing.size > 0) labels.add("Entity");
      for (const label of labels) {
        await api.executeCypherOnDatabase(
          name,
          `CREATE INDEX graphify_${label.toLowerCase()}_id IF NOT EXISTS FOR (n:${label}) ON (n.id)`,
        );
      }

      const scalar = (data: Record<string, unknown>): Record<string, unknown> =>
        Object.fromEntries(
          Object.entries(data).filter(
            ([key, value]) =>
              !key.startsWith("_") &&
              (typeof value === "string" ||
                typeof value === "number" ||
                typeof value === "boolean"),
          ),
        );

      for (const [label, group] of byLabel) {
        for (const batch of chunk(group, 2000)) {
          const rows = batch.map((node) => {
            const props: Record<string, unknown> = {
              ...scalar(node as unknown as Record<string, unknown>),
              id: String(node.id),
              updated_at:
                typeof node.updated_at === "number"
                  ? node.updated_at
                  : Date.now() / 1000,
            };
            if (typeof node.body === "string") {
              props.body = node.body;
            }
            props.props_hash = contentHash(props);
            return { id: String(node.id), props };
          });
          // Create missing nodes with their full properties...
          await api.executeCypherOnDatabase(
            name,
            `UNWIND $rows AS row MERGE (n:${label} {id: row.id}) ON CREATE SET n += row.props`,
            { rows },
          );
          // ...then gently update only nodes whose content hash differs from
          // the artifact copy, so unchanged nodes are never rewritten (and
          // never re-embedded).
          await api.executeCypherOnDatabase(
            name,
            `UNWIND $rows AS row MATCH (n:${label} {id: row.id}) ` +
              `WHERE n.props_hash IS NULL OR n.props_hash <> row.props_hash ` +
              `SET n += row.props`,
            { rows },
          );
          done += batch.length;
          report(
            `ingesting nodes ${Math.min(done, artifactNodes.length)}/${artifactNodes.length}`,
            5 + Math.round((85 * done) / total),
          );
        }
      }

      if (missing.size > 0) {
        const rows = Array.from(missing).map((id) => {
          const props: Record<string, unknown> = {
            id,
            updated_at: Date.now() / 1000,
          };
          props.props_hash = contentHash(props);
          return { id, props };
        });
        await api.executeCypherOnDatabase(
          name,
          `UNWIND $rows AS row MERGE (n:Entity {id: row.id}) ON CREATE SET n += row.props`,
          { rows },
        );
        await api.executeCypherOnDatabase(
          name,
          `UNWIND $rows AS row MATCH (n:Entity {id: row.id}) ` +
            `WHERE n.props_hash IS NULL OR n.props_hash <> row.props_hash ` +
            `SET n += row.props`,
          { rows },
        );
        done += rows.length;
      }

      // Group links by (source label, target label, relation) and ingest.
      const linkGroups = new Map<
        string,
        { sl: string; tl: string; rel: string; rows: Array<{ src: string; tgt: string; props: Record<string, unknown> }> }
      >();
      for (const link of rawLinks) {
        const sl = graphifyLabel(idToFileType.get(String(link.source)));
        const tl = graphifyLabel(idToFileType.get(String(link.target)));
        const rel = sanitizeRelation(String(link.relation ?? "RELATED_TO"));
        const key = `${sl}|${tl}|${rel}`;
        const group = linkGroups.get(key) ?? { sl, tl, rel, rows: [] };
        const props: Record<string, unknown> = {};
        for (const [propKey, value] of Object.entries(
          link as unknown as Record<string, unknown>,
        )) {
          if (
            propKey === "source" ||
            propKey === "target" ||
            propKey.startsWith("_")
          ) {
            continue;
          }
          if (
            typeof value === "string" ||
            typeof value === "number" ||
            typeof value === "boolean"
          ) {
            props[propKey] = value;
          }
        }
        props.props_hash = contentHash(props);
        group.rows.push({
          src: String(link.source),
          tgt: String(link.target),
          props,
        });
        linkGroups.set(key, group);
      }
      for (const group of linkGroups.values()) {
        for (const batch of chunk(group.rows, 2000)) {
          // Single MERGE keeps edge writes on the engine's batched
          // UNWIND-MERGE fast path. Edges carry no embeddings, so in-place
          // rewrites are cheap (nodes stay hash-guarded).
          await api.executeCypherOnDatabase(
            name,
            `UNWIND $rows AS row MATCH (a:${group.sl} {id: row.src}), (b:${group.tl} {id: row.tgt}) ` +
              `MERGE (a)-[r:${group.rel}]->(b) SET r += row.props`,
            { rows: batch },
          );
          done += batch.length;
          report(
            `ingesting links ${Math.min(done - artifactNodes.length - missing.size, rawLinks.length)}/${rawLinks.length}`,
            5 + Math.round((85 * done) / total),
          );
        }
      }

      // Incremental sync: delete stale edges and nodes that no longer
      // appear in the artifact, scoped to importer-managed labels.
      report("syncing deletions...", 92);
      const managedLabels = Array.from(labels);
      if (managedLabels.length > 0) {
        const labelClause = managedLabels.map((label) => `n:${label}`).join(" OR ");
        const incomingIds = new Set<string>([...known, ...missing]);
        const existingResp = await api.executeCypherOnDatabase(
          name,
          `MATCH (n) WHERE n.id IS NOT NULL AND (${labelClause}) RETURN n.id AS id`,
        );
        const existingIds = new Set(
          rowsFromCypher(existingResp).map((row) => String(row.id)),
        );
        const staleIds = Array.from(existingIds).filter((id) => !incomingIds.has(id));

        const incomingEdges = new Set<string>(
          rawLinks.map((link) => {
            const rel = sanitizeRelation(String(link.relation ?? "RELATED_TO"));
            return `${String(link.source)}|${String(link.target)}|${rel}`;
          }),
        );
        // Only relationships between importer-managed nodes are candidates;
        // edges touching unrelated data are never considered stale.
        const aClause = managedLabels.map((label) => `a:${label}`).join(" OR ");
        const bClause = managedLabels.map((label) => `b:${label}`).join(" OR ");
        const edgesResp = await api.executeCypherOnDatabase(
          name,
          `MATCH (a)-[r]->(b) WHERE a.id IS NOT NULL AND b.id IS NOT NULL ` +
            `AND (${aClause}) AND (${bClause}) ` +
            `RETURN a.id AS src, b.id AS tgt, type(r) AS rel`,
        );
        const staleEdges = rowsFromCypher(edgesResp)
          .map((row) => ({
            src: String(row.src),
            tgt: String(row.tgt),
            rel: String(row.rel),
          }))
          .filter((edge) => !incomingEdges.has(`${edge.src}|${edge.tgt}|${edge.rel}`));

        for (const batch of chunk(staleEdges, 1000)) {
          await api.executeCypherOnDatabase(
            name,
            `UNWIND $rows AS row MATCH (a {id: row.src})-[r]->(b {id: row.tgt}) ` +
              `WHERE type(r) = row.rel DELETE r`,
            { rows: batch.map((edge) => ({ src: edge.src, tgt: edge.tgt, rel: edge.rel })) },
          );
        }
        for (const batch of chunk(staleIds, 1000)) {
          await api.executeCypherOnDatabase(
            name,
            `UNWIND $ids AS id MATCH (n) WHERE n.id = id AND (${labelClause}) DETACH DELETE n`,
            { ids: batch },
          );
        }
        if (staleIds.length > 0 || staleEdges.length > 0) {
          report(
            `sync removed ${staleIds.length} stale nodes and ${staleEdges.length} stale edges`,
            94,
          );
        }
      }

      report("ingestion complete — loading tree...", 96);
      const names = await api.listDatabaseNames();
      setDatabases(names);
      setDatabase(name);
      customRootRef.current = null;
      setCustomRoot(null);
      setUploadOpen(false);
      setUploadFile(null);
      setUploadProgress(null);
      // Automatically load the database exactly like the Load tree button.
      await loadFromDatabase(name);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(`Ingestion failed: ${message}`);
      setStatus("ingestion failed");
      setUploadProgress(null);
    } finally {
      setLoading(false);
    }
  }, [uploadFile, uploadDbName, databases, updateExisting, loadFromDatabase]);

  // --- Search ---------------------------------------------------------------

  // --- Semantic search & similar arms ---------------------------------------

  const resolveInternalId = useCallback(
    async (publicId: string): Promise<string | null> => {
      const cached = publicToInternalRef.current.get(publicId);
      if (cached) return cached;
      if (!database) return null;
      const resp = await api.executeCypherOnDatabase(
        database,
        `MATCH (n {id: $graphifyId}) RETURN id(n) AS internalId LIMIT 1`,
        { graphifyId: publicId },
      );
      const rows = rowsFromCypher(resp);
      const internalId =
        rows[0]?.internalId != null ? String(rows[0].internalId) : null;
      if (internalId) {
        publicToInternalRef.current.set(publicId, internalId);
      }
      return internalId;
    },
    [database],
  );

  const mergeIntoGraph = useCallback((nodes: GNode[], links: GLink[]) => {
    const fg = graphRef.current;
    if (!fg) return;
    const live = fg.graphData();
    const byId = nodeByIdRef.current;
    for (const node of nodes) {
      if (!byId.has(node.id)) {
        byId.set(node.id, node);
        live.nodes.push(node);
      }
    }
    const seenKeys = new Set<string>(
      live.links.map((l) => {
        const s = typeof l.source === "object" ? l.source.id : String(l.source);
        const t = typeof l.target === "object" ? l.target.id : String(l.target);
        return `${s}|${t}|${l.relation}`;
      }),
    );
    for (const link of links) {
      const s = typeof link.source === "object" ? link.source.id : String(link.source);
      const t = typeof link.target === "object" ? link.target.id : String(link.target);
      const key = `${s}|${t}|${link.relation}`;
      if (!seenKeys.has(key)) {
        seenKeys.add(key);
        live.links.push(link);
      }
    }
    // Keep the top-down orientation valid after new arms are spawned, then
    // hand the merged data to the renderer once.
    orientGraph(fg, live.links, live.nodes, rootIdRef.current);
    fg.graphData(live);
    // Re-apply the accessors so node/link color changes (e.g. newly
    // flagged similar nodes) repaint immediately.
    fg.nodeColor(fg.nodeColor());
    fg.linkColor(fg.linkColor());
    fg.linkWidth(fg.linkWidth());
    setLoadedLinks(live.links.length);
  }, []);

  const findSimilarArm = useCallback(async () => {
    if (!selected || !database) return;
    setSimilarLoading(true);
    setSimilarCount(null);
    setStatus(`finding nodes similar to ${selected.label}...`);
    try {
      const internalId = await resolveInternalId(selected.id);
      if (!internalId) {
        setError(`could not resolve an internal id for ${selected.id}`);
        return;
      }
      const results = await api.findSimilar(internalId, 10, database);
      const addedNodes: GNode[] = [];
      const links: GLink[] = [];
      let added = 0;
      for (const result of results) {
        const props = result.node?.properties ?? {};
        const publicId =
          props.id != null ? String(props.id) : String(result.node.id);
        if (!publicId || publicId === selected.id) {
          continue;
        }
        const internalIdOfHit = String(result.node.id);
        publicToInternalRef.current.set(publicId, internalIdOfHit);
        let node = nodeByIdRef.current.get(publicId);
        if (!node) {
          node = {
            id: publicId,
            label: String(props.label ?? publicId),
            fileType: String(props.file_type ?? "Entity"),
            sourceFile:
              typeof props.source_file === "string"
                ? props.source_file
                : undefined,
            sourceLocation:
              typeof props.source_location === "string"
                ? props.source_location
                : undefined,
            nodeKind:
              typeof props.node_kind === "string"
                ? props.node_kind
                : undefined,
            body: typeof props.body === "string" ? props.body : undefined,
            degree: 0,
            highlight: false,
            selected: false,
          };
          addedNodes.push(node);
        }
        if (!node.body && typeof props.body === "string") {
          node.body = props.body;
        }
        // Existing nodes are re-flagged so the whole arm reads as one
        // visually distinct cluster; new nodes join the graph in pink.
        node.similar = true;
        node.internalId = internalIdOfHit;
        links.push({
          source: selected.id,
          target: publicId,
          relation: "SIMILAR",
          highlight: false,
        });
        added += 1;
      }
      mergeIntoGraph(addedNodes, links);
      setSimilarCount(added);
      setStatus(
        `${selected.label}: ${added} similar nodes spawned (rrf vector similarity)` +
          (addedNodes.length > 0 ? ` · ${addedNodes.length} new` : " · already in graph, re-flagged"),
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(`similar search failed: ${message}`);
      setStatus("similar search failed");
    } finally {
      setSimilarLoading(false);
    }
  }, [selected, database, resolveInternalId, mergeIntoGraph]);

  const onSearch = useCallback(
    (value: string) => {
      setSearch(value);
      if (searchTimerRef.current != null) {
        window.clearTimeout(searchTimerRef.current);
      }
      if (value.trim().length < 2) {
        setSearchResults([]);
        return;
      }
      searchTimerRef.current = window.setTimeout(async () => {
        try {
          const results = await api.searchNodes(value.trim(), 12, database || undefined);
          setSearchResults(
            results.map((r) => {
              const props = r.node?.properties ?? {};
              return {
                id: String(props.id ?? r.node.id),
                label: String(props.label ?? props.id ?? r.node.id),
                sourceFile:
                  typeof props.source_file === "string"
                    ? props.source_file
                    : undefined,
                score: r.rrf_score ?? r.score,
              };
            }),
          );
        } catch {
          setSearchResults([]);
        }
      }, 250);
    },
    [database],
  );

  const focusSearchResult = useCallback(
    async (result: { id: string; label: string; sourceFile?: string; score?: number }) => {
      const existing = nodeByIdRef.current.get(result.id);
      if (existing) {
        focusNode(existing.id);
        return;
      }
      // Spawn the hit into the graph and select it.
      const node: GNode = {
        id: result.id,
        label: result.label,
        fileType: "Entity",
        sourceFile: result.sourceFile,
        degree: 0,
        highlight: false,
        selected: false,
      };
      mergeIntoGraph([node], []);
      focusNode(node.id);
    },
    [focusNode, mergeIntoGraph],
  );

  // --- Legend / stats -------------------------------------------------------

  const nodeCount = nodeByIdRef.current.size;
  const selectableDatabases = databases.filter((d) => d !== "system");

  // Dev-only diagnostic hook for orientation checks.
  useEffect(() => {
    if (!import.meta.env.DEV) return;
    (window as unknown as {
      __graphifyPositions?: () => Array<{ id: string; y: number }>;
    }).__graphifyPositions = () =>
      Array.from(nodeByIdRef.current.values()).map((n) => ({
        id: n.id,
        y: Math.round(n.y ?? 0),
      }));
    return () => {
      delete (window as unknown as {
        __graphifyPositions?: () => Array<{ id: string; y: number }>;
      }).__graphifyPositions;
    };
  }, []);

  return (
    <div className="relative w-screen h-screen overflow-hidden bg-[#04060c] text-norse-silver font-display">
      <div ref={containerRef} className="absolute inset-0" />

      {/* Title strip */}
      <div className="absolute top-4 left-4 z-10 pointer-events-none select-none">
        <div className="flex items-baseline gap-3">
          <span className="text-2xl font-semibold tracking-wide text-white">
            NornicDB
          </span>
          <span className="text-nornic-accent text-sm uppercase tracking-[0.3em]">
            Code Graph
          </span>
        </div>
        <div className="mt-1 text-xs text-norse-silver/70 max-w-md">
          {source
            ? `${source.kind === "db" ? `database: ${source.name}` : source.name} · ${nodeCount} nodes · ${loadedLinks} links`
            : "graphify tree explorer"}
        </div>
        <div className="mt-1 flex items-center gap-2 text-[10px] text-norse-silver/50 font-mono flex-wrap">
          {KIND_LEGEND.map(([kind, color]) => (
            <span key={kind} className="inline-flex items-center gap-1">
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{ backgroundColor: color }}
              />
              {kind}
            </span>
          ))}
        </div>
      </div>

      {/* Controls (top-right) */}
      <div className="absolute top-4 right-4 z-10 w-80 max-h-[calc(100vh-4rem)] overflow-y-auto rounded-lg border border-sky-500/30 bg-norse-shadow/85 backdrop-blur px-4 py-3 shadow-[0_0_24px_rgba(56,189,248,0.15)]">
        <div className="flex items-baseline justify-between">
          <span className="text-xs uppercase tracking-[0.25em] text-sky-300">
            Graph Source
          </span>
          <span className="text-[10px] text-norse-silver/60 font-mono">graphify</span>
        </div>

        <div className="mt-3 flex items-center gap-2">
          <select
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            className="flex-1 rounded border border-norse-rune bg-norse-night px-2 py-1.5 text-xs text-norse-silver focus:outline-none focus:border-sky-400"
          >
            {selectableDatabases.length === 0 && (
              <option value="">no databases</option>
            )}
            {selectableDatabases.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
          <button
            onClick={() => void loadFromDatabase()}
            disabled={!database || loading}
            className="rounded bg-sky-500/90 hover:bg-sky-400 text-slate-950 text-xs font-semibold px-3 py-1.5 disabled:opacity-40"
          >
            {loading ? "loading…" : "Load tree"}
          </button>
        </div>

        <div className="mt-2 flex items-center gap-2">
          <label className="flex items-center gap-1 text-xs text-norse-silver/70">
            depth
            <input
              type="number"
              min={1}
              max={20}
              value={depth}
              onChange={(e) => {
                const parsed = Number(e.target.value);
                setDepth(Number.isFinite(parsed) ? Math.min(20, Math.max(1, Math.round(parsed))) : depth);
              }}
              className="w-14 rounded border border-norse-rune bg-norse-night px-2 py-1.5 text-xs text-norse-silver focus:outline-none focus:border-sky-400"
            />
          </label>
          <label className="flex items-center gap-1 text-xs text-norse-silver/80 select-none cursor-pointer">
            <input
              type="checkbox"
              checked={showTests}
              onChange={(e) => setShowTests(e.target.checked)}
              className="mr-0.5"
            />
            show test files
          </label>
          <button
            onClick={() => {
              customRootRef.current = null;
              setCustomRoot(null);
              setTimeout(() => void loadFromDatabase(), 0);
            }}
            disabled={loading}
            title="re-root at the main entry of the graphify graph"
            className="flex-1 rounded border border-norse-rune bg-norse-night px-2 py-1.5 text-xs text-norse-silver/80 hover:border-sky-400 disabled:opacity-40"
          >
            ↺ main entry
          </button>
        </div>

        <div className="mt-3 border-t border-norse-rune/60 pt-2">
          <div className="flex items-baseline justify-between">
            <span className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/60">
              Filters
            </span>
            <span className="text-[10px] text-norse-silver/50 font-mono">
              include · exclude
            </span>
          </div>
          <div className="mt-2 space-y-2">
            <FilterChipList
              label="include labels"
              entries={includeLabels}
              onAdd={setIncludeLabels}
              onRemove={(entry) =>
                setIncludeLabels(includeLabels.filter((item) => item !== entry))
              }
              placeholder="e.g. Code"
            />
            <FilterChipList
              label="exclude labels"
              entries={excludeLabels}
              onAdd={setExcludeLabels}
              onRemove={(entry) =>
                setExcludeLabels(excludeLabels.filter((item) => item !== entry))
              }
              placeholder="e.g. Context"
            />
            <FilterChipList
              label="include edge types"
              entries={includeEdgeTypes}
              onAdd={setIncludeEdgeTypes}
              onRemove={(entry) =>
                setIncludeEdgeTypes(
                  includeEdgeTypes.filter((item) => item !== entry),
                )
              }
              placeholder="e.g. CALLS"
            />
            <FilterChipList
              label="exclude edge types"
              entries={excludeEdgeTypes}
              onAdd={setExcludeEdgeTypes}
              onRemove={(entry) =>
                setExcludeEdgeTypes(
                  excludeEdgeTypes.filter((item) => item !== entry),
                )
              }
              placeholder="e.g. IMPORTS"
            />
            <FilterChipList
              label="include properties"
              entries={includeProps}
              onAdd={setIncludeProps}
              onRemove={(entry) =>
                setIncludeProps(includeProps.filter((item) => item !== entry))
              }
              placeholder="e.g. Code.entry or key:value"
            />
            <FilterChipList
              label="exclude properties"
              entries={excludeProps}
              onAdd={setExcludeProps}
              onRemove={(entry) =>
                setExcludeProps(excludeProps.filter((item) => item !== entry))
              }
              placeholder="e.g. context.Context or key:value"
            />
          </div>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={() => void loadFromDatabase()}
              disabled={!database || loading}
              className="flex-1 rounded bg-sky-500/90 hover:bg-sky-400 text-slate-950 text-xs font-semibold px-2 py-1.5 disabled:opacity-40"
            >
              apply filters
            </button>
            <button
              type="button"
              onClick={() => {
                setIncludeLabels([]);
                setIncludeEdgeTypes([...CALL_RELATION_TYPES]);
                setIncludeProps([]);
                setExcludeLabels([]);
                setExcludeEdgeTypes([]);
                setExcludeProps([]);
              }}
              disabled={loading}
              className="rounded border border-norse-rune bg-norse-night px-2 py-1.5 text-xs text-norse-silver/80 hover:border-sky-400 disabled:opacity-40"
            >
              reset
            </button>
          </div>
          <div className="mt-1.5 text-[10px] text-norse-silver/50 font-mono leading-snug">
            properties: dotted paths "key", "Label.key", "Type.key",
            optionally ":value" to match the value, or a bare dotted symbol
            name to hide it — exclusions may split the graph into components
          </div>
        </div>

        <div className="mt-2 flex items-center gap-2">
          <button
            onClick={() => {
              setUploadOpen(true);
              setUploadError(null);
              setUploadProgress(null);
              if (!uploadDbName.trim()) {
                setUploadDbName("nornicdbcode");
              }
            }}
            className="flex-1 text-center rounded border border-dashed border-norse-rune bg-norse-night/60 px-2 py-1.5 text-xs text-norse-silver/70 hover:border-sky-400 cursor-pointer"
          >
            upload graphify artifact
          </button>
        </div>

        <div className="mt-2 text-[10px] text-norse-silver/50 font-mono leading-snug">
          starts at the graphify main entry · click a symbol and use
          “start graph from this symbol” to re-root · drag to rotate · scroll
          to zoom · right-drag to pan
        </div>
      </div>

      {/* Upload dialog */}
      {uploadOpen && (
        <div className="fixed inset-0 z-20 flex items-center justify-center bg-black/60">
          <div className="w-96 rounded-lg border border-sky-500/40 bg-norse-shadow/95 backdrop-blur p-4 shadow-[0_0_24px_rgba(56,189,248,0.2)]">
            <div className="text-xs uppercase tracking-[0.25em] text-sky-300">
              Ingest graphify artifact
            </div>
            <label className="mt-3 block cursor-pointer rounded border border-dashed border-norse-rune bg-norse-night/60 px-3 py-2 text-xs text-norse-silver/70 hover:border-sky-400">
              {uploadFile ? uploadFile.name : "choose graph.json artifact"}
              <input
                type="file"
                accept=".json,application/json"
                className="hidden"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) {
                    setUploadFile(file);
                    setUploadDbName((previous) => {
                      const derived =
                        previous ||
                        file.name
                          .replace(/\.json$/i, "")
                          .replace(/[^A-Za-z0-9_]/g, "");
                      setUploadError(
                        validateUploadDatabaseName(derived, databases, updateExisting),
                      );
                      return derived;
                    });
                  }
                  e.target.value = "";
                }}
              />
            </label>
            <div className="mt-3">
              <label className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/50">
                database name
              </label>
              <input
                type="text"
                value={uploadDbName}
                onChange={(e) => {
                  setUploadDbName(e.target.value);
                  setUploadError(
                    validateUploadDatabaseName(e.target.value, databases, updateExisting),
                  );
                }}
                className={`mt-1 w-full rounded border bg-norse-night px-2 py-1.5 text-xs text-norse-silver focus:outline-none ${
                  uploadError
                    ? "border-red-500"
                    : "border-norse-rune focus:border-sky-400"
                }`}
              />
              {uploadError && (
                <div className="mt-1 text-[11px] text-red-400">{uploadError}</div>
              )}
              {databases.includes(uploadDbName.trim()) && (
                <label className="mt-2 flex items-start gap-2 text-xs text-norse-silver/80">
                  <input
                    type="checkbox"
                    checked={updateExisting}
                    onChange={(e) => {
                      setUpdateExisting(e.target.checked);
                      setUploadError(
                        validateUploadDatabaseName(
                          uploadDbName,
                          databases,
                          e.target.checked,
                        ),
                      );
                    }}
                    className="mt-0.5"
                  />
                  <span>
                    update existing database — sync creates new nodes, updates
                    changed ones, and deletes stale nodes/edges without
                    dropping unrelated data
                  </span>
                </label>
              )}
            </div>
            {uploadProgress && (
              <div className="mt-3">
                <div className="h-1.5 rounded-full bg-norse-rune/50 overflow-hidden">
                  <div
                    className="h-full bg-sky-400 transition-all duration-200"
                    style={{ width: `${Math.min(100, Math.max(0, uploadProgress.percent))}%` }}
                  />
                </div>
                <div className="mt-1 text-[10px] text-norse-silver/60 font-mono truncate">
                  {uploadProgress.label}
                </div>
              </div>
            )}
            <div className="mt-4 flex justify-end gap-2">
              <button
                onClick={() => {
                  if (!loading) {
                    setUploadOpen(false);
                    setUploadError(null);
                  }
                }}
                disabled={loading}
                className="rounded border border-norse-rune bg-norse-night px-3 py-1.5 text-xs text-norse-silver/80 hover:border-sky-400 disabled:opacity-40"
              >
                cancel
              </button>
              <button
                onClick={() => void ingestArtifact()}
                disabled={!uploadFile || !!uploadError || loading}
                className="rounded bg-sky-500/90 hover:bg-sky-400 text-slate-950 text-xs font-semibold px-3 py-1.5 disabled:opacity-40"
              >
                {loading ? "ingesting…" : "ingest into database"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Search (top-center) */}
      <div className="absolute top-4 left-1/2 -translate-x-1/2 z-10 w-72">
        <input
          type="text"
          value={search}
          onChange={(e) => onSearch(e.target.value)}
          placeholder="search nodes…"
          className="w-full rounded border border-norse-rune bg-norse-shadow/85 backdrop-blur px-3 py-1.5 text-xs text-norse-silver placeholder:text-norse-silver/40 focus:outline-none focus:border-sky-400"
        />
        {searchResults.length > 0 && (
          <div className="mt-1 max-h-64 overflow-y-auto rounded border border-norse-rune bg-norse-shadow/95 backdrop-blur">
            {searchResults.map((result) => (
              <button
                key={result.id}
                onClick={() => void focusSearchResult(result)}
                className="block w-full text-left px-3 py-1.5 text-xs text-norse-silver hover:bg-norse-rune/40 truncate"
              >
                <span className="truncate">{result.label}</span>
                <span className="text-norse-silver/40 ml-1">
                  {result.sourceFile ?? ""}
                  {result.score != null && (
                    <span className="text-pink-300 ml-1">
                      rrf {result.score.toFixed(3)}
                    </span>
                  )}
                </span>
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Disconnected components (appear when exclusion filters fragment the
          graph) */}
      {components.length > 1 && (
        <div className="absolute bottom-20 right-4 z-10 w-72 rounded-lg border border-norse-rune bg-norse-shadow/85 backdrop-blur px-3 py-2">
          <div className="flex items-center justify-between">
            <span className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/60">
              disconnected components ({components.length})
            </span>
            {activeComponent != null && (
              <button
                type="button"
                onClick={() => setActiveComponent(null)}
                className="text-[10px] text-sky-300 hover:text-sky-200"
              >
                show all
              </button>
            )}
          </div>
          <div className="mt-1.5 max-h-40 overflow-y-auto space-y-1">
            {components.map((component, index) => (
              <button
                key={`${component.nodes[0]?.id ?? "empty"}-${index}`}
                type="button"
                onClick={() =>
                  setActiveComponent(activeComponent === index ? null : index)
                }
                className={`block w-full text-left rounded px-2 py-1 text-[11px] font-mono ${
                  activeComponent === index
                    ? "bg-sky-500/20 text-sky-200"
                    : "text-norse-silver hover:bg-norse-rune/40"
                }`}
              >
                {index + 1}. {component.meta.node_count} nodes ·{" "}
                {component.meta.edge_count} edges
                {activeComponent === index ? " ✓" : ""}
              </button>
            ))}
          </div>
          <div className="mt-1 text-[10px] text-norse-silver/50">
            selecting a component dims the rest and fits the camera
          </div>
        </div>
      )}

      {/* Status bar (bottom) */}
      <div className="absolute bottom-4 left-4 right-4 z-10 flex items-center gap-4 rounded-lg border border-norse-rune bg-norse-shadow/80 backdrop-blur px-4 py-3">
        <div
          className={`w-2.5 h-2.5 rounded-full ${
            loading ? "bg-frost-glacier animate-pulse" : error ? "bg-red-500" : "bg-nornic-primary status-connected"
          }`}
        />
        <div className="flex-1 min-w-0 text-xs text-norse-silver/80 truncate font-mono">
          {status}
        </div>
        {uploadProgress && (
          <div className="w-44 h-1.5 rounded-full bg-norse-rune/50 overflow-hidden shrink-0" title={uploadProgress.label}>
            <div
              className="h-full bg-sky-400 transition-all duration-200"
              style={{ width: `${Math.min(100, Math.max(0, uploadProgress.percent))}%` }}
            />
          </div>
        )}
        {error && (
          <button
            onClick={() => setError(null)}
            className="text-xs text-red-300 hover:text-red-200"
          >
            dismiss
          </button>
        )}
      </div>

      {/* Detail panel (right, below the controls, when a node is selected) */}
      {selected && (
        <div
          ref={panelRef}
          style={
            panelPos
              ? {
                  left: panelPos.left,
                  top: panelPos.top,
                  maxHeight: `calc(100vh - ${Math.max(0, panelPos.top)}px - 24px)`,
                }
              : undefined
          }
          className={`absolute z-10 w-96 max-w-[calc(100vw-2rem)] rounded-lg border border-purple-500/30 bg-norse-shadow/90 backdrop-blur overflow-y-auto shadow-[0_0_24px_rgba(168,85,247,0.15)] ${
            panelPos ? "" : "top-[16.5rem] right-4 max-h-[calc(100vh-18rem)]"
          }`}
        >
          <div
            onPointerDown={(e) => {
              // Ignore drags that start on the close button so its click fires.
              if ((e.target as HTMLElement).closest("button")) return;
              const panel = panelRef.current;
              if (!panel) return;
              const rect = panel.getBoundingClientRect();
              dragRef.current = {
                startX: e.clientX,
                startY: e.clientY,
                left: rect.left,
                top: rect.top,
              };
              (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
            }}
            onPointerMove={(e) => {
              const drag = dragRef.current;
              if (!drag) return;
              const left = Math.min(
                Math.max(0, drag.left + e.clientX - drag.startX),
                Math.max(0, window.innerWidth - 160),
              );
              const top = Math.min(
                Math.max(0, drag.top + e.clientY - drag.startY),
                Math.max(0, window.innerHeight - 160),
              );
              setPanelPos({ left, top });
            }}
            onPointerUp={() => {
              dragRef.current = null;
            }}
            onPointerCancel={() => {
              dragRef.current = null;
            }}
            className="sticky top-0 z-10 bg-norse-shadow/95 backdrop-blur px-4 py-3 border-b border-norse-rune/60 flex items-start justify-between cursor-grab active:cursor-grabbing touch-none"
          >
            <div className="min-w-0">
              <div className="text-sm font-semibold text-white truncate">
                {selected.label}
              </div>
              <div className="text-[10px] text-norse-silver/50 font-mono truncate">
                {selected.id}
              </div>
            </div>
            <button
              onClick={() => {
                selectedIdRef.current = null;
                setSelected(null);
                setBody(null);
                setBodySource(null);
                setNeighbors([]);
                applySelectionVisuals(null);
              }}
              className="ml-2 text-norse-silver/60 hover:text-white text-xs"
            >
              ✕
            </button>
          </div>

          <div className="px-4 py-3 text-xs space-y-2">
            <div className="grid grid-cols-[88px_1fr] gap-x-2 gap-y-1 font-mono">
              <span className="text-norse-silver/50">file_type</span>
              <span className="text-sky-300">{selected.fileType}</span>
              <span className="text-norse-silver/50">source</span>
              <span className="truncate">
                {selected.sourceFile ?? "—"}
                {selected.sourceLocation ? `:${selected.sourceLocation}` : ""}
              </span>
              {selected.nodeKind && (
                <>
                  <span className="text-norse-silver/50">kind</span>
                  <span className="truncate">{selected.nodeKind}</span>
                </>
              )}
              <span className="text-norse-silver/50">degree</span>
              <span>{selected.degree}</span>
            </div>

            {database && (
              <button
                onClick={() => {
                  const root = { id: selected.id, label: selected.label };
                  customRootRef.current = root;
                  setCustomRoot(root);
                  setTimeout(() => void loadFromDatabase(), 0);
                }}
                disabled={loading}
                className="w-full rounded bg-purple-500/90 hover:bg-purple-400 text-slate-950 text-xs font-semibold px-3 py-1.5 disabled:opacity-40"
              >
                ⭮ start graph from this symbol
              </button>
            )}

            {database && (
              <button
                onClick={() => void findSimilarArm()}
                disabled={similarLoading}
                className="w-full rounded bg-pink-500/90 hover:bg-pink-400 text-slate-950 text-xs font-semibold px-3 py-1.5 disabled:opacity-40"
              >
                {similarLoading
                  ? "searching similar…"
                  : `◈ find similar nodes${similarCount != null ? ` (${similarCount} spawned)` : ""}`}
              </button>
            )}

            <div>
              <div className="flex items-baseline justify-between">
                <span className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/50">
                  body{bodySource === "db" ? " (from database)" : bodySource === "artifact" ? " (in graph)" : ""}
                </span>
                {bodyLoading && (
                  <span className="text-[10px] text-purple-300 animate-pulse">
                    fetching…
                  </span>
                )}
              </div>
              {body ? (
                <pre className="mt-1 max-h-[40vh] overflow-auto rounded bg-norse-night/80 border border-norse-rune/50 p-3 text-[11px] leading-snug text-norse-silver whitespace-pre-wrap font-mono">
                  {body}
                </pre>
              ) : (
                <div className="mt-1 text-norse-silver/40">
                  no body available{bodyLoading ? "" : " (not in artifact or database)"}
                </div>
              )}
            </div>

            {neighbors.length > 0 && (
              <div>
                <div className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/50">
                  links ({neighbors.length} shown)
                </div>
                <div className="mt-1 max-h-56 overflow-y-auto">
                  {neighbors.map((n) => (
                    <button
                      key={`${n.id}|${n.relation}`}
                      onClick={() => focusNode(n.id)}
                      className="block w-full text-left px-2 py-1 rounded text-[11px] font-mono hover:bg-norse-rune/40"
                    >
                      <span className="text-purple-300">
                        {n.direction === "in" ? "← " : "→ "}
                      </span>
                      <span className="text-sky-300">{n.relation}</span>
                      <span className="text-norse-silver/50">
                        {n.direction === "in" ? " ← " : " → "}
                      </span>
                      <span className="text-norse-silver truncate">{n.label}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
