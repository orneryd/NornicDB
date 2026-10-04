import { useCallback, useEffect, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance } from "3d-force-graph";
import { api, type CypherResponse } from "../utils/api";

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
  internalId?: string;
  body?: string;
  x?: number;
  y?: number;
  z?: number;
  vx?: number;
  vy?: number;
  vz?: number;
}

interface GLink {
  source: string | GNode;
  target: string | GNode;
  relation: string;
  highlight: boolean;
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

const FILE_TYPE_COLORS: Record<string, string> = {
  code: "#38bdf8",
  document: "#fbbf24",
  entity: "#64748b",
};

const DEFAULT_NODE_COLOR = "#a78bfa";
const LINK_COLOR = "rgba(125,211,252,0.35)";
const LINK_HIGHLIGHT = "rgba(168,85,247,0.9)";
const HIGHLIGHT_COLOR = "#a855f7";
// Color for semantically similar nodes spawned as a new arm of the graph.
const SIMILAR_COLOR = "#f472b6";

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

// Custom d3-force that keeps nodes in their depth bands along y.
function layeredYForce(targets: Map<string, number>, strength: number) {
  let nodes: GNode[] = [];
  const force = (alpha: number) => {
    const scale = strength * alpha;
    for (const node of nodes) {
      const target = targets.get(node.id);
      if (target == null) continue;
      node.vy = (node.vy ?? 0) + (target - (node.y ?? 0)) * scale;
    }
  };
  force.initialize = (initialized: unknown[]) => {
    nodes = initialized as GNode[];
  };
  return force;
}

// Configure the layout orientation: DAG top-down when acyclic, otherwise
// a layered y-force keeps the root at the top of the scene.
function orientGraph(fg: GraphifyForceGraph, links: GLink[], nodes: GNode[], rootId: string | null): void {
  if (isAcyclic(links)) {
    setDagMode(fg, "td");
    fg.d3Force("layers", null);
    // Seed a radial spread by depth so the top-down tree is readable without
    // a long-running force simulation, then freeze the simulation (the dag
    // pins fy anyway) for fast, static rendering.
    const depths = computeDepths(links, rootId ?? undefined);
    const byDepth = new Map<number, GNode[]>();
    for (const node of nodes) {
      const depth = depths.get(node.id) ?? 0;
      const group = byDepth.get(depth) ?? [];
      group.push(node);
      byDepth.set(depth, group);
    }
    for (const [depth, group] of byDepth) {
      group.forEach((node, index) => {
        const angle = (2 * Math.PI * index) / group.length + depth * 0.35;
        const radius = 24 + depth * 26;
        node.x = Math.cos(angle) * radius;
        node.z = Math.sin(angle) * radius;
      });
    }
    fg.d3AlphaMin(0.9);
  } else {
    setDagMode(fg, null);
    const depths = computeDepths(links, rootId ?? undefined);
    const targets = depthTargets(depths, 72);
    for (const node of nodes) {
      const target = targets.get(node.id);
      if (target != null) {
        node.y = target;
        node.x = (Math.random() - 0.5) * 24;
        node.z = (Math.random() - 0.5) * 24;
      }
    }
    fg.d3Force("layers", layeredYForce(targets, 0.45) as unknown as (alpha: number) => void);
    // Settle the banded layout faster than the defaults.
    fg.d3AlphaDecay(0.05);
    fg.d3VelocityDecay(0.65);
    fg.d3AlphaMin(0.001);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function fileColor(fileType: string): string {
  return FILE_TYPE_COLORS[fileType.toLowerCase()] ?? DEFAULT_NODE_COLOR;
}

interface Neighbor {
  id: string;
  label: string;
  relation: string;
  direction: "in" | "out";
}

// Kahn's algorithm over the link set: dagMode needs an acyclic graph.
function isAcyclic(links: GLink[]): boolean {
  const outDegree = new Map<string, number>();
  const children = new Map<string, string[]>();
  const idOf = (end: string | GNode) => (typeof end === "object" ? end.id : end);
  for (const link of links) {
    const source = idOf(link.source);
    const target = idOf(link.target);
    if (source === target) continue;
    outDegree.set(source, (outDegree.get(source) ?? 0) + 1);
    const list = children.get(target) ?? [];
    list.push(source);
    children.set(target, list);
  }
  const queue: string[] = [];
  for (const end of new Set<string>(links.flatMap((l) => [idOf(l.source), idOf(l.target)]))) {
    if ((outDegree.get(end) ?? 0) === 0) queue.push(end);
  }
  let visited = 0;
  while (queue.length > 0) {
    const current = queue.pop() as string;
    visited += 1;
    for (const parent of children.get(current) ?? []) {
      const next = (outDegree.get(parent) ?? 1) - 1;
      outDegree.set(parent, next);
      if (next === 0) queue.push(parent);
    }
  }
  return visited >= new Set<string>(links.flatMap((l) => [idOf(l.source), idOf(l.target)])).size;
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
  const [depth, setDepth] = useState<number>(DEFAULT_DEPTH);
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
        if (!names.includes("graphify")) {
          setDatabase(names[0] ?? "");
          if (names.length > 0) {
            setStatus(
              "database 'graphify' not found — run scripts/graphify_local.py first, then load",
            );
          }
        } else {
          setDatabase("graphify");
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
        if (n.selected) return HIGHLIGHT_COLOR;
        if (n.highlight) return "#f0abfc";
        if (n.similar) return SIMILAR_COLOR;
        return fileColor(n.fileType);
      })
      .linkOpacity(0.4)
      .linkWidth((l) => (l.highlight ? 1.6 : 0.6))
      .linkColor((l) => (l.highlight ? LINK_HIGHLIGHT : LINK_COLOR))
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
          `${n.label}${n.sourceFile ? ` · ${n.sourceFile}${n.sourceLocation ? ":" + n.sourceLocation : ""}` : ""}`,
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

  const loadFromDatabase = useCallback(async () => {
    if (!database) return;
    setLoading(true);
    setError(null);
    try {
      // Rooted neighborhood: resolve the entry node (the main entry of the
      // graphify graph, or the symbol last chosen with "start graph from
      // this symbol"), then walk its neighborhood at the configured depth.
      let rootId = customRoot?.id ?? null;
      let rootLabel = customRoot?.label ?? "main()";
      if (!rootId) {
        setStatus("resolving main entry...");
        try {
          const mainResp = await api.executeCypherOnDatabase(
            database,
            MAIN_ENTRY_QUERY,
          );
          const mainRows = rowsFromCypher(mainResp);
          rootId = mainRows[0]?.id != null ? String(mainRows[0].id) : MAIN_ENTRY_ID;
        } catch {
          rootId = MAIN_ENTRY_ID;
        }
      }
      setStatus(
        `walking ${rootLabel} neighborhood at depth ${depth} in '${database}'...`,
      );
      // The neighborhood endpoint seeds by internal id(n), not by the
      // graphify id property; resolve the seed first.
      const eidResp = await api.executeCypherOnDatabase(
        database,
        `MATCH (n {id: $graphifyId}) RETURN id(n) AS internalId LIMIT 1`,
        { graphifyId: rootId },
      );
      const eidRows = rowsFromCypher(eidResp);
      const seedInternalId =
        eidRows[0]?.internalId != null
          ? String(eidRows[0].internalId)
          : rootId;
      const hood = await api.getGraphNeighborhood({
        nodeIds: [seedInternalId],
        depth,
        limit: NEIGHBORHOOD_LIMIT,
        relationshipTypes: CALL_RELATION_TYPES,
        direction: "both",
        database,
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
      buildGraph(rawNodes, rawLinks, rootId);
      publicToInternalRef.current = publicToInternal;
      setSource({ kind: "db", name: database });
      // Retain the selected node (and its detail panel) across re-roots:
      // the re-rooted neighborhood always contains its new root.
      const keepId = selectedIdRef.current;
      if (keepId) {
        const node = nodeByIdRef.current.get(keepId);
        if (node) {
          void selectNode(keepId);
        } else {
          selectedIdRef.current = null;
          setSelected(null);
        }
      }
      setStatus(
        `rooted at ${rootLabel} · depth ${depth} · ${rawNodes.length} nodes · ${rawLinks.length} links` +
          (hood.meta?.truncated ? " (truncated)" : ""),
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(`Failed to load '${database}': ${message}`);
      setStatus("load failed");
    } finally {
      setLoading(false);
    }
  }, [database, customRoot, depth, buildGraph, selectNode]);

  const onUpload = useCallback(
    async (file: File) => {
      setLoading(true);
      setError(null);
      setStatus(`parsing ${file.name}...`);
      try {
        const text = await file.text();
        setStatus(`indexing ${file.name}...`);
        const parsed = JSON.parse(text) as {
          nodes?: ArtifactNode[];
          links?: ArtifactLink[];
          edges?: ArtifactLink[];
        };
        const rawNodes = parsed.nodes ?? [];
        const rawLinks = parsed.links ?? parsed.edges ?? [];
        buildGraph(rawNodes, rawLinks, null);
        setSource({ kind: "artifact", name: file.name });
        setStatus(
          `loaded ${rawNodes.length} nodes and ${rawLinks.length} links from ${file.name}`,
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(`Failed to parse artifact: ${message}`);
        setStatus("upload failed");
      } finally {
        setLoading(false);
      }
    },
    [buildGraph],
  );

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
        <div className="mt-1 flex items-center gap-2 text-[10px] text-norse-silver/50 font-mono">
          <span className="inline-block w-2 h-2 rounded-full bg-sky-400" /> code
          <span className="inline-block w-2 h-2 rounded-full bg-amber-400" /> document
          <span className="inline-block w-2 h-2 rounded-full bg-violet-400" /> other
          <span className="inline-block w-2 h-2 rounded-full bg-pink-400" /> similar
        </div>
      </div>

      {/* Controls (top-right) */}
      <div className="absolute top-4 right-4 z-10 w-80 rounded-lg border border-sky-500/30 bg-norse-shadow/85 backdrop-blur px-4 py-3 shadow-[0_0_24px_rgba(56,189,248,0.15)]">
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
          <button
            onClick={() => {
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

        <div className="mt-2 flex items-center gap-2">
          <label className="flex-1 text-center rounded border border-dashed border-norse-rune bg-norse-night/60 px-2 py-1.5 text-xs text-norse-silver/70 hover:border-sky-400 cursor-pointer">
            upload graphify artifact
            <input
              type="file"
              accept=".json,application/json"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) void onUpload(file);
                e.target.value = "";
              }}
            />
          </label>
        </div>

        <div className="mt-2 text-[10px] text-norse-silver/50 font-mono leading-snug">
          starts at the graphify main entry · click a symbol and use
          “start graph from this symbol” to re-root · drag to rotate · scroll
          to zoom · right-drag to pan
        </div>
      </div>

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
                  setCustomRoot({ id: selected.id, label: selected.label });
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
