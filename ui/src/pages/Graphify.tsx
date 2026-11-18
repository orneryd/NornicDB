import {MAIN_NODE_QUERY,NAMED_ENTRY_QUERY,NAMED_ENTRY_PARAMS,ENTRY_CANDIDATE_QUERY,rankEntryCandidates,isTestSource,type RankedEntry,type EntryCandidate} from "../utils/graphifyEntry";
import {PageHeader} from "../components/common/PageHeader";
import "./Graphify.css";
import {useViewState} from '../utils/useUrlState';
import { useCallback, useEffect, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance } from "3d-force-graph";
import { forceCollide, forceLink, forceX, forceY, forceZ } from "d3-force-3d";
import { ChevronDown, ChevronUp, Grip, PanelRightClose, PanelRightOpen } from "lucide-react";
import type { PerspectiveCamera } from "three";
import { callTreeLayout, layoutCallBranches, layoutSymbolBundles, packageForSymbol, rootCameraFrame } from "../utils/graphifyLayout";
import { clampPanelBounds } from "../utils/floatingPanelBounds";
import {
  api,
  type CypherResponse,
  type GraphNeighborhoodResponse,
  type GraphPropertyFilter,
} from "../utils/api";
import {
  FilterChipList,
  PropertyFilterList,
} from "../components/graphFilterControls";

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
  // Graphify community (cluster) this node belongs to, when the database has them.
  community?: string;
  communityName?: string;
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
  community?: unknown;
  community_name?: unknown;
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

// Communities are Graphify's clusters of closely related symbols. Their colors
// are spread around the hue wheel by the golden angle so neighbors differ, and
// a community keeps its color for a given id.
function communityColor(community: string): string {
  const index = Number.parseInt(community, 10);
  const seed = Number.isFinite(index) ? index : community.length;
  const hue = (seed * 137.508) % 360;
  const lightness = 58 + ((seed * 7) % 3) * 6;
  return `hsl(${hue.toFixed(1)}, 72%, ${lightness}%)`;
}

const COMMUNITY_LEGEND_SIZE = 10;
const COMMUNITY_COHESION = 0.035;
const COMMUNITY_MEMBERSHIP_QUERY = `MATCH (n)-[:IN_COMMUNITY]->(c:Community)
WHERE n.id IN $ids
RETURN n.id AS id, c.community AS community, c.name AS name`;

// Pulls every node toward the live centroid of its community. It is weak enough
// that the call-tree branches still set the shape of the graph; it only makes
// each community read as one group within that shape.
function communityCohesion(communityOf: ReadonlyMap<string, string>, rootId: string | null, strength: number) {
  let nodes: GNode[] = [];
  const force = (alpha: number) => {
    const sums = new Map<string, { x: number; y: number; z: number; count: number }>();
    for (const node of nodes) {
      const community = communityOf.get(node.id);
      if (!community) continue;
      const sum = sums.get(community) ?? { x: 0, y: 0, z: 0, count: 0 };
      sum.x += node.x ?? 0;
      sum.y += node.y ?? 0;
      sum.z += node.z ?? 0;
      sum.count += 1;
      sums.set(community, sum);
    }
    for (const node of nodes) {
      const community = communityOf.get(node.id);
      const sum = community ? sums.get(community) : undefined;
      if (!sum || sum.count < 2 || node.id === rootId) continue;
      node.vx = (node.vx ?? 0) + (sum.x / sum.count - (node.x ?? 0)) * strength * alpha;
      node.vy = (node.vy ?? 0) + (sum.y / sum.count - (node.y ?? 0)) * strength * alpha;
      node.vz = (node.vz ?? 0) + (sum.z / sum.count - (node.z ?? 0)) * strength * alpha;
    }
  };
  force.initialize = (initial: GNode[]) => {
    nodes = initial;
  };
  return force;
}

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

function orientGraph(
  fg: GraphifyForceGraph,
  links: GLink[],
  nodes: GNode[],
  rootId: string | null,
  groupByCommunity = true,
): void {
  links = [...links];
  nodes = [...nodes];
  fg.onEngineTick(() => {});
  fg.onEngineStop(() => {});
  for (const name of ["bundles", "branch-x", "branch-y", "branch-z", "community"]) {
    fg.d3Force(name, null);
  }
  fg.graphData({ nodes, links });
  setDagMode(fg, null);
  fg.d3Force("layers", null);
  fg.d3Force("center", null);
  const byId = new Map(nodes.map(node => [node.id, node]));
  // Order: the call tree branches from the root as before; symbols outside it group by community, then by
  // package inside each community.
  const communityOf = new Map<string, string>();
  if (groupByCommunity) {
    for (const node of nodes) if (node.community) communityOf.set(node.id, node.community);
  }
  const kinds = nodes.map(node => ({
    id: node.id,
    kind: classifySymbolKind(node),
    packageName: `${communityOf.get(node.id) ?? ""}|${packageForSymbol(node)}`,
  }));
  const packages = new Map(nodes.map(node => [node.id, packageForSymbol(node)]));
  const branches = layoutCallBranches(nodes.map(node => node.id), links, rootId, packages, communityOf.size > 0 ? communityOf : undefined);
  const bundles = layoutSymbolBundles(kinds, links, rootId);
  const bundleLinks: Array<{ source: string; target: string }> = [];
  for (const node of nodes) {
    delete node.fx;
    delete node.fy;
    delete node.fz;
    const bundle = bundles.get(node.id);
    const anchor = bundle?.anchorId ? branches.get(bundle.anchorId) : undefined;
    const position = branches.get(node.id)!;
    node.x = anchor && bundle ? anchor.x + bundle.x : position.x;
    node.y = anchor && bundle ? anchor.y + bundle.y : position.y;
    node.z = anchor && bundle ? anchor.z + bundle.z : position.z;
    node.vx = 0;
    node.vy = 0;
    node.vz = 0;
    if (bundle?.anchorId) bundleLinks.push({ source: node.id, target: bundle.anchorId });
  }
  const branchStrength = (node: GNode) => node.id === rootId ? 0.12 : bundles.get(node.id)?.anchorId ? 0 : 0.018;
  fg.onEngineTick(() => {
    const activeLinks = fg.d3Force("link") as unknown as { links(): GLink[] };
    if (activeLinks.links() !== links) return;
    fg.onEngineTick(() => {});
    fg.d3Force("branch-x", forceX<GNode>(node => branches.get(node.id)!.x).strength(branchStrength));
    fg.d3Force("branch-y", forceY<GNode>(node => branches.get(node.id)!.y).strength(branchStrength));
    fg.d3Force("branch-z", forceZ<GNode>(node => branches.get(node.id)!.z).strength(branchStrength));
    fg.d3Force("bundles", forceLink<GNode>(bundleLinks).id(node => node.id).distance(65).strength(0.65));
    fg.d3Force("collide", forceCollide<GNode>(node => node.id === rootId ? 10 : 5).strength(0.7));
    if (communityOf.size > 0) fg.d3Force("community", communityCohesion(communityOf, rootId, COMMUNITY_COHESION));
    const linkForce = fg.d3Force("link") as unknown as {
      distance(value: (link: GLink) => number): unknown;
      strength(value: (link: GLink) => number): unknown;
    };
    linkForce.distance(link => link.relation === "CALLS" || link.relation === "METHOD" ? 85 : 65);
    linkForce.strength(link => {
      const source = typeof link.source === "string" ? byId.get(link.source) : link.source;
      const target = typeof link.target === "string" ? byId.get(link.target) : link.target;
      const degree = Math.max(source?.degree ?? 1, target?.degree ?? 1, 1);
      return (link.relation === "CALLS" || link.relation === "METHOD" ? 0.16 : 0.012) / Math.sqrt(degree);
    });
    fg.d3VelocityDecay(0.35);
    fg.d3AlphaMin(0.001);
    fg.cooldownTicks(220);
    fg.onEngineStop(() => {
      fg.onEngineStop(() => {});
      frameRootGraph(fg, rootId);
      if (import.meta.env.DEV) {
        const debug = (window as unknown as Record<string, { settled?: boolean }>).__graphifyDebug;
        if (debug) debug.settled = true;
      }
    });
    fg.d3ReheatSimulation();
  });
  if (import.meta.env.DEV) {
    const depths = callTreeLayout(links, rootId);
    (window as unknown as Record<string, unknown>).__graphifyDebug = {
      rootId,
      packages: [...packages],
      callTree: { nodes: kinds.filter(node => node.kind === "call").length, maxDepth: Math.max(0, ...depths.values()) },
      bundles: [...bundles].map(([id, bundle]) => ({ id, anchorId: bundle.anchorId })),
    };
  }
  fg.d3ReheatSimulation();
}

function frameRootGraph(fg: GraphifyForceGraph, rootId: string | null): void {
  const frame = rootCameraFrame(fg.graphData().nodes, rootId, fg.width(), fg.height(), (fg.camera() as PerspectiveCamera).fov);
  fg.cameraPosition(frame.position, frame.target, 400);
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


  const [databases, setDatabases] = useState<string[]>([]);
  const [database, setDatabase] = useViewState<string>('database', "");
  const [loaded,setLoaded]=useViewState('loaded',false);
  const [selectedId,setSelectedId]=useViewState<string|null>('node',null);
  const lastLoadSignatureRef=useRef<string|null>(null);
  const [customRoot, setCustomRoot] = useViewState<{ id: string; label: string } | null>('root',null);
  // Mirror of customRoot for stale-closure-free reads inside
  // loadFromDatabase: handlers update it synchronously before scheduling the
  // reload, so the fetch always sees the root that was just chosen.
  const customRootRef = useRef<{ id: string; label: string } | null>(customRoot);
  customRootRef.current=customRoot;
  const [depth, setDepth] = useViewState<number>('depth', DEFAULT_DEPTH);
  const [showTests, setShowTests] = useViewState('tests', false);
  const [colorBy, setColorBy] = useViewState<"kind" | "community">('color', "kind");
  const [groupByCommunity, setGroupByCommunity] = useViewState('group', true);
  const [communityLegend, setCommunityLegend] = useState<Array<{ id: string; name: string; size: number }>>([]);
  const colorByRef = useRef<"kind" | "community">("kind");
  const groupByCommunityRef = useRef(true);
  const [advanced,setAdvanced]=useViewState('advanced',false);
  const [floating,setFloating]=useViewState('floating',false);
  const [help,setHelp]=useViewState('help',false);
  const [bodyExpanded,setBodyExpanded]=useViewState('sourceBody',true);
  const [linksExpanded,setLinksExpanded]=useViewState('connections',true);
  const [symbolList,setSymbolList]=useViewState('symbols',false);
  const [entryCandidates,setEntryCandidates]=useState<RankedEntry[]>([]);
  const [controlsVisible, setControlsVisible] = useViewState('controls', true);
  // Filter entries are managed as add/remove lists in the filter panel;
  // applying them reloads the graph with the lists sent to the
  // neighborhood endpoint. The URL owns all non-secret view state.
  const [includeLabels, setIncludeLabels] = useViewState<string[]>('graphify.includeLabels', []);
  const [includeEdgeTypes, setIncludeEdgeTypes] = useViewState<string[]>('graphify.includeEdgeTypes', [...CALL_RELATION_TYPES]);
  const [includeNames, setIncludeNames] = useViewState<string[]>('graphify.includeNames', []);
  const [includeProps, setIncludeProps] = useViewState<GraphPropertyFilter[]>('graphify.includeProps', []);
  const [excludeLabels, setExcludeLabels] = useViewState<string[]>('graphify.excludeLabels', []);
  const [excludeEdgeTypes, setExcludeEdgeTypes] = useViewState<string[]>('graphify.excludeEdgeTypes', []);
  const [excludeNames, setExcludeNames] = useViewState<string[]>('graphify.excludeNames', []);
  const [excludeProps, setExcludeProps] = useViewState<GraphPropertyFilter[]>('graphify.excludeProps', []);
  // Disconnected subgraphs reported by the endpoint after exclusion
  // filters; selecting one dims the rest of the scene.
  const [components, setComponents] = useState<GraphNeighborhoodResponse[]>([]);
  const [activeComponent, setActiveComponent] = useViewState<number | null>('component', null);
  const [componentsVisible, setComponentsVisible] = useViewState('components', false);
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
  const [search, setSearch] = useViewState('q', "");
  const [searchResults, setSearchResults] = useState<Array<{ id: string; label: string; sourceFile?: string; score?: number }>>([]);
  const [searchLoading,setSearchLoading]=useState(false);
  const [searchError,setSearchError]=useState('');
  const [similarLoading, setSimilarLoading] = useState(false);
  const [similarCount, setSimilarCount] = useState<number | null>(null);
  const [defaultPanelBounds] = useState(() => clampPanelBounds({
    left: window.innerWidth - 400,
    top: Math.min(264, window.innerHeight - 496),
    width: 384,
    height: 480,
  }, window.innerWidth, window.innerHeight));
  const [panelBounds,setPanelBounds]=useViewState('inspectorBounds',defaultPanelBounds,true);
  const [uploadOpen, setUploadOpen] = useViewState('upload', false);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadDbName, setUploadDbName] = useState("");
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState<{ label: string; percent: number } | null>(null);
  const [updateExisting, setUpdateExisting] = useState(false);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef<{ startX: number; startY: number; left: number; top: number } | null>(null);
  const resizeRef = useRef<{ startX: number; startY: number; width: number; height: number } | null>(null);
  const selectedIdRef = useRef<string | null>(null);
  const rootIdRef = useRef<string | null>(null);

  useEffect(() => {
    if(!floating)return;
    const clamp = () => setPanelBounds(current => {
      const panel = panelRef.current?.getBoundingClientRect();
      const next = clampPanelBounds({
        ...current,
        width: panel?.width ?? current.width,
        height: panel?.height ?? current.height,
      }, window.innerWidth, window.innerHeight);
      return Object.keys(next).every(key => next[key as keyof typeof next] === current[key as keyof typeof current]) ? current : next;
    });
    const observer = new ResizeObserver(clamp);
    if (panelRef.current) observer.observe(panelRef.current);
    window.addEventListener("resize", clamp);
    clamp();
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", clamp);
    };
  }, [selected != null,floating]);

  // --- Database list -------------------------------------------------------

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const names = await api.listDatabaseNames();
        if (cancelled) return;
        setDatabases(names);
        if(database)return;
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

  // Recolor in place when the color mode flips.
  useEffect(() => {
    colorByRef.current = colorBy;
    const fg = graphRef.current;
    if (fg) fg.nodeColor(fg.nodeColor());
  }, [colorBy]);

  // Re-lay out the loaded graph when grouping by community is switched.
  useEffect(() => {
    groupByCommunityRef.current = groupByCommunity;
    const fg = graphRef.current;
    if (!fg) return;
    const { nodes, links } = fg.graphData();
    if (nodes.length > 0) orientGraph(fg, links as GLink[], nodes as GNode[], rootIdRef.current, groupByCommunity);
  }, [groupByCommunity]);

  // Filters apply automatically: adding or removing any entry reloads the
  // graph with the updated lists (the first render is skipped so the Load
  // tree button drives the initial fetch).
  const filtersKey = JSON.stringify([
    includeLabels,
    includeEdgeTypes,
    includeNames,
    includeProps,
    excludeLabels,
    excludeEdgeTypes,
    excludeNames,
    excludeProps,
  ]);
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
        if (colorByRef.current === "community" && n.community) return communityColor(n.community);
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
      .linkDirectionalArrowLength(0)
      .linkDirectionalArrowRelPos(1)
      .linkCurvature(0)
      .dagLevelDistance(72)
      .linkDirectionalParticles(0)
      .linkLabel((l) => {
        const s = typeof l.source === "object" ? l.source.label : String(l.source);
        const t = typeof l.target === "object" ? l.target.label : String(l.target);
        return `${s} -[${l.relation}]-> ${t}`;
      })
      .nodeLabel(
        (n) =>
          `${classifySymbolKind(n)} · ${n.label}${n.communityName ? ` · community: ${n.communityName}` : ""}${n.sourceFile ? ` · ${n.sourceFile}${n.sourceLocation ? ":" + n.sourceLocation : ""}` : ""}`,
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
    async (id: string, focusCamera = true) => {
      const node = nodeByIdRef.current.get(id);
      if (!node) return;
      selectedIdRef.current = id;
      setSelected(node);
      setSelectedId(id);
      applySelectionVisuals(id);

      const fg = graphRef.current;
      if (fg && focusCamera) {
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
              (components[index]?.nodes ?? []).map((node) =>
                node.properties.id != null ? String(node.properties.id) : node.id,
              ),
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
          community: raw.community == null ? undefined : String(raw.community),
          communityName: typeof raw.community_name === "string" && raw.community_name ? raw.community_name : undefined,
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
        orientGraph(fg, links, nodes, rootId, groupByCommunityRef.current);
      }
      const sizes = new Map<string, { name: string; size: number }>();
      for (const node of nodes) {
        if (!node.community) continue;
        const entry = sizes.get(node.community) ?? { name: "", size: 0 };
        entry.size += 1;
        entry.name = entry.name || node.communityName || "";
        sizes.set(node.community, entry);
      }
      setCommunityLegend(
        [...sizes].map(([id, entry]) => ({ id, name: entry.name || `community ${id}`, size: entry.size }))
          .sort((a, b) => b.size - a.size).slice(0, COMMUNITY_LEGEND_SIZE),
      );
      if (fg && rootId) {
        window.requestAnimationFrame(() => {
          window.requestAnimationFrame(() => frameRootGraph(fg, rootId));
        });
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
    lastLoadSignatureRef.current=JSON.stringify([dbName,customRootRef.current,depth,showTests,filtersKey]);
    setLoaded(true);
    if (!dbName) return;
    setLoading(true);
    setError(null);
    try {
      // Rooted neighborhood: resolve the entry node (the main entry of the
      // graphify graph, or the symbol last chosen with "start graph from
      // this symbol"), then walk its neighborhood at the configured depth.
      let rootId = customRootRef.current?.id ?? null;
      let rootLabel = customRootRef.current?.label ?? "main entry";
      if (!rootId) {
        setStatus("Finding suggested starting points…");
        const candidates: EntryCandidate[]=[];
        for(const [query,explicit] of [[MAIN_NODE_QUERY,true],[NAMED_ENTRY_QUERY,false],[ENTRY_CANDIDATE_QUERY,false]] as const){
          const response=await api.executeCypherOnDatabase(dbName,query,query===NAMED_ENTRY_QUERY?NAMED_ENTRY_PARAMS:{});
          if(response.errors?.length)throw new Error(response.errors.map(error=>error.message).join('; '));
          for(const row of rowsFromCypher(response))if(row.id!=null)candidates.push({
            id:String(row.id),label:String(row.label??row.id),source_file:typeof row.source_file==='string'?row.source_file:undefined,
            degree:Number(row.degree??0),explicit,internalId:row.internalId!=null?String(row.internalId):undefined,
          });
          if(query===ENTRY_CANDIDATE_QUERY){
            const seeds=rankEntryCandidates(candidates).filter(entry=>entry.internalId).slice(0,128);
            if(seeds.length){
              const sample=await api.getGraphNeighborhood({nodeIds:seeds.map(entry=>entry.internalId!),depth:1,limit:2000,relationshipTypes:CALL_RELATION_TYPES,direction:'both',database:dbName});
              const adjacent=new Map<string,Set<string>>();
              for(const edge of sample.edges){for(const [from,to] of [[edge.source,edge.target],[edge.target,edge.source]]){if(from===to)continue;const ids=adjacent.get(from)??new Set<string>();ids.add(to);adjacent.set(from,ids);}}
              for(const entry of candidates)entry.degree=adjacent.get(entry.internalId??'')?.size??0;
            }
          }
          // Explicit markers and recognizable entries do not need a whole-graph connectivity scan.
          if((rankEntryCandidates(candidates)[0]?.score??0)>=200)break;
        }
        const ranked=rankEntryCandidates(candidates);
        setEntryCandidates(ranked);
        if(ranked[0]){rootId=ranked[0].id;rootLabel=ranked[0].label;}
      }
      setStatus(
        `walking ${rootLabel} neighborhood at depth ${depth} in '${dbName}'...`,
      );
      // The neighborhood endpoint seeds by internal id(n), not by the
      // graphify id property; resolve the seed first.
      let eidRows = rootId
        ? rowsFromCypher(
            await api.executeCypherOnDatabase(
              dbName,
              `MATCH (n {id: $graphifyId}) RETURN id(n) AS internalId LIMIT 1`,
              { graphifyId: rootId },
            ),
          )
        : [];
      if (eidRows[0]?.internalId == null) {
        // No :Main node and no function with connections (e.g. a graph
        // from an older ingest): root at the first node in the database.
        const anyResp = await api.executeCypherOnDatabase(
          dbName,
          `MATCH (n) WHERE n.id IS NOT NULL RETURN n.id AS id, n.label AS label ORDER BY n.id LIMIT 1`,
        );
        const anyRows = rowsFromCypher(anyResp);
        if (anyRows[0]?.id != null) {
          rootId = String(anyRows[0].id);
          rootLabel =
            anyRows[0].label != null
              ? String(anyRows[0].label)
              : rootLabel;
          eidRows = rowsFromCypher(
            await api.executeCypherOnDatabase(
              dbName,
              `MATCH (n {id: $graphifyId}) RETURN id(n) AS internalId LIMIT 1`,
              { graphifyId: rootId },
            ),
          );
        }
      }
      if (!rootId) {
        throw new Error(`database '${dbName}' has no graphify nodes`);
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
        includeNames,
        excludeNames,
        includeProperties: includeProps,
        excludeProperties: excludeProps,
        excludeLabels,
        excludeRelationshipTypes: excludeEdgeTypes,
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
          community: payload.properties.community,
          community_name: payload.properties.community_name,
        };
      });
      // Databases loaded by the current importers keep communities as Community nodes joined by
      // IN_COMMUNITY edges (not properties, so relabelling never rewrites the symbols).
      if (rawNodes.length > 0 && rawNodes.every((node) => node.community == null)) {
        try {
          const byPublicId = new Map(rawNodes.map((node) => [node.id, node]));
          const ids = [...byPublicId.keys()];
          for (let offset = 0; offset < ids.length; offset += 2000) {
            const resp = await api.executeCypherOnDatabase(
              dbName,
              COMMUNITY_MEMBERSHIP_QUERY,
              { ids: ids.slice(offset, offset + 2000) },
            );
            for (const row of rowsFromCypher(resp)) {
              const node = byPublicId.get(String(row.id));
              if (!node) continue;
              node.community = row.community;
              node.community_name = typeof row.name === "string" ? row.name : undefined;
            }
          }
        } catch {
          // no communities in this database: the layout falls back to packages
        }
      }
      const rawLinks: ArtifactLink[] = hood.edges.map((edge) => ({
        source: internalToPublic.get(edge.source) ?? edge.source,
        target: internalToPublic.get(edge.target) ?? edge.target,
        relation: edge.type,
      }));
      // Test-file nodes (and their edges) are filtered out unless "show test
      // files" is checked. The root stays visible so a re-root into test code
      // never empties the graph.
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
        visibleNodes = rawNodes.filter((node) => visible.has(node.id));
        visibleLinks = links;
      }
      buildGraph(visibleNodes, visibleLinks, rootId);
      publicToInternalRef.current = publicToInternal;
      setSource({ kind: "db", name: dbName });
      setComponents(hood.components ?? []);
      setActiveComponent(null);
      // Auto-select the root (or the newly chosen re-root symbol) so its
      // details pane opens immediately after every (re)load.
      const focusId = selectedId ?? customRootRef.current?.id ?? rootId;
      const focusNode = nodeByIdRef.current.get(focusId);
      if (focusNode) {
        selectedIdRef.current = focusId;
        void selectNode(focusId, false);
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
    includeNames,
    includeProps,
    excludeLabels,
    excludeEdgeTypes,
    excludeNames,
    excludeProps,
  ]);

  const loadRef=useRef(loadFromDatabase);loadRef.current=loadFromDatabase;
  const loadSignature=JSON.stringify([database,customRoot,depth,showTests,filtersKey]);
  useEffect(()=>{if(loaded && database && lastLoadSignatureRef.current!==loadSignature)void loadRef.current();},[loaded,database,loadSignature]);
  useEffect(()=>{
    if(selectedId && nodeByIdRef.current.has(selectedId))void selectNode(selectedId,false);
    else if(!selectedId){setSelected(null);applySelectionVisuals(null);}
  },[selectedId,source,selectNode,applySelectionVisuals]);

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
    orientGraph(fg, live.links, live.nodes, rootIdRef.current);
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

  useEffect(()=>{
    let live=true;
    setSearchError('');setSearchResults([]);
    if(search.trim().length<2||!database){setSearchLoading(false);return;}
    setSearchLoading(true);
    const timer=window.setTimeout(async()=>{
      try{
        const results=await api.searchNodes(search.trim(),12,database);
        if(live)setSearchResults(results.map(result=>{const props=result.node?.properties??{};return {
          id:String(props.id??result.node.id),label:String(props.label??props.id??result.node.id),
          sourceFile:typeof props.source_file==='string'?props.source_file:undefined,score:result.rrf_score??result.score,
        };}));
      }catch(error){if(live)setSearchError(error instanceof Error?error.message:'Symbol search failed');}
      finally{if(live)setSearchLoading(false);}
    },250);
    return()=>{live=false;window.clearTimeout(timer);};
  },[search,database]);

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
    <div className={`graphify-workspace ${controlsVisible?"has-source":""} ${selected&&!floating?"has-inspector":""} ${floating?"floating-inspector":""}`}>
      <PageHeader title="Code graph" actions={<><button type="button" onClick={()=>setHelp(value=>!value)} aria-expanded={help}>Graph controls</button><button type="button" onClick={()=>setSymbolList(value=>!value)} aria-expanded={symbolList}>Browse symbols</button></>}/>
      <div className="graphify-canvas" ref={containerRef} role="region" aria-label="Interactive 3D code graph" />
      {!source&&<section className="graphify-empty"><h2>Explore how a project connects</h2><p>Load a code graph database or import a Graphify graph.json artifact. Choose a starting point, then follow its calls and references.</p></section>}

      {symbolList&&<section className="graphify-symbols" aria-label="Loaded symbols"><h2>Loaded symbols ({nodeCount})</h2><p>Select a symbol to inspect it without using the canvas.</p><ul>{Array.from(nodeByIdRef.current.values()).filter(node=>!search||`${node.label} ${node.sourceFile}`.toLowerCase().includes(search.toLowerCase())).slice(0,200).map(node=><li key={node.id}><button aria-pressed={selectedId===node.id} onClick={()=>focusNode(node.id)}>{node.label}<small>{node.sourceFile}</small></button></li>)}</ul>{nodeCount>200&&<p>Showing up to 200 matches. Search to narrow the list.</p>}</section>}

      {/* Title strip */}
      <div className="graphify-context">
        <div className="flex items-baseline gap-3">
          <span className="text-2xl font-semibold tracking-wide text-white">
            Project map
          </span>
          <span className="text-nornic-accent text-sm uppercase tracking-[0.3em]">
            3D call graph
          </span>
        </div>
        <div className="mt-1 text-xs text-norse-silver max-w-md">
          {source
            ? `${source.kind === "db" ? `database: ${source.name}` : source.name} · ${nodeCount} nodes · ${loadedLinks} links`
            : "graphify tree explorer"}
        </div>
        <div className="mt-1 flex items-center gap-2 text-[10px] text-norse-silver font-mono flex-wrap">
          {colorBy === "community" && communityLegend.length > 0
            ? communityLegend.map((entry) => (
                <span key={entry.id} className="inline-flex items-center gap-1" title={`${entry.size} nodes`}>
                  <span
                    className="inline-block w-2 h-2 rounded-full"
                    style={{ backgroundColor: communityColor(entry.id) }}
                  />
                  {entry.name}
                </span>
              ))
            : null}
          {colorBy === "kind" && KIND_LEGEND.map(([kind, color]) => (
            <span key={kind} className="inline-flex items-center gap-1">
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{ backgroundColor: color }}
              />
              {kind}
            </span>
          ))}
        </div>
      {help&&<section className="graphify-help"><h2>Navigate the graph</h2><p>Drag to rotate · scroll or pinch to zoom · right-drag to pan. Select a symbol to inspect its source and connections. Browse symbols provides keyboard-accessible selection.</p><button onClick={()=>graphRef.current&&frameRootGraph(graphRef.current,rootIdRef.current)}>Return to starting point</button></section>}
      </div>

      {/* Source setup and scope stay beside the graph. */}
      <button
        type="button"
        aria-label={controlsVisible ? "Hide filters" : "Show filters"}
        title={controlsVisible ? "Hide filters" : "Show filters"}
        aria-expanded={controlsVisible}
        aria-controls="graphify-filter-pane"
        onClick={() => setControlsVisible(visible => !visible)}
        className="graphify-source-toggle"
      >
        {controlsVisible ? <PanelRightClose size={16} /> : <PanelRightOpen size={16} />} {controlsVisible?"Hide source & filters":"Source & filters"}
      </button>
      {controlsVisible && (
      <div id="graphify-filter-pane" className="graphify-source">
        <div className="flex items-baseline justify-between pr-7">
          <span className="text-xs uppercase tracking-[0.25em] text-sky-300">
            Source & scope
          </span>
          <span className="text-[10px] text-norse-silver font-mono">graphify</span>
        </div>

        <div className="mt-3 flex items-center gap-2">
          <select
            value={database}
            aria-label="Code graph database"
            onChange={(e) => {customRootRef.current=null;setCustomRoot(null);setSelectedId(null);setEntryCandidates([]);setDatabase(e.target.value);}}
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

        <div className="graphify-scope-controls">
          <label className="flex items-center gap-1 text-xs text-norse-silver">
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
          <label className="flex items-center gap-1 text-xs text-norse-silver select-none cursor-pointer">
            <input
              type="checkbox"
              checked={groupByCommunity}
              onChange={(e) => setGroupByCommunity(e.target.checked)}
              className="mr-0.5"
            />
            group by community
          </label>
          <label className="flex items-center gap-1 text-xs text-norse-silver select-none">
            color
            <select
              value={colorBy}
              onChange={(e) => setColorBy(e.target.value as "kind" | "community")}
              className="rounded border border-norse-rune bg-norse-night px-1 py-1 text-xs text-norse-silver focus:outline-none focus:border-sky-400"
            >
              <option value="kind">symbol kind</option>
              <option value="community">community</option>
            </select>
          </label>
          <label className="flex items-center gap-1 text-xs text-norse-silver select-none cursor-pointer">
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
            className="flex-1 rounded border border-norse-rune bg-norse-night px-2 py-1.5 text-xs text-norse-silver hover:border-sky-400 disabled:opacity-40"
          >
            Detect starting point
          </button>
        </div>

        {entryCandidates.length>0&&<div className="graphify-entry"><label htmlFor="graphify-entry">Starting point</label><select id="graphify-entry" value={customRoot?.id??rootIdRef.current??''} onChange={event=>{const entry=entryCandidates.find(item=>item.id===event.target.value);if(entry)setCustomRoot({id:entry.id,label:entry.label});}}>{customRoot&&!entryCandidates.some(entry=>entry.id===customRoot.id)&&<option value={customRoot.id}>{customRoot.label} (selected symbol)</option>}{entryCandidates.map(entry=><option key={entry.id} value={entry.id}>{entry.label} · {entry.source_file}</option>)}</select><p>{entryCandidates.find(entry=>entry.id===(customRoot?.id??rootIdRef.current))?.reason??'Manually selected symbol'}. Suggestions are heuristics; a library may have no single main.</p></div>}
        <details className="graphify-advanced" open={advanced} onToggle={event=>{if(event.currentTarget.open!==advanced)setAdvanced(event.currentTarget.open)}}><summary>Advanced filters</summary>
          <div className="flex items-baseline justify-between">
            <span className="text-[10px] uppercase tracking-[0.2em] text-norse-silver">
              Filters
            </span>
            <span className="text-[10px] text-norse-silver font-mono">
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
              label="include symbol names"
              entries={includeNames}
              onAdd={setIncludeNames}
              onRemove={(entry) =>
                setIncludeNames(includeNames.filter((item) => item !== entry))
              }
              placeholder="e.g. main()"
            />
            <FilterChipList
              label="exclude symbol names"
              entries={excludeNames}
              onAdd={setExcludeNames}
              onRemove={(entry) =>
                setExcludeNames(excludeNames.filter((item) => item !== entry))
              }
              placeholder="e.g. context.Context"
            />
            <PropertyFilterList
              label="include properties"
              entries={includeProps}
              onAdd={setIncludeProps}
              onRemove={(entry) =>
                setIncludeProps(includeProps.filter((item) => item !== entry))
              }
            />
            <PropertyFilterList
              label="exclude properties"
              entries={excludeProps}
              onAdd={setExcludeProps}
              onRemove={(entry) =>
                setExcludeProps(excludeProps.filter((item) => item !== entry))
              }
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
                setIncludeNames([]);
                setIncludeProps([]);
                setExcludeLabels([]);
                setExcludeEdgeTypes([]);
                setExcludeNames([]);
                setExcludeProps([]);
              }}
              disabled={loading}
              className="rounded border border-norse-rune bg-norse-night px-2 py-1.5 text-xs text-norse-silver hover:border-sky-400 disabled:opacity-40"
            >
              reset
            </button>
          </div>
          <div className="mt-1.5 text-[10px] text-norse-silver font-mono leading-snug">
            filters apply automatically when you add or remove an entry ·
            names match the symbol name exactly · properties match key +
            optional value, optionally scoped to a label or edge type —
            exclusions may split the graph into components
          </div>
        </details>

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
            className="flex-1 text-center rounded border border-dashed border-norse-rune bg-norse-night/60 px-2 py-1.5 text-xs text-norse-silver hover:border-sky-400 cursor-pointer"
          >
            upload graphify artifact
          </button>
        </div>

        <div className="mt-2 text-[10px] text-norse-silver font-mono leading-snug">
          starts at the graphify main entry · click a symbol and use
          “start graph from this symbol” to re-root · drag to rotate · scroll
          to zoom · right-drag to pan
        </div>
      </div>
      )}

      {/* Upload dialog */}
      {uploadOpen && (
        <div className="graphify-upload" role="region" aria-label="Import Graphify artifact">
          <div className="graphify-upload-content">
            <div className="text-xs uppercase tracking-[0.25em] text-sky-300">
              Import Graphify artifact
            </div>
            <p className="mt-2 text-sm text-norse-silver">Imports graph.json into a new database. You can delete the demo database at any time from Databases. Updating an existing database requires the explicit sync option below.</p>
            <label className="mt-3 block cursor-pointer rounded border border-dashed border-norse-rune bg-norse-night/60 px-3 py-2 text-xs text-norse-silver hover:border-sky-400">
              {uploadFile ? uploadFile.name : "choose graph.json artifact"}
              <input
                type="file"
                accept=".json,application/json"
                aria-label="Graphify artifact file"
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
              <label htmlFor="graphify-upload-database" className="text-[10px] uppercase tracking-[0.2em] text-norse-silver">
                database name
              </label>
              <input
                type="text"
                id="graphify-upload-database"
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
                <label className="mt-2 flex items-start gap-2 text-xs text-norse-silver">
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
                <div className="mt-1 text-[10px] text-norse-silver font-mono truncate">
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
                className="rounded border border-norse-rune bg-norse-night px-3 py-1.5 text-xs text-norse-silver hover:border-sky-400 disabled:opacity-40"
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
      <div className="graphify-search" role="search" aria-label="Find a code symbol">
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Find a symbol or source file…"
          aria-label="Find a symbol or source file"
          className="w-full rounded border border-norse-rune bg-norse-shadow/85 backdrop-blur px-3 py-1.5 text-xs text-norse-silver placeholder:text-norse-silver focus:outline-none focus:border-sky-400"
        />
        {searchLoading&&<p role="status">Searching symbols…</p>}
        {searchError&&<p role="alert">{searchError}</p>}
        {!searchLoading&&!searchError&&search.trim().length>=2&&searchResults.length===0&&<p>No matching symbols. Try a function name or source file.</p>}
        {searchResults.length > 0 && (
          <div className="mt-1 max-h-64 overflow-y-auto rounded border border-norse-rune bg-norse-shadow/95 backdrop-blur">
            {searchResults.map((result) => (
              <button
                key={result.id}
                onClick={() => void focusSearchResult(result)}
                className="block w-full text-left px-3 py-1.5 text-xs text-norse-silver hover:bg-norse-rune/40 truncate"
              >
                <span className="truncate">{result.label}</span>
                <span className="text-norse-silver ml-1">
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
        <div
          data-testid="graphify-components-drawer"
          className="absolute top-0 left-1/2 z-30 w-72 max-w-[calc(100%-2rem)] transition-transform duration-200 ease-out motion-reduce:transition-none"
          style={{ transform: `translate(-50%, ${componentsVisible ? "0" : "-100%"})` }}
        >
          <div
            id="graphify-components"
            inert={!componentsVisible}
            aria-hidden={!componentsVisible}
            className="rounded-b-lg border border-t-0 border-norse-rune bg-norse-shadow/95 backdrop-blur px-3 py-2 shadow-lg"
          >
          <div className="flex items-center justify-between">
            <span className="text-[10px] uppercase tracking-[0.2em] text-norse-silver">
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
          <div className="mt-1.5 max-h-[min(15rem,40vh)] overflow-y-auto space-y-1">
            {components.map((component, index) => (
              <button
                key={`${component.nodes[0]?.id ?? "empty"}-${index}`}
                type="button"
                aria-pressed={activeComponent === index}
                onClick={() => {
                  setActiveComponent(index);
                  applyComponentDim(index);
                }}
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
          </div>
          <button
            type="button"
            aria-label={componentsVisible ? "Close components" : "Open components"}
            title={componentsVisible ? "Close components" : "Open components"}
            aria-expanded={componentsVisible}
            aria-controls="graphify-components"
            onClick={() => setComponentsVisible((visible) => !visible)}
            className="absolute top-full left-1/2 flex h-7 w-16 -translate-x-1/2 items-center justify-center rounded-b-md border border-t-0 border-norse-rune bg-norse-shadow/95 text-sky-300 shadow hover:text-white focus-visible:outline focus-visible:outline-2 focus-visible:outline-sky-400"
          >
            {componentsVisible ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
          </button>
        </div>
      )}

      {/* Status bar (bottom) */}
      <div className="graphify-status" role="status" aria-live="polite">
        <div
          className={`w-2.5 h-2.5 rounded-full ${
            loading ? "bg-frost-glacier animate-pulse" : error ? "bg-red-500" : "bg-nornic-primary status-connected"
          }`}
        />
        <div className="flex-1 min-w-0 text-xs text-norse-silver truncate font-mono">
          {error??status}
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
          data-testid="graphify-details"
          aria-label="Symbol inspector"
          style={floating?{
            ...panelBounds,
            minWidth: "min(280px, calc(100vw - 32px))",
            minHeight: "min(180px, calc(100vh - 32px))",
            maxWidth: `calc(100vw - ${panelBounds.left + 16}px)`,
            maxHeight: `calc(100vh - ${panelBounds.top + 16}px)`,
          }:undefined}
          className="graphify-inspector"
        >
          <div
            onPointerDown={(e) => {
              if (!floating || e.button !== 0) return;
              if ((e.target as HTMLElement).closest("button")) return;
              e.preventDefault();
              e.stopPropagation();
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
              setPanelBounds(current => clampPanelBounds({
                ...current,
                left: drag.left + e.clientX - drag.startX,
                top: drag.top + e.clientY - drag.startY,
              }, window.innerWidth, window.innerHeight));
            }}
            onPointerUp={() => {
              dragRef.current = null;
            }}
            onPointerCancel={() => {
              dragRef.current = null;
            }}
            onLostPointerCapture={() => {
              dragRef.current = null;
            }}
            data-testid="graphify-details-header"
            className="shrink-0 z-10 bg-norse-shadow/95 backdrop-blur px-4 py-3 border-b border-norse-rune/60 flex items-start justify-between cursor-grab active:cursor-grabbing touch-none"
          >
            <div className="min-w-0">
              <div className="text-sm font-semibold text-white truncate">
                {selected.label}
              </div>
              <div className="text-[10px] text-norse-silver font-mono truncate">
                {selected.id}
              </div>
            </div>
            <button type="button" aria-pressed={floating} onClick={()=>setFloating(value=>!value)}>{floating?'Dock inspector':'Float inspector'}</button>
            <button
              aria-label="Close symbol inspector"
              onClick={() => {
                selectedIdRef.current = null;
                setSelectedId(null);
                setSelected(null);
                setBody(null);
                setBodySource(null);
                setNeighbors([]);
                applySelectionVisuals(null);
              }}
              className="ml-2 text-norse-silver hover:text-white text-xs"
            >
              ✕
            </button>
          </div>

          {floating&&<button
            type="button"
            aria-label="Resize details"
            title="Resize details"
            onPointerDown={event => {
              if (event.button !== 0) return;
              event.preventDefault();
              event.stopPropagation();
              resizeRef.current = { startX: event.clientX, startY: event.clientY, width: panelBounds.width, height: panelBounds.height };
              event.currentTarget.setPointerCapture(event.pointerId);
            }}
            onPointerMove={event => {
              const resizing = resizeRef.current;
              if (!resizing) return;
              setPanelBounds(current => clampPanelBounds({
                ...current,
                width: Math.min(window.innerWidth - current.left - 16, resizing.width + event.clientX - resizing.startX),
                height: Math.min(window.innerHeight - current.top - 16, resizing.height + event.clientY - resizing.startY),
              }, window.innerWidth, window.innerHeight));
            }}
            onPointerUp={() => { resizeRef.current = null; }}
            onPointerCancel={() => { resizeRef.current = null; }}
            onLostPointerCapture={() => { resizeRef.current = null; }}
            className="absolute bottom-0 right-0 z-30 p-1 text-norse-silver hover:text-white cursor-nwse-resize touch-none"
          >
            <Grip size={14} />
          </button>}
          <div className="px-4 py-3 text-xs space-y-2 flex-1 min-h-0 overflow-auto">
            <div className="grid grid-cols-[88px_1fr] gap-x-2 gap-y-1 font-mono">
              <span className="text-norse-silver">file_type</span>
              <span className="text-sky-300">{selected.fileType}</span>
              <span className="text-norse-silver">source</span>
              <span className="truncate">
                {selected.sourceFile ?? "—"}
                {selected.sourceLocation ? `:${selected.sourceLocation}` : ""}
              </span>
              {selected.nodeKind && (
                <>
                  <span className="text-norse-silver">kind</span>
                  <span className="truncate">{selected.nodeKind}</span>
                </>
              )}
              <span className="text-norse-silver">degree</span>
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
                className="w-full rounded bg-nornic-primary hover:bg-nornic-secondary text-slate-950 text-xs font-semibold px-3 py-1.5 disabled:opacity-40"
              >
                Start graph from this symbol
              </button>
            )}

            {database && (
              <button
                onClick={() => void findSimilarArm()}
                disabled={similarLoading}
                className="w-full rounded bg-norse-rune hover:bg-norse-night text-norse-silver text-xs font-semibold px-3 py-1.5 disabled:opacity-40"
              >
                {similarLoading
                  ? "searching similar…"
                  : `Find similar symbols${similarCount != null ? ` (${similarCount} added)` : ""}`}
              </button>
            )}

            <details open={bodyExpanded} onToggle={event=>{if(event.currentTarget.open!==bodyExpanded)setBodyExpanded(event.currentTarget.open)}}>
              <summary>Source code</summary>
              <div className="flex items-baseline justify-between">
                <span className="text-[10px] uppercase tracking-[0.2em] text-norse-silver">
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
                <div className="mt-1 text-norse-silver">
                  no body available{bodyLoading ? "" : " (not in artifact or database)"}
                </div>
              )}
            </details>

            {neighbors.length > 0 && (
              <details open={linksExpanded} onToggle={event=>{if(event.currentTarget.open!==linksExpanded)setLinksExpanded(event.currentTarget.open)}}><summary>Connections ({neighbors.length})</summary>
                <div className="text-[10px] uppercase tracking-[0.2em] text-norse-silver">
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
                      <span className="text-norse-silver">
                        {n.direction === "in" ? " ← " : " → "}
                      </span>
                      <span className="text-norse-silver truncate">{n.label}</span>
                    </button>
                  ))}
                </div>
              </details>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
