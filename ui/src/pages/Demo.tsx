import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance } from "3d-force-graph";
import * as THREE from "three";
import { api, type CypherResponse } from "../utils/api";
import { generateGalaxy, type DemoGalaxy } from "../utils/demoSeed";

const DEMO_DB = "d3_demo";
const SEED_BATCH = 400;
const MAX_LATENCY_HISTORY = 24;

type Phase =
  | "checking"
  | "creating"
  | "seeding"
  | "indexing"
  | "ready"
  | "traversing"
  | "error";

interface LatencySample {
  label: string;
  ms: number;
  hops: number;
  ts: number;
}

interface GraphNode {
  id: string;
  name: string;
  sector: number;
  hue: number;
  mass: number;
  highlight: boolean;
  selected: boolean;
  step?: number;
  x?: number;
  y?: number;
  z?: number;
}

interface GraphLink {
  source: string;
  target: string;
  highlight: boolean;
}

type DemoForceGraph = ForceGraph3DInstance<GraphNode, GraphLink>;

interface PathInfo {
  hops: number;
  starIds: string[];
  // totalMs: wall-clock time as observed by JS (request → response handled).
  // Includes any main-thread contention while the await resumes. Best for
  // user-perceived latency.
  totalMs: number;
  // wireMs: time the browser reports the request actually spent on the
  // network (Resource Timing). Authoritative network number; surfaced
  // separately so the HUD can show "what curl sees" alongside "what the
  // app feels".
  wireMs: number;
  startName: string;
  endName: string;
  source: "auto" | "manual";
}

// All Cypher pinned to named hot-path shapes from
// docs/performance/hot-path-query-cookbook.md:
//   - 7.3  Bulk Ingestion (UnwindSimpleMergeBatch)        seed nodes
//   - 7.3h Bulk-Seed (UnwindMultiMatchCreateBatch)        seed edges
//   - shortestPath((a)-[:T*]-(b))                         unbounded shortest path
//                                                         (pkg/cypher/shortest_path.go)
const CYPHER_CREATE_INDEX = `CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)`;

const CYPHER_SEED_STARS = `UNWIND $rows AS row
MERGE (n:Star {starId: row.starId})
SET n.name = row.name,
    n.sector = row.sector,
    n.hue = row.hue,
    n.mass = row.mass,
    n.x = row.x,
    n.y = row.y,
    n.z = row.z`;

const CYPHER_SEED_EDGES = `UNWIND $rows AS row
MATCH (a:Star {starId: row.fromId})
MATCH (b:Star {starId: row.toId})
CREATE (a)-[:HYPERLANE {distance: row.distance}]->(b)`;

// Unbounded shortest-path traversal — dedicated executor in
// pkg/cypher/shortest_path.go. Undirected to traverse across the seeded
// forward+reverse hyperlanes regardless of stored direction.
const CYPHER_SHORTEST_PATH = `MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId})
MATCH p = shortestPath((start)-[:HYPERLANE*]-(end))
RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops
LIMIT 1`;

interface ParsedRow {
  [k: string]: unknown;
}

function rowsFromCypher(resp: CypherResponse): ParsedRow[] {
  const r = resp.results?.[0];
  const cols = r?.columns ?? [];
  const data = r?.data ?? [];
  return data.map((d) => {
    const out: ParsedRow = {};
    for (let i = 0; i < cols.length; i++) {
      out[cols[i]] = d.row[i];
    }
    return out;
  });
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function nowMs(): number {
  return typeof performance !== "undefined" ? performance.now() : Date.now();
}

function sectorColor(hue: number): string {
  return `hsl(${hue}, 80%, 60%)`;
}

const HIGHLIGHT_COLOR = "#a855f7"; // purple-500
const HIGHLIGHT_HALO = 0xa855f7;
const SELECTED_COLOR = "#f0abfc"; // pink-300, for click-selected endpoints
const NORMAL_LINK_COLOR = "rgba(125,211,252,0.55)";
const NORMAL_LINK_WIDTH = 0.7;
const HIGHLIGHT_LINK_WIDTH = 1.4;

export function Demo() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<DemoForceGraph | null>(null);

  const [galaxy] = useState<DemoGalaxy>(() => generateGalaxy(0xfeed_d3));

  const [phase, setPhase] = useState<Phase>("checking");
  const [statusLine, setStatusLine] = useState("Connecting to NornicDB...");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [seededCount, setSeededCount] = useState(0);
  const [seedTotal, setSeedTotal] = useState(0);
  const [latency, setLatency] = useState<LatencySample[]>([]);
  const [pathInfo, setPathInfo] = useState<PathInfo | null>(null);
  const [selection, setSelection] = useState<{
    fromId: string | null;
    toId: string | null;
  }>({ fromId: null, toId: null });
  const [seedReady, setSeedReady] = useState(false);

  const recordTraversalLatency = useCallback(
    (label: string, ms: number, hops: number) => {
      setLatency((prev) => {
        const next = [...prev, { label, ms, hops, ts: Date.now() }];
        return next.length > MAX_LATENCY_HISTORY
          ? next.slice(next.length - MAX_LATENCY_HISTORY)
          : next;
      });
    },
    [],
  );

  const starById = useMemo(() => {
    const m = new Map<string, GraphNode>();
    for (const s of galaxy.stars) {
      m.set(s.id, {
        id: s.id,
        name: s.name,
        sector: s.sector,
        hue: s.hue,
        mass: s.mass,
        highlight: false,
        selected: false,
        x: s.x,
        y: s.y,
        z: s.z,
      });
    }
    return m;
  }, [galaxy]);

  // --- Seed lifecycle ----------------------------------------------------

  useEffect(() => {
    let cancelled = false;

    const ensureDatabase = async (): Promise<void> => {
      setPhase("checking");
      setStatusLine("Probing system catalog for d3_demo...");

      const dbList = await api.listDatabaseNames();
      if (cancelled) return;

      if (!dbList.includes(DEMO_DB)) {
        setPhase("creating");
        setStatusLine(`Forging database '${DEMO_DB}'...`);
        await api.createDatabase(DEMO_DB);
        if (cancelled) return;

        setPhase("indexing");
        setStatusLine("Provisioning Star index...");
        await api.executeCypherOnDatabase(DEMO_DB, CYPHER_CREATE_INDEX);
        if (cancelled) return;

        setPhase("seeding");
        const starRows = galaxy.stars.map((s) => ({
          starId: s.id,
          name: s.name,
          sector: s.sector,
          hue: s.hue,
          mass: s.mass,
          x: s.x,
          y: s.y,
          z: s.z,
        }));
        const edgeRows = galaxy.edges.map((e) => ({
          fromId: e.source,
          toId: e.target,
          distance: e.distance,
        }));
        const total = starRows.length + edgeRows.length;
        setSeedTotal(total);
        setSeededCount(0);

        let written = 0;
        for (const batch of chunk(starRows, SEED_BATCH)) {
          if (cancelled) return;
          setStatusLine(
            `Seeding stars (${written}/${total})... UnwindSimpleMergeBatch`,
          );
          await api.executeCypherOnDatabase(DEMO_DB, CYPHER_SEED_STARS, {
            rows: batch,
          });
          written += batch.length;
          setSeededCount(written);
        }

        for (const batch of chunk(edgeRows, SEED_BATCH)) {
          if (cancelled) return;
          setStatusLine(
            `Linking hyperlanes (${written}/${total})... UnwindMultiMatchCreateBatch`,
          );
          await api.executeCypherOnDatabase(DEMO_DB, CYPHER_SEED_EDGES, {
            rows: batch,
          });
          written += batch.length;
          setSeededCount(written);
        }
      } else {
        setStatusLine(`Database '${DEMO_DB}' detected; reusing.`);
      }
    };

    (async () => {
      try {
        await ensureDatabase();
        if (cancelled) return;
        setSeedReady(true);
      } catch (err) {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setErrorMessage(message);
        setPhase("error");
        setStatusLine(`Failed: ${message}`);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [galaxy]);

  // --- Traversal --------------------------------------------------------

  const runShortestPath = useCallback(
    async (startId: string, endId: string, source: "auto" | "manual") => {
      const startStar = starById.get(startId);
      const endStar = starById.get(endId);
      if (!startStar || !endStar) return;

      // Pin the force-graph simulation: ticks render ~1000 nodes / 10K
      // edges via WebGL on the main thread, which delays microtask
      // resolution of the fetch await and pollutes any latency timer
      // that wraps it.
      const fg = graphRef.current;
      fg?.pauseAnimation();

      // Sample the wall-clock timer BEFORE any React state updates so a
      // queued render between await-resume and the second timer read
      // doesn't get attributed to network time. setPhase/setStatusLine
      // below are intentionally scheduled, not awaited — React batches
      // them outside the measurement window.
      const start = nowMs();
      setPhase("traversing");
      setStatusLine(`shortestPath ${startStar.name} ↔ ${endStar.name}`);

      try {
        const resp = await api.executeCypherOnDatabase(
          DEMO_DB,
          CYPHER_SHORTEST_PATH,
          { startId, endId },
        );
        const totalMs = nowMs() - start;
        // Bolt-over-WS sessions don't surface wire timings via the
        // browser's Resource Timing buffer (that API only covers HTTP
        // fetches). The JS-observed total is what the user feels.
        const wireMs = totalMs;
        fg?.resumeAnimation();
        const rows = rowsFromCypher(resp);
        const first = rows[0];
        const ids = (first?.pathIds as string[] | undefined) ?? [];
        const hops =
          (first?.hops as number | undefined) ?? Math.max(0, ids.length - 1);
        if (ids.length === 0) {
          setStatusLine(
            `No path between ${startStar.name} and ${endStar.name}`,
          );
          setPhase("ready");
          return;
        }
        setPathInfo({
          hops,
          starIds: ids,
          totalMs,
          wireMs,
          startName: startStar.name,
          endName: endStar.name,
          source,
        });
        recordTraversalLatency(
          `${startStar.name} → ${endStar.name}`,
          wireMs,
          hops,
        );
        setPhase("ready");
        setStatusLine(
          `${hops} hops · ${wireMs.toFixed(1)} ms wire · ${totalMs.toFixed(1)} ms total · ${startStar.name} → ${endStar.name}`,
        );
      } catch (err) {
        fg?.resumeAnimation();
        const message = err instanceof Error ? err.message : String(err);
        setStatusLine(`Traversal failed: ${message}`);
        setPhase("error");
      }
    },
    [recordTraversalLatency, starById],
  );

  // First traversal once the data is in: longest available end-to-end so the
  // visual highlight is always present on first render.
  useEffect(() => {
    if (!seedReady) return;
    if (pathInfo) return;
    runShortestPath(galaxy.startId, galaxy.endId, "auto");
  }, [seedReady, pathInfo, galaxy.startId, galaxy.endId, runShortestPath]);

  // --- Graph init (once) ------------------------------------------------

  useEffect(() => {
    if (!containerRef.current) return;
    if (graphRef.current) return;

    const el = containerRef.current;
    const fgRaw = new ForceGraph3D(el);
    const fg = fgRaw as unknown as DemoForceGraph;

    fg.backgroundColor("#04060c")
      .showNavInfo(false)
      .width(el.clientWidth)
      .height(el.clientHeight)
      .nodeRelSize(2)
      .nodeOpacity(0.9)
      // For 50K nodes we let 3d-force-graph's built-in sphere renderer do
      // the work (one shared geometry, per-node color attribute) instead
      // of building a Mesh per node — that would blow GPU memory.
      .nodeVal((n) => 1 + Math.sqrt(n.mass) * 0.6)
      .nodeColor((n) => {
        if (n.selected) return SELECTED_COLOR;
        if (n.highlight) return HIGHLIGHT_COLOR;
        return sectorColor(n.hue);
      })
      // nodeThreeObject only fires for selected/highlight nodes (a tiny
      // subset) to add the halo. nodeThreeObjectExtend lets the built-in
      // sphere render under the halo overlay.
      .nodeThreeObjectExtend(true)
      .nodeThreeObject((node) => {
        if (!node.highlight && !node.selected)
          return null as unknown as THREE.Object3D;
        const radius = (1 + Math.sqrt(node.mass) * 0.6) * 2.2;
        const haloGeom = new THREE.SphereGeometry(radius, 10, 10);
        const haloMat = new THREE.MeshBasicMaterial({
          color: node.selected ? 0xf0abfc : HIGHLIGHT_HALO,
          transparent: true,
          opacity: 0.25,
        });
        return new THREE.Mesh(haloGeom, haloMat);
      })
      .linkOpacity(0.4)
      .linkWidth((l) =>
        l.highlight ? HIGHLIGHT_LINK_WIDTH : NORMAL_LINK_WIDTH,
      )
      .linkColor((l) => (l.highlight ? HIGHLIGHT_COLOR : NORMAL_LINK_COLOR))
      .linkHoverPrecision(8)
      .linkLabel((l) => {
        const s =
          typeof l.source === "object"
            ? (l.source as GraphNode).name
            : String(l.source);
        const t =
          typeof l.target === "object"
            ? (l.target as GraphNode).name
            : String(l.target);
        return `${s} → ${t}`;
      })
      .nodeLabel((n) => `${n.name} · sector ${n.sector}`)
      // Cooldown the simulation hard — positions arrive pre-computed from
      // the procedural galaxy, so let the force solver settle quickly
      // rather than running indefinitely on 50K nodes.
      .cooldownTicks(60)
      .warmupTicks(0);

    const charge = fg.d3Force("charge") as unknown as
      | { strength: (n: number) => unknown }
      | undefined;
    // Weaker repulsion at high node count so the simulation doesn't
    // catastrophically blow apart and use less CPU per tick.
    charge?.strength(-12);
    const linkForce = fg.d3Force("link") as unknown as
      | { distance: (n: number) => unknown }
      | undefined;
    linkForce?.distance(20);

    // Build initial graph data from the procedural galaxy with seeded positions.
    const seenLinkKeys = new Set<string>();
    const initialLinks: GraphLink[] = [];
    for (const e of galaxy.edges) {
      const k =
        e.source < e.target
          ? `${e.source}|${e.target}`
          : `${e.target}|${e.source}`;
      if (seenLinkKeys.has(k)) continue;
      seenLinkKeys.add(k);
      initialLinks.push({
        source: e.source,
        target: e.target,
        highlight: false,
      });
    }
    fg.graphData({
      nodes: galaxy.stars.map((s) => ({
        id: s.id,
        name: s.name,
        sector: s.sector,
        hue: s.hue,
        mass: s.mass,
        highlight: false,
        selected: false,
        x: s.x,
        y: s.y,
        z: s.z,
      })),
      links: initialLinks,
    });

    graphRef.current = fg;

    const ro = new ResizeObserver(() => {
      fg.width(el.clientWidth);
      fg.height(el.clientHeight);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [galaxy]);

  // --- Click handler: pick two endpoints --------------------------------

  useEffect(() => {
    const fg = graphRef.current;
    if (!fg) return;
    fg.onNodeClick((node: GraphNode) => {
      if (!node) return;
      setSelection((prev) => {
        if (!prev.fromId) {
          return { fromId: node.id, toId: null };
        }
        if (!prev.toId && node.id !== prev.fromId) {
          return { fromId: prev.fromId, toId: node.id };
        }
        // Third click resets selection to the new node.
        return { fromId: node.id, toId: null };
      });
    });
  }, []);

  // When a second endpoint is selected, run the shortest-path traversal.
  useEffect(() => {
    if (!seedReady) return;
    if (selection.fromId && selection.toId) {
      runShortestPath(selection.fromId, selection.toId, "manual");
    }
  }, [selection, seedReady, runShortestPath]);

  // --- Re-apply highlights and selection without resetting positions ----

  useEffect(() => {
    const fg = graphRef.current;
    if (!fg) return;
    const live = fg.graphData();
    const highlightSet = new Set(pathInfo?.starIds ?? []);
    const stepIndex = new Map<string, number>();
    (pathInfo?.starIds ?? []).forEach((id, i) => stepIndex.set(id, i));

    const selectedSet = new Set<string>();
    if (selection.fromId) selectedSet.add(selection.fromId);
    if (selection.toId) selectedSet.add(selection.toId);

    for (const n of live.nodes) {
      const id = n.id as string;
      n.highlight = highlightSet.has(id);
      n.selected = selectedSet.has(id);
      n.step = stepIndex.get(id);
    }
    for (const l of live.links) {
      const sId =
        typeof l.source === "object"
          ? (l.source as GraphNode).id
          : (l.source as string);
      const tId =
        typeof l.target === "object"
          ? (l.target as GraphNode).id
          : (l.target as string);
      const aIdx = stepIndex.get(sId);
      const bIdx = stepIndex.get(tId);
      l.highlight =
        aIdx !== undefined && bIdx !== undefined && Math.abs(aIdx - bIdx) === 1;
    }
    fg.nodeThreeObject(fg.nodeThreeObject())
      .linkColor(fg.linkColor())
      .linkWidth(fg.linkWidth());
  }, [pathInfo, selection]);

  // --- HUD --------------------------------------------------------------

  const lastLatency = latency[latency.length - 1];
  const avgLatency = useMemo(() => {
    if (latency.length === 0) return 0;
    const sum = latency.reduce((s, x) => s + x.ms, 0);
    return sum / latency.length;
  }, [latency]);
  const maxLatency = useMemo(
    () => latency.reduce((m, x) => Math.max(m, x.ms), 1),
    [latency],
  );

  const sectors = galaxy.sectorCount;
  const totalNodes = galaxy.stars.length;
  const totalEdges = galaxy.edges.length;

  const titlePathLine = pathInfo
    ? `shortest path: ${pathInfo.hops} hops · ${pathInfo.wireMs.toFixed(1)} ms wire · ${pathInfo.startName} → ${pathInfo.endName}`
    : seedReady
      ? "click any two stars to traverse"
      : "preparing galaxy...";

  const selectionHint = (() => {
    if (selection.fromId && !selection.toId) {
      const s = starById.get(selection.fromId);
      return `Selected: ${s?.name ?? selection.fromId} — click another star`;
    }
    if (selection.fromId && selection.toId) {
      const a = starById.get(selection.fromId);
      const b = starById.get(selection.toId);
      return `${a?.name ?? selection.fromId}  →  ${b?.name ?? selection.toId}`;
    }
    return "click any two stars to traverse the mesh";
  })();

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
            Galactic Mesh
          </span>
        </div>
        <div className="mt-1 text-xs text-norse-silver/70 max-w-md">
          {sectors} sectors · {totalNodes} stars · {totalEdges} hyperlanes
        </div>
        <div className="mt-1 text-xs text-purple-300/90 font-mono max-w-md truncate">
          {titlePathLine}
        </div>
      </div>

      {/* Latency HUD (top-right) */}
      <div className="absolute top-4 right-4 z-10 w-80 rounded-lg border border-purple-500/30 bg-norse-shadow/85 backdrop-blur px-4 py-3 shadow-[0_0_24px_rgba(168,85,247,0.18)]">
        <div className="flex items-baseline justify-between">
          <span className="text-xs uppercase tracking-[0.25em] text-purple-300">
            Traversal Latency
          </span>
          <span className="text-[10px] text-norse-silver/60">shortestPath</span>
        </div>
        <div className="mt-2 flex items-baseline gap-2">
          <span className="text-3xl font-mono font-semibold text-white tabular-nums">
            {lastLatency ? lastLatency.ms.toFixed(1) : "—"}
          </span>
          <span className="text-sm text-norse-silver/70 font-mono">ms</span>
          {lastLatency && (
            <span className="ml-auto text-xs font-mono text-purple-300 tabular-nums">
              {lastLatency.hops} hops
            </span>
          )}
        </div>
        <div className="mt-1 text-[11px] text-norse-silver/70 truncate font-mono">
          {lastLatency ? lastLatency.label : "waiting for first traversal..."}
        </div>

        <div className="mt-3 flex items-end gap-[2px] h-12">
          {Array.from({ length: MAX_LATENCY_HISTORY }).map((_, i) => {
            const idx = latency.length - MAX_LATENCY_HISTORY + i;
            const sample = idx >= 0 ? latency[idx] : null;
            const h = sample ? Math.max(2, (sample.ms / maxLatency) * 100) : 0;
            return (
              <div
                key={i}
                className="flex-1 rounded-sm transition-all"
                title={
                  sample ? `${sample.label} — ${sample.ms.toFixed(1)} ms` : ""
                }
                style={{
                  height: `${h}%`,
                  background: sample
                    ? "linear-gradient(180deg, #c084fc 0%, #a855f7 100%)"
                    : "rgba(42,50,71,0.5)",
                }}
              />
            );
          })}
        </div>

        <div className="mt-2 grid grid-cols-3 text-[10px] font-mono">
          <div>
            <div className="text-norse-silver/50">avg</div>
            <div className="text-white tabular-nums">
              {avgLatency.toFixed(1)}
            </div>
          </div>
          <div>
            <div className="text-norse-silver/50">peak</div>
            <div className="text-white tabular-nums">
              {maxLatency.toFixed(1)}
            </div>
          </div>
          <div>
            <div className="text-norse-silver/50">samples</div>
            <div className="text-white tabular-nums">{latency.length}</div>
          </div>
        </div>

        <div className="mt-3 pt-3 border-t border-norse-rune/60 text-[11px] text-purple-200/80 font-mono leading-snug">
          {selectionHint}
        </div>
      </div>

      {/* Status / progress bar (bottom) */}
      <div className="absolute bottom-4 left-4 right-4 z-10 flex items-center gap-4 rounded-lg border border-norse-rune bg-norse-shadow/80 backdrop-blur px-4 py-3">
        <div
          className={`w-2.5 h-2.5 rounded-full ${
            phase === "ready"
              ? "bg-nornic-primary status-connected"
              : phase === "error"
                ? "bg-red-500"
                : "bg-frost-glacier animate-pulse"
          }`}
        />
        <div className="flex-1 min-w-0">
          <div className="text-xs uppercase tracking-[0.2em] text-norse-silver/60">
            {phase === "checking" && "checking"}
            {phase === "creating" && "creating database"}
            {phase === "indexing" && "indexing"}
            {phase === "seeding" && "seeding"}
            {phase === "traversing" && "traversing"}
            {phase === "ready" && "ready"}
            {phase === "error" && "error"}
          </div>
          <div className="text-sm text-white truncate font-mono">
            {statusLine}
          </div>
          {phase === "seeding" && seedTotal > 0 && (
            <div className="mt-1 h-1 w-full bg-norse-rune rounded">
              <div
                className="h-1 bg-nornic-primary rounded transition-all"
                style={{
                  width: `${Math.min(100, (seededCount / seedTotal) * 100)}%`,
                }}
              />
            </div>
          )}
        </div>
        {pathInfo && (
          <div className="text-right hidden sm:block">
            <div className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/60">
              Hops
            </div>
            <div className="font-mono text-purple-300 text-sm">
              {pathInfo.hops}
            </div>
          </div>
        )}
      </div>

      {errorMessage && (
        <div className="absolute inset-0 z-20 flex items-center justify-center bg-black/70">
          <div className="max-w-lg rounded-lg border border-red-500/40 bg-norse-shadow p-6">
            <div className="text-red-400 text-sm uppercase tracking-[0.2em]">
              Demo failed
            </div>
            <div className="mt-2 text-white font-mono text-sm break-words">
              {errorMessage}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
