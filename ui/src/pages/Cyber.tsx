import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance } from "3d-force-graph";
import * as THREE from "three";
import { api, type CypherResponse } from "../utils/api";
import { generateFleet, type CyberFleet, type DroneSpec } from "../utils/cyberSeed";
import {
  CyberSimulator,
  defaultSimulationConfig,
  type TickEvent,
} from "../utils/cyberSim";

// Cyber-physical experimentation harness. Mirrors /demo's storage chain
// (NornicDB acts as the oracle: a stateful query target that the tick loop
// pokes for derived insight) but runs a free-running simulator on top of a
// drone fleet.
//
// Design notes:
//  - Sim state is in a class instance held via useRef. Putting it in
//    useState would re-render the component every tick at high cadence.
//  - The 3D viz is updated by directly mutating node positions on the
//    force-graph live data — same pattern /demo uses, no React state churn.
//  - Oracle queries run on a slower cadence than the sim tick (every
//    ORACLE_EVERY_N ticks). Each query writes its result into a small
//    OracleSnapshot piece of state that drives the dashboard side panel.

const CYBER_DB = "c_cyber_demo";
// 50ms = 20Hz. Backend cypher round-trip is ~2ms even with the 3 per-tick
// writes parallelized, leaving ~48ms slack — plenty of headroom for the
// browser's render frame too. Crank lower if the dashboard ever feels jerky.
const TICK_MS = 50;
// Oracle runs every 5 ticks at 20Hz → 4Hz. Cheap (3 short cypher reads in
// parallel) but not free; keep it bounded so a slow query can't queue up.
const ORACLE_EVERY_N = 5;
// Re-render the tick counter at most every Nth tick. Decouples HUD repaint
// cost from the simulation rate so 20Hz ticks don't trigger 20Hz React.
const HUD_REFRESH_EVERY_N = 5;
const SEED_BATCH = 200;
const MAX_EVENT_LOG = 60;

const CYPHER_INDEX = `CREATE INDEX drone_id_idx IF NOT EXISTS FOR (n:Drone) ON (n.droneId)`;

const CYPHER_SEED_DRONES = `UNWIND $rows AS row
MERGE (n:Drone {droneId: row.droneId})
SET n.callsign = row.callsign,
    n.squad = row.squad,
    n.role = row.role,
    n.x = row.x,
    n.y = row.y,
    n.z = row.z,
    n.battery = row.battery,
    n.status = row.status`;

const CYPHER_SEED_LINKS = `UNWIND $rows AS row
MATCH (a:Drone {droneId: row.fromId})
MATCH (b:Drone {droneId: row.toId})
CREATE (a)-[:COMMS {distance: row.distance}]->(b)`;

// Tick-time updates: position + battery + status. Idempotent; the
// simulator hands a row per drone that actually changed this tick.
const CYPHER_TICK_DRONES = `UNWIND $rows AS row
MATCH (n:Drone {droneId: row.droneId})
SET n.x = row.x, n.y = row.y, n.z = row.z,
    n.battery = row.battery, n.status = row.status`;

// Comms graph deltas. We delete dropped links by node-pair (any direction)
// then create added links. Using two queries keeps the per-tick payload
// tiny when nothing changed.
const CYPHER_DROP_LINKS = `UNWIND $rows AS row
MATCH (a:Drone {droneId: row.fromId})-[r:COMMS]-(b:Drone {droneId: row.toId})
DELETE r`;

const CYPHER_ADD_LINKS = `UNWIND $rows AS row
MATCH (a:Drone {droneId: row.fromId})
MATCH (b:Drone {droneId: row.toId})
CREATE (a)-[:COMMS {distance: row.distance}]->(b)`;

// Oracle queries. Each one represents a question we'd ask a real cyber-
// physical system: what's at risk, where are the islands, who's in range
// of whom.
//
// 1. Low-battery drones: surfaces emergent failure clusters.
// 2. Squad isolation: how many drones in each squad are reachable from
//    that squad's "anchor" (drone 0 in the squad). Tests the comms graph.
// 3. Comms-degree distribution: how many neighbors does each drone see?
//    Stand-in for "graph connectivity health" the operator should monitor.

const ORACLE_LOW_BATTERY = `MATCH (n:Drone)
WHERE n.battery < 30
RETURN n.droneId AS droneId, n.callsign AS callsign, n.battery AS battery, n.status AS status
ORDER BY n.battery ASC
LIMIT 10`;

const ORACLE_DEGREE = `MATCH (n:Drone)
OPTIONAL MATCH (n)-[r:COMMS]-()
WITH n, count(DISTINCT r) AS deg
RETURN n.droneId AS droneId, n.callsign AS callsign, deg AS degree
ORDER BY deg ASC
LIMIT 10`;

const ORACLE_FLEET_SUMMARY = `MATCH (n:Drone)
RETURN
  count(n) AS total,
  sum(CASE WHEN n.status = 'online' THEN 1 ELSE 0 END) AS online,
  sum(CASE WHEN n.status = 'degraded' THEN 1 ELSE 0 END) AS degraded,
  sum(CASE WHEN n.status = 'lost' THEN 1 ELSE 0 END) AS lost,
  avg(n.battery) AS avgBattery`;

type Phase =
  | "checking"
  | "creating"
  | "seeding"
  | "running"
  | "paused"
  | "error";

interface OracleSnapshot {
  tick: number;
  fleet: { total: number; online: number; degraded: number; lost: number; avgBattery: number };
  lowBattery: { droneId: string; callsign: string; battery: number; status: string }[];
  isolated: { droneId: string; callsign: string; degree: number }[];
  oracleMs: number;
}

interface GraphNode {
  id: string;
  callsign: string;
  squad: number;
  role: string;
  status: string;
  battery: number;
  x?: number;
  y?: number;
  z?: number;
}

interface GraphLink {
  source: string;
  target: string;
}

type CyberForceGraph = ForceGraph3DInstance<GraphNode, GraphLink>;

const SQUAD_COLORS = [
  "#a855f7", // purple
  "#22d3ee", // cyan
  "#facc15", // amber
  "#f87171", // red
];

function rowsFromCypher(resp: CypherResponse): Record<string, unknown>[] {
  const r = resp.results?.[0];
  const cols = r?.columns ?? [];
  const data = r?.data ?? [];
  return data.map((d) => {
    const out: Record<string, unknown> = {};
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

function statusColor(status: string, squad: number): string {
  if (status === "lost") return "#1f2937"; // grey
  if (status === "degraded") return "#fb923c"; // orange
  return SQUAD_COLORS[squad % SQUAD_COLORS.length];
}

export function Cyber() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<CyberForceGraph | null>(null);
  const simRef = useRef<CyberSimulator | null>(null);
  const tickHandleRef = useRef<number | null>(null);
  const oraclePendingRef = useRef(false);
  const tickInFlightRef = useRef(false);
  // The actual running tick lives in a ref so the simulator advances at
  // its real cadence without paying for a React re-render every step.
  // The displayed counter is kept in state and only synced every
  // HUD_REFRESH_EVERY_N ticks — enough to feel live, infrequent enough
  // that the dashboard's lists/charts don't churn.
  const tickRef = useRef(0);

  const [fleet] = useState<CyberFleet>(() => generateFleet({ seed: 0xcafef00d }));
  const [phase, setPhase] = useState<Phase>("checking");
  const [statusLine, setStatusLine] = useState("Probing system catalog for c_cyber_demo...");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [seedReady, setSeedReady] = useState(false);
  const [tickCount, setTickCount] = useState(0);
  const [oracle, setOracle] = useState<OracleSnapshot | null>(null);
  const [eventLog, setEventLog] = useState<TickEvent[]>([]);
  const [running, setRunning] = useState(true);

  // --- Database lifecycle ----------------------------------------------

  useEffect(() => {
    let cancelled = false;

    const ensureDatabase = async (): Promise<void> => {
      setPhase("checking");
      setStatusLine(`Probing system catalog for ${CYBER_DB}...`);
      const dbList = await api.listDatabaseNames();
      if (cancelled) return;

      if (!dbList.includes(CYBER_DB)) {
        setPhase("creating");
        setStatusLine(`Forging database '${CYBER_DB}'...`);
        await api.createDatabase(CYBER_DB);
        if (cancelled) return;

        setStatusLine("Provisioning Drone index...");
        await api.executeCypherOnDatabase(CYBER_DB, CYPHER_INDEX);
        if (cancelled) return;

        setPhase("seeding");
        const droneRows = fleet.drones.map((d) => ({
          droneId: d.id,
          callsign: d.callsign,
          squad: d.squad,
          role: d.role,
          x: d.x,
          y: d.y,
          z: d.z,
          battery: d.battery,
          status: d.status,
        }));
        for (const batch of chunk(droneRows, SEED_BATCH)) {
          if (cancelled) return;
          setStatusLine(`Seeding drones (${batch.length})...`);
          await api.executeCypherOnDatabase(CYBER_DB, CYPHER_SEED_DRONES, {
            rows: batch,
          });
        }

        const linkRows = fleet.links.map((l) => ({
          fromId: l.source,
          toId: l.target,
          distance: l.distance,
        }));
        for (const batch of chunk(linkRows, SEED_BATCH)) {
          if (cancelled) return;
          setStatusLine(`Linking comms (${batch.length})...`);
          await api.executeCypherOnDatabase(CYBER_DB, CYPHER_SEED_LINKS, {
            rows: batch,
          });
        }
      } else {
        setStatusLine(`Database '${CYBER_DB}' detected; reusing.`);
      }
    };

    (async () => {
      try {
        await ensureDatabase();
        if (cancelled) return;
        simRef.current = new CyberSimulator(fleet, defaultSimulationConfig);
        setSeedReady(true);
        setPhase("running");
        setStatusLine("Simulation running.");
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
  }, [fleet]);

  // --- 3D graph init (once) --------------------------------------------

  useEffect(() => {
    if (!containerRef.current) return;
    if (graphRef.current) return;

    const el = containerRef.current;
    const fgRaw = new ForceGraph3D(el);
    const fg = fgRaw as unknown as CyberForceGraph;

    fg.backgroundColor("#04060c")
      .showNavInfo(false)
      .width(el.clientWidth)
      .height(el.clientHeight)
      .nodeRelSize(3)
      .nodeOpacity(0.95)
      .nodeColor((n) => statusColor(n.status, n.squad))
      .nodeLabel((n) =>
        `${n.callsign} · squad ${n.squad} · ${n.role} · ${Math.round(n.battery)}% · ${n.status}`,
      )
      .linkOpacity(0.5)
      .linkWidth(0.6)
      .linkColor(() => "rgba(125,211,252,0.55)")
      // Disable the force simulation entirely — drone positions come from
      // the simulator, not from a layout solver. This keeps frame budget
      // for the constant tick stream.
      .cooldownTicks(0)
      .warmupTicks(0);

    const charge = fg.d3Force("charge") as unknown as
      | { strength: (n: number) => unknown }
      | undefined;
    charge?.strength(0);
    const linkForce = fg.d3Force("link") as unknown as
      | { distance: (n: number) => unknown; strength: (n: number) => unknown }
      | undefined;
    linkForce?.strength(0);

    fg.graphData({
      nodes: fleet.drones.map((d) => ({
        id: d.id,
        callsign: d.callsign,
        squad: d.squad,
        role: d.role,
        status: d.status,
        battery: d.battery,
        x: d.x,
        y: d.y,
        z: d.z,
      })),
      links: fleet.links.map((l) => ({ source: l.source, target: l.target })),
    });

    graphRef.current = fg;

    const ro = new ResizeObserver(() => {
      fg.width(el.clientWidth);
      fg.height(el.clientHeight);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [fleet]);

  // --- Tick loop --------------------------------------------------------

  const applyTick = useCallback(
    async (tick: number): Promise<void> => {
      const sim = simRef.current;
      const fg = graphRef.current;
      if (!sim || !fg) return;
      if (tickInFlightRef.current) return; // skip if previous write hasn't returned
      tickInFlightRef.current = true;

      const result = sim.step(tick);

      // Mutate the live graph data directly so the next render frame picks
      // up new positions without React ever touching it.
      const live = fg.graphData();
      const idx = new Map<string, GraphNode>();
      for (const n of live.nodes) idx.set(n.id, n);
      for (const updated of result.changedDrones) {
        const node = idx.get(updated.id);
        if (!node) continue;
        node.x = updated.x;
        node.y = updated.y;
        node.z = updated.z;
        node.battery = updated.battery;
        node.status = updated.status;
      }

      // Apply comms link deltas to the live graph too. force-graph holds
      // links by reference so we have to splice carefully.
      if (result.droppedLinks.length > 0 || result.addedLinks.length > 0) {
        const dropKey = (a: string, b: string): string =>
          a < b ? `${a}|${b}` : `${b}|${a}`;
        const dropped = new Set<string>(
          result.droppedLinks.map((l) => dropKey(l.source, l.target)),
        );
        const filtered = live.links.filter((l) => {
          const sId =
            typeof l.source === "object"
              ? (l.source as GraphNode).id
              : (l.source as string);
          const tId =
            typeof l.target === "object"
              ? (l.target as GraphNode).id
              : (l.target as string);
          return !dropped.has(dropKey(sId, tId));
        });
        for (const a of result.addedLinks) {
          filtered.push({ source: a.source, target: a.target });
        }
        fg.graphData({ nodes: live.nodes, links: filtered });
      } else {
        // Reapply nodeColor accessor so status changes repaint without a
        // full graphData reset.
        fg.nodeColor(fg.nodeColor());
      }

      if (result.events.length > 0) {
        setEventLog((prev) => {
          const next = [...result.events, ...prev];
          return next.length > MAX_EVENT_LOG
            ? next.slice(0, MAX_EVENT_LOG)
            : next;
        });
      }

      // Push deltas to NornicDB. The three write categories (drone state,
      // dropped links, added links) are independent of each other this
      // tick — fire them in parallel so the per-tick wall-clock matches a
      // single round-trip rather than three. The executor's plan and
      // edge-body caches keep each individual write at ~1-2ms.
      try {
        const writes: Promise<unknown>[] = [];
        if (result.changedDrones.length > 0) {
          const rows = result.changedDrones.map((d: DroneSpec) => ({
            droneId: d.id,
            x: d.x,
            y: d.y,
            z: d.z,
            battery: d.battery,
            status: d.status,
          }));
          writes.push(
            api.executeCypherOnDatabase(CYBER_DB, CYPHER_TICK_DRONES, {
              rows,
            }),
          );
        }
        if (result.droppedLinks.length > 0) {
          writes.push(
            api.executeCypherOnDatabase(CYBER_DB, CYPHER_DROP_LINKS, {
              rows: result.droppedLinks.map((l) => ({
                fromId: l.source,
                toId: l.target,
              })),
            }),
          );
        }
        if (result.addedLinks.length > 0) {
          writes.push(
            api.executeCypherOnDatabase(CYBER_DB, CYPHER_ADD_LINKS, {
              rows: result.addedLinks.map((l) => ({
                fromId: l.source,
                toId: l.target,
                distance: l.distance,
              })),
            }),
          );
        }
        if (writes.length > 0) {
          await Promise.all(writes);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setStatusLine(`Tick write failed: ${message}`);
      } finally {
        tickInFlightRef.current = false;
      }
    },
    [],
  );

  const runOracle = useCallback(async (tick: number): Promise<void> => {
    if (oraclePendingRef.current) return;
    oraclePendingRef.current = true;
    const start = nowMs();
    try {
      const [fleetResp, lowResp, degreeResp] = await Promise.all([
        api.executeCypherOnDatabase(CYBER_DB, ORACLE_FLEET_SUMMARY),
        api.executeCypherOnDatabase(CYBER_DB, ORACLE_LOW_BATTERY),
        api.executeCypherOnDatabase(CYBER_DB, ORACLE_DEGREE),
      ]);
      const oracleMs = nowMs() - start;
      const fleetRow = rowsFromCypher(fleetResp)[0] ?? {};
      const lowRows = rowsFromCypher(lowResp);
      const degRows = rowsFromCypher(degreeResp);

      setOracle({
        tick,
        fleet: {
          total: Number(fleetRow.total ?? 0),
          online: Number(fleetRow.online ?? 0),
          degraded: Number(fleetRow.degraded ?? 0),
          lost: Number(fleetRow.lost ?? 0),
          avgBattery: Number(fleetRow.avgBattery ?? 0),
        },
        lowBattery: lowRows.map((r) => ({
          droneId: String(r.droneId),
          callsign: String(r.callsign),
          battery: Number(r.battery),
          status: String(r.status),
        })),
        isolated: degRows.map((r) => ({
          droneId: String(r.droneId),
          callsign: String(r.callsign),
          degree: Number(r.degree),
        })),
        oracleMs,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setStatusLine(`Oracle failed: ${message}`);
    } finally {
      oraclePendingRef.current = false;
    }
  }, []);

  useEffect(() => {
    if (!seedReady) return;
    if (!running) return;

    const id = window.setInterval(() => {
      tickRef.current += 1;
      const tick = tickRef.current;
      void applyTick(tick);
      if (tick % ORACLE_EVERY_N === 0) {
        void runOracle(tick);
      }
      if (tick % HUD_REFRESH_EVERY_N === 0) {
        setTickCount(tick);
      }
    }, TICK_MS);
    tickHandleRef.current = id;
    return () => {
      if (tickHandleRef.current !== null) {
        window.clearInterval(tickHandleRef.current);
        tickHandleRef.current = null;
      }
    };
  }, [seedReady, running, applyTick, runOracle]);

  // --- HUD --------------------------------------------------------------

  const totalDrones = fleet.drones.length;
  const totalLinks = fleet.links.length;

  const phaseLabel = useMemo(() => {
    if (phase === "running" && !running) return "paused";
    return phase;
  }, [phase, running]);

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
            Cyber-Physical Oracle
          </span>
        </div>
        <div className="mt-1 text-xs text-norse-silver/70 max-w-md">
          {totalDrones} drones · {totalLinks} comms links · tick{" "}
          {tickCount.toString().padStart(4, "0")}
        </div>
      </div>

      {/* Oracle dashboard */}
      <div className="absolute top-4 right-4 z-10 w-96 rounded-lg border border-purple-500/30 bg-norse-shadow/85 backdrop-blur px-4 py-3 shadow-[0_0_24px_rgba(168,85,247,0.18)] text-xs">
        <div className="flex items-baseline justify-between">
          <span className="text-xs uppercase tracking-[0.25em] text-purple-300">
            Oracle Snapshot
          </span>
          <span className="text-[10px] text-norse-silver/60">
            {oracle ? `tick ${oracle.tick} · ${oracle.oracleMs.toFixed(1)} ms` : "warming up..."}
          </span>
        </div>

        {oracle && (
          <>
            <div className="mt-3 grid grid-cols-4 gap-2 text-center font-mono">
              <Stat label="online" value={oracle.fleet.online} color="#22d3ee" />
              <Stat
                label="degraded"
                value={oracle.fleet.degraded}
                color="#fb923c"
              />
              <Stat label="lost" value={oracle.fleet.lost} color="#f87171" />
              <Stat
                label="avg %"
                value={Math.round(oracle.fleet.avgBattery)}
                color="#a855f7"
              />
            </div>

            <Section title="Lowest battery">
              {oracle.lowBattery.length === 0 ? (
                <Empty>fleet healthy</Empty>
              ) : (
                <ul className="space-y-1 font-mono">
                  {oracle.lowBattery.map((d) => (
                    <li key={d.droneId} className="flex justify-between">
                      <span className="text-purple-200/90 truncate">
                        {d.callsign}
                      </span>
                      <span className="tabular-nums text-norse-silver/80">
                        {Math.round(d.battery)}% · {d.status}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </Section>

            <Section title="Most isolated">
              {oracle.isolated.length === 0 ? (
                <Empty>fully connected</Empty>
              ) : (
                <ul className="space-y-1 font-mono">
                  {oracle.isolated.map((d) => (
                    <li key={d.droneId} className="flex justify-between">
                      <span className="text-purple-200/90 truncate">
                        {d.callsign}
                      </span>
                      <span className="tabular-nums text-norse-silver/80">
                        deg {d.degree}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </Section>
          </>
        )}
      </div>

      {/* Event log */}
      <div className="absolute bottom-20 right-4 z-10 w-96 max-h-72 overflow-y-auto rounded-lg border border-norse-rune bg-norse-shadow/80 backdrop-blur px-4 py-3 text-xs">
        <div className="text-xs uppercase tracking-[0.25em] text-purple-300">
          Event log
        </div>
        {eventLog.length === 0 ? (
          <div className="mt-2 text-norse-silver/60 italic">no events yet</div>
        ) : (
          <ul className="mt-2 space-y-0.5 font-mono">
            {eventLog.map((e, i) => (
              <li key={`${e.tick}-${e.droneId}-${i}`} className="flex gap-2">
                <span className="text-norse-silver/50 tabular-nums">
                  {e.tick.toString().padStart(4, "0")}
                </span>
                <span className="text-purple-200/90">{e.kind}</span>
                <span className="text-norse-silver/70 truncate">
                  {e.droneId}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* Bottom status / controls */}
      <div className="absolute bottom-4 left-4 right-4 z-10 flex items-center gap-4 rounded-lg border border-norse-rune bg-norse-shadow/80 backdrop-blur px-4 py-3">
        <div
          className={`w-2.5 h-2.5 rounded-full ${
            phase === "running" && running
              ? "bg-nornic-primary status-connected"
              : phase === "error"
                ? "bg-red-500"
                : "bg-frost-glacier animate-pulse"
          }`}
        />
        <div className="flex-1 min-w-0">
          <div className="text-xs uppercase tracking-[0.2em] text-norse-silver/60">
            {phaseLabel}
          </div>
          <div className="text-sm text-white truncate font-mono">
            {statusLine}
          </div>
        </div>
        <button
          type="button"
          onClick={() => setRunning((v) => !v)}
          disabled={!seedReady}
          className="px-3 py-1.5 rounded-md text-xs uppercase tracking-[0.2em] border border-purple-500/40 text-purple-200 hover:bg-purple-500/15 disabled:opacity-40"
        >
          {running ? "Pause" : "Resume"}
        </button>
      </div>

      {errorMessage && (
        <div className="absolute inset-0 z-20 flex items-center justify-center bg-black/70">
          <div className="max-w-lg rounded-lg border border-red-500/40 bg-norse-shadow p-6">
            <div className="text-red-400 text-sm uppercase tracking-[0.2em]">
              Cyber demo failed
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

function Stat({
  label,
  value,
  color,
}: {
  label: string;
  value: number;
  color: string;
}) {
  return (
    <div>
      <div className="text-[9px] uppercase tracking-[0.2em] text-norse-silver/50">
        {label}
      </div>
      <div className="text-base tabular-nums" style={{ color }}>
        {value}
      </div>
    </div>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="mt-3 pt-2 border-t border-norse-rune/60">
      <div className="text-[10px] uppercase tracking-[0.2em] text-norse-silver/50 mb-1">
        {title}
      </div>
      {children}
    </div>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-norse-silver/60 italic font-mono">{children}</div>
  );
}

// Three.js identifier — reserved for future per-node mesh customization
// (low-battery halos, role icons). Keep the import live to make adding
// those changes a one-line edit instead of a re-import.
void THREE;

// Suppress unused-import warning if any of the above accidentally shed
// dependencies during refactor. Pure type re-export, zero runtime.
export type { CyberForceGraph };
