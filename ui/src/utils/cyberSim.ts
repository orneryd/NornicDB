// Tick-loop simulator for the /cyber demo.
//
// The simulator keeps an in-memory mirror of fleet state (so the 3D
// visualization can render at 60fps without round-tripping every frame to
// the DB) and pushes batched per-tick deltas to NornicDB. The oracle reads
// back via cypher queries — those represent the "experimentation cycles":
// each tick we ask the graph a question and surface the answer.

import type { CyberFleet, DroneSpec } from "./cyberSeed";

export type TickEventKind =
  | "low_battery"
  | "comms_lost"
  | "comms_recovered"
  | "out_of_bounds";

export interface TickEvent {
  tick: number;
  kind: TickEventKind;
  droneId: string;
  payload?: Record<string, unknown>;
}

export interface SimulationConfig {
  // World bounds; matches the seed.
  bounds: number;
  // How far a drone moves per tick along its heading (world units).
  speed: number;
  // Battery drain per tick; rate scales with role.
  batteryDrainBase: number;
  // Comms range — links snap on/off as drones cross this distance.
  commsRange: number;
  // Probability per-tick that a drone reorients (turns).
  turnProbability: number;
  // Probability per-tick that a fault gets injected (random sensor noise).
  perturbProbability: number;
}

export const defaultSimulationConfig: SimulationConfig = {
  bounds: 800,
  speed: 12,
  batteryDrainBase: 0.15,
  commsRange: 220,
  turnProbability: 0.05,
  perturbProbability: 0.01,
};

// Mutable runtime state — kept in a class so the React component can hand
// it to the tick loop without pulling state into useState (which would
// re-render every tick and kill performance on a 30+ drone fleet).
export class CyberSimulator {
  readonly bounds: number;
  readonly squadCount: number;
  readonly drones: Map<string, DroneSpec>;
  // Adjacency stored as a simple set of "a|b" keys (canonical order).
  readonly commsLinks: Set<string>;
  readonly cfg: SimulationConfig;
  private rng: () => number;

  constructor(fleet: CyberFleet, cfg: SimulationConfig, seed = 1) {
    this.bounds = fleet.bounds;
    this.squadCount = fleet.squadCount;
    this.drones = new Map(fleet.drones.map((d) => [d.id, { ...d }]));
    this.commsLinks = new Set();
    for (const link of fleet.links) {
      this.commsLinks.add(linkKey(link.source, link.target));
    }
    this.cfg = cfg;

    let s = seed >>> 0;
    this.rng = () => {
      s = (s * 1664525 + 1013904223) >>> 0;
      return s / 0x100000000;
    };
  }

  // step advances one simulation tick, mutating drone state and rewiring
  // comms links. Returns:
  //   - changedDrones: drones whose persistent state needs an update push
  //   - addedLinks / droppedLinks: comms-graph deltas to apply
  //   - events: notable events to surface in the dashboard
  step(tick: number): {
    changedDrones: DroneSpec[];
    addedLinks: { source: string; target: string; distance: number }[];
    droppedLinks: { source: string; target: string }[];
    events: TickEvent[];
  } {
    const events: TickEvent[] = [];
    const changedDrones: DroneSpec[] = [];

    for (const drone of this.drones.values()) {
      if (drone.status === "lost") continue;

      // Random heading reorient.
      if (this.rng() < this.cfg.turnProbability) {
        const theta = this.rng() * Math.PI * 2;
        const z = this.rng() * 2 - 1;
        const r = Math.sqrt(1 - z * z);
        drone.hx = r * Math.cos(theta);
        drone.hy = r * Math.sin(theta);
        drone.hz = z;
      }

      // Advance position along heading.
      drone.x += drone.hx * this.cfg.speed;
      drone.y += drone.hy * this.cfg.speed;
      drone.z += drone.hz * this.cfg.speed;

      // Bounce off the world boundary so drones stay visible. Faults are
      // emitted as out_of_bounds when this happens — they're the kind of
      // sensor anomaly the oracle is meant to surface.
      const half = this.bounds / 2;
      if (
        Math.abs(drone.x) > half ||
        Math.abs(drone.y) > half ||
        Math.abs(drone.z) > half
      ) {
        drone.x = clamp(drone.x, -half, half);
        drone.y = clamp(drone.y, -half, half);
        drone.z = clamp(drone.z, -half, half);
        drone.hx = -drone.hx;
        drone.hy = -drone.hy;
        drone.hz = -drone.hz;
        events.push({ tick, kind: "out_of_bounds", droneId: drone.id });
      }

      // Battery drain. Carriers drain twice as fast (heavier lift), scouts
      // half as fast (efficient sensing). Random perturbation occasionally
      // injects a noticeable drop so the oracle sees something to react to.
      const roleMult =
        drone.role === "carrier" ? 2 : drone.role === "scout" ? 0.5 : 1;
      drone.battery -= this.cfg.batteryDrainBase * roleMult;
      if (this.rng() < this.cfg.perturbProbability) {
        drone.battery -= 1.5;
      }
      if (drone.battery < 0) drone.battery = 0;

      // Status transitions. Lost drones are filtered out at the top of
      // the loop so we only need to handle the online/degraded → lost
      // and online ↔ degraded transitions here.
      if (drone.battery <= 5) {
        drone.status = "lost";
        events.push({
          tick,
          kind: "low_battery",
          droneId: drone.id,
          payload: { battery: drone.battery },
        });
      } else if (drone.battery <= 25 && drone.status === "online") {
        drone.status = "degraded";
        events.push({
          tick,
          kind: "low_battery",
          droneId: drone.id,
          payload: { battery: drone.battery },
        });
      } else if (drone.battery > 25 && drone.status === "degraded") {
        drone.status = "online";
      }

      changedDrones.push({ ...drone });
    }

    // Rewire comms graph. Recompute current set of links by proximity, diff
    // against the previous set. This is O(N²) in fleet size; fine for the
    // demo's ~30 drones, swap to spatial hashing if we ever scale beyond
    // a few hundred.
    const allDrones = Array.from(this.drones.values());
    const desired = new Set<string>();
    const distances = new Map<string, number>();
    for (let i = 0; i < allDrones.length; i++) {
      const a = allDrones[i];
      if (a.status === "lost") continue;
      for (let j = i + 1; j < allDrones.length; j++) {
        const b = allDrones[j];
        if (b.status === "lost") continue;
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const dz = a.z - b.z;
        const d = Math.sqrt(dx * dx + dy * dy + dz * dz);
        if (d <= this.cfg.commsRange) {
          const k = linkKey(a.id, b.id);
          desired.add(k);
          distances.set(k, d);
        }
      }
    }

    const addedLinks: {
      source: string;
      target: string;
      distance: number;
    }[] = [];
    const droppedLinks: { source: string; target: string }[] = [];

    for (const k of desired) {
      if (this.commsLinks.has(k)) continue;
      const [a, b] = k.split("|");
      addedLinks.push({
        source: a,
        target: b,
        distance: Math.round(distances.get(k) ?? 0),
      });
      events.push({
        tick,
        kind: "comms_recovered",
        droneId: a,
        payload: { peer: b },
      });
    }
    for (const k of this.commsLinks) {
      if (desired.has(k)) continue;
      const [a, b] = k.split("|");
      droppedLinks.push({ source: a, target: b });
      events.push({
        tick,
        kind: "comms_lost",
        droneId: a,
        payload: { peer: b },
      });
    }

    this.commsLinks.clear();
    for (const k of desired) this.commsLinks.add(k);

    return { changedDrones, addedLinks, droppedLinks, events };
  }
}

function linkKey(a: string, b: string): string {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v;
}
