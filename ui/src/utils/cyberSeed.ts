// Procedural drone-fleet seed for the /cyber experimentation demo.
//
// Layout: a cube of side BOUNDS centered on the origin. Drones spawn at
// random positions, are assigned a role, and join one of N squads.
// Comms links are seeded by initial proximity (within COMMS_RANGE); the
// tick loop will rewire them as drones move.
//
// The seed populates the DB once on first load; from there the UI pushes
// per-tick deltas and the oracle reads back via cypher.

export type DroneRole = "scout" | "relay" | "interceptor" | "carrier";

export interface DroneSpec {
  id: string;
  callsign: string;
  squad: number;
  role: DroneRole;
  // Position in arbitrary world units; bounds are ±BOUNDS / 2 along each axis.
  x: number;
  y: number;
  z: number;
  // heading is a unit-ish vector; we don't care about strict normalization
  // because the tick loop renormalizes after each move.
  hx: number;
  hy: number;
  hz: number;
  battery: number; // 0..100
  status: "online" | "degraded" | "lost";
}

export interface CommsLinkSpec {
  source: string;
  target: string;
  // distance at seed time; tick loop recomputes when drones move.
  distance: number;
}

export interface CyberFleet {
  drones: DroneSpec[];
  links: CommsLinkSpec[];
  bounds: number;
  squadCount: number;
}

const ROLES: DroneRole[] = ["scout", "relay", "interceptor", "carrier"];
const CALLSIGN_PREFIX = [
  "HUGIN",
  "MUNIN",
  "SLEIPNIR",
  "GERI",
  "FREKI",
  "TANNGRISNIR",
  "TANNGNJOSTR",
  "GULLINBURSTI",
  "FENRIR",
  "JORMUNGANDR",
  "NIDHOGG",
  "RATATOSKR",
];

const BOUNDS = 800;
const COMMS_RANGE = 220;

function lcg(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0x100000000;
  };
}

function unitVector(rng: () => number): [number, number, number] {
  // Marsaglia-ish; not strictly uniform on the sphere, but plenty good
  // enough for visualization and motion seeding.
  const theta = rng() * Math.PI * 2;
  const z = rng() * 2 - 1;
  const r = Math.sqrt(1 - z * z);
  return [r * Math.cos(theta), r * Math.sin(theta), z];
}

export function generateFleet(opts?: {
  seed?: number;
  droneCount?: number;
  squadCount?: number;
}): CyberFleet {
  const seed = opts?.seed ?? 0xdeadbeef;
  const droneCount = opts?.droneCount ?? 30;
  const squadCount = opts?.squadCount ?? 4;

  const rng = lcg(seed);
  const drones: DroneSpec[] = [];
  for (let i = 0; i < droneCount; i++) {
    const role = ROLES[i % ROLES.length];
    const squad = i % squadCount;
    const prefix = CALLSIGN_PREFIX[(i * 3) % CALLSIGN_PREFIX.length];
    const [hx, hy, hz] = unitVector(rng);
    drones.push({
      id: `d${i}`,
      callsign: `${prefix}-${(i + 1).toString().padStart(2, "0")}`,
      squad,
      role,
      x: (rng() - 0.5) * BOUNDS,
      y: (rng() - 0.5) * BOUNDS,
      z: (rng() - 0.5) * BOUNDS,
      hx,
      hy,
      hz,
      battery: 60 + Math.floor(rng() * 40),
      status: "online",
    });
  }

  // Comms links by initial proximity. Each drone keeps up to LINKS_PER_DRONE
  // closest neighbors so the comms graph isn't dense at high fleet sizes.
  const LINKS_PER_DRONE = 4;
  const links: CommsLinkSpec[] = [];
  const seen = new Set<string>();
  for (const a of drones) {
    type cand = { id: string; d: number };
    const candidates: cand[] = [];
    for (const b of drones) {
      if (a.id === b.id) continue;
      const dx = a.x - b.x;
      const dy = a.y - b.y;
      const dz = a.z - b.z;
      const d = Math.sqrt(dx * dx + dy * dy + dz * dz);
      if (d <= COMMS_RANGE) candidates.push({ id: b.id, d });
    }
    candidates.sort((p, q) => p.d - q.d);
    for (const c of candidates.slice(0, LINKS_PER_DRONE)) {
      const k = a.id < c.id ? `${a.id}|${c.id}` : `${c.id}|${a.id}`;
      if (seen.has(k)) continue;
      seen.add(k);
      links.push({ source: a.id, target: c.id, distance: Math.round(c.d) });
      links.push({ source: c.id, target: a.id, distance: Math.round(c.d) });
    }
  }

  return { drones, links, bounds: BOUNDS, squadCount };
}
