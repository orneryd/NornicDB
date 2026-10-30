// Procedurally generates a 3D galactic mesh used by the /demo page.
//
// Layout: 12 "sectors" arranged on a Fibonacci sphere. Each sector contains
// ~36 stars with intra-sector hyperlanes; sectors are stitched together by
// a small number of gateway stars that link only to the next sector in the
// chain. A traversal from sector 0 to sector 11 must therefore cross every
// sector boundary — yielding natural 11+ hop shortest paths.

export interface DemoStar {
  id: string;
  name: string;
  sector: number;
  hue: number;
  mass: number;
  x: number;
  y: number;
  z: number;
}

export interface DemoEdge {
  source: string;
  target: string;
  distance: number;
}

export interface DemoGalaxy {
  stars: DemoStar[];
  edges: DemoEdge[];
  startId: string;
  endId: string;
  sectorCount: number;
}

const STAR_PREFIX = [
  "Yggdra",
  "Nidh",
  "Mim",
  "Gjall",
  "Heidr",
  "Surt",
  "Vidar",
  "Hodr",
  "Skuld",
  "Urda",
  "Verda",
  "Bragi",
  "Iduna",
  "Freyja",
  "Tyr",
  "Vanir",
  "Asgar",
  "Mjoll",
  "Gunn",
  "Sleip",
  "Hugin",
  "Munin",
  "Ratat",
  "Fenris",
];
const STAR_SUFFIX = [
  "Prime",
  "Major",
  "Minor",
  "Reach",
  "Crown",
  "Spire",
  "Halo",
  "Drift",
  "Verge",
  "Anchor",
  "Bastion",
  "Forge",
  "Echo",
  "Hollow",
  "Cradle",
  "Mark",
];

function pseudoRand(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0x100000000;
  };
}

function fibSpherePoint(
  i: number,
  n: number,
  radius: number,
): [number, number, number] {
  const phi = Math.acos(1 - (2 * (i + 0.5)) / n);
  const theta = Math.PI * (1 + Math.sqrt(5)) * i;
  const x = radius * Math.cos(theta) * Math.sin(phi);
  const y = radius * Math.sin(theta) * Math.sin(phi);
  const z = radius * Math.cos(phi);
  return [x, y, z];
}

export function generateGalaxy(seed = 42): DemoGalaxy {
  // 125 sectors × 400 stars = 50,000 nodes. Sectors form a linear chain;
  // each adjacent pair is bridged by exactly 2 gateway hyperlanes (between
  // 2 nodes on each side), so traversal across the chain still produces
  // deep multi-hop paths and the visual topology stays the same as the
  // smaller demo — just bigger.
  const sectorCount = 20;
  const starsPerSector = 100;
  // Intra-sector edges are by far the largest contributor to total edge
  // count (200K+ at this scale). Two per star keeps the sector visually
  // connected without flooding the seeder.
  const intraEdgesPerStar = 7;
  const gatewaysPerLink = 2;

  const rand = pseudoRand(seed);
  const stars: DemoStar[] = [];
  const edges: DemoEdge[] = [];
  const sectorMembers: string[][] = Array.from(
    { length: sectorCount },
    () => [],
  );

  // Sector centers spread on a sphere. Radius scales with sector count so
  // a 125-sector galaxy doesn't pack adjacent sectors on top of each other.
  const sphereRadius = 200 + sectorCount * 8;
  const sectorCenters: [number, number, number][] = [];
  for (let s = 0; s < sectorCount; s++) {
    sectorCenters.push(fibSpherePoint(s, sectorCount, sphereRadius));
  }

  // Generate stars per sector, clustered around their sector center.
  for (let s = 0; s < sectorCount; s++) {
    const [cx, cy, cz] = sectorCenters[s];
    const hue = Math.round((s / sectorCount) * 340);
    for (let i = 0; i < starsPerSector; i++) {
      const id = `s${s}-${i}`;
      const namePrefix = STAR_PREFIX[(s * 7 + i) % STAR_PREFIX.length];
      const nameSuffix = STAR_SUFFIX[(i * 3 + s) % STAR_SUFFIX.length];
      const designation = `${namePrefix} ${nameSuffix} ${s}-${i.toString().padStart(2, "0")}`;
      // Jitter scales with sqrt(starsPerSector) so 400-star sectors don't
      // collapse into a tight ball.
      const jitter = 30 + Math.sqrt(starsPerSector) * 6;
      const x = cx + (rand() - 0.5) * jitter;
      const y = cy + (rand() - 0.5) * jitter;
      const z = cz + (rand() - 0.5) * jitter;
      stars.push({
        id,
        name: designation,
        sector: s,
        hue,
        mass: 1 + Math.round(rand() * 18),
        x,
        y,
        z,
      });
      sectorMembers[s].push(id);
    }
  }

  // Build a connected backbone within each sector + a few extra random links.
  const seenEdge = new Set<string>();
  const starById = new Map<string, DemoStar>();
  for (const s of stars) starById.set(s.id, s);
  // Stars are bidirectionally connected: emit one edge per direction so a
  // directed bounded traversal (cookbook 6.3) can reach both ways. The
  // visualizer dedupes for rendering.
  const addEdge = (a: string, b: string) => {
    if (a === b) return;
    const key = a < b ? `${a}|${b}` : `${b}|${a}`;
    if (seenEdge.has(key)) return;
    seenEdge.add(key);
    const sa = starById.get(a)!;
    const sb = starById.get(b)!;
    const dx = sa.x - sb.x;
    const dy = sa.y - sb.y;
    const dz = sa.z - sb.z;
    const distance = Math.round(Math.sqrt(dx * dx + dy * dy + dz * dz));
    edges.push({ source: a, target: b, distance });
    edges.push({ source: b, target: a, distance });
  };

  for (let s = 0; s < sectorCount; s++) {
    const members = sectorMembers[s];
    // Spanning chain so the sector subgraph is always connected.
    for (let i = 1; i < members.length; i++) {
      addEdge(members[i - 1], members[i]);
    }
    // Extra short-range links so the visual is dense but not chaotic.
    for (const id of members) {
      for (let k = 0; k < intraEdgesPerStar; k++) {
        const other = members[Math.floor(rand() * members.length)];
        addEdge(id, other);
      }
    }
  }

  // Stitch sectors in a chain (0 → 1 → 2 ... → 11). Only adjacent sectors
  // are linked, so traversal across the chain forces deep multi-hop paths.
  for (let s = 0; s < sectorCount - 1; s++) {
    const a = sectorMembers[s];
    const b = sectorMembers[s + 1];
    for (let g = 0; g < gatewaysPerLink; g++) {
      const fromId = a[Math.floor(rand() * a.length)];
      const toId = b[Math.floor(rand() * b.length)];
      addEdge(fromId, toId);
    }
  }

  // Pick endpoints in opposite-end sectors so the demo always has a deep
  // path to highlight.
  const startId =
    sectorMembers[0][Math.floor(rand() * sectorMembers[0].length)];
  const endId =
    sectorMembers[sectorCount - 1][
      Math.floor(rand() * sectorMembers[sectorCount - 1].length)
    ];

  return { stars, edges, startId, endId, sectorCount };
}
