import { generateGalaxy } from "./demoSeed";
import { generateFleet } from "./cyberSeed";
import { sampleSales } from "./pivotDemo";

export const demoKinds = ["galaxy", "fleet", "pivot"] as const;
export type DemoKind = (typeof demoKinds)[number];
export const demoDescriptions: Record<
  DemoKind,
  { title: string; description: string }
> = {
  galaxy: {
    title: "Galaxy traversal",
    description:
      "Follow shortest paths through a connected graph of stars and compare request timings.",
  },
  fleet: {
    title: "Fleet simulation",
    description:
      "Watch graph updates and live queries track a moving fleet and changing communications links.",
  },
  pivot: {
    title: "Pivot analysis",
    description:
      "Aggregate 25,000 synthetic orders into 1,000 account rows, then explore dimensions and virtualized results.",
  },
};
export type SeedStep = {
  label: string;
  query: string;
  rows?: Record<string, unknown>[];
};
export function demoPlan(kind: DemoKind): SeedStep[] {
  if (kind === "galaxy") {
    const galaxy = generateGalaxy(0xfeed_d3);
    return [
      {
        label: "Indexing stars",
        query:
          "CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)",
      },
      {
        label: "Seeding stars",
        query:
          "UNWIND $rows AS row MERGE (n:Star {starId: row.starId, embedding_skipped: 'demo dataset'}) SET n.name=row.name, n.sector=row.sector, n.hue=row.hue, n.mass=row.mass, n.x=row.x, n.y=row.y, n.z=row.z",
        rows: galaxy.stars.map((s) => ({
          starId: s.id,
          name: s.name,
          sector: s.sector,
          hue: s.hue,
          mass: s.mass,
          x: s.x,
          y: s.y,
          z: s.z,
        })),
      },
      {
        label: "Linking hyperlanes",
        query:
          "UNWIND $rows AS row MATCH (a:Star {starId: row.fromId}) MATCH (b:Star {starId: row.toId}) MERGE (a)-[r:HYPERLANE]->(b) SET r.distance=row.distance",
        rows: galaxy.edges.map((e) => ({
          fromId: e.source,
          toId: e.target,
          distance: e.distance,
        })),
      },
    ];
  }
  if (kind === "fleet") {
    const fleet = generateFleet({ seed: 0xcafef00d });
    return [
      {
        label: "Indexing drones",
        query:
          "CREATE INDEX drone_id_idx IF NOT EXISTS FOR (n:Drone) ON (n.droneId)",
      },
      {
        label: "Seeding drones",
        query:
          "UNWIND $rows AS row MERGE (n:Drone {droneId: row.droneId, embedding_skipped: 'demo dataset'}) SET n.callsign=row.callsign,n.squad=row.squad,n.role=row.role,n.x=row.x,n.y=row.y,n.z=row.z,n.battery=row.battery,n.status=row.status",
        rows: fleet.drones.map((d) => ({
          droneId: d.id,
          callsign: d.callsign,
          squad: d.squad,
          role: d.role,
          x: d.x,
          y: d.y,
          z: d.z,
          battery: d.battery,
          status: d.status,
        })),
      },
      {
        label: "Linking communications",
        query:
          "UNWIND $rows AS row MATCH (a:Drone {droneId: row.fromId}) MATCH (b:Drone {droneId: row.toId}) MERGE (a)-[r:COMMS]->(b) SET r.distance=row.distance",
        rows: fleet.links.map((l) => ({
          fromId: l.source,
          toId: l.target,
          distance: l.distance,
        })),
      },
    ];
  }
  return [
    {
      label: "Indexing orders",
      query:
        "CREATE INDEX demo_sale_id IF NOT EXISTS FOR (n:DemoSale) ON (n.orderId)",
    },
    {
      label: "Seeding sales",
      query:
        "UNWIND $rows AS row MERGE (sale:DemoSale {orderId: row.orderId, embedding_skipped: 'demo dataset'}) SET sale.account=row.account,sale.region=row.region,sale.product=row.product,sale.quarter=row.quarter,sale.channel=row.channel,sale.revenue=row.revenue,sale.units=row.units",
      rows: sampleSales(25000).map((sale, index) => ({
        ...sale,
        orderId: `order-${index}`,
      })),
    },
  ];
}
