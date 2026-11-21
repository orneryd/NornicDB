#!/usr/bin/env node
// Neo4j → NornicDB migration (Node).
//
// Bolt → Bolt. Reads constraints, indexes, nodes, and relationships from a
// source Neo4j instance and replays them into NornicDB using shared queries.json
// destination statements and verified applied-row counts.
//
// Phases:
//   1. Schema  — constraints, then indexes (vector and fulltext last).
//   2. Nodes   — MERGE on _neo4j_id, preserve labels as data for promotion.
//   3. Edges   — match visible endpoints, MERGE by source ID and SET props.
//
// Install:
//   npm install neo4j-driver
//
// Run:
//   node migrate.mjs \
//     --source-url bolt://neo4j.prod:7687 --source-user neo4j --source-pass <pw> \
//     --target-url bolt://nornicdb.local:7687 \
//     --batch-size 500
//
// Flags: --skip-schema, --skip-nodes, --skip-edges, --labels A,B, --types T,U,
// --dry-run.

import neo4j from "neo4j-driver";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

function parseArgs(argv) {
  const args = {
    sourceUrl: "",
    sourceUser: "neo4j",
    sourcePass: "",
    sourceDb: "neo4j",
    targetUrl: "bolt://localhost:7687",
    targetUser: "neo4j",
    targetPass: "password",
    targetDb: "neo4j",
    batchSize: 500,
    skipSchema: false,
    skipNodes: false,
    skipEdges: false,
    labels: "",
    types: "",
    dryRun: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    switch (a) {
      case "--source-url":      args.sourceUrl = next(); break;
      case "--source-user":     args.sourceUser = next(); break;
      case "--source-pass":     args.sourcePass = next(); break;
      case "--source-database": args.sourceDb = next(); break;
      case "--target-url":      args.targetUrl = next(); break;
      case "--target-user":     args.targetUser = next(); break;
      case "--target-pass":     args.targetPass = next(); break;
      case "--target-database": args.targetDb = next(); break;
      case "--batch-size":      args.batchSize = parseInt(next(), 10); break;
      case "--skip-schema":     args.skipSchema = true; break;
      case "--skip-nodes":      args.skipNodes = true; break;
      case "--skip-edges":      args.skipEdges = true; break;
      case "--labels":          args.labels = next(); break;
      case "--types":           args.types = next(); break;
      case "--dry-run":         args.dryRun = true; break;
      default:
        console.error(`unknown flag: ${a}`);
        process.exit(2);
    }
  }
  if (!args.sourceUrl || !args.sourcePass) {
    console.error("--source-url and --source-pass are required");
    process.exit(2);
  }
  return args;
}

const csvSet = (s) =>
  new Set(s.split(",").map((x) => x.trim()).filter(Boolean));

// ---- Schema ----------------------------------------------------------------

async function fetchAll(sess, q) {
  const r = await sess.run(q);
  return r.records.map((rec) => Object.fromEntries(rec.keys.map((k) => [k, rec.get(k)])));
}

function constraintToCypher(c) {
  const name = c.name ?? "";
  const ctype = String(c.type ?? "").toUpperCase();
  const entity = String(c.entityType ?? "").toUpperCase();
  const labels = c.labelsOrTypes ?? [];
  const props = c.properties ?? [];
  if (!labels.length || !props.length) return null;
  const label = labels[0];
  const propParts = props.map((p) => `n.${p}`);
  const propList = props.length > 1 ? `(${propParts.join(", ")})` : propParts[0];

  if (entity === "NODE") {
    if (ctype === "UNIQUENESS")
      return `CREATE CONSTRAINT ${name} IF NOT EXISTS FOR (n:${label}) REQUIRE ${propList} IS UNIQUE`;
    if (ctype === "NODE_KEY")
      return `CREATE CONSTRAINT ${name} IF NOT EXISTS FOR (n:${label}) REQUIRE ${propList} IS NODE KEY`;
    if (ctype === "NODE_PROPERTY_EXISTENCE")
      return `CREATE CONSTRAINT ${name} IF NOT EXISTS FOR (n:${label}) REQUIRE ${propList} IS NOT NULL`;
  } else if (entity === "RELATIONSHIP") {
    if (ctype === "RELATIONSHIP_PROPERTY_EXISTENCE")
      return `CREATE CONSTRAINT ${name} IF NOT EXISTS FOR ()-[r:${label}]-() REQUIRE r.${props[0]} IS NOT NULL`;
    if (ctype === "RELATIONSHIP_UNIQUENESS")
      return `CREATE CONSTRAINT ${name} IF NOT EXISTS FOR ()-[r:${label}]-() REQUIRE r.${props[0]} IS UNIQUE`;
  }
  return null;
}

function indexToCypher(idx) {
  if (idx.owningConstraint) return null;
  const name = idx.name ?? "";
  const itype = String(idx.type ?? "").toUpperCase();
  const entity = String(idx.entityType ?? "").toUpperCase();
  const labels = idx.labelsOrTypes ?? [];
  const props = idx.properties ?? [];
  if (!labels.length || !props.length || entity !== "NODE") return null;
  const label = labels[0];

  if (itype === "RANGE" || itype === "BTREE") {
    if (props.length === 1)
      return `CREATE INDEX ${name} IF NOT EXISTS FOR (n:${label}) ON (n.${props[0]})`;
    return `CREATE INDEX ${name} IF NOT EXISTS FOR (n:${label}) ON (${props
      .map((p) => `n.${p}`)
      .join(", ")})`;
  }
  if (itype === "FULLTEXT") {
    return `CREATE FULLTEXT INDEX ${name} IF NOT EXISTS FOR (n:${label}) ON EACH [${props
      .map((p) => `n.${p}`)
      .join(", ")}]`;
  }
  if (itype === "VECTOR") {
    const cfg = idx.options?.indexConfig ?? {};
    const dims = Number(cfg["vector.dimensions"]);
    const sim = cfg["vector.similarity_function"] ?? "cosine";
    if (!Number.isFinite(dims) || dims <= 0) return null;
    return `CREATE VECTOR INDEX ${name} IF NOT EXISTS FOR (n:${label}) ON (n.${props[0]}) OPTIONS {indexConfig: {\`vector.dimensions\`: ${dims}, \`vector.similarity_function\`: '${sim}'}}`;
  }
  return null;
}

async function replicateSchema(srcSess, dstSess, dryRun) {
  console.log("\n[schema]");
  const constraints = await fetchAll(srcSess, "SHOW CONSTRAINTS");
  for (const c of constraints) {
    const ddl = constraintToCypher(c);
    if (!ddl) {
      console.log(`  · skip constraint (unmapped): ${c.name}`);
      continue;
    }
    console.log(`  → ${ddl}`);
    if (!dryRun) await dstSess.run(ddl);
  }

  const indexes = await fetchAll(srcSess, "SHOW INDEXES");
  for (const i of indexes) {
    const ddl = indexToCypher(i);
    if (!ddl) continue;
    console.log(`  → ${ddl}`);
    if (!dryRun) await dstSess.run(ddl);
  }
}

// ---- Nodes ----------------------------------------------------------------

// One destination statement owner is shared with the Go and Python clients.
const QUERIES = JSON.parse(readFileSync(new URL("./queries.json", import.meta.url), "utf8"));
const NODE_UPSERT_CYPHER = QUERIES.nodes;

async function applyBatch(dst, query, rows) {
  const result = await dst.run(query, { rows });
  const applied = result.records.length === 1 ? Number(result.records[0].get("applied")) : NaN;
  if (!Number.isInteger(applied) || applied !== rows.length) {
    throw new Error(`migration batch incomplete: target applied ${applied} of ${rows.length} source rows`);
  }
  return applied;
}

async function fetchLabels(sess) {
  const rs = await fetchAll(sess, "CALL db.labels() YIELD label RETURN label");
  return rs.map((r) => r.label);
}

async function* streamNodes(sess, label, batch) {
  let lastId = null;
  for (;;) {
    let res;
    if (lastId === null) {
      res = await sess.run(
        `MATCH (n:\`${label}\`) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props ORDER BY elementId(n) LIMIT $limit`,
        { limit: neo4j.int(batch) },
      );
    } else {
      res = await sess.run(
        `MATCH (n:\`${label}\`) WHERE elementId(n) > $cursor RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props ORDER BY elementId(n) LIMIT $limit`,
        { cursor: lastId, limit: neo4j.int(batch) },
      );
    }
    const rows = res.records.map((rec) => ({
      id: rec.get("id"),
      labels: rec.get("labels"),
      props: rec.get("props"),
    }));
    if (rows.length === 0) return;
    yield rows;
    lastId = rows[rows.length - 1].id;
    if (rows.length < batch) return;
  }
}

async function replicateNodes(srcSess, dstSess, labels, filter, batch, dryRun) {
  console.log("\n[nodes]");
  let total = 0;
  for (const label of labels) {
    if (filter.size > 0 && !filter.has(label)) continue;
    if (label.startsWith("_")) continue;
    let moved = 0;
    for await (const chunk of streamNodes(srcSess, label, batch)) {
      const applied = dryRun ? chunk.length : await applyBatch(dstSess, NODE_UPSERT_CYPHER, chunk);
      moved += applied;
      total += applied;
      console.log(`  · :${label}: ${moved}`);
    }
  }
  console.log(`  ${dryRun ? "source node rows read (dry run)" : "target node rows applied"}: ${total}`);
}

// ---- Edges ----------------------------------------------------------------

async function fetchRelTypes(sess) {
  const rs = await fetchAll(
    sess,
    "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
  );
  return rs.map((r) => r.relationshipType);
}

async function* streamEdges(sess, relType, batch) {
  let lastId = null;
  for (;;) {
    let res;
    if (lastId === null) {
      res = await sess.run(
        `MATCH (a)-[r:\`${relType}\`]->(b) RETURN elementId(r) AS id, elementId(a) AS startId, elementId(b) AS endId, properties(r) AS props ORDER BY elementId(r) LIMIT $limit`,
        { limit: neo4j.int(batch) },
      );
    } else {
      res = await sess.run(
        `MATCH (a)-[r:\`${relType}\`]->(b) WHERE elementId(r) > $cursor RETURN elementId(r) AS id, elementId(a) AS startId, elementId(b) AS endId, properties(r) AS props ORDER BY elementId(r) LIMIT $limit`,
        { cursor: lastId, limit: neo4j.int(batch) },
      );
    }
    const rows = res.records.map((rec) => ({
      id: rec.get("id"),
      startId: rec.get("startId"),
      endId: rec.get("endId"),
      props: rec.get("props"),
    }));
    if (rows.length === 0) return;
    yield rows;
    lastId = rows[rows.length - 1].id;
    if (rows.length < batch) return;
  }
}

const edgeUpsertCypher = (t) =>
  QUERIES.edges.replaceAll("{{type}}", "`" + t.replaceAll("`", "``") + "`");

async function replicateEdges(srcSess, dstSess, types, filter, batch, dryRun) {
  console.log("\n[edges]");
  let total = 0;
  for (const t of types) {
    if (filter.size > 0 && !filter.has(t)) continue;
    let moved = 0;
    const createQ = edgeUpsertCypher(t);
    for await (const chunk of streamEdges(srcSess, t, batch)) {
      const applied = dryRun ? chunk.length : await applyBatch(dstSess, createQ, chunk);
      moved += applied;
      total += applied;
      console.log(`  · :${t}: ${moved}`);
    }
  }
  console.log(`  ${dryRun ? "source relationship rows read (dry run)" : "target relationship rows applied"}: ${total}`);
}

// ---- Main -----------------------------------------------------------------

async function main() {
  const args = parseArgs(process.argv);
  const labelFilter = csvSet(args.labels);
  const typeFilter = csvSet(args.types);

  const srcDrv = neo4j.driver(
    args.sourceUrl,
    neo4j.auth.basic(args.sourceUser, args.sourcePass),
  );
  const dstDrv = neo4j.driver(
    args.targetUrl,
    neo4j.auth.basic(args.targetUser, args.targetPass),
  );

  const srcSess = srcDrv.session({
    database: args.sourceDb,
    defaultAccessMode: neo4j.session.READ,
  });
  const dstSess = dstDrv.session({
    database: args.targetDb,
    defaultAccessMode: neo4j.session.WRITE,
  });

  const started = Date.now();
  try {
    if (!args.skipSchema) await replicateSchema(srcSess, dstSess, args.dryRun);
    if (!args.skipNodes) {
      const labels = await fetchLabels(srcSess);
      await replicateNodes(srcSess, dstSess, labels, labelFilter, args.batchSize, args.dryRun);
    }
    if (!args.skipEdges) {
      const types = await fetchRelTypes(srcSess);
      await replicateEdges(srcSess, dstSess, types, typeFilter, args.batchSize, args.dryRun);
    }
  } finally {
    await srcSess.close();
    await dstSess.close();
    await srcDrv.close();
    await dstDrv.close();
  }

  console.log(`\nDone in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

export { applyBatch, edgeUpsertCypher, NODE_UPSERT_CYPHER, replicateNodes, replicateEdges };

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
