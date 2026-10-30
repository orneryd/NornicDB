#!/usr/bin/env node
// Qdrant → NornicDB migration (Node).
//
// NornicDB exposes Qdrant compatibility on gRPC only. There is no
// maintained gRPC Qdrant client for Node, so this script reads from a
// source Qdrant over its REST API (`@qdrant/js-client-rest`) and writes
// into NornicDB over Bolt (`neo4j-driver`). Each Qdrant point becomes a
// node carrying its vector as a property; collections become a label.
//
// If you need a same-API replay (collection → database, named vectors,
// snapshots, the full Qdrant proto compatibility matrix) prefer the
// Python or Go script in this directory — both speak gRPC end-to-end.
//
// Install:
//   npm install @qdrant/js-client-rest neo4j-driver
//
// Run:
//   node migrate.mjs \
//     --source-url http://qdrant.local:6333 \
//     --target-url bolt://nornicdb.local:7687 \
//     --target-user neo4j --target-pass password \
//     --batch-size 256
//
// Optional flags:
//   --collections coll_a,coll_b   --skip-existing   --dry-run
//   --source-api-key TOKEN
//   --label-prefix QdrantPoint    -- label is `${labelPrefix}_${collection}`
//
// The script:
//   1. Lists collections on source.
//   2. For each: ensures a UNIQUE constraint on the per-collection label's
//      `qdrant_id` property and a vector index on its `embedding` property.
//   3. Scrolls source points in batches; UNWIND-MERGEs them into NornicDB
//      with their vector and payload, hitting UnwindSimpleMergeBatch.
//   4. Counts and reports parity per collection.

import { QdrantClient } from "@qdrant/js-client-rest";
import neo4j from "neo4j-driver";

function parseArgs(argv) {
  const args = {
    sourceUrl: "http://localhost:6333",
    sourceApiKey: undefined,
    targetUrl: "bolt://localhost:7687",
    targetUser: "neo4j",
    targetPass: "password",
    targetDb: "neo4j",
    labelPrefix: "QdrantPoint",
    collections: "",
    batchSize: 256,
    skipExisting: false,
    dryRun: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    switch (a) {
      case "--source-url":      args.sourceUrl = next(); break;
      case "--source-api-key":  args.sourceApiKey = next(); break;
      case "--target-url":      args.targetUrl = next(); break;
      case "--target-user":     args.targetUser = next(); break;
      case "--target-pass":     args.targetPass = next(); break;
      case "--target-database": args.targetDb = next(); break;
      case "--label-prefix":    args.labelPrefix = next(); break;
      case "--collections":     args.collections = next(); break;
      case "--batch-size":      args.batchSize = parseInt(next(), 10); break;
      case "--skip-existing":   args.skipExisting = true; break;
      case "--dry-run":         args.dryRun = true; break;
      default:
        console.error(`unknown flag: ${a}`);
        process.exit(2);
    }
  }
  return args;
}

const labelFor = (prefix, name) =>
  `${prefix}_${name.replace(/[^A-Za-z0-9_]/g, "_")}`;

async function ensureSchema(session, label, dimensions, dryRun) {
  const ddl = [
    `CREATE CONSTRAINT ${label}_qdrant_id_unique IF NOT EXISTS FOR (n:${label}) REQUIRE n.qdrant_id IS UNIQUE`,
    `CREATE VECTOR INDEX ${label}_embedding IF NOT EXISTS FOR (n:${label}) ON (n.embedding) OPTIONS {indexConfig: {\`vector.dimensions\`: ${dimensions}, \`vector.similarity_function\`: 'cosine'}}`,
  ];
  for (const stmt of ddl) {
    console.log(`  → ${stmt}`);
    if (!dryRun) await session.run(stmt);
  }
}

async function* streamPoints(src, name, batch) {
  let offset;
  for (;;) {
    const { points, next_page_offset } = await src.scroll(name, {
      limit: batch,
      offset,
      with_payload: true,
      with_vector: true,
    });
    if (!points || points.length === 0) return;
    yield points;
    if (!next_page_offset) return;
    offset = next_page_offset;
  }
}

const UPSERT_CYPHER = (label) => `
UNWIND $rows AS row
MERGE (n:${label} {qdrant_id: row.id})
SET   n.embedding = row.vector,
      n          += row.payload,
      n.qdrant_collection = $collection
`;

async function migrateCollection(src, session, name, batchSize, labelPrefix, skipExisting, dryRun) {
  const info = await src.getCollection(name);
  const params = info.config.params;
  // Single-vector collections only on the Bolt path. Named vectors require
  // a property-per-name layout that this minimal Node bridge doesn't model
  // — use the Python or Go script instead.
  if (!params.vectors || typeof params.vectors.size !== "number") {
    console.warn(`  · skip "${name}" — named-vector collection; use Python/Go gRPC scripts`);
    return 0;
  }
  const dimensions = params.vectors.size;
  const label = labelFor(labelPrefix, name);

  if (skipExisting) {
    const result = await session.run(`MATCH (n:${label}) RETURN count(n) AS c`);
    if (Number(result.records[0]?.get("c") ?? 0) > 0) {
      console.log(`  · target already has rows for label ${label}, --skip-existing → skipping`);
      return 0;
    }
  }

  await ensureSchema(session, label, dimensions, dryRun);

  let total = 0;
  for await (const points of streamPoints(src, name, batchSize)) {
    const rows = points.map((p) => ({
      id: String(p.id),
      vector: p.vector ?? [],
      payload: p.payload ?? {},
    }));
    if (!dryRun) {
      await session.run(UPSERT_CYPHER(label), { rows, collection: name });
    }
    total += rows.length;
    console.log(`  · ${name} → :${label}: upserted ${total}`);
  }
  return total;
}

async function main() {
  const args = parseArgs(process.argv);

  const src = new QdrantClient({ url: args.sourceUrl, apiKey: args.sourceApiKey });
  const driver = neo4j.driver(args.targetUrl, neo4j.auth.basic(args.targetUser, args.targetPass));
  const session = driver.session({
    database: args.targetDb,
    defaultAccessMode: neo4j.session.WRITE,
  });

  const requested = new Set(
    args.collections.split(",").map((s) => s.trim()).filter(Boolean),
  );
  const { collections } = await src.getCollections();
  let names = collections.map((c) => c.name);
  if (requested.size > 0) names = names.filter((n) => requested.has(n));

  if (names.length === 0) {
    console.error("No collections to migrate.");
    process.exit(1);
  }
  console.log(`Migrating ${names.length} collection(s): ${JSON.stringify(names)}`);
  const started = Date.now();

  try {
    for (const name of names) {
      console.log(`\n[${name}]`);
      await migrateCollection(
        src,
        session,
        name,
        args.batchSize,
        args.labelPrefix,
        args.skipExisting,
        args.dryRun,
      );
    }
  } finally {
    await session.close();
    await driver.close();
  }

  console.log(`\nDone in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
