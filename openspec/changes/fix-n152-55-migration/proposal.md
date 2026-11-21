# Correct documented Neo4j migration (N152-55 / NOR-170)

The maintained Go/Python/Node clients write nodes under an internal underscore label. Ordinary endpoint MATCH and the documented promotion cannot see those nodes; a completed migration silently has no relationships or source-labelled reads. The promotion also removes the bridge/list on its first label, and bare relationship CREATE duplicates a rerun.

Correct the migration owner, preserving database internal-node visibility. Share the destination statements across the existing clients; use a visible Neo4jMigrated bridge, stable source IDs, relationship MERGE plus properties, and returned applied-row counts. Keep the bridge/IDs for repeatable batches after label promotion. Add every source label before removing the label-list bookkeeping. Do not change APOC placeholders, global visibility, schema translation or implement one-time repair of existing hidden data.

Verification must drive the actual shared statements and documented completion through Execute under both parsers. Client-side tests cover consumption and rejected partial batches. This is destination-flow verification, not a claim of a live Neo4j/Bolt/schema migration or full release qualification.
