# Visibility Suppression and Deindex

When a node or edge's decay score drops below the visibility threshold, NornicDB suppresses it — hiding it from normal queries while preserving its data. This guide covers suppression behavior, deindex cleanup, and the `reveal()` bypass.

## Visibility Threshold

Each decay profile specifies a `DECAY VISIBILITY THRESHOLD` (default: `0.05`). When an entity's score falls below this value:

1. The entity is marked with `suppressed: true`
2. It becomes invisible to normal `MATCH` queries
3. It is excluded from search results (BM25 and vector)
4. Its data remains intact in primary storage

### Default Threshold

The global default is configured via:

```yaml
memory:
  visibility_threshold: 0.05
```

Or per-profile via the `DECAY VISIBILITY THRESHOLD` directive inside an APPLY block:

```cypher
CREATE DECAY PROFILE document_retention
FOR (n:Document)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.05
}
```

## Suppression vs Deletion

Suppression is **not** deletion:

| Aspect | Suppression | Deletion |
|--------|------------|----------|
| Data preserved | Yes | No |
| Recoverable | Yes (boost score or use `reveal()`) | No |
| Storage used | Yes | Freed |
| Index entries | Removed by deindex cleanup | Removed immediately |
| Relationships | Preserved | Cascaded/orphaned |

## Querying Suppressed Entities

### Using `reveal()`

The `reveal()` Cypher function bypasses visibility suppression for a specific entity:

```cypher
-- See a specific suppressed node
MATCH (n:Document {id: $id})
CALL reveal(n)
RETURN n, decayScore(n)

-- List all suppressed documents
MATCH (n:Document)
CALL reveal(n)
WHERE n.suppressed = true
RETURN n.id, decayScore(n), n.suppressed_at
ORDER BY decayScore(n)
```

`reveal()` is a query-time bypass — it does not change the entity's suppression status. The entity remains suppressed for all other queries.

### Inspecting Suppression with Cypher

```cypher
CALL nornicdb.knowledgepolicy.resolve('node-123', '', '');
```

This returns the full scoring resolution, including the effective threshold, final score, suppression eligibility, and explanation of why the entity was suppressed.

## Deindex Cleanup

Suppressed entities retain their primary data but their secondary index entries (BM25 full-text, vector embeddings) consume space unnecessarily. The deindex cleanup job runs periodically to remove these index entries.

### How It Works

1. When an entity is suppressed, a `DeindexWorkItem` is enqueued
2. The background cleanup job scans for pending work items
3. For each item, it removes the entity's entries from BM25 and vector indexes
4. The primary node/edge data is untouched

### Configuration

The deindex job runs every 24 hours by default. No configuration is required — it runs automatically when decay is enabled.

### Monitoring

Check deindex status with the supported Cypher procedure:

```cypher
CALL nornicdb.knowledgepolicy.deindexStatus();
```

Example result columns:

```text
pending_count | supported | message | workItemId | targetId | targetScope | enqueuedAt | status
```

The browser UI also shows deindex status on the **Security > Knowledge Policies > Deindex Status** tab.

## Restoring Suppressed Entities

To restore a suppressed entity, boost its score above the visibility threshold:

```cypher
-- Reset decay by updating the entity (if scoreFrom is 'VERSION')
MATCH (n:Document {id: $id})
SET n.updated_at = datetime()

-- Or explicitly unsuppress
MATCH (n:Document {id: $id})
REMOVE n.suppressed, n.suppressed_at
```

After restoration, the entity's index entries will be rebuilt on the next search index rebuild:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:7474/nornicdb/search/rebuild"
```

## CLI Commands

```bash
# Suppress nodes below threshold
nornicdb suppress --data-dir ./data --threshold 0.10

# View decay statistics
nornicdb decay stats --data-dir ./data
```

## See Also

- **[Knowledge-Layer Policies](knowledge-layer-policies.md)** — System overview
- **[Decay Profiles](decay-profiles.md)** — Defining decay behavior
- **[Promotion Policies](promotion-policies.md)** — Boosting scores to prevent suppression
- **[Ebbinghaus-Roynard Bootstrap](ebbinghaus-roynard-bootstrap.md)** — Complete working example with visibility thresholds and suppression
- **[CLI Commands](../operations/cli-commands.md)** — CLI reference
