package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCodeStateChangeQueryShapes_ImpactsAndHasState(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Seed states for IMPACTS relation matching.
	seedQueries := []string{
		`CREATE (:CodeState {state_id: 'cs-1', code_key: 'k-1', tx_id: 'tx-a'})`,
		`CREATE (:CodeState {state_id: 'cs-2', code_key: 'k-2', tx_id: 'tx-b'})`,
		`CREATE (:CodeState {state_id: 'cs-3', code_key: 'k-fallback', tx_id: 'tx-fb'})`,
	}
	for _, q := range seedQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// A) Single-row CodeChange -> IMPACTS -> CodeState
	_, err := exec.Execute(ctx, strings.TrimSpace(`
MERGE (cc:CodeChange {change_id: $change_id})
SET cc.tx_id = $tx_id,
    cc.actor = $actor,
    cc.timestamp = datetime($timestamp_iso),
    cc.op_type = $op_type,
    cc.commit_hash = $commit_hash
WITH cc
MATCH (cs:CodeState {state_id: $affected_state_id})
MERGE (cc)-[:IMPACTS]->(cs);
`), map[string]interface{}{
		"change_id":         "chg-1",
		"tx_id":             "tx-a",
		"actor":             "alice",
		"timestamp_iso":     "2026-03-20T10:00:00Z",
		"op_type":           "update",
		"commit_hash":       "abc123",
		"affected_state_id": "cs-1",
	})
	require.NoError(t, err)

	// A) Batch UNWIND CodeChange -> IMPACTS -> CodeState (state_id path)
	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (cc:CodeChange {change_id: row.change_id})
SET cc.tx_id = row.tx_id,
    cc.actor = row.actor,
    cc.timestamp = datetime(row.timestamp_iso),
    cc.op_type = row.op_type,
    cc.commit_hash = row.commit_hash
WITH cc, row
MATCH (cs:CodeState {state_id: row.affected_state_id})
MERGE (cc)-[:IMPACTS]->(cs);
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"change_id":         "chg-2",
				"tx_id":             "tx-b",
				"actor":             "bob",
				"timestamp_iso":     "2026-03-20T10:01:00Z",
				"op_type":           "create",
				"commit_hash":       "def456",
				"affected_state_id": "cs-2",
			},
		},
	})
	require.NoError(t, err)

	// A) Fallback UNWIND CodeChange -> IMPACTS -> CodeState (code_key + tx_id path)
	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (cc:CodeChange {change_id: row.change_id})
SET cc.tx_id = row.tx_id,
    cc.actor = row.actor,
    cc.timestamp = datetime(row.timestamp_iso),
    cc.op_type = row.op_type,
    cc.commit_hash = row.commit_hash
WITH cc, row
MATCH (cs:CodeState {code_key: row.affected_code_key, tx_id: row.tx_id})
MERGE (cc)-[:IMPACTS]->(cs);
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"change_id":         "chg-3",
				"tx_id":             "tx-fb",
				"actor":             "carol",
				"timestamp_iso":     "2026-03-20T10:02:00Z",
				"op_type":           "delete",
				"commit_hash":       "ghi789",
				"affected_code_key": "k-fallback",
			},
		},
	})
	require.NoError(t, err)

	// B) Single-row relation_type family via CodeKey -> HAS_STATE
	_, err = exec.Execute(ctx, strings.TrimSpace(`
MERGE (ck:CodeKey {entity_id: $entity_id, relation_type: $relation_type})
MERGE (cs:CodeState {state_id: $state_id})
SET cs.code_key = $code_key,
    cs.tx_id = $tx_id,
    cs.commit_hash = $commit_hash,
    cs.valid_from_iso = $valid_from_iso,
    cs.valid_from = datetime($valid_from_iso),
    cs.value_json = $value_json
MERGE (ck)-[:HAS_STATE]->(cs);
`), map[string]interface{}{
		"entity_id":      "ent-calls-1",
		"relation_type":  "calls",
		"state_id":       "state-calls-1",
		"code_key":       "ck-calls-1",
		"tx_id":          "tx-calls-1",
		"commit_hash":    "hash-calls-1",
		"valid_from_iso": "2026-03-20T11:00:00Z",
		"value_json":     `{"kind":"calls","n":1}`,
	})
	require.NoError(t, err)

	// B) Batch UNWIND relation_type families calls/contains/import
	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (ck:CodeKey {entity_id: row.entity_id, relation_type: row.relation_type})
MERGE (cs:CodeState {state_id: row.state_id})
SET cs.code_key = row.code_key,
    cs.tx_id = row.tx_id,
    cs.commit_hash = row.commit_hash,
    cs.valid_from_iso = row.valid_from_iso,
    cs.valid_from = datetime(row.valid_from_iso),
    cs.value_json = row.value_json
MERGE (ck)-[:HAS_STATE]->(cs);
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":      "ent-calls-2",
				"relation_type":  "calls",
				"state_id":       "state-calls-2",
				"code_key":       "ck-calls-2",
				"tx_id":          "tx-calls-2",
				"commit_hash":    "hash-calls-2",
				"valid_from_iso": "2026-03-20T11:01:00Z",
				"value_json":     `{"kind":"calls","n":2}`,
			},
			{
				"entity_id":      "ent-contains-1",
				"relation_type":  "contains",
				"state_id":       "state-contains-1",
				"code_key":       "ck-contains-1",
				"tx_id":          "tx-contains-1",
				"commit_hash":    "hash-contains-1",
				"valid_from_iso": "2026-03-20T11:02:00Z",
				"value_json":     `{"kind":"contains","n":1}`,
			},
			{
				"entity_id":      "ent-import-1",
				"relation_type":  "import",
				"state_id":       "state-import-1",
				"code_key":       "ck-import-1",
				"tx_id":          "tx-import-1",
				"commit_hash":    "hash-import-1",
				"valid_from_iso": "2026-03-20T11:03:00Z",
				"value_json":     `{"kind":"import","n":1}`,
			},
		},
	})
	require.NoError(t, err)

	// Validation reads (exact shapes)
	impactsRes, err := exec.Execute(ctx, `MATCH (:CodeChange)-[:IMPACTS]->(:CodeState) RETURN count(*) AS impacts_count;`, nil)
	require.NoError(t, err)
	require.Len(t, impactsRes.Rows, 1)
	require.Equal(t, int64(3), impactsRes.Rows[0][0])

	callsRes, err := exec.Execute(ctx, `MATCH (ck:CodeKey {relation_type: 'calls'})-[:HAS_STATE]->(:CodeState) RETURN count(*) AS calls_states;`, nil)
	require.NoError(t, err)
	require.Len(t, callsRes.Rows, 1)
	require.Equal(t, int64(2), callsRes.Rows[0][0])

	containsRes, err := exec.Execute(ctx, `MATCH (ck:CodeKey {relation_type: 'contains'})-[:HAS_STATE]->(:CodeState) RETURN count(*) AS contains_states;`, nil)
	require.NoError(t, err)
	require.Len(t, containsRes.Rows, 1)
	require.Equal(t, int64(1), containsRes.Rows[0][0])

	importRes, err := exec.Execute(ctx, `MATCH (ck:CodeKey {relation_type: 'import'})-[:HAS_STATE]->(:CodeState) RETURN count(*) AS import_states;`, nil)
	require.NoError(t, err)
	require.Len(t, importRes.Rows, 1)
	require.Equal(t, int64(1), importRes.Rows[0][0])
}

func TestCodeStateChangeQueryShapes_ImpactsWithOptionalCoalesce(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Seed states for direct-id and fallback (code_key + tx_id) matches.
	seedQueries := []string{
		`CREATE (:CodeState {state_id: 'state-by-id-1', code_key: 'ck-a', tx_id: 'tx-a'})`,
		`CREATE (:CodeState {state_id: 'state-by-id-2', code_key: 'ck-b', tx_id: 'tx-b'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-1', code_key: 'ck-fallback-1', tx_id: 'tx-fb-1'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-2', code_key: 'ck-fallback-2', tx_id: 'tx-fb-2'})`,
	}
	for _, q := range seedQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// Single-row equivalent shape
	_, err := exec.Execute(ctx, strings.TrimSpace(`
MERGE (cc:CodeChange {change_id: $change_id})
SET cc.tx_id = $tx_id,
    cc.actor = $actor,
    cc.timestamp = datetime($timestamp_iso),
    cc.op_type = $op_type,
    cc.commit_hash = $commit_hash
MERGE (c:Commit {hash: $commit_hash})
ON CREATE SET c.timestamp = datetime($timestamp_iso), c.tx_id = $tx_id, c.actor = $actor
MERGE (c)-[:EMITTED]->(cc)
OPTIONAL MATCH (csByID:CodeState {state_id: $affected_state_id})
OPTIONAL MATCH (csByKey:CodeState {code_key: $affected_code_key, tx_id: $tx_id})
WITH cc, coalesce(csByID, csByKey) AS cs
WHERE cs IS NOT NULL
MERGE (cc)-[:IMPACTS]->(cs)
`), map[string]interface{}{
		"change_id":         "chg-single",
		"tx_id":             "tx-a",
		"actor":             "alice",
		"timestamp_iso":     "2026-03-20T12:00:00Z",
		"op_type":           "update",
		"commit_hash":       "commit-single",
		"affected_state_id": "state-by-id-1",
		"affected_code_key": "does-not-matter",
	})
	require.NoError(t, err)

	// Exact UNWIND shape with optional id/key resolution.
	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (cc:CodeChange {change_id: row.change_id})
SET cc.tx_id = row.tx_id,
    cc.actor = row.actor,
    cc.timestamp = datetime(row.timestamp_iso),
    cc.op_type = row.op_type,
    cc.commit_hash = row.commit_hash
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.timestamp = datetime(row.timestamp_iso), c.tx_id = row.tx_id, c.actor = row.actor
MERGE (c)-[:EMITTED]->(cc)
WITH cc, row
OPTIONAL MATCH (csByID:CodeState {state_id: row.affected_state_id})
WITH cc, row, csByID
OPTIONAL MATCH (csByKey:CodeState {code_key: row.affected_code_key, tx_id: row.tx_id})
WITH cc, coalesce(csByID, csByKey) AS cs
WHERE cs IS NOT NULL
MERGE (cc)-[:IMPACTS]->(cs)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"change_id":         "chg-row-id",
				"tx_id":             "tx-b",
				"actor":             "bob",
				"timestamp_iso":     "2026-03-20T12:01:00Z",
				"op_type":           "create",
				"commit_hash":       "commit-unwind-a",
				"affected_state_id": "state-by-id-2",
				"affected_code_key": "not-used",
			},
			{
				"change_id":         "chg-row-fallback-1",
				"tx_id":             "tx-fb-1",
				"actor":             "carol",
				"timestamp_iso":     "2026-03-20T12:02:00Z",
				"op_type":           "update",
				"commit_hash":       "commit-unwind-b",
				"affected_state_id": "missing-id",
				"affected_code_key": "ck-fallback-1",
			},
			{
				"change_id":         "chg-row-fallback-2",
				"tx_id":             "tx-fb-2",
				"actor":             "dave",
				"timestamp_iso":     "2026-03-20T12:03:00Z",
				"op_type":           "delete",
				"commit_hash":       "commit-unwind-c",
				"affected_state_id": "",
				"affected_code_key": "ck-fallback-2",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected generalized unwind merge chain hot path")

	impactsRes, err := exec.Execute(ctx, `MATCH (:CodeChange)-[:IMPACTS]->(:CodeState) RETURN count(*) AS impacts_count`, nil)
	require.NoError(t, err)
	require.Len(t, impactsRes.Rows, 1)
	require.Equal(t, int64(4), impactsRes.Rows[0][0])

	emittedRes, err := exec.Execute(ctx, `MATCH (:Commit)-[:EMITTED]->(:CodeChange) RETURN count(*) AS emitted_count`, nil)
	require.NoError(t, err)
	require.Len(t, emittedRes.Rows, 1)
	require.Equal(t, int64(4), emittedRes.Rows[0][0])
}

func TestCodeStateChangeQueryShapes_UnwindVersionsWithUniqueCommitHash(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT commit_hash_unique IF NOT EXISTS FOR (c:Commit) REQUIRE c.hash IS UNIQUE`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (ck:CodeKey {entity_id: row.entity_id, relation_type: row.relation_type})
MERGE (cs:CodeState {state_id: row.state_id})
SET cs.code_key = row.code_key,
    cs.tx_id = row.tx_id,
    cs.commit_hash = row.commit_hash,
    cs.valid_from_iso = row.valid_from_iso,
    cs.valid_from = datetime(row.valid_from_iso),
    cs.value_json = row.value_json,
    cs.valid_to = CASE WHEN row.valid_to_iso IS NULL THEN null ELSE datetime(row.valid_to_iso) END,
    cs.asserted_at = datetime(row.asserted_at_iso),
    cs.asserted_by = row.asserted_by,
    cs.semantic_type = row.semantic_type
MERGE (ck)-[:HAS_STATE]->(cs)
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.timestamp = datetime(row.asserted_at_iso), c.tx_id = row.tx_id, c.actor = row.asserted_by
MERGE (c)-[:CHANGED]->(cs)
MERGE (c)-[:TOUCHED]->(ck)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":       "entity-1",
				"relation_type":   "calls",
				"state_id":        "state-1",
				"code_key":        "repo_fact|calls|a",
				"tx_id":           "tx-1",
				"commit_hash":     "c3c34374e0c7a2cfb9769d56b14da648abb5b827",
				"valid_from_iso":  "2026-03-20T20:22:20Z",
				"value_json":      `{"repo":"git-to-graph","source":"a","target":"b"}`,
				"valid_to_iso":    nil,
				"asserted_at_iso": "2026-03-20T20:22:20Z",
				"asserted_by":     "TJ Sweet",
				"semantic_type":   "CallEdgeVersion",
			},
			{
				"entity_id":       "entity-2",
				"relation_type":   "contains",
				"state_id":        "state-2",
				"code_key":        "repo_fact|contains|x",
				"tx_id":           "tx-1",
				"commit_hash":     "c3c34374e0c7a2cfb9769d56b14da648abb5b827",
				"valid_from_iso":  "2026-03-20T20:22:20Z",
				"value_json":      `{"repo":"git-to-graph","parent":"x","child":"y"}`,
				"valid_to_iso":    nil,
				"asserted_at_iso": "2026-03-20T20:22:20Z",
				"asserted_by":     "TJ Sweet",
				"semantic_type":   "ContainsEdgeVersion",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected generalized unwind merge chain hot path")

	commitCount, err := exec.Execute(ctx, `MATCH (c:Commit) RETURN count(c)`, nil)
	require.NoError(t, err)
	require.Len(t, commitCount.Rows, 1)
	require.Equal(t, int64(1), commitCount.Rows[0][0])

	changedCount, err := exec.Execute(ctx, `MATCH (:Commit)-[:CHANGED]->(:CodeState) RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Len(t, changedCount.Rows, 1)
	require.Equal(t, int64(2), changedCount.Rows[0][0])

	touchedCount, err := exec.Execute(ctx, `MATCH (:Commit)-[:TOUCHED]->(:CodeKey) RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Len(t, touchedCount.Rows, 1)
	require.Equal(t, int64(2), touchedCount.Rows[0][0])
}
