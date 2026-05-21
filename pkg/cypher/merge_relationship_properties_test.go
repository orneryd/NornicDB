package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeRelationshipStandaloneSetPersistsProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
MERGE (a:RelPropProbe {id: $a})
MERGE (b:RelPropProbe {id: $b})
MERGE (a)-[rel:REL_PROP_PROBE]->(b)
SET rel.resolved_id = $resolved_id,
    rel.evidence_count = $evidence_count,
    rel.evidence_kinds = $evidence_kinds
RETURN rel.resolved_id AS resolved_id,
       rel.evidence_count AS evidence_count,
       rel.evidence_kinds AS evidence_kinds,
       properties(rel) AS props
`, map[string]interface{}{
		"a":              "a",
		"b":              "b",
		"resolved_id":    "resolved-1",
		"evidence_count": int64(2),
		"evidence_kinds": []string{"A", "B"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"resolved_id", "evidence_count", "evidence_kinds", "props"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "resolved-1", result.Rows[0][0])
	require.Equal(t, int64(2), result.Rows[0][1])
	// Caller wrote []string{"A","B"}; the SET-RETURN must preserve the
	// declared shape end-to-end (no widening to []interface{}). The same
	// strict shape comes back from the MATCH-RETURN re-read below, which
	// is why the result.Rows / verify.Rows equality below holds.
	require.Equal(t, []string{"A", "B"}, result.Rows[0][2])
	require.Equal(t, map[string]interface{}{
		"resolved_id":    "resolved-1",
		"evidence_count": int64(2),
		"evidence_kinds": []string{"A", "B"},
	}, result.Rows[0][3])

	verify, err := exec.Execute(ctx, `
MATCH (:RelPropProbe {id: $a})-[rel:REL_PROP_PROBE]->(:RelPropProbe {id: $b})
RETURN rel.resolved_id AS resolved_id,
       rel.evidence_count AS evidence_count,
       rel.evidence_kinds AS evidence_kinds,
       properties(rel) AS props
`, map[string]interface{}{
		"a": "a",
		"b": "b",
	})
	require.NoError(t, err)
	require.Equal(t, result.Rows, verify.Rows)
}

func TestMergeRelationshipStandaloneSetSkipsOnCreateSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
MERGE (a:RelPropProbe {id: $a})
MERGE (b:RelPropProbe {id: $b})
MERGE (a)-[rel:REL_PROP_PROBE]->(b)
ON CREATE SET rel.created = $created
SET rel.resolved_id = $resolved_id
RETURN rel.created AS created,
       rel.resolved_id AS resolved_id,
       properties(rel) AS props
`, map[string]interface{}{
		"a":           "a",
		"b":           "b",
		"created":     true,
		"resolved_id": "resolved-2",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"created", "resolved_id", "props"}, result.Columns)
	require.Len(t, result.Rows, 1)
	if result.Rows[0][0] != nil {
		require.Equal(t, true, result.Rows[0][0])
	}
	require.Equal(t, "resolved-2", result.Rows[0][1])
	props, ok := result.Rows[0][2].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "resolved-2", props["resolved_id"])
	if created, ok := props["created"]; ok {
		require.Equal(t, true, created)
	}
}
