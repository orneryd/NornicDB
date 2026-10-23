package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestExplicitTransactionUnwindDeleteCommitAndRollback prevents a regression
// where explicit transactions routed DELETE before binding a top-level UNWIND
// variable, silently committing zero relationship deletions.
func TestExplicitTransactionUnwindDeleteCommitAndRollback(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		terminal      string
		remainingEdge int64
	}{
		{name: "commit", terminal: "COMMIT", remainingEdge: 0},
		{name: "rollback", terminal: "ROLLBACK", remainingEdge: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "test"))
			ctx := context.Background()

			for _, query := range []string{
				"CREATE (:Function {uid: 'source'}), (:Function {uid: 'target'})",
				"MATCH (s:Function {uid: 'source'}), (t:Function {uid: 'target'}) CREATE (s)-[:TAINT_FLOWS_TO {evidence_source: 'wanted'}]->(t)",
			} {
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
			}

			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, `UNWIND $uids AS suid
MATCH (s:Function {uid: suid})-[rel:TAINT_FLOWS_TO]->()
WHERE rel.evidence_source = $evidence_source
DELETE rel`, map[string]any{
				"uids":            []string{"source", "missing", "source"},
				"evidence_source": "wanted",
			})
			require.NoError(t, err)
			require.Equal(t, 1, result.Stats.RelationshipsDeleted)

			_, err = exec.Execute(ctx, testCase.terminal, nil)
			require.NoError(t, err)

			result, err = exec.Execute(ctx, "MATCH (:Function {uid: 'source'})-[rel:TAINT_FLOWS_TO]->(:Function {uid: 'target'}) RETURN count(rel)", nil)
			require.NoError(t, err)
			require.Equal(t, testCase.remainingEdge, countFromResult(t, result))
		})
	}
}

func TestExplicitTransactionUnwindDeleteEmptyInput(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "test"))
	ctx := context.Background()

	for _, query := range []string{
		"CREATE (:Function {uid: 'source'}), (:Function {uid: 'target'})",
		"MATCH (s:Function {uid: 'source'}), (t:Function {uid: 'target'}) CREATE (s)-[:TAINT_FLOWS_TO]->(t)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, `UNWIND $uids AS suid
MATCH (s:Function {uid: suid})-[rel:TAINT_FLOWS_TO]->()
DELETE rel`, map[string]any{"uids": []string{}})
	require.NoError(t, err)
	require.Zero(t, result.Stats.RelationshipsDeleted)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (:Function {uid: 'source'})-[rel:TAINT_FLOWS_TO]->(:Function {uid: 'target'}) RETURN count(rel)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), countFromResult(t, result))
}
