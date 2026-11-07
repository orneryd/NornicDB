package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNOR300CallSubqueryWriteClassification(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		write       bool
	}{
		{"registered write", "MATCH (n:Doc) CALL kp.suppress(n) RETURN n", true},
		{"registered read", "CALL db.labels() YIELD label RETURN label", false},
		{"unknown procedure", "CALL missing.unknown() YIELD value RETURN value", true},
		{"dynamic procedure", "CALL apoc.cypher.run('RETURN 1', {}) YIELD value RETURN value", true},
		{"quoted keyword", "RETURN 'CREATE' AS value", false},
		{"mutation", "CREATE (n:Doc) RETURN n", true},
		{"nested mutation", "FOREACH (x IN [1] | CREATE (:Doc))", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := callSubqueryQueryIsWrite(context.Background(), tc.query); got != tc.write {
				t.Fatalf("write=%v want %v for %q", got, tc.write, tc.query)
			}
		})
	}
}

func TestNOR300SuppressionInvalidatesCachedMatch(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor300_cache"), QueryCachePolicy{MaxEntries: 64})
	ctx := t.Context()
	_, err := executor.Execute(ctx, "CREATE (:CacheProbe {id: 1})", nil)
	require.NoError(t, err)
	const read = "MATCH (n:CacheProbe) RETURN n.id AS id"
	first, err := executor.Execute(ctx, read, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	_, err = executor.Execute(ctx, read, nil)
	require.NoError(t, err)
	hits, _, _, _, _ := executor.cache.Stats()
	require.Positive(t, hits, "control must exercise the result cache")
	_, err = executor.Execute(ctx, "MATCH (n:CacheProbe) CALL kp.suppress(n)", nil)
	require.NoError(t, err)
	after, err := executor.Execute(ctx, read, nil)
	require.NoError(t, err)
	require.Empty(t, after.Rows, "suppressed nodes must not survive a cached MATCH")
}

func TestNOR300FabricProcedureWriteClassification(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor300_fabric"))
	for _, tc := range []struct {
		name, query string
		write       bool
	}{
		{"registered write", "MATCH (n:Doc) CALL kp.suppress(n) RETURN n", true},
		{"registered read", "CALL db.labels() YIELD label RETURN label", false},
		{"unknown procedure", "CALL missing.unknown() YIELD value RETURN value", true},
		{"dynamic procedure", "CALL apoc.cypher.run('RETURN 1', {}) YIELD value RETURN value", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fragment, err := executor.fabricProcedureWritePlan(t.Context(), &fabric.FragmentExec{Input: &fabric.FragmentInit{}, Query: tc.query})
			if err != nil {
				t.Fatal(err)
			}
			if got := fragment.(*fabric.FragmentExec).IsWrite; got != tc.write {
				t.Fatalf("write=%v want %v for %q", got, tc.write, tc.query)
			}
		})
	}
}
