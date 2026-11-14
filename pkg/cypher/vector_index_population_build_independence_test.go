package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A successful per-index population is the index's own state. A later build of
// the service that failed for a reason outside the index (here a vector of
// another label, which this index does not read, that the service's budget
// cannot admit) must not turn the index FAILED or refuse its queries. The build
// of a service that never populated the index still decides that service's
// report: a replacement whose build failed keeps the index FAILED. Report
// 53.22.
func TestSuccessfulVectorIndexPopulationOutlivesAnUnrelatedFailedBuild(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "CREATE (:Doc {id:'a', emb:[1.0, 0.0, 0.0]})", nil)
	require.NoError(t, err)

	// One 3-dimensional vector fits the vector budget; a second does not.
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 40})
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(ctx), "the first vector fits the budget")

	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	state := func(executor *StorageExecutor) []interface{} {
		t.Helper()
		shown, err := executor.Execute(ctx, "SHOW VECTOR INDEXES YIELD name, state, failureMessage RETURN name, state, failureMessage", nil)
		require.NoError(t, err)
		require.Len(t, shown.Rows, 1)
		return shown.Rows[0]
	}
	const query = "CALL db.index.vector.queryNodes('docs', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score ORDER BY node.id"

	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	require.NoError(t, err, "the population of the index succeeds")
	online := state(exec)
	require.Equal(t, []interface{}{"docs", search.VectorIndexOnline, ""}, online)

	// A vector of another label exhausts the budget of the service's next
	// build. That label is not the index's, so the build's failure is not the
	// index's population failing. The write is made without a search service,
	// so it is the build that meets the refusal.
	_, err = writer.Execute(ctx, "CREATE (:Other {id:'x', emb:[0.0, 1.0, 0.0]})", nil)
	require.NoError(t, err)
	buildErr := svc.BuildIndexes(ctx)
	require.ErrorIs(t, buildErr, search.ErrIndexMemoryBudgetExceeded, "the build fails on the other label's vector")
	require.False(t, svc.IsReady())

	// The index's population succeeded, so its state stays ONLINE and its
	// query still answers.
	after := state(exec)
	require.Equal(t, search.VectorIndexOnline, after[1], "an unrelated failed build does not fail a populated index")
	require.Empty(t, after[2])
	rows, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a", 1.0}}, rows.Rows, "the index's query answers")

	// The same store under a service that never populated the index: its own
	// failed build is the index's state, ONLINE only once that build succeeds.
	replacement := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
	t.Cleanup(func() { require.NoError(t, replacement.Close()) })
	require.ErrorIs(t, replacement.BuildIndexes(ctx), search.ErrIndexMemoryBudgetExceeded)
	exec.SetSearchService(replacement)
	replaced := state(exec)
	require.Equal(t, search.VectorIndexFailed, replaced[1], "a failed replacement without a local population is not ONLINE")
	require.Contains(t, replaced[2], "index memory budget exceeded")
	_, err = exec.Execute(ctx, query, nil)
	require.ErrorContains(t, err, "entered a FAILED state")
}
