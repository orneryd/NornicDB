package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A vector index's state belongs to the service that serves it. The per-index
// population record is per service, so an index whose population failed is
// FAILED until it is dropped and created again, and a service that replaces the
// failing one reports the index from its own build: a failed replacement build
// is FAILED with that build's cause, an unfinished build is POPULATING, and a
// build that succeeded serves the index ONLINE. Report 53.22 (group B18).
func TestFailedVectorIndexPopulationIsNotOnlineAfterServiceReplacement(t *testing.T) {
	ctx := context.Background()
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	const query = "CALL db.index.vector.queryNodes('docs', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score"
	state := func(exec *StorageExecutor) []interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name, state, populationPercent, failureMessage RETURN name, state, populationPercent, failureMessage", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		return result.Rows[0]
	}

	store := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]}), (:Doc {id: 'b', emb: [0.0, 1.0, 0.0]})", nil)
	require.NoError(t, err)

	// A vector budget that cannot hold both vectors makes the population fail.
	first := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
	t.Cleanup(func() { _ = first.Close() })
	require.ErrorContains(t, first.BuildIndexes(ctx), "index memory budget exceeded")
	exec.SetSearchService(first)
	_, err = exec.Execute(ctx, create, nil)
	require.ErrorContains(t, err, "population failed")
	failed := state(exec)
	require.Equal(t, "docs", failed[0])
	require.Equal(t, search.VectorIndexFailed, failed[1])
	require.Contains(t, failed[3], "index memory budget exceeded")

	// The service is replaced by one that cannot serve the index either. That
	// service's own build failed, and it holds no population record for the
	// index, so its build decides the index's state.
	second := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
	t.Cleanup(func() { _ = second.Close() })
	require.ErrorContains(t, second.BuildIndexes(ctx), "index memory budget exceeded")
	exec.SetSearchService(second)
	replaced := state(exec)
	require.Equal(t, search.VectorIndexFailed, replaced[1],
		"a failed replacement build does not make a failed population ONLINE")
	require.Contains(t, replaced[3], "index memory budget exceeded",
		"the replacement reports the cause its own build failed with")
	_, err = exec.Execute(ctx, query, nil)
	require.ErrorContains(t, err, "entered a FAILED state")

	// The replacement is followed by a service that has not built yet: nothing
	// serves the index, so it is not ONLINE.
	unbuilt := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = unbuilt.Close() })
	exec.SetSearchService(unbuilt)
	pending := state(exec)
	require.Equal(t, search.VectorIndexPopulating, pending[1], "an unbuilt service does not report the index ONLINE")
	require.Zero(t, pending[2], "an index that is not online reports no population progress")
	require.Empty(t, pending[3])

	// That service finishes its build: the index it serves is ONLINE and its
	// vectors answer the query.
	require.NoError(t, unbuilt.BuildIndexes(ctx))
	require.Equal(t, search.VectorIndexOnline, state(exec)[1], "a successful rebuild serves the index")
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2, "the rebuilt service answers for every stored vector")
}
