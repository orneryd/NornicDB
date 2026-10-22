package cypher

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const populationSourceSize = 4096

// scanObservationAt is the observation a scan makes at its item'th item:
// its first item, then one per rowCancellationInterval items.
func scanObservationAt(item int) int {
	return item/rowCancellationInterval + 1
}

// streamedLabelFixture is a label population the executor streams from
// storage, each node with one relationship for an EXISTS filter.
func streamedLabelFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for i := 0; i < populationSourceSize; i++ {
		p := storage.NodeID(fmt.Sprintf("p-%04d", i))
		q := storage.NodeID(fmt.Sprintf("q-%04d", i))
		_, err := store.CreateNode(&storage.Node{ID: p, Labels: []string{"P"}, Properties: map[string]interface{}{"id": int64(i)}})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: q, Labels: []string{"Q"}, Properties: map[string]interface{}{"id": int64(i)}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID("r-" + string(p)), Type: "R", StartNode: p, EndNode: q}))
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
}

// relationshipPopulationFixture holds one node pair joined by many parallel
// relationships and one hub with many outgoing ones.
func relationshipPopulationFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for _, node := range []*storage.Node{
		{ID: "pair-a", Labels: []string{"PairA"}},
		{ID: "pair-b", Labels: []string{"PairB"}},
		{ID: "hub", Labels: []string{"Hub"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	for i := 0; i < populationSourceSize; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("par-%04d", i)), Type: "PAR", StartNode: "pair-a", EndNode: "pair-b"}))
		leaf := storage.NodeID(fmt.Sprintf("leaf-%04d", i))
		_, err := store.CreateNode(&storage.Node{ID: leaf, Labels: []string{"Leaf"}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("fan-%04d", i)), Type: "FAN", StartNode: "hub", EndNode: leaf}))
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
}

// Streamed label scans stop inside the storage callback, and a client's
// LIMIT, however large, sizes nothing: through public Execute, the context
// expires inside the stream at its second observation, a quarter of the
// population in (a scan observes once per rowCancellationInterval items).
func TestStreamedLabelPopulationsExecuteCancellation(t *testing.T) {
	const query = "MATCH (n:P) RETURN n.id AS id LIMIT $lim"
	cases := []entityPopulationCase{
		{name: "filtered_count", query: "MATCH (n:P) WHERE EXISTS { (n)-[:R]->() } RETURN count(n) AS c", consumer: "tryStreamPipelineFilteredNodeCount", after: scanObservationAt(populationSourceSize / 4), rows: 1, fixture: streamedLabelFixture},
		{name: "projection_limit_1e9", query: query, consumer: "visitNodesWithStreamingProjection", after: scanObservationAt(populationSourceSize / 4), rows: populationSourceSize, fixture: streamedLabelFixture},
	}
	runEntityPopulationCases(t, cases, map[string]interface{}{"lim": int64(1_000_000_000)})
	runEntityPopulationCases(t, []entityPopulationCase{
		{name: "projection_limit_2_40", query: query, consumer: "visitNodesWithStreamingProjection", after: scanObservationAt(populationSourceSize / 4), rows: populationSourceSize, fixture: streamedLabelFixture},
	}, map[string]interface{}{"lim": int64(1) << 40})
}

// A client's LIMIT is not the population: a scan of a few thousand nodes
// under LIMIT 1e9 or 2^40 allocates what the scan yields.
func TestStreamedLabelLimitAllocatesThePopulation(t *testing.T) {
	executor := streamedLabelFixture(t)
	for _, limit := range []int64{1_000_000_000, int64(1) << 40} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		result, err := executor.Execute(context.Background(), "MATCH (n:P) RETURN n.id AS id LIMIT $lim", map[string]interface{}{"lim": limit})
		runtime.ReadMemStats(&after)
		require.NoError(t, err)
		require.Len(t, result.Rows, populationSourceSize)
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20), "LIMIT %d", limit)
	}
}

// One node pair's or one node's relationships stop inside the pass over
// them: MERGE over parallel relationships, a hub's degree and a DELETE that
// checks a node's remaining relationships.
func TestRelationshipPopulationsExecuteCancellation(t *testing.T) {
	cases := []entityPopulationCase{
		{name: "merge_parallel_rows", query: "MATCH (a:PairA), (b:PairB) MERGE (a)-[r:PAR]->(b) RETURN count(r) AS c", consumer: "mergeRelationshipRows", after: 2, rows: 1, fixture: relationshipPopulationFixture},
		{name: "merge_identity_direct", query: "MATCH (a:PairA), (b:PairB) MERGE (a)-[r:PAR]->(b) RETURN count(r) AS c", consumer: "findRelationshipsForMerge", after: scanObservationAt(populationSourceSize / 4), rows: 1, fixture: relationshipPopulationFixture},
		{name: "hub_degree", query: "MATCH (h:Hub) RETURN COUNT { (h)-[:FAN]->() } AS c", consumer: "nodeDegreeCount", after: 2, rows: 1, fixture: relationshipPopulationFixture},
		{name: "delete_residual", query: "MATCH (h:Hub)-[r:FAN]->() DELETE r, h", consumer: "nodeHasResidualRelationships", after: 2, rows: 0, fixture: relationshipPopulationFixture},
	}
	runEntityPopulationCases(t, cases, nil)
}

// The statement's relationship identity cache reads one node pair's
// relationships through the scan adapter: canceled inside that read, the
// lookup returns the cancellation instead of a partial set.
func TestRelationshipMergeIdentityCacheLookupCancellation(t *testing.T) {
	executor := relationshipPopulationFixture(t)
	for _, deadline := range []bool{false, true} {
		ctx := &rowConsumerContext{Context: context.Background(), consumer: "relationshipMergeIdentityCache).relationships", deadline: deadline, after: scanObservationAt(populationSourceSize / 4), done: make(chan struct{})}
		edges, err := newRelationshipMergeIdentityCache().relationships(ctx, executor.storage, "pair-a", "pair-b", "PAR", nil)
		want := context.Canceled
		if deadline {
			want = context.DeadlineExceeded
		}
		require.ErrorIs(t, err, want)
		require.Nil(t, edges)
		require.Equal(t, scanObservationAt(populationSourceSize/4), ctx.checks, "must cancel inside the pair's relationships")
	}
	edges, err := newRelationshipMergeIdentityCache().relationships(context.Background(), executor.storage, "pair-a", "pair-b", "PAR", nil)
	require.NoError(t, err)
	require.Len(t, edges, populationSourceSize)
}
