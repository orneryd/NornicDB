package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Cancel during candidate scoring, after topology has already found multiple
// candidates. Cancellation before topology would miss a scorer loop that keeps
// visiting the already-built candidate population.
func TestNOR357LinkPredictionStopsScoringRemainingCandidatesOnCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		observed := &nor357RecoveryReadStore{Engine: newTestMemoryEngine(t)}
		native := storage.NewNamespacedEngine(observed, "nor357_scoring")
		nodes := map[string]storage.NodeID{}
		for _, id := range []string{"source", "middle", "c1", "c2", "c3", "c4"} {
			n, err := native.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"k": id}})
			require.NoError(t, err)
			nodes[id] = n
		}
		for _, pair := range [][2]string{{"source", "middle"}, {"middle", "c1"}, {"middle", "c2"}, {"middle", "c3"}, {"middle", "c4"}} {
			require.NoError(t, native.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(pair[0] + "_" + pair[1]), StartNode: nodes[pair[0]], EndNode: nodes[pair[1]], Type: "R"}))
		}
		source, err := native.GetNode(t.Context(), nodes["source"])
		require.NoError(t, err)
		params := map[string]interface{}{"source": source}
		exec := NewStorageExecutor(native, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		const query = "CALL gds.linkPrediction.predict.stream({sourceNode:$source,topK:4}) YIELD node1,node2,score RETURN node1,node2,score"
		request := context.WithValue(t.Context(), nor357RecoveryRequestKey{}, "candidate_scoring")
		observed.watch(nil, "node", 0)
		healthy, err := exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Len(t, healthy.Rows, 4)
		healthyReads, _, seen := observed.snapshot()
		require.Equal(t, 5, healthyReads, "one source read plus four candidate reads")
		requireNOR357RequestOnEveryRead(t, request, seen)
		canceled, cancel := context.WithCancel(request)
		defer cancel()
		observed.watch(cancel, "node", 2)
		result, err := exec.Execute(canceled, query, params)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		canceledReads, _, seen := observed.snapshot()
		requireNOR357RequestOnEveryRead(t, canceled, seen)
		t.Logf("healthy candidate reads=%d; canceled at read 2, total reads=%d", healthyReads, canceledReads)
		// Verify the healthy retry before the fail-first stop-work assertion.
		observed.watch(nil, "node", 0)
		retry, err := exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Equal(t, healthy.Rows, retry.Rows)
		require.Equal(t, 2, canceledReads, "after the first candidate observes cancellation, no remaining candidate should be scored")
	})
}
