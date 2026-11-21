package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type nor357RecoveryRequestKey struct{}

// nor357RecoveryReadStore observes the native reads a statement performs and
// can cancel it immediately before one of them. The armed read returns the
// caller's cancellation, which is what a native engine does when it admits an
// already canceled request; the decoration is transparent otherwise (it
// forwards every method and implements no capability the executor could take
// instead of the decorated engine).
type nor357RecoveryReadStore struct {
	storage.Engine

	mu            sync.Mutex
	nodeReads     int
	adjacentReads int
	seen          []context.Context
	cancel        context.CancelFunc
	kind          string
	armAt         int
}

func (s *nor357RecoveryReadStore) beforeRead(ctx context.Context, kind string) error {
	s.mu.Lock()
	count := &s.nodeReads
	if kind == "adjacent" {
		count = &s.adjacentReads
	}
	*count++
	s.seen = append(s.seen, ctx)
	cancel := s.cancel
	fire := cancel != nil && s.kind == kind && *count == s.armAt
	if fire {
		s.cancel = nil
	}
	s.mu.Unlock()
	if fire {
		cancel()
		return ctx.Err()
	}
	return nil
}

func (s *nor357RecoveryReadStore) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if err := s.beforeRead(ctx, "node"); err != nil {
		return nil, err
	}
	return s.Engine.GetNode(ctx, id)
}

func (s *nor357RecoveryReadStore) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if err := s.beforeRead(ctx, "adjacent"); err != nil {
		return nil, err
	}
	return s.Engine.ReadAdjacentEdges(ctx, request)
}

// watch starts a fresh observation window: every native read is recorded, and
// the read of kind at position at cancels the statement (cancel nil: none).
func (s *nor357RecoveryReadStore) watch(cancel context.CancelFunc, kind string, at int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeReads = 0
	s.adjacentReads = 0
	s.seen = nil
	s.cancel = cancel
	s.kind = kind
	s.armAt = at
}

func (s *nor357RecoveryReadStore) snapshot() (int, int, []context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeReads, s.adjacentReads, append([]context.Context(nil), s.seen...)
}

// requireNOR357RequestOnEveryRead requires every observed native read to carry
// the caller's request value. A read that received the internal fallback (or
// the wrong context) fails.
func requireNOR357RequestOnEveryRead(t *testing.T, request context.Context, seen []context.Context) {
	t.Helper()
	require.NotEmpty(t, seen, "the statement must perform a native read")
	want := request.Value(nor357RecoveryRequestKey{})
	for index, read := range seen {
		require.Equal(t, want, read.Value(nor357RecoveryRequestKey{}),
			"native read %d must inherit the caller's statement context", index)
	}
}

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
