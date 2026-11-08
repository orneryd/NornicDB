package search

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review round 8 of #1753 (reviewer probe, kept verbatim): a storage-following
// refresh refused by a running build's admission withdraws the replaced
// snapshot entry.
func TestReview1753R8BuildAdmissionFailureWithdrawsStoredRefresh(t *testing.T) {
	ctx := context.Background()
	engine := newNodeSnapshotGateEngine(t)
	old := buildWriteNode("nornic:a", "oldterm", []float32{1, 0, 0})
	_, err := engine.CreateNode(ctx, old)
	require.NoError(t, err)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(ctx) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("build did not capture snapshot")
	}
	changed := buildWriteNode("nornic:a", "currentterm", []float32{0, 1})
	require.NoError(t, engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
	refreshed := make(chan error, 1)
	go func() { refreshed <- svc.RefreshNode(ctx, changed.ID) }()
	select {
	case err := <-refreshed:
		require.ErrorIs(t, err, ErrDimensionMismatch)
	case <-time.After(5 * time.Second):
		t.Fatal("refresh unexpectedly waited for build")
	}
	engine.open()
	require.NoError(t, <-built)
	require.NoError(t, svc.CatchUpPendingRefreshes(ctx), "no pending repair is retained")
	hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Limit: 10})
	require.NoError(t, err)
	t.Logf("vector query after failed refresh: %+v", hits)
	t.Logf("search after completed refresh and successful catch-up: %v", buildWriteSearchIDs(t, svc, "oldterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "oldterm"), "a failed storage-following refresh must withdraw the replaced snapshot")
}

// storedRefreshFlakyEngine fails the search service's next node read while
// armed, under a build gated on its snapshot.
type storedRefreshFlakyEngine struct {
	*nodeSnapshotGateEngine
	failRead atomic.Bool
}

func (e *storedRefreshFlakyEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.failRead.Swap(false) {
		return nil, errors.New("transient node read failure")
	}
	return e.nodeSnapshotGateEngine.GetNode(ctx, id)
}

// Storage-following refreshes admitted while a build is in flight are
// installed entry by entry after the build's snapshot, never leaving a
// replaced entry served with nothing pending: one with an embedding of other
// dimensions installs its text and withdraws the replaced vector (the caller
// is told of the refusal at once); one whose read fails keeps its reservation
// for the retry owner, which installs it; a successful one is installed.
func TestStoredRefreshDuringBuildInstallsEntryByEntry(t *testing.T) {
	ctx := context.Background()
	gate := newNodeSnapshotGateEngine(t)
	engine := &storedRefreshFlakyEngine{nodeSnapshotGateEngine: gate}
	for _, node := range []*storage.Node{
		buildWriteNode("nornic:dims", "dimsold", []float32{1, 0, 0}),
		buildWriteNode("nornic:read", "readold", []float32{0, 1, 0}),
		buildWriteNode("nornic:ok", "okold", []float32{0, 0, 1}),
	} {
		_, err := gate.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(gate.open)
	var retries []RefreshRetry
	svc.SetRefreshRetryOwner(func(work RefreshRetry, err error) { retries = append(retries, work) })
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(ctx) }()
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("build did not capture its snapshot")
	}
	update := func(node *storage.Node) {
		t.Helper()
		require.NoError(t, gate.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}))
	}
	refresh := func(id storage.NodeID) error {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- svc.RefreshNode(ctx, id) }()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("a refresh waited for the build")
			return nil
		}
	}
	update(buildWriteNode("nornic:dims", "dimsnew", []float32{0, 1}))
	require.ErrorIs(t, refresh("nornic:dims"), ErrDimensionMismatch)
	update(buildWriteNode("nornic:read", "readnew", []float32{1, 0, 0}))
	engine.failRead.Store(true)
	require.Error(t, refresh("nornic:read"))
	update(buildWriteNode("nornic:ok", "oknew", []float32{1, 0, 0}))
	require.NoError(t, refresh("nornic:ok"))
	gate.open()
	require.NoError(t, <-built, "a refused entry is not a build failure")

	require.Empty(t, buildWriteSearchIDs(t, svc, "dimsold"), "the replaced text is not served")
	require.Equal(t, []string{"nornic:dims"}, buildWriteSearchIDs(t, svc, "dimsnew"), "the stored text is installed")
	svc.indexMu.RLock()
	_, dimsVector := svc.nodeNamedVector["nornic:dims"]["v"]
	svc.indexMu.RUnlock()
	require.False(t, dimsVector, "the replaced vector is withdrawn; the stored one has other dimensions")
	require.Equal(t, []string{"nornic:ok"}, buildWriteSearchIDs(t, svc, "oknew"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "okold"))
	// The failed read lags while its retry is pending, then lands.
	require.Len(t, retries, 1)
	require.True(t, retries[0].Current())
	require.NoError(t, svc.RefreshNodeWithMutation(ctx, retries[0].Node))
	require.Equal(t, []string{"nornic:read"}, buildWriteSearchIDs(t, svc, "readnew"), "the retry installs the stored node")
	require.Empty(t, buildWriteSearchIDs(t, svc, "readold"))
	require.NoError(t, svc.CatchUpPendingRefreshes(ctx))
}

// A refresh that follows storage decides each kind on its own (NOR-386 rounds
// 9 and 13). With the BM25 budget reached, a text write refuses the text only:
// the node's vectors stay served as stored, unchanged or changed, and its
// replaced text is withdrawn. A vector budget refuses the node's vectors whole:
// none of them stays served, so no other representation or stale vector takes
// the place of a refused one, nothing of them is charged, and a later write
// that fits installs them. A relationship's refused vectors are withdrawn
// whole with its type. The payload API still rejects the whole write and
// keeps the previous entries.
func TestStoredRefreshRefusesEntryByEntry(t *testing.T) {
	ctx := context.Background()
	query := func(t *testing.T, svc *Service, property string, vector []float32) map[string]float64 {
		t.Helper()
		hits, err := svc.VectorQueryNodes(ctx, vector, VectorQuerySpec{Label: "Doc", Property: property, Limit: 10})
		require.NoError(t, err)
		scores := map[string]float64{}
		for _, hit := range hits {
			scores[hit.ID] = hit.Score
		}
		return scores
	}
	requireScore := func(t *testing.T, scores map[string]float64, id string, want float64, msg string) {
		t.Helper()
		require.Contains(t, scores, id, msg)
		require.InDelta(t, want, scores[id], 1e-3, msg)
	}
	write := func(t *testing.T, engine storage.Engine, id storage.NodeID, change func(*storage.Node)) {
		t.Helper()
		stored, err := engine.GetNode(ctx, id)
		require.NoError(t, err)
		changed := storage.CopyNode(stored)
		change(changed)
		require.NoError(t, engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
	}

	t.Run("text beyond the BM25 budget", func(t *testing.T) {
		engine := newNamespacedMemoryEngine(t)
		for _, node := range []*storage.Node{populationNode("a", []float64{1, 0, 0}, "short"), populationNode("b", []float64{0, 1, 0}, "short")} {
			_, err := engine.CreateNode(ctx, node)
			require.NoError(t, err)
		}
		svc := NewService(engine, 3, &ServiceOptions{BM25MemoryMaxBytes: 4096})
		t.Cleanup(func() { _ = svc.Close() })
		require.NoError(t, svc.BuildIndexes(ctx))
		requireScore(t, query(t, svc, "emb", []float32{1, 0, 0}), "a", 1, "before any refusal")

		write(t, engine, "a", func(n *storage.Node) { n.Properties["text"] = strings.Repeat("payload ", 1024) })
		require.ErrorIs(t, svc.RefreshNode(ctx, "a"), ErrIndexMemoryBudgetExceeded)
		requireScore(t, query(t, svc, "emb", []float32{1, 0, 0}), "a", 1, "an unchanged vector stays served")
		require.Equal(t, []string{"b"}, buildWriteSearchIDs(t, svc, "short"), "the replaced text is withdrawn")
		require.Empty(t, buildWriteSearchIDs(t, svc, "payload"), "the text beyond the budget is refused")

		write(t, engine, "a", func(n *storage.Node) { n.Properties["emb"] = []float64{0.6, 0.8, 0} })
		require.ErrorIs(t, svc.RefreshNode(ctx, "a"), ErrIndexMemoryBudgetExceeded)
		// Scores are Neo4j's (1 + cosine) / 2: the stored [0.6, 0.8, 0] scores 0.8.
		requireScore(t, query(t, svc, "emb", []float32{1, 0, 0}), "a", 0.8, "a changed vector is installed")

		write(t, engine, "a", func(n *storage.Node) { n.Properties["text"] = "fits" })
		require.NoError(t, svc.RefreshNode(ctx, "a"))
		require.Equal(t, []string{"a"}, buildWriteSearchIDs(t, svc, "fits"))
	})

	t.Run("vectors beyond the vector budget", func(t *testing.T) {
		engine := newNamespacedMemoryEngine(t)
		node := populationNode("a", []float64{1, 0, 0}, "short")
		node.Properties["emb2"] = []float64{0, 1, 0}
		_, err := engine.CreateNode(ctx, node)
		require.NoError(t, err)
		// Two 3-dimensional vectors fit; three do not.
		svc := NewService(engine, 3, &ServiceOptions{VectorMemoryMaxBytes: 60})
		t.Cleanup(func() { _ = svc.Close() })
		require.NoError(t, svc.BuildIndexes(ctx))

		write(t, engine, "a", func(n *storage.Node) {
			n.Properties["emb2"] = []float64{0, 0, 1}
			n.Properties["emb3"] = []float64{0, 1, 0}
			n.Properties["text"] = "changed"
		})
		require.ErrorIs(t, svc.RefreshNode(ctx, "a"), ErrIndexMemoryBudgetExceeded)
		for _, property := range []string{"emb", "emb2", "emb3"} {
			require.NotContains(t, query(t, svc, property, []float32{1, 0, 0}), "a", "the refused vectors are withdrawn whole: %s", property)
		}
		require.Zero(t, svc.vectorResidentBytes, "nothing of the refused vectors is charged")
		require.Equal(t, []string{"a"}, buildWriteSearchIDs(t, svc, "changed"), "the text is installed")

		write(t, engine, "a", func(n *storage.Node) { delete(n.Properties, "emb3") })
		require.NoError(t, svc.RefreshNode(ctx, "a"))
		requireScore(t, query(t, svc, "emb", []float32{1, 0, 0}), "a", 1, "a write that fits installs them")
		requireScore(t, query(t, svc, "emb2", []float32{0, 0, 1}), "a", 1, "a write that fits installs them")

		// The payload API rejects the whole write and keeps what it held.
		payload, err := engine.GetNode(ctx, "a")
		require.NoError(t, err)
		payload = storage.CopyNode(payload)
		payload.Properties["emb3"] = []float64{0, 1, 0}
		payload.Properties["text"] = "payloadwrite"
		require.ErrorIs(t, svc.IndexNode(payload), ErrIndexMemoryBudgetExceeded)
		require.Equal(t, []string{"a"}, buildWriteSearchIDs(t, svc, "changed"))
	})

	t.Run("relationship vectors beyond the vector budget", func(t *testing.T) {
		engine := newNamespacedMemoryEngine(t)
		for _, id := range []storage.NodeID{"s", "e"} {
			_, err := engine.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(ctx, &storage.Edge{ID: "r", StartNode: "s", EndNode: "e", Type: "R", Properties: map[string]any{"emb": []float64{1, 0, 0}, "emb2": []float64{0, 1, 0}}}))
		// Two 3-dimensional relationship vectors fit; three do not.
		svc := NewService(engine, 3, &ServiceOptions{VectorMemoryMaxBytes: 30})
		t.Cleanup(func() { _ = svc.Close() })
		require.NoError(t, svc.BuildIndexes(ctx))
		held := func() map[string][]float32 {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			return maps.Clone(svc.edgePropVector["r"])
		}
		require.Len(t, held(), 2)
		stored, err := engine.GetEdge(ctx, "r")
		require.NoError(t, err)
		changed := storage.CopyEdge(stored)
		changed.Properties["emb2"] = []float64{0, 0, 1}
		changed.Properties["emb3"] = []float64{0, 1, 0}
		require.NoError(t, engine.UpdateEdge(ctx, storage.EdgeUpdateRequest{After: changed}))
		require.ErrorIs(t, svc.RefreshEdge(ctx, "r"), ErrIndexMemoryBudgetExceeded)
		require.Empty(t, held(), "the refused vectors are withdrawn whole")
		svc.mu.RLock()
		_, typed := svc.edgeTypes["r"]
		svc.mu.RUnlock()
		require.False(t, typed, "with the relationship's type")
		require.Zero(t, svc.vectorResidentBytes, "nothing of them is charged")
	})
}
