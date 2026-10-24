package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitCacheFinalRelationshipImage(t *testing.T) {
	runCommitCacheFinalImages(t, 0, 2)
}

// Generate ordinary repeated SETs to the same node and relationship, with
// unrelated operations interleaved. The schema pause is an existing callback
// wait, not a cache mutation. Every public read must see the final write.
func TestCommitCacheGeneratedFinalImages(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runCommitCacheFinalImages(t, seed, 2+int(seed%5))
		})
	}
}

func runCommitCacheFinalImages(t *testing.T, seed int64, writes int) {
	t.Helper()
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetDecayEnabled(false)
	for _, id := range []NodeID{"test:multi-a", "test:multi-b", "test:multi-middle"} {
		_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{{1}}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:multi-e", StartNode: "test:multi-a", EndNode: "test:multi-b", Type: "R", Properties: map[string]any{"v": int64(0)}}
	require.NoError(t, b.CreateEdge(edge))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	first := copyEdge(edge)
	first.Properties["v"] = int64(1)
	require.NoError(t, tx.UpdateEdge(first))
	middle, err := tx.GetNode("test:multi-middle")
	require.NoError(t, err)
	middle.Properties["v"] = int64(1)
	require.NoError(t, tx.UpdateNode(middle))
	rng := rand.New(rand.NewSource(seed))
	nodeFinal := int64(1)
	for version := 2; version <= writes; version++ {
		if seed != 0 && rng.Intn(2) == 0 {
			middle, err := tx.GetNode("test:multi-middle")
			require.NoError(t, err)
			nodeFinal++
			middle.Properties["v"] = nodeFinal
			require.NoError(t, tx.UpdateNode(middle))
		}
		next := copyEdge(edge)
		next.Properties["v"] = int64(version)
		require.NoError(t, tx.UpdateEdge(next))
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	restore := setCommitTailHook(func() { close(entered); <-resume })
	done := make(chan error, 1)
	locked := false
	t.Cleanup(func() {
		release()
		if locked {
			b.schemasMu.Unlock()
		}
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("commit did not finish")
		}
		restore()
	})
	go func() { done <- tx.Commit() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("commit did not reach post-publication seam")
	}
	b.schemasMu.Lock()
	locked = true
	release()
	// The reviewed head first publishes1; the correction publishes only the
	// final image. Accept either arrival here so the unchanged value assertions
	// below fail on the old implementation without requiring the bug to occur.
	require.Eventually(t, func() bool {
		b.edgeCacheMu.RLock()
		defer b.edgeCacheMu.RUnlock()
		e := b.edgeCache[edge.ID].Edge
		return e != nil && e.Properties["v"] != int64(0)
	}, 10*time.Second, time.Millisecond, "committed relationship image never published")
	point, err := b.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, int64(writes), point.Properties["v"])
	for _, read := range []struct {
		name string
		fn   func(NodeID) ([]*Edge, error)
		id   NodeID
	}{
		{"stored outgoing", b.GetStoredOutgoingEdges, edge.StartNode},
		{"stored incoming", b.GetStoredIncomingEdges, edge.EndNode},
		{"outgoing", b.GetOutgoingEdges, edge.StartNode},
		{"incoming", b.GetIncomingEdges, edge.EndNode},
	} {
		for attempt := 0; attempt < 2; attempt++ {
			rows, err := read.fn(read.id)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			if rows[0].Properties["v"] != int64(writes) {
				t.Errorf("%s returned intermediate v=%v; durable v=%d", read.name, rows[0].Properties["v"], writes)
			}
		}
	}
	storedNode, err := b.GetStoredNode("test:multi-middle")
	require.NoError(t, err)
	require.Equal(t, nodeFinal, storedNode.Properties["v"], "node callback also must not publish an intermediate image")
}
