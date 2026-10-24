package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every written node must remain warm after an uncontended commit, including
// writes after a deletion and separate statements drained in one generation.
func TestNodeCommitCacheKeepsBatchWarm(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, mixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%t/mixed=%t", buffered, mixed), func(t *testing.T) {
				var b *BadgerEngine
				if buffered {
					b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
				} else {
					b = NewMemoryEngine().BadgerEngine
					t.Cleanup(func() { _ = b.Close() })
				}
				b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}, IncludeLabels: true})
				nodes := make([]*Node, 16)
				for i := range nodes {
					nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:batch%d", i)), Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{{1, 0}}}
					_, err := b.CreateNode(nodes[i])
					require.NoError(t, err)
				}
				written := make(map[NodeID]*Node)
				var deleted []NodeID
				batches, batchSize := 1, len(nodes)
				if buffered {
					batches, batchSize = 2, len(nodes)/2
				}
				// Buffered statements share one later physical commit; ordinary
				// explicit transactions publish their own cache callbacks.
				for batch := 0; batch < batches; batch++ {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					if buffered {
						require.NoError(t, tx.SetImplicit(true))
						require.NoError(t, tx.SetNamespace("test"))
					}
					for i := batch * batchSize; i < (batch+1)*batchSize; i++ {
						n := nodes[i]
						if mixed && i%3 == 0 {
							require.NoError(t, tx.DeleteNode(n.ID))
							deleted = append(deleted, n.ID)
							continue
						}
						n.Properties["v"] = int64(1)
						if mixed && i%3 == 1 {
							n.ID = NodeID(fmt.Sprintf("test:created%d", i))
							_, err = tx.CreateNode(n)
							require.NoError(t, err)
						} else {
							require.NoError(t, tx.UpdateNode(n))
						}
						written[n.ID] = n
					}
					require.NoError(t, tx.Commit())
				}
				if buffered {
					require.NoError(t, b.writeBehind.Flush())
				}
				// Check before GetNode could hide a cold entry by filling it.
				cached := make(map[NodeID]*Node, len(written))
				b.nodeCacheMu.RLock()
				for id := range written {
					cached[id], _ = b.cachedNodeLocked(id, false, 0)
				}
				b.nodeCacheMu.RUnlock()
				for id, n := range written {
					require.NotNil(t, cached[id], "committed node %s must stay warm", id)
					require.Equal(t, n.Properties, cached[id].Properties)
					require.Equal(t, n.ChunkEmbeddings, cached[id].ChunkEmbeddings)
				}
				for _, id := range deleted {
					_, err := b.GetNode(id)
					require.ErrorIs(t, err, ErrNotFound)
				}
			})
		}
	}
}

// Advancing over an earlier callback from the same commit must not allow a
// later callback to recapture the generation after another writer overtakes it.
func TestNodeCommitCacheBatchStopsAfterExternalWrite(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { _ = b.Close() })
	nodes := []*Node{
		{ID: "test:first", ChunkEmbeddings: [][]float32{{1, 0}}},
		{ID: "test:deleted", ChunkEmbeddings: [][]float32{{0, 1}}},
		{ID: "test:last", ChunkEmbeddings: [][]float32{{1, 1}}},
	}
	for _, n := range nodes {
		_, err := b.CreateNode(n)
		require.NoError(t, err)
	}
	// Declare the batch through the real commit owner before exercising late
	// fixture publications. Its timestamp comes from the oracle, not a sample.
	require.NoError(t, b.withUpdateUnits(bootstrapNodeCacheWrites(nodes), nil))
	before := [1]uint64{b.nodeCacheGen.current()}
	b.publishCommittedNodeFixture(nodes[0], before[:]...)
	require.Equal(t, b.nodeCacheGen.current(), before[0])
	ownGeneration := before[0]
	require.NoError(t, b.DeleteNode(nodes[1].ID))
	b.publishCommittedNodeFixture(nodes[1], before[:]...)
	b.publishCommittedNodeFixture(nodes[2], before[:]...)
	require.Equal(t, ownGeneration, before[0], "an overtaken token must stay stale")
	b.nodeCacheMu.RLock()
	_, lastWarm := b.nodeCache[nodes[2].ID]
	b.nodeCacheMu.RUnlock()
	require.False(t, lastWarm, "later callbacks must not recover an overtaken token")
	_, err := b.GetNode(nodes[1].ID)
	require.ErrorIs(t, err, ErrNotFound)
}
