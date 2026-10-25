package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitCacheReplayFinalImages(t *testing.T) {
	for _, route := range []string{"buffered-flush", "wal-recovery"} {
		for seed := int64(1); seed <= 4; seed++ {
			t.Run(fmt.Sprintf("%s/seed=%d", route, seed), func(t *testing.T) {
				runCommitCacheReplayImages(t, route, seed)
			})
		}
	}
}

func runCommitCacheReplayImages(t *testing.T, route string, seed int64) {
	t.Helper()
	producer := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: route == "wal-recovery"})
	producer.writeBehind.Close() // Replay is explicit, not a scheduling race.
	producer.SetDecayEnabled(false)
	for _, id := range []NodeID{"test:replay-a", "test:replay-b", "test:replay-middle"} {
		_, err := producer.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{{1}}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:replay-edge", StartNode: "test:replay-a", EndNode: "test:replay-b", Type: "R", Properties: map[string]any{"v": int64(0)}}
	require.NoError(t, producer.CreateEdge(edge))
	tx, err := producer.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	writes := 2 + int(seed)
	rng := rand.New(rand.NewSource(seed))
	nodeFinal := int64(0)
	for version := 1; version <= writes; version++ {
		next := copyEdge(edge)
		next.Properties["v"] = int64(version)
		next.Properties["step"] = fmt.Sprint(version)
		require.NoError(t, tx.UpdateEdge(next))
		if version == 1 || rng.Intn(2) == 0 {
			node, err := tx.GetNode("test:replay-middle")
			require.NoError(t, err)
			nodeFinal++
			node.Properties["v"] = nodeFinal
			require.NoError(t, tx.UpdateNode(node))
		}
	}
	if route == "wal-recovery" {
		require.NoError(t, tx.Commit())
	}
	b := producer
	replay := producer.FlushWriteBehind
	if route == "wal-recovery" {
		wal := producer.bufferedCommitWAL
		entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
		require.NoError(t, err)
		var entry WALEntry
		for _, candidate := range entries {
			if candidate.Operation == OpBufferedCommit {
				entry = candidate
				break
			}
		}
		require.NotEmpty(t, entry.Data)
		dir := producer.dataDir
		crashClose(t, producer, wal)
		b, err = NewBadgerEngine(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Close()) })
		b.SetDecayEnabled(false)
		replay = func() error { return ReplayWALEntry(b, entry) }
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	var mu sync.Mutex
	var observed []string
	b.OnEdgeUpdated(func(image *Edge) {
		if image.ID != edge.ID {
			return
		}
		value, _ := image.Properties["step"].(string)
		mu.Lock()
		observed = append(observed, value)
		mu.Unlock()
		if value == "1" {
			close(entered)
			<-resume
		}
	})
	if route == "buffered-flush" {
		require.NoError(t, tx.Commit())
	}
	done := make(chan error, 1)
	t.Cleanup(func() {
		release()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("replay did not finish")
		}
	})
	go func() { done <- replay() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("replay callback was not reached")
	}
	// Observe the actual commit publisher while its first ordinary listener is
	// paused. The live overlay can mask a bad buffered cache, so check that
	// cache image too; WAL recovery has no overlay to mask it.
	b.edgeCacheMu.RLock()
	cached := b.edgeCache[edge.ID].Edge
	if cached == nil || cached.Properties["v"] != int64(writes) {
		t.Errorf("replay published an intermediate relationship: got%v want%d", cached, writes)
	}
	b.edgeCacheMu.RUnlock()
	point, err := b.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, int64(writes), point.Properties["v"])
	for _, read := range []struct {
		fn func(NodeID) ([]*Edge, error)
		id NodeID
	}{
		{b.GetStoredOutgoingEdges, edge.StartNode}, {b.GetStoredIncomingEdges, edge.EndNode},
		{b.GetOutgoingEdges, edge.StartNode}, {b.GetIncomingEdges, edge.EndNode},
	} {
		rows, err := read.fn(read.id)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, int64(writes), rows[0].Properties["v"])
	}
	node, err := b.GetStoredNode("test:replay-middle")
	require.NoError(t, err)
	require.Equal(t, nodeFinal, node.Properties["v"])
	release()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) == writes
	}, 10*time.Second, time.Millisecond)
	mu.Lock()
	for i, value := range observed {
		require.Equal(t, fmt.Sprint(i+1), value, "ordered notifications are retained")
	}
	mu.Unlock()
}
