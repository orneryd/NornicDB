package nornicdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// openPublicationBenchDB opens a ready database with flushNodes source nodes
// and a short deferred-flush debounce.
func openPublicationBenchDB(b *testing.B, flushNodes int) (*DB, *dbSearchService) {
	b.Helper()
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open(b.TempDir(), config)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	dbName := db.defaultDatabaseName()
	_, err = db.EnsureSearchIndexes(context.Background(), dbName, nil, SearchIndexBuildWait)
	require.NoError(b, err)
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	require.NotNil(b, entry)
	entry.pendingFlushDelay = time.Millisecond
	for i := range flushNodes {
		_, err := db.storage.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("bench-%d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "seed text"}})
		require.NoError(b, err)
	}
	waitFlushIdle(b, entry)
	return db, entry
}

func waitFlushIdle(b *testing.B, entry *dbSearchService) {
	b.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		entry.pendingFlushMu.Lock()
		running := entry.pendingFlushRunning
		entry.pendingFlushMu.Unlock()
		if !running && !entry.hasPending() {
			return
		}
		require.True(b, time.Now().Before(deadline), "the deferred flush never drained")
		time.Sleep(100 * time.Microsecond)
	}
}

// BenchmarkDeferredSearchFlushThroughput measures how fast the deferred search
// flush publishes a burst of committed node updates: from the first update
// until the flush has drained them all.
func BenchmarkDeferredSearchFlushThroughput(b *testing.B) {
	const nodes = 256
	db, entry := openPublicationBenchDB(b, nodes)
	b.ResetTimer()
	for n := range b.N {
		for i := range nodes {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("bench-%d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": fmt.Sprintf("update %d text", n)}}
			require.NoError(b, db.storage.UpdateNode(storage.NodeUpdateRequest{After: node}))
		}
		waitFlushIdle(b, entry)
	}
	b.ReportMetric(float64(b.N*nodes)/b.Elapsed().Seconds(), "nodes/s")
}
