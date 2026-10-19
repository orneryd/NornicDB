package nornicdb

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// DB.UpdateNode writes storage and publishes the node to search before it
// returns.
func BenchmarkSearchRefreshDBUpdateNode(b *testing.B) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open(b.TempDir(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.EnsureSearchIndexesBuilt(ctx, db.defaultDatabaseName(), nil); err != nil {
		b.Fatal(err)
	}
	if _, err := db.CreateNodeWithID(ctx, "source", []string{"Doc"}, map[string]interface{}{"content": "alpha"}); err != nil {
		b.Fatal(err)
	}
	words := []string{"alpha", "bravo"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := db.UpdateNode(ctx, "source", map[string]interface{}{"content": words[i%2]}); err != nil {
			b.Fatal(err)
		}
	}
}

// Storage events for 256 written nodes are queued and drained into search by
// the deferred flush; one operation is one complete drain.
func BenchmarkSearchRefreshEventDrain(b *testing.B) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingDimensions = 4
	db, err := Open("", config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	dbName := db.defaultDatabaseName()
	if _, err := db.EnsureSearchIndexesBuilt(ctx, dbName, nil); err != nil {
		b.Fatal(err)
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	entry.pendingFlushDelay = time.Microsecond
	nodes := make([]*storage.Node, 256)
	for i := range nodes {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": fmt.Sprintf("word%d", i)}, ChunkEmbeddings: [][]float32{{1, 0, 0, float32(i)}}}
		if _, err := db.storage.CreateNode(node); err != nil {
			b.Fatal(err)
		}
		node.ID = storage.NodeID(dbName + ":" + string(node.ID))
		nodes[i] = node
	}
	drained := func() bool {
		entry.pendingFlushMu.Lock()
		defer entry.pendingFlushMu.Unlock()
		return !entry.pendingFlushRunning && entry.pendingFlushTimer == nil && !entry.hasPending()
	}
	for !drained() {
		time.Sleep(time.Millisecond)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, node := range nodes {
			db.indexNodeFromEvent(node)
		}
		for !drained() {
			runtime.Gosched()
		}
	}
}

// A cached Cypher read: the executor resolves the database's search service
// to key the result cache on every statement.
func BenchmarkSearchRefreshCypherCachedRead(b *testing.B) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open("", config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.EnsureSearchIndexesBuilt(ctx, db.defaultDatabaseName(), nil); err != nil {
		b.Fatal(err)
	}
	if _, err := db.ExecuteCypher(ctx, "CREATE (:Doc {content: 'alpha'})", nil); err != nil {
		b.Fatal(err)
	}
	const query = "MATCH (n:Doc) RETURN count(n)"
	if _, err := db.ExecuteCypher(ctx, query, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := db.ExecuteCypher(ctx, query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
