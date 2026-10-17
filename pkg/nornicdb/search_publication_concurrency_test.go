package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Storage commits dispatch their node events (which queue deferred search
// mutations and start flushes) while flushes publish stored state and embed
// callbacks publish through the search owner, which reads storage under its
// own lock. Nothing may deadlock, and once everything settles the search
// indexes must match storage exactly: no deleted node is searchable and every
// surviving node is searchable by its latest text.
func TestSearchPublicationConcurrentWithCommitsAndEmbedCallbacks(t *testing.T) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dbName := db.defaultDatabaseName()
	svc, err := db.EnsureSearchIndexesBuilt(ctx, dbName, nil)
	require.NoError(t, err)
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, entry)
	entry.pendingFlushDelay = time.Millisecond

	const nodes, rounds = 24, 30
	id := func(i int) storage.NodeID { return storage.NodeID(fmt.Sprintf("n%d", i)) }
	term := func(i, round int) string { return fmt.Sprintf("tok%dr%d", i, round) }
	for i := range nodes {
		_, err := db.storage.CreateNode(&storage.Node{ID: id(i), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": term(i, 0)}})
		require.NoError(t, err)
	}

	sidecar, ok := db.storage.(storage.EmbeddingSidecarUpdater)
	require.True(t, ok)
	dims := config.Memory.EmbeddingDimensions
	var wg sync.WaitGroup
	done := make(chan struct{})
	for w := range 3 {
		wg.Add(1)
		go func(w int) { // commits with event callbacks
			defer wg.Done()
			for round := 1; round <= rounds; round++ {
				i := (round*7 + w*5) % nodes
				switch round % 5 {
				case 0:
					_ = db.storage.DeleteNode(id(i))
				case 1:
					_, _ = db.storage.CreateNode(&storage.Node{ID: id(i), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": term(i, round)}})
				default:
					_ = db.storage.UpdateNode(&storage.Node{ID: id(i), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": term(i, round)}})
				}
			}
		}(w)
	}
	for w := range 2 {
		wg.Add(1)
		go func(w int) { // embed callbacks, possibly late and out of order
			defer wg.Done()
			for round := range rounds {
				i := (round*3 + w) % nodes
				stored, err := db.storage.GetNode(id(i))
				if err != nil {
					continue
				}
				embedded := storage.CopyNode(stored)
				vector := make([]float32, dims)
				vector[i%dims] = 1
				embedded.ChunkEmbeddings = [][]float32{vector}
				if sidecar.UpdateNodeEmbeddingSidecar(embedded) != nil {
					continue
				}
				qualified := storage.CopyNode(embedded)
				qualified.ID = storage.NodeID(dbName + ":" + string(id(i)))
				db.onNodeEmbedded(qualified)
			}
		}(w)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("commits, flushes and embed callbacks deadlocked")
	}

	// Settle: every queued mutation is published.
	require.Eventually(t, func() bool {
		entry.pendingFlushMu.Lock()
		running := entry.pendingFlushRunning
		entry.pendingFlushMu.Unlock()
		return !running && !entry.hasPending()
	}, time.Minute, time.Millisecond)

	opts := search.DefaultSearchOptions()
	opts.Limit, opts.MinRRFScore = 10, 0
	for i := range nodes {
		stored, err := db.storage.GetNode(id(i))
		for round := 0; round <= rounds; round++ {
			resp, searchErr := svc.Search(ctx, term(i, round), nil, opts)
			require.NoError(t, searchErr)
			current := err == nil && stored.Properties["content"] == term(i, round)
			if current {
				require.Len(t, resp.Results, 1, "node %s must be searchable by its latest text %s", id(i), term(i, round))
				require.Equal(t, id(i), resp.Results[0].NodeID)
			} else {
				require.Empty(t, resp.Results, "node %s: text %s is deleted or superseded", id(i), term(i, round))
			}
		}
	}
}
