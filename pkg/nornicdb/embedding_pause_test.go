package nornicdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func openEmbeddingPauseDB(t *testing.T, dir string) *DB {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = true
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := Open(dir, config)
	require.NoError(t, err)
	return db
}

// The pause holds across a restart, from before the first worker starts,
// and through provider loads and reloads; the nodes written meanwhile stay
// pending and are embedded after the resume (#1021).
func TestEmbeddingPauseSurvivesRestartAndProviderReload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := openEmbeddingPauseDB(t, dir)
	require.False(t, db.EmbeddingStatus().Paused)
	status, err := db.SetEmbeddingPaused(ctx, true)
	require.NoError(t, err)
	require.True(t, status.Paused)
	status, err = db.SetEmbeddingPaused(ctx, true) // idempotent
	require.NoError(t, err)
	require.True(t, status.Paused)
	require.True(t, db.GetEmbedQueue().Paused())
	require.NoError(t, db.Close())

	db = openEmbeddingPauseDB(t, dir)
	t.Cleanup(func() { _ = db.Close() })
	require.True(t, db.EmbeddingStatus().Paused, "the pause survives a restart")
	require.True(t, db.GetEmbedQueue().Paused(), "the queue is created paused")

	embedder := &staticTestEmbedder{dims: db.config.Memory.EmbeddingDimensions}
	db.SetEmbedder(embedder)
	_, err = db.ExecuteCypher(ctx, "CREATE (:Doc {content: 'held during the pause'})", nil)
	require.NoError(t, err)
	db.SetEmbedder(&staticTestEmbedder{dims: db.config.Memory.EmbeddingDimensions}) // provider re-initialisation
	// Open's warmup starts the first queue's workers; let it finish before
	// the queue is replaced.
	first := db.GetEmbedQueue()
	require.Eventually(t, func() bool {
		first.mu.Lock()
		defer first.mu.Unlock()
		return first.workersStarted
	}, 10*time.Second, 10*time.Millisecond)
	db.StopEmbedQueue()
	db.SetEmbedder(embedder) // a new queue
	require.True(t, db.GetEmbedQueue().Paused(), "a provider (re)load does not resume")
	db.GetEmbedQueue().TriggerImmediate()
	time.Sleep(1500 * time.Millisecond)
	status = db.EmbeddingStatus()
	require.True(t, status.Paused)
	require.Positive(t, status.Pending, "the node stays pending, neither embedded nor dropped")
	require.Zero(t, db.GetEmbedQueue().Stats().Processed)
	require.Zero(t, db.GetEmbedQueue().Stats().Failed)

	status, err = db.SetEmbeddingPaused(ctx, false)
	require.NoError(t, err)
	require.False(t, status.Paused)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, db.WaitForEmbeddings(waitCtx))
	require.Zero(t, db.EmbeddingStatus().Pending)
	require.Equal(t, 1, db.GetEmbedQueue().Stats().Processed)
	require.NoError(t, db.Close())

	db = openEmbeddingPauseDB(t, dir)
	t.Cleanup(func() { _ = db.Close() })
	require.False(t, db.EmbeddingStatus().Paused, "the resume survives a restart too")
	require.False(t, db.GetEmbedQueue().Paused())
}

// Relationship embedding, automatic too, waits out a pause without
// polling; a shutdown ends the wait with an error, so the relationship
// stays pending.
func TestEmbeddingPauseHoldsRelationshipEmbedding(t *testing.T) {
	ctx := context.Background()
	db := openEmbeddingPauseDB(t, t.TempDir())
	t.Cleanup(func() { _ = db.Close() })
	db.SetEmbedder(&staticTestEmbedder{dims: db.config.Memory.EmbeddingDimensions})
	_, err := db.SetEmbeddingPaused(ctx, true)
	require.NoError(t, err)
	embed := db.relationshipEmbedFunc(db.defaultDatabaseName())
	edge := &storage.Edge{ID: "r", Type: "CITES", Properties: map[string]any{"reason": "held"}}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = embed(cancelled, edge)
	require.True(t, errors.Is(err, context.Canceled))

	done := make(chan error, 1)
	go func() {
		_, err := embed(ctx, edge)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("relationship embedded while paused: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	_, err = db.SetEmbeddingPaused(ctx, false)
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the resume did not release relationship embedding")
	}
}

// Pausing works with automatic embedding off, and is kept for when it is
// turned on.
func TestEmbeddingPauseWithoutEmbedQueue(t *testing.T) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	dir := t.TempDir()
	db, err := Open(dir, config)
	require.NoError(t, err)
	require.Nil(t, db.GetEmbedQueue())
	status, err := db.SetEmbeddingPaused(ctx, true)
	require.NoError(t, err)
	require.True(t, status.Paused)
	require.NoError(t, db.Close())

	db = openEmbeddingPauseDB(t, dir)
	t.Cleanup(func() { _ = db.Close() })
	require.True(t, db.EmbeddingStatus().Paused)
	require.True(t, db.GetEmbedQueue().Paused())
}

// The status surfaces read the queue pointer under db.mu, so a status
// request racing StopEmbedQueue neither races nor reads a nil queue. The DB
// holds only a queue: Open's background goroutines would order the two
// sides through shared locks and hide the race from the detector.
func TestEmbeddingStatusRacesQueueStop(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	db := &DB{embedQueue: NewEmbedQueue(nil, engine, &EmbedWorkerConfig{NumWorkers: 1, DeferWorkerStart: true})}
	started, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for first := true; ; first = false {
			_ = db.EmbeddingStatus()
			_ = db.EmbedQueueStats()
			if first {
				close(started)
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	db.StopEmbedQueue()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-done
	require.Nil(t, db.EmbedQueueStats())
	require.Zero(t, db.EmbeddingStatus().Pending)
}
