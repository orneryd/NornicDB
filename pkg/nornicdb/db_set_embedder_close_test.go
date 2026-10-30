package nornicdb

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// A closed database ignores SetEmbedder: no query embedder, no queue
// embedder, and its lifetime context has ended.
func TestDB_SetEmbedder_IgnoredAfterClose(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	require.NoError(t, db.LifetimeContext().Err())
	require.NoError(t, db.Close())
	require.Error(t, db.LifetimeContext().Err())

	m := &mockEmbedder{dims: 4, model: "late"}
	require.False(t, db.SetEmbedder(m), "a closed database refuses the embedder")
	db.SetDefaultEmbedConfig(&embed.Config{Provider: "openai", Model: "late", Dimensions: 4})

	require.Nil(t, db.GetCypherExecutor().GetEmbedder())
	if queue := db.GetEmbedQueue(); queue != nil {
		require.Nil(t, queue.currentEmbedder())
	}
	db.embedderRegistryMu.RLock()
	defer db.embedderRegistryMu.RUnlock()
	require.Empty(t, db.embedderRegistry)
}

// SetEmbedder racing Close either installs before Close begins, so Close
// stops what it started, or is refused; -race checks there is no data race.
func TestDB_SetEmbedder_ConcurrentWithClose(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		db.SetEmbedder(&mockEmbedder{dims: 4, model: "racing"})
	}()
	close(start)
	require.NoError(t, db.Close())
	wg.Wait()

	if queue := db.GetEmbedQueue(); queue != nil {
		require.True(t, queue.closed.Load())
	}
}

// Close waits for a background task, which ends with the lifetime context, and
// a closing database refuses new ones.
func TestDB_RunBackgroundTask_JoinedByClose(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)

	var finished atomic.Bool
	require.True(t, db.RunBackgroundTask(func() {
		<-db.LifetimeContext().Done()
		finished.Store(true)
	}))
	require.NoError(t, db.Close())
	require.True(t, finished.Load(), "Close returned before the background task did")
	require.False(t, db.RunBackgroundTask(func() { t.Error("task ran after Close") }))
}

// closingTestEmbedder counts Close calls.
type closingTestEmbedder struct {
	mockEmbedder
	closes atomic.Int32
}

func (e *closingTestEmbedder) Close() error {
	e.closes.Add(1)
	return nil
}

// The embedder a closed database refuses is closed, bare or behind the
// cache wrapper: nobody else will release it (#1394 residual).
func TestDB_SetEmbedder_RefusedEmbedderIsClosed(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	bare := &closingTestEmbedder{mockEmbedder: mockEmbedder{dims: 4, model: "late"}}
	db.SetEmbedder(bare)
	require.EqualValues(t, 1, bare.closes.Load(), "the refused embedder is closed")

	wrapped := &closingTestEmbedder{mockEmbedder: mockEmbedder{dims: 4, model: "late"}}
	db.SetEmbedder(embed.NewCachedEmbedder(wrapped, 16))
	require.EqualValues(t, 1, wrapped.closes.Load(), "the model behind a refused cached embedder is closed")
}
