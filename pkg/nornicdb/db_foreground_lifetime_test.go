package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestDBCloseQuiescesCypherProviderConsumers(t *testing.T) {
	for _, mode := range []string{"Cypher", "ExecuteCypher", "scoped executor"} {
		t.Run(mode, func(t *testing.T) {
			db, err := Open("", nil)
			require.NoError(t, err)
			e := &solReviewForegroundEmbedder{
				mockEmbedder: mockEmbedder{dims: 4}, entered: make(chan struct{}),
				proceed: make(chan struct{}), closeCalled: make(chan struct{}),
			}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(e.proceed) }); _ = db.Close() })
			require.True(t, db.SetEmbedder(e))
			queryDone := make(chan error, 1)
			go func() {
				const query = "CALL db.index.vector.embed('held query') YIELD embedding RETURN embedding"
				var queryErr error
				switch mode {
				case "Cypher":
					_, queryErr = db.Cypher(context.Background(), query, nil)
				case "ExecuteCypher":
					_, queryErr = db.ExecuteCypher(context.Background(), query, nil)
				default:
					exec := cypher.NewStorageExecutor(db.GetStorage())
					exec.ShareEmbedderFrom(db.GetCypherExecutor())
					_, queryErr = exec.Execute(context.Background(), query, nil)
				}
				queryDone <- queryErr
			}()
			select {
			case <-e.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Cypher did not enter the provider")
			}
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			select {
			case err := <-queryDone:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("Close could not cancel the blocked Cypher provider")
			}
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not join the Cypher call")
			}
			require.Zero(t, e.unsafeCloses.Load())
		})
	}
}

type uncancellableForegroundEmbedder struct {
	solReviewForegroundEmbedder
	cancelled chan struct{}
}

func (e *uncancellableForegroundEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	e.active.Add(1)
	defer e.active.Add(-1)
	close(e.entered)
	<-ctx.Done()
	close(e.cancelled)
	<-e.proceed // Native calls may take time to finish after cancellation.
	return []float32{1, 0, 0, 0}, nil
}

func TestDBCloseJoinsUncancellableCallAndRejectsRetainedSnapshots(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	e := &uncancellableForegroundEmbedder{
		solReviewForegroundEmbedder: solReviewForegroundEmbedder{
			mockEmbedder: mockEmbedder{dims: 4}, entered: make(chan struct{}),
			proceed: make(chan struct{}), closeCalled: make(chan struct{}),
		},
		cancelled: make(chan struct{}),
	}
	var release sync.Once
	allowQuery := func() { release.Do(func() { close(e.proceed) }) }
	t.Cleanup(func() { allowQuery(); _ = db.Close() })
	require.True(t, db.SetEmbedder(e))
	snapshot := db.QueryEmbeddingForDB("nornic")
	request := db.WithQueryEmbeddingSnapshot(context.Background())
	// Remove unrelated queue teardown from the ordering observation.
	db.GetEmbedQueue().Close()
	queryDone := make(chan error, 1)
	go func() { _, err := db.EmbedQuery(context.Background(), "held query"); queryDone <- err }()
	select {
	case <-e.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("query did not enter the provider")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-e.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground context was not cancelled")
	}
	// Even a retained raw snapshot/context must not admit another provider use.
	_, err = snapshot.EmbedQuery(context.Background(), "late query")
	require.ErrorIs(t, err, ErrClosed)
	_, err = snapshot.ChunkQuery(context.Background(), "late query")
	require.ErrorIs(t, err, ErrClosed)
	_, _, err = snapshot.EmbedQueryChunks(context.Background(), "late query")
	require.ErrorIs(t, err, ErrClosed)
	_, err = db.EmbedQuery(request, "late query")
	require.ErrorIs(t, err, ErrClosed)
	select {
	case <-e.closeCalled:
		t.Error("model was freed before the uncancellable call returned")
	case <-closed:
		t.Error("Close returned before the uncancellable call returned")
	case <-time.After(time.Second):
	}
	allowQuery()
	require.NoError(t, <-queryDone)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after the query returned")
	}
	require.Zero(t, e.unsafeCloses.Load())
}
