package cypher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanceledLabellessCountPredicateRetainsFallbackContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base := newTestMemoryEngine(t)
		namespace := storage.NewNamespacedEngine(base, "test")
		seed := NewStorageExecutor(namespace)
		_, err := seed.Execute(t.Context(), "CREATE (:CountPerson {k: 1}), (:CountPerson {k: 2})", nil)
		require.NoError(t, err)
		_, err = seed.Execute(t.Context(), "CREATE INDEX count_k FOR (n:CountPerson) ON (n.k)", nil)
		require.NoError(t, err)
		store := &pausedLabelCountEngine{NamespacedEngine: namespace, counted: make(chan struct{}), resume: make(chan struct{}), countContext: make(chan context.Context, 1), scanContext: make(chan context.Context, 4)}
		executor := NewStorageExecutor(store)
		type requestKey struct{}
		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), requestKey{}, "count-predicate"))
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(store.resume) }) }
		type outcome struct {
			result *ExecuteResult
			err    error
		}
		done := make(chan outcome, 1)
		joined := make(chan struct{})
		t.Cleanup(func() {
			cancel()
			release()
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("count-predicate query worker did not join")
			}
		})
		const query = "MATCH (n {k: 1}) RETURN n.k"
		go func() {
			defer close(joined)
			result, err := executor.Execute(ctx, query, nil)
			done <- outcome{result, err}
		}()
		select {
		case <-store.counted:
		case <-time.After(5 * time.Second):
			t.Fatal("public query did not reach real label-counter predicate")
		}
		var countCtx context.Context
		select {
		case countCtx = <-store.countContext:
		case <-time.After(5 * time.Second):
			t.Fatal("label counter context was not observed")
		}
		require.Equal(t, "count-predicate", countCtx.Value(requestKey{}))
		cancel()
		select {
		case got := <-done:
			require.ErrorIs(t, got.err, context.Canceled)
			require.Nil(t, got.result)
		case <-time.After(time.Second):
			t.Fatal("canceled count predicate or fallback ignored the request lifetime")
		}
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Fatal("query worker did not join after result")
		}
		select {
		case fallbackCtx := <-store.scanContext:
			require.Equal(t, "count-predicate", fallbackCtx.Value(requestKey{}))
			require.ErrorIs(t, fallbackCtx.Err(), context.Canceled, "fallback must not start an uncancelled native scan")
			t.Log("public canceled predicate reached native fallback with canceled caller context")
		default:
			t.Log("public canceled predicate stopped before native fallback admission")
		}
		release()
		result, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		count, err := storage.CountNodesWithLabel(t.Context(), namespace, "CountPerson")
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
	})
}
