package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestPreparedEntityCommitAdmitsWithRequestContext(t *testing.T) {
	engine := newTestMemoryEngine(t)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	type requestKey struct{}
	request := context.WithValue(t.Context(), requestKey{}, "original")
	waiting := make(chan context.Context, 1)
	restore := setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner == &tx.mu {
			select {
			case waiting <- seen:
			default:
			}
		}
	})
	defer restore()
	tx.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(tx.mu.Unlock) }
	defer unlock()
	type result struct {
		prepared *PreparedEntityCommit
		err      error
	}
	done := make(chan result, 1)
	go func() {
		prepared, err := tx.PrepareEntityCommit(request, EntityCommitRequest{LogicalID: "optional-context"})
		done <- result{prepared, err}
	}()
	joined := false
	defer func() {
		unlock()
		if !joined {
			got := <-done
			if got.prepared != nil {
				got.prepared.Abort()
			}
		}
	}()
	select {
	case seen := <-waiting:
		require.Same(t, request, seen)
		require.Equal(t, "original", seen.Value(requestKey{}))
	case <-time.After(5 * time.Second):
		unlock()
		got := <-done
		joined = true
		if got.prepared != nil {
			got.prepared.Abort()
		}
		t.Fatal("preparation never reached required-context admission")
	}
	unlock()
	got := <-done
	joined = true
	require.NoError(t, got.err)
	require.NotNil(t, got.prepared)
	got.prepared.Abort()
}
