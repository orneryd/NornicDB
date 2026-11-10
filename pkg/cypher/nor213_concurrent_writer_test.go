package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Competing writes to the same existing node must wait at the write, then
// apply to the committed predecessor; both independent properties survive.
func TestNOR213ExistingNodeWritersWaitAndPreserveBothUpdates(t *testing.T) {
	ctx := context.Background()
	store, setup := newMergeRaceStore(t)
	_, err := setup.Execute(ctx, "CREATE (:Doc {id: 'shared'})", nil)
	require.NoError(t, err)
	a, b := NewStorageExecutor(store), NewStorageExecutor(store)
	_, err = a.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = b.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = a.Execute(ctx, "MATCH (n:Doc {id:'shared'}) SET n.a = 1", nil)
	require.NoError(t, err)
	started, done := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		_, err := b.Execute(ctx, "MATCH (n:Doc {id:'shared'}) SET n.b = 2", nil)
		done <- err
	}()
	<-started
	var early bool
	var bErr error
	select {
	case bErr = <-done:
		early = true
	case <-time.After(250 * time.Millisecond):
	}
	_, err = a.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	if !early {
		select {
		case bErr = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("second writer did not resume after commit")
		}
	}
	if bErr == nil {
		_, bErr = b.Execute(ctx, "COMMIT", nil)
	} else {
		_, _ = b.Execute(ctx, "ROLLBACK", nil)
	}
	require.False(t, early, "the second writer must wait for the first transaction; second commit: %v", bErr)
	require.NoError(t, bErr)
	result, err := setup.Execute(ctx, "MATCH (n:Doc {id:'shared'}) RETURN n.a, n.b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows)
}

func BenchmarkNOR213UncontendedNodeUpdate(b *testing.B) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	if _, err := exec.Execute(ctx, "CREATE (:Doc {id: 'bench', value: 0})", nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, "MATCH (n:Doc {id:'bench'}) SET n.value = $value", map[string]interface{}{"value": int64(i)}); err != nil {
			b.Fatal(err)
		}
	}
}
