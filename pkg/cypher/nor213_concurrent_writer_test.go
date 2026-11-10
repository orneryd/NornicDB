package cypher

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
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

// TestNOR213WritersOfOneEntityWaitOnEveryWriteForm: every statement form
// that writes an existing node or relationship waits for the transaction
// that wrote it first, then applies its own write on top of that commit. The
// second statement starts only once the first holds the entity; the order is
// set by the lock wait itself, not by timing.
func TestNOR213WritersOfOneEntityWaitOnEveryWriteForm(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	for _, form := range []struct {
		hold, write, check string
		want               []interface{}
	}{
		{"MATCH (n:Doc {id: $k}) SET n.a = 1", "MATCH (n:Doc {id: $k}) SET n.b = 2", "MATCH (n:Doc {id: $k}) RETURN n.a, n.b", []interface{}{int64(1), int64(2)}},
		{"MATCH (n:Doc {id: $k}) SET n.c = 5", "MATCH (n:Doc {id: $k}) SET n.c = n.c + 1", "MATCH (n:Doc {id: $k}) RETURN n.c", []interface{}{int64(6)}},
		{"MATCH (n:Doc {id: $k}) SET n.a = 1", "MATCH (n:Doc {id: $k}) SET n += {b: 2}", "MATCH (n:Doc {id: $k}) RETURN n.a, n.b", []interface{}{int64(1), int64(2)}},
		{"MATCH (n:Doc {id: $k}) SET n.a = 1", "MATCH (n:Doc {id: $k}) SET n:Seen", "MATCH (n:Doc:Seen {id: $k}) RETURN n.a", []interface{}{int64(1)}},
		{"MATCH (n:Doc {id: $k}) SET n.a = 1", "MATCH (n:Doc {id: $k}) REMOVE n.gone", "MATCH (n:Doc {id: $k}) RETURN n.a, n.gone", []interface{}{int64(1), nil}},
		{"MATCH (n:Doc {id: $k}) SET n.a = 1", "MATCH (n:Doc {id: $k}) REMOVE n:Old", "MATCH (n:Doc {id: $k}) RETURN n.a, labels(n)", []interface{}{int64(1), []interface{}{"Doc"}}},
		{"MATCH (n:Doc {id: $k}) SET n.a = 1", "MATCH (n:Doc {id: $k}) DETACH DELETE n", "MATCH (n:Doc {id: $k}) RETURN count(n)", []interface{}{int64(0)}},
		{"MATCH (:Doc {id: $k})-[r:HAS]->() SET r.a = 1", "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.b = 2", "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.a, r.b", []interface{}{int64(1), int64(2)}},
		{"MATCH (:Doc {id: $k})-[r:HAS]->() SET r.c = 5", "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.c = r.c + 1", "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.c", []interface{}{int64(6)}},
		{"MATCH (:Doc {id: $k})-[r:HAS]->() SET r.a = 1", "MATCH (:Doc {id: $k})-[r:HAS]->() REMOVE r.gone", "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.a, r.gone", []interface{}{int64(1), nil}},
		{"MATCH (:Doc {id: $k})-[r:HAS]->() SET r.a = 1", "MATCH (:Doc {id: $k})-[r:HAS]->() DELETE r", "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN count(r)", []interface{}{int64(0)}},
	} {
		for _, explicit := range []bool{false, true} {
			key := form.write + map[bool]string{false: " auto", true: " explicit"}[explicit]
			params := map[string]interface{}{"k": key}
			_, err := setup.Execute(ctx, "CREATE (:Doc:Old {id: $k, gone: 1, c: 0})-[:HAS {gone: 1, c: 0}]->(:Part)", params)
			require.NoError(t, err)
			drainMergeKeyWaits(waits)

			first := NewStorageExecutor(store)
			_, err = first.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = first.Execute(ctx, form.hold, params)
			require.NoError(t, err, key)
			done := make(chan error, 1)
			go func() { done <- runMergeStatement(ctx, store, explicit, form.write, params) }()
			var secondErr error
			waited := false
			select {
			case <-waits:
				waited = true
			case secondErr = <-done:
			}
			_, err = first.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err, key)
			if waited {
				secondErr = <-done
			}
			require.NoError(t, secondErr, key)
			require.True(t, waited, "%s: the second writer waits for the first", key)
			result, err := setup.Execute(ctx, form.check, params)
			require.NoError(t, err, key)
			require.Equal(t, [][]interface{}{form.want}, result.Rows, key)
		}
	}
}

// TestNOR213WaitingWriterFollowsARollback: the waiting writer of a node
// whose first writer rolls back writes the node as it was committed.
func TestNOR213WaitingWriterFollowsARollback(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE (:Doc {id: 'rolled', c: 0})", nil)
	require.NoError(t, err)
	first := NewStorageExecutor(store)
	_, err = first.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = first.Execute(ctx, "MATCH (n:Doc {id: 'rolled'}) SET n.c = 40", nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- runMergeStatement(ctx, store, true, "MATCH (n:Doc {id: 'rolled'}) SET n.c = n.c + 1", nil)
	}()
	<-waits
	_, err = first.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.NoError(t, <-done)
	result, err := setup.Execute(ctx, "MATCH (n:Doc {id: 'rolled'}) RETURN n.c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// TestNOR213WritersClosingACycleDeadlock: two transactions that each hold a
// node and write the other's. The write that would close the cycle fails
// with the transient deadlock error; the other transaction commits once the
// failed one ends.
func TestNOR213WritersClosingACycleDeadlock(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE (:Doc {id: 'x'}), (:Doc {id: 'y'})", nil)
	require.NoError(t, err)
	a, b := NewStorageExecutor(store), NewStorageExecutor(store)
	for session, held := range map[*StorageExecutor]string{a: "x", b: "y"} {
		_, err = session.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = session.Execute(ctx, "MATCH (n:Doc {id: $k}) SET n.held = true", map[string]interface{}{"k": held})
		require.NoError(t, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := a.Execute(ctx, "MATCH (n:Doc {id: 'y'}) SET n.a = 1", nil)
		done <- err
	}()
	<-waits
	_, err = b.Execute(ctx, "MATCH (n:Doc {id: 'x'}) SET n.b = 1", nil)
	require.ErrorIs(t, err, storage.ErrDeadlock)
	_, _ = b.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, <-done)
	_, err = a.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	result, err := setup.Execute(ctx, "MATCH (n:Doc) WHERE n.id IN ['x', 'y'] RETURN n.id, n.a, n.b ORDER BY n.id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x", nil, nil}, {"y", int64(1), nil}}, result.Rows)
}

// TestNOR213WaitEndsWithItsStatementAndAtTheTransactionTimeout: a writer
// waiting for a node stops when its statement is canceled, and at the
// server's transaction timeout; the node stays the holder's.
func TestNOR213WaitEndsWithItsStatementAndAtTheTransactionTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: timeout}},
	}))
	require.NoError(t, err)
	base := runtimeFixtureOwner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	ctx := context.Background()
	setup := NewStorageExecutor(store)
	_, err = setup.Execute(ctx, "CREATE (:Doc {id: 'held'})", nil)
	require.NoError(t, err)
	waits := mergeKeyWaits(t)
	holder := NewStorageExecutor(store)
	_, err = holder.Execute(WithClientTransactionTimeout(ctx), "BEGIN", nil)
	require.NoError(t, err)
	_, err = holder.Execute(ctx, "MATCH (n:Doc {id: 'held'}) SET n.a = 1", nil)
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := NewStorageExecutor(store).Execute(canceled, "MATCH (n:Doc {id: 'held'}) SET n.b = 2", nil)
		done <- err
	}()
	<-waits
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	started := time.Now()
	_, err = NewStorageExecutor(store).Execute(ctx, "MATCH (n:Doc {id: 'held'}) SET n.b = 2", nil)
	require.ErrorIs(t, err, storage.ErrLockWaitTimeout)
	require.GreaterOrEqual(t, time.Since(started), timeout)

	_, err = holder.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	result, err := setup.Execute(ctx, "MATCH (n:Doc {id: 'held'}) RETURN n.a, n.b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), nil}}, result.Rows)
}

// TestNOR213ConcurrentIncrementsAreNotLost: writers that read the value they
// write (SET n.c = n.c + 1) serialize on the entity's lock, so every
// increment survives and none fails.
func TestNOR213ConcurrentIncrementsAreNotLost(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE (:Doc {id: 'n', c: 0})-[:HAS {c: 0}]->(:Part)", nil)
	require.NoError(t, err)
	const workers, rounds = 8, 25
	for _, query := range []string{
		"MATCH (n:Doc {id: 'n'}) SET n.c = n.c + 1",
		"MATCH (:Doc {id: 'n'})-[r:HAS]->() SET r.c = r.c + 1",
	} {
		var group sync.WaitGroup
		failures := make(chan error, workers*rounds)
		for worker := 0; worker < workers; worker++ {
			group.Add(1)
			go func() {
				defer group.Done()
				for round := 0; round < rounds; round++ {
					if err := runMergeStatement(ctx, store, (worker+round)%2 == 0, query, nil); err != nil {
						failures <- err
					}
				}
			}()
		}
		group.Wait()
		close(failures)
		for err := range failures {
			require.NoError(t, err, "%s", query)
		}
	}
	result, err := setup.Execute(ctx, "MATCH (n:Doc {id: 'n'})-[r:HAS]->() RETURN n.c, r.c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(workers * rounds), int64(workers * rounds)}}, result.Rows)
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

// BenchmarkNOR213ContendedNodeUpdate measures auto-commit writers that
// contend: eight sessions incrementing one node (every write waits for the
// entity lock) or each its own node (the writers share only the lock registry
// and the commit path), with durable commits and with the write-behind
// buffer. Besides ns/op it reports the p50 and p99 statement latency, and the
// share of statements that failed with a conflict.
func BenchmarkNOR213ContendedNodeUpdate(b *testing.B) {
	const sessions = 8
	for _, mode := range []string{"sync", "writeBehind"} {
		for _, shape := range []string{"oneNode", "ownNodes"} {
			b.Run(mode+"/"+shape, func(b *testing.B) {
				ctx := context.Background()
				base, err := storage.NewMemoryEngineWithOptions(storage.BadgerOptions{WriteBehind: mode == "writeBehind"})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = base.Close() })
				store := storage.NewNamespacedEngine(base, "test")
				setup := NewStorageExecutor(store)
				if _, err := setup.Execute(ctx, "CREATE INDEX doc_id FOR (n:Doc) ON (n.id)", nil); err != nil {
					b.Fatal(err)
				}
				if _, err := setup.Execute(ctx, "UNWIND range(0, 63) AS i CREATE (:Doc {id: i, value: 0})", nil); err != nil {
					b.Fatal(err)
				}
				var next, conflicts atomic.Int64
				var mu sync.Mutex
				var latencies []time.Duration
				b.SetParallelism(sessions)
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					exec := NewStorageExecutor(store)
					id := int64(0)
					if shape == "ownNodes" {
						id = next.Add(1) % 64
					}
					params := map[string]interface{}{"id": id}
					local := make([]time.Duration, 0, 1024)
					for pb.Next() {
						started := time.Now()
						_, err := exec.Execute(ctx, "MATCH (n:Doc {id: $id}) SET n.value = n.value + 1", params)
						local = append(local, time.Since(started))
						if errors.Is(err, storage.ErrConflict) {
							conflicts.Add(1)
						} else if err != nil {
							b.Error(err)
							return
						}
					}
					mu.Lock()
					latencies = append(latencies, local...)
					mu.Unlock()
				})
				b.StopTimer()
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				if n := len(latencies); n > 0 {
					b.ReportMetric(float64(latencies[n/2].Microseconds()), "p50-µs")
					b.ReportMetric(float64(latencies[n*99/100].Microseconds()), "p99-µs")
					b.ReportMetric(float64(conflicts.Load())/float64(n), "conflicts/op")
				}
			})
		}
	}
}
