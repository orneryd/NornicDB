package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Bounded write-path benchmarks for the write-behind commit order. Every batch
// opens a fresh store seeded with the same fixed nodes, runs at most
// orderBenchBatchOps timed operations, then validates and removes the store
// with the timer stopped, so the work per operation does not depend on b.N.
// They use only APIs that predate the commit order, so they run unchanged on
// its base.

const (
	orderBenchNodes    = 64
	orderBenchBatchOps = 512
	orderBenchWorkers  = 8
)

type orderBenchStore struct {
	b         *testing.B
	dir       string
	db        *BadgerEngine
	ids       []NodeID
	delivered *atomic.Int64
}

func openOrderBenchStore(b *testing.B, root string, writeBehind bool) *orderBenchStore {
	b.Helper()
	dir, err := os.MkdirTemp(root, "batch-")
	if err != nil {
		b.Fatal(err)
	}
	db, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatal(err)
	}
	s := &orderBenchStore{b: b, dir: dir, db: db}
	for i := 0; i < orderBenchNodes; i++ {
		id := NodeID(fmt.Sprintf("bench:n%d", i))
		if _, err := db.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"cnt": int64(0)}}); err != nil {
			s.close()
			b.Fatal(err)
		}
		s.ids = append(s.ids, id)
	}
	if err := db.FlushWriteBehind(); err != nil {
		s.close()
		b.Fatal(err)
	}
	if os.Getenv("NORNIC_ORDER_LISTENERS") == "1" {
		s.delivered = orderBenchSubscribe(db)
	}
	return s
}

func (s *orderBenchStore) close() {
	_ = s.db.Close()
	_ = os.RemoveAll(s.dir)
}

// increment adds one to a seeded node's counter in its own transaction.
func (s *orderBenchStore) increment(id NodeID, implicit bool) error {
	tx, err := s.db.BeginTransaction()
	if err != nil {
		return err
	}
	if err := tx.SetImplicit(implicit); err != nil {
		_ = tx.Rollback()
		return err
	}
	node, err := tx.GetNode(id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	node.Properties["cnt"] = node.Properties["cnt"].(int64) + 1
	if err := tx.UpdateNode(node); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// verifyCounters checks, after a flush, that the counters add up to the
// increments the batch made.
func (s *orderBenchStore) verifyCounters(want int64) {
	s.b.Helper()
	if err := s.db.FlushWriteBehind(); err != nil {
		s.b.Fatal(err)
	}
	var sum int64
	for _, id := range s.ids {
		node, err := s.db.GetNode(id)
		if err != nil {
			s.b.Fatal(err)
		}
		sum += node.Properties["cnt"].(int64)
	}
	if sum != want {
		s.b.Fatalf("counters sum to %d, want %d", sum, want)
	}
}

// runOrderBenchBatches times b.N operations across fresh batches: batch runs
// a batch's ops timed operations, verify checks them untimed.
// One temporary directory per benchmark holds each batch's store, removed
// with the batch.
func runOrderBenchBatches(b *testing.B, writeBehind bool, writesPerOperation int, batch func(s *orderBenchStore, ops int), verify func(s *orderBenchStore, ops int)) {
	b.ReportAllocs()
	b.StopTimer()
	root, err := os.MkdirTemp("", "wb-order-bench-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(root)
	var ackNanos, drainNanos int64
	for done := 0; done < b.N; {
		ops := min(orderBenchBatchOps, b.N-done)
		s := openOrderBenchStore(b, root, writeBehind)
		b.StartTimer()
		ackStart := time.Now()
		batch(s, ops)
		ackNanos += time.Since(ackStart).Nanoseconds()
		start := time.Now()
		if err := s.db.FlushWriteBehind(); err != nil {
			b.Fatal(err)
		}
		drainNanos += time.Since(start).Nanoseconds()
		b.StopTimer()
		if s.delivered != nil && s.delivered.Load() != int64(ops*writesPerOperation) {
			b.Fatalf("delivered %d events, want %d", s.delivered.Load(), ops*writesPerOperation)
		}
		verify(s, ops)
		s.close()
		done += ops
	}
	b.ReportMetric(float64(ackNanos)/float64(b.N), "ack-ns/op")
	b.ReportMetric(float64(drainNanos)/float64(b.N), "drain-ns/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "completion-ns/op")
}

func benchmarkOrderSequentialIncrements(b *testing.B, writeBehind, implicit bool) {
	runOrderBenchBatches(b, writeBehind, 1, func(s *orderBenchStore, ops int) {
		for i := 0; i < ops; i++ {
			if err := s.increment(s.ids[i%len(s.ids)], implicit); err != nil {
				b.Fatal(err)
			}
		}
	}, func(s *orderBenchStore, ops int) { s.verifyCounters(int64(ops)) })
}

// Autocommit read-modify-write updates acknowledged through the buffer.
func BenchmarkWriteBehindOrderImplicitIncrements(b *testing.B) {
	benchmarkOrderSequentialIncrements(b, true, true)
}

// The same updates with write-behind disabled: the synchronous control.
func BenchmarkWriteBehindOrderSyncOffImplicitIncrements(b *testing.B) {
	benchmarkOrderSequentialIncrements(b, false, true)
}

// Concurrent autocommit updates of disjoint nodes through the buffer.
func BenchmarkWriteBehindOrderParallelImplicitIncrements(b *testing.B) {
	runOrderBenchBatches(b, true, 1, func(s *orderBenchStore, ops int) {
		var wg sync.WaitGroup
		errs := make(chan error, orderBenchWorkers)
		for w := 0; w < orderBenchWorkers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// i ≡ w (mod workers), and the node count is a multiple of
				// the worker count, so each worker owns its nodes.
				for i := w; i < ops; i += orderBenchWorkers {
					if err := s.increment(s.ids[i%len(s.ids)], true); err != nil {
						errs <- err
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
	}, func(s *orderBenchStore, ops int) { s.verifyCounters(int64(ops)) })
}

// Autocommit creates of fixed, non-generated IDs through the buffer.
func BenchmarkWriteBehindOrderImplicitCreates(b *testing.B) {
	runOrderBenchBatches(b, true, 1, func(s *orderBenchStore, ops int) {
		for i := 0; i < ops; i++ {
			tx, err := s.db.BeginTransaction()
			if err != nil {
				b.Fatal(err)
			}
			if err := tx.SetImplicit(true); err != nil {
				b.Fatal(err)
			}
			if _, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("bench:c%d", i)), Labels: []string{"Created"}}); err != nil {
				b.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}, func(s *orderBenchStore, ops int) {
		if err := s.db.FlushWriteBehind(); err != nil {
			b.Fatal(err)
		}
		if count, err := s.db.NodeCountByLabel("Created"); err != nil || count != int64(ops) {
			b.Fatalf("created count %d (%v), want %d", count, err, ops)
		}
	})
}

// Each operation acknowledges an autocommit update into the buffer and then
// commits an explicit update of another node, which lands synchronously.
func BenchmarkWriteBehindOrderExplicitAfterImplicit(b *testing.B) {
	runOrderBenchBatches(b, true, 2, func(s *orderBenchStore, ops int) {
		for i := 0; i < ops; i++ {
			if err := s.increment(s.ids[(2*i)%len(s.ids)], true); err != nil {
				b.Fatal(err)
			}
			if err := s.increment(s.ids[(2*i+1)%len(s.ids)], false); err != nil {
				b.Fatal(err)
			}
		}
	}, func(s *orderBenchStore, ops int) { s.verifyCounters(int64(2 * ops)) })
}

// Each operation acknowledges an autocommit update into the buffer and then
// writes another, unbuffered node directly through the engine.
func BenchmarkWriteBehindOrderDirectAfterImplicit(b *testing.B) {
	runOrderBenchBatches(b, true, 2, func(s *orderBenchStore, ops int) {
		for i := 0; i < ops; i++ {
			if err := s.increment(s.ids[(2*i)%len(s.ids)], true); err != nil {
				b.Fatal(err)
			}
			node, err := s.db.GetNode(s.ids[(2*i+1)%len(s.ids)])
			if err != nil {
				b.Fatal(err)
			}
			node.Properties["cnt"] = node.Properties["cnt"].(int64) + 1
			if err := s.db.UpdateNode(node); err != nil {
				b.Fatal(err)
			}
		}
	}, func(s *orderBenchStore, ops int) { s.verifyCounters(int64(2 * ops)) })
}
