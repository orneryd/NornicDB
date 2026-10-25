package storage

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This benchmark carrier uses only APIs present on #1548. Its callback mimics
// the search listener's nonblocking debounced enqueue, excluding index work.
func orderBenchSubscribe(db *BadgerEngine) *atomic.Int64 {
	var delivered atomic.Int64
	queue := make(chan struct{}, 1)
	node := func(*Node) {
		delivered.Add(1)
		select {
		case queue <- struct{}{}:
		default:
		}
	}
	db.OnNodeCreated(node)
	db.OnNodeUpdated(node)
	return &delivered
}

func BenchmarkDispatcherParallelCost(b *testing.B) {
	for _, direct := range []bool{false, true} {
		for _, wb := range []bool{false, true} {
			for _, listeners := range []bool{false, true} {
				for _, contended := range []bool{false, true} {
					name := fmt.Sprintf("direct=%v/wb=%v/listeners=%v/contended=%v", direct, wb, listeners, contended)
					b.Run(name, func(b *testing.B) {
						b.ReportAllocs()
						b.StopTimer()
						root, err := os.MkdirTemp("", "dispatcher-cost-")
						if err != nil {
							b.Fatal(err)
						}
						defer os.RemoveAll(root)
						var retries, peak, drainNanos, ackNanos int64
						for done := 0; done < b.N; {
							ops := min(256, b.N-done)
							s := openOrderBenchStore(b, root, wb)
							var delivered *atomic.Int64
							if listeners {
								delivered = orderBenchSubscribe(s.db)
							}
							var acknowledged, conflicts, backlog atomic.Int64
							var wg sync.WaitGroup
							errs := make(chan error, 8)
							b.StartTimer()
							ackStart := time.Now()
							for worker := 0; worker < 8; worker++ {
								wg.Add(1)
								go func(worker int) {
									defer wg.Done()
									for i := worker; i < ops; i += 8 {
										id := s.ids[i%len(s.ids)]
										if contended {
											id = s.ids[0]
										}
										var err error
										for attempt := 0; attempt < 1000; attempt++ {
											if direct {
												err = s.db.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"cnt": int64(i + 1)}})
											} else {
												err = s.increment(id, false)
											}
											if err == nil {
												break
											}
											if !errors.Is(err, ErrConflict) && !errors.Is(err, badger.ErrConflict) {
												break
											}
											conflicts.Add(1)
										}
										if err != nil {
											errs <- err
											return
										}
										ack := acknowledged.Add(1)
										if delivered != nil {
											n := ack - delivered.Load()
											for p := backlog.Load(); n > p && !backlog.CompareAndSwap(p, n); p = backlog.Load() {
											}
										}
									}
								}(worker)
							}
							wg.Wait()
							ackNanos += time.Since(ackStart).Nanoseconds()
							start := time.Now()
							if err := s.db.FlushWriteBehind(); err != nil {
								b.Fatal(err)
							}
							drainNanos += time.Since(start).Nanoseconds()
							b.StopTimer()
							close(errs)
							for err := range errs {
								if err != nil {
									b.Fatal(err)
								}
							}
							if delivered != nil && delivered.Load() != int64(ops) {
								b.Fatalf("events %d, successful writes %d", delivered.Load(), ops)
							}
							if !direct {
								s.verifyCounters(int64(ops))
							}
							if count, err := s.db.NodeCount(); err != nil || count != orderBenchNodes {
								b.Fatalf("stored count=%d err=%v", count, err)
							}
							retries += conflicts.Load()
							if backlog.Load() > peak {
								peak = backlog.Load()
							}
							s.close()
							done += ops
						}
						b.ReportMetric(float64(ackNanos)/float64(b.N), "ack-ns/op")
						b.ReportMetric(float64(retries)/float64(b.N), "conflicts/op")
						b.ReportMetric(float64(drainNanos)/float64(b.N), "drain-ns/op")
						b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "completion-ns/op")
						b.ReportMetric(float64(peak), "pending-observed-peak")
					})
				}
			}
		}
	}
}
