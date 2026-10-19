package storage

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// The write path's latency with concurrent writers (F-PUBLISH): every
// commit's publication runs in one ordered stage, and a writer returns once
// the stage has passed its own commit, so a writer can wait for the
// publications of the commits ahead of it. Each benchmark reports
// throughput and the p50 and p99 time for a write to return.

func newPublicationBenchEngine(b *testing.B) *BadgerEngine {
	b.Helper()
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	sm := engine.GetSchemaForNamespace("db")
	if err := sm.AddPropertyIndex("bench_l_p", "L", []string{"p"}); err != nil {
		b.Fatal(err)
	}
	if err := sm.AddUniqueConstraint("bench_l_u", "L", "u"); err != nil {
		b.Fatal(err)
	}
	sm.MarkPropertyIndexesFilled()
	if err := RefreshUniqueConstraintValuesForEngine(engine, sm); err != nil {
		b.Fatal(err)
	}
	return engine
}

func publicationBenchNode(i int, value string) *Node {
	return &Node{ID: NodeID(fmt.Sprintf("db:n%d", i)), Labels: []string{"L"}, Properties: map[string]any{"p": value, "u": fmt.Sprintf("u%d", i)}}
}

// reportLatencies reports p50 and p99 of latencies and the writes per second.
func reportLatencies(b *testing.B, latencies []time.Duration, elapsed time.Duration) {
	b.Helper()
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	at := func(q float64) float64 { return float64(latencies[int(q*float64(len(latencies)-1))].Nanoseconds()) }
	b.ReportMetric(at(0.50), "p50-ns")
	b.ReportMetric(at(0.99), "p99-ns")
	b.ReportMetric(float64(len(latencies))/elapsed.Seconds(), "writes/s")
}

// updateRetrying updates node until it commits; a write-write conflict with
// a concurrent writer of the same node is retried, as a client would.
func updateRetrying(engine *BadgerEngine, node *Node) error {
	for {
		err := engine.UpdateNode(node)
		if err == nil || !(errors.Is(err, ErrConflict) || errors.Is(err, badger.ErrConflict)) {
			return err
		}
	}
}

func BenchmarkPublicationConcurrentWriters(b *testing.B) {
	const population, writers = 1000, 8
	for _, mode := range []struct {
		name string
		hot  int // nodes the writers share; 0 means each write picks its own node
	}{{"distinct", 0}, {"overlapping", 16}} {
		b.Run(fmt.Sprintf("%s/writers=%d", mode.name, writers), func(b *testing.B) {
			engine := newPublicationBenchEngine(b)
			for i := 0; i < population; i++ {
				if _, err := engine.CreateNode(publicationBenchNode(i, "v0")); err != nil {
					b.Fatal(err)
				}
			}
			latencies := make([][]time.Duration, writers)
			var next atomic.Int64
			var failed atomic.Value
			b.ReportAllocs()
			b.ResetTimer()
			start := time.Now()
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for {
						i := int(next.Add(1) - 1)
						if i >= b.N {
							return
						}
						id := i % population
						if mode.hot > 0 {
							id = i % mode.hot
						}
						began := time.Now()
						if err := updateRetrying(engine, publicationBenchNode(id, fmt.Sprintf("v%d", i))); err != nil {
							failed.Store(err)
							return
						}
						latencies[w] = append(latencies[w], time.Since(began))
					}
				}(w)
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()
			if err, _ := failed.Load().(error); err != nil {
				b.Fatal(err)
			}
			var all []time.Duration
			for _, l := range latencies {
				all = append(all, l...)
			}
			reportLatencies(b, all, elapsed)
		})
	}
}

// Small writes while 10,000-node transactions commit alongside: the
// head-of-line cost of a large commit's publication for the writes behind
// it.
func BenchmarkPublicationLargeCommitThenSmallWrites(b *testing.B) {
	const population, large = 1000, 10000
	engine := newPublicationBenchEngine(b)
	for i := 0; i < population; i++ {
		if _, err := engine.CreateNode(publicationBenchNode(i, "v0")); err != nil {
			b.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var bulkErr atomic.Value
	var bulkDone sync.WaitGroup
	bulkDone.Add(1)
	go func() {
		defer bulkDone.Done()
		for round := 0; ; round++ {
			select {
			case <-stop:
				return
			default:
			}
			tx, err := engine.BeginTransaction()
			if err != nil {
				bulkErr.Store(err)
				return
			}
			for j := 0; j < large; j++ {
				if _, err := tx.CreateNode(publicationBenchNode(population+round*large+j, "bulk")); err != nil {
					_ = tx.Rollback()
					bulkErr.Store(err)
					return
				}
			}
			if err := tx.Commit(); err != nil {
				bulkErr.Store(err)
				return
			}
		}
	}()
	latencies := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		began := time.Now()
		if err := updateRetrying(engine, publicationBenchNode(i%population, fmt.Sprintf("v%d", i))); err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(began))
	}
	elapsed := time.Since(start)
	b.StopTimer()
	close(stop)
	bulkDone.Wait()
	if err, _ := bulkErr.Load().(error); err != nil {
		b.Fatal(err)
	}
	reportLatencies(b, latencies, elapsed)
}
