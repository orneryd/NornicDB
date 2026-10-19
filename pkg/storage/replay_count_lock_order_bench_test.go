package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Measure the full ACK/replay pair with ordinary and multi-batch generations.
// Total-count readers exercise the longer publication boundary without running
// a synchronous writer that would deadlock the unfixed control.
func BenchmarkReplayCountLockOrder(b *testing.B) {
	for _, size := range []int{1, 3000} {
		for _, readers := range []int{0, 4} {
			b.Run(fmt.Sprintf("nodes=%d/total_readers=%d", size, readers), func(b *testing.B) {
				e, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = e.Close() })
				pad := strings.Repeat("x", 700)
				initial, err := e.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < size; i++ {
					if _, err := initial.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"pad": pad}}); err != nil {
						b.Fatal(err)
					}
				}
				if err := initial.CreateEdge(&Edge{ID: "test:r", StartNode: "test:n0", EndNode: "test:n0", Type: "LINK0"}); err != nil {
					b.Fatal(err)
				}
				if err := initial.Commit(); err != nil {
					b.Fatal(err)
				}
				stop := make(chan struct{})
				var wg sync.WaitGroup
				readerNanos := make([]int64, readers)
				readerCalls := make([]int64, readers)
				for i := 0; i < readers; i++ {
					wg.Add(1)
					go func(reader int) {
						defer wg.Done()
						for {
							select {
							case <-stop:
								return
							default:
								start := time.Now()
								if _, err := e.NodeCount(); err != nil {
									b.Error(err)
									return
								}
								readerNanos[reader] += time.Since(start).Nanoseconds()
								readerCalls[reader]++
							}
						}
					}(i)
				}
				defer func() {
					close(stop)
					wg.Wait()
					var nanos, calls int64
					for i := range readerCalls {
						nanos += readerNanos[i]
						calls += readerCalls[i]
					}
					if calls != 0 {
						b.ReportMetric(float64(nanos)/float64(calls), "ns/read")
						b.ReportMetric(float64(calls)/float64(b.N), "reads/op")
					}
				}()
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					tx, err := e.BeginTransaction()
					if err != nil {
						b.Fatal(err)
					}
					if err := tx.SetImplicit(true); err != nil {
						b.Fatal(err)
					}
					for i := 0; i < size; i++ {
						if err := tx.UpdateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"pad": pad, "revision": iteration}}); err != nil {
							b.Fatal(err)
						}
					}
					if err := tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:n0", EndNode: "test:n0", Type: fmt.Sprintf("LINK%d", (iteration+1)%2)}); err != nil {
						b.Fatal(err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatal(err)
					}
					if err := e.FlushWriteBehind(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
