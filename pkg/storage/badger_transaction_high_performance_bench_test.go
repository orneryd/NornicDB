package storage

import (
	"fmt"
	"sync"
	"testing"
)

// BenchmarkHighPerformanceDisjointEdgeUpdates measures real parallel updates in
// the on-disk default server storage mode. Each worker owns an edge but shares
// its endpoints and namespace with the other workers. Setup and truth checks
// are outside the timer; unexpected conflicts fail instead of being retried.
func BenchmarkHighPerformanceDisjointEdgeUpdates(b *testing.B) {
	const workers = 4
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), HighPerformance: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := engine.Close(); err != nil {
			b.Error(err)
		}
	})
	for _, id := range []NodeID{"bench:from", "bench:to"} {
		if _, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Node"}}); err != nil {
			b.Fatal(err)
		}
	}
	var ids [workers]EdgeID
	for worker := range ids {
		ids[worker] = EdgeID(fmt.Sprintf("bench:edge-%d", worker))
		if err := engine.CreateEdge(&Edge{ID: ids[worker], StartNode: "bench:from", EndNode: "bench:to", Type: "LINKS", Properties: map[string]interface{}{"round": int64(-1)}}); err != nil {
			b.Fatal(err)
		}
	}
	var failures [workers]error
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for worker := range ids {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := worker; round < b.N; round += workers {
				tx, err := engine.BeginTransaction()
				if err != nil {
					failures[worker] = err
					return
				}
				edge, err := tx.GetEdge(ids[worker])
				if err == nil {
					edge.Properties["round"] = int64(round)
					err = tx.UpdateEdge(edge)
				}
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
				if err != nil {
					failures[worker] = err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	b.StopTimer()
	for worker, err := range failures {
		if err != nil {
			b.Fatalf("worker%d: %v", worker, err)
		}
	}
	reader, err := engine.BeginTransaction()
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Rollback()
	for worker, id := range ids {
		edge, err := reader.GetEdge(id)
		if err != nil {
			b.Fatal(err)
		}
		want := -1
		if worker < b.N {
			want = worker + (b.N-1-worker)/workers*workers
		}
		if fmt.Sprint(edge.Properties["round"]) != fmt.Sprint(want) {
			b.Fatalf("edge%s finalround=%v want%d", id, edge.Properties["round"], want)
		}
		if edge.StartNode != "bench:from" || edge.EndNode != "bench:to" || edge.Type != "LINKS" {
			b.Fatalf("edge identity changed: %+v", edge)
		}
	}
}
