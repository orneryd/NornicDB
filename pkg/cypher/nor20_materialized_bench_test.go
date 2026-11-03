package cypher

// NOR20 materialized cancellation and ordering workload benchmarks.
//
// BenchmarkNor20OrderPipelineRowsWorkloads measures the materialized ORDER BY
// owner on the four scan orders a statement produces: already in key order,
// reverse, pseudo-random and tie-heavy, at a small and a large row count.
// Each workload measures the production owner. Prior implementations and
// rejected scratch-buffer experiments are retained outside the checkout as
// task evidence rather than as parallel implementations in the test suite.
//
// BenchmarkNor20CandidateScanMaterialized is the projected label scan the
// profile measured as the longest uninterruptible stage: it reports the cost
// of the visitor's per-node caller-cancellation observation on the healthy
// path.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// nor20OrderTerms is the ORDER BY of the profiled workload: three constant tie
// terms and one unique rank term, so the owner sorts on the last key.
func nor20OrderTerms() []orderByTerm {
	return parseOrderByTerms(context.Background(), "ORDER BY n.tie1, n.tie2, n.tie3, n.rank")
}

// nor20WorkloadRows builds one scan order per workload. payload records the
// scan position, so an ordered row's payload witnesses the scan order of equal
// keys; the tied workload gives most rows the same full key.
func nor20WorkloadRows(workload string, size int) ([]pipelineRow, []pipelineRow) {
	rows := make([]pipelineRow, 0, size)
	scopes := make([]pipelineRow, 0, size)
	order := make([]int, size)
	for index := range order {
		order[index] = index
	}
	switch workload {
	case "sorted":
	case "reverse":
		for index := range order {
			order[index] = size - 1 - index
		}
	case "random":
		state := uint64(1)
		for index := size - 1; index > 0; index-- {
			state = state*6364136223846793005 + 1442695040888963407
			other := int((state >> 33) % uint64(index+1))
			order[index], order[other] = order[other], order[index]
		}
	}
	for index := 0; index < size; index++ {
		row := pipelineRow{"payload": nor20Payload(index)}
		if workload == "tied" {
			row["n.tie1"] = int64(index % 5)
			row["n.tie2"] = int64(index % 3)
			row["n.tie3"] = int64(0)
			row["n.rank"] = int64(index % 2)
		} else {
			row["n.tie1"] = int64(0)
			row["n.tie2"] = int64(0)
			row["n.tie3"] = int64(0)
			row["n.rank"] = int64(order[index])
		}
		rows = append(rows, row)
		scopes = append(scopes, row)
	}
	return rows, scopes
}

func nor20RequireWorkloadOrder(b *testing.B, terms []orderByTerm, input, output []pipelineRow) {
	b.Helper()
	position := make(map[string]int, len(input))
	for index, row := range input {
		position[row["payload"].(string)] = index
	}
	for index := 1; index < len(output); index++ {
		comparison := nor20CompareTerms(terms, output[index-1], output[index])
		if comparison > 0 {
			b.Fatalf("rows %d and %d are not in key order", index-1, index)
		}
		if comparison == 0 {
			previous := position[output[index-1]["payload"].(string)]
			current := position[output[index]["payload"].(string)]
			if previous >= current {
				b.Fatalf("equal keys at rows %d and %d lost their scan order", index-1, index)
			}
		}
	}
}

func BenchmarkNor20OrderPipelineRowsWorkloads(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "nor20-bench"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	terms := nor20OrderTerms()
	ctx := context.Background()
	for _, size := range []int{1000, 100000} {
		for _, workload := range []string{"sorted", "reverse", "random", "tied"} {
			rows, scopes := nor20WorkloadRows(workload, size)
			b.Run(fmt.Sprintf("%s/rows_%d", workload, size), func(b *testing.B) {
				control := append([]pipelineRow(nil), rows...)
				if !exec.orderPipelineRowsWithScopes(ctx, control, scopes, terms) {
					b.Fatal("ordering declined the workload")
				}
				nor20RequireWorkloadOrder(b, terms, rows, control)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					work := append([]pipelineRow(nil), rows...)
					if !exec.orderPipelineRowsWithScopes(ctx, work, scopes, terms) {
						b.Fatal("ordering declined the workload")
					}
				}
			})
		}
	}
}

func BenchmarkNor20CandidateScanMaterialized(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "nor20-bench")
	for index := 0; index < 20000; index++ {
		_, err := store.CreateNode(b.Context(), &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("nor20-bench-%06d", index)),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"rank":    int64(index),
				"payload": nor20Payload(index),
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := exec.collectNodesWithStreamingProjection(ctx, []string{"Item"}, nil, "n", "", -1, []string{"payload", "rank"})
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != 20000 {
			b.Fatalf("collected %d nodes", len(nodes))
		}
	}
}
