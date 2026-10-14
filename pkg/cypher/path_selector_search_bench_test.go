package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newMentionGraphExecutor is the graph of #1212: (:Doc)-[:REL]->(:Mention)
// -[:REL]->(:Thing), 50 documents with 4 mentions each. Every document's
// first two mentions name the hub things h0 and h1; its third and fourth
// name the things o(i mod 20) and o((i+7) mod 20).
func newMentionGraphExecutor(tb testing.TB) (*StorageExecutor, context.Context) {
	tb.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(tb), "mention_graph"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, mentionGraphStatement(), nil)
	require.NoError(tb, err)
	return exec, ctx
}

// mentionGraphStatement creates the graph of newMentionGraphExecutor.
func mentionGraphStatement() string {
	var b strings.Builder
	b.WriteString("CREATE (h0:Thing {id: 'h0'}), (h1:Thing {id: 'h1'})")
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&b, ", (o%d:Thing {id: 'o%d'})", i, i)
	}
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&b, ", (d%d:Doc {id: 'd%d'})", i, i)
		things := []string{"h0", "h1", fmt.Sprintf("o%d", i%20), fmt.Sprintf("o%d", (i+7)%20)}
		for j, thing := range things {
			fmt.Fprintf(&b, ", (d%d)-[:REL]->(:Mention {id: 'm%d_%d'})-[:REL]->(%s)", i, i, j, thing)
		}
	}
	return b.String()
}

// newChainExecutor is a chain (:C {i: 0})-[:NEXT]->…->(:C {i: n}).
func newChainExecutor(tb testing.TB, n int) *StorageExecutor {
	tb.Helper()
	var b strings.Builder
	b.WriteString("CREATE (c0:C {i: 0})")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, ", (c%d:C {i: %d}), (c%d)-[:NEXT]->(c%d)", i, i, i-1, i)
	}
	return newStatementExecutor(tb, "chain", b.String())
}

// newGridExecutor is a side × side grid of (:G {x, y}) with a relationship
// (:E) from each node to its right and upper neighbours.
func newGridExecutor(tb testing.TB, side int) *StorageExecutor {
	tb.Helper()
	var b strings.Builder
	b.WriteString("CREATE ")
	for x := 0; x < side; x++ {
		for y := 0; y < side; y++ {
			if x > 0 || y > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "(g%d_%d:G {x: %d, y: %d})", x, y, x, y)
			if x > 0 {
				fmt.Fprintf(&b, ", (g%d_%d)-[:E]->(g%d_%d)", x-1, y, x, y)
			}
			if y > 0 {
				fmt.Fprintf(&b, ", (g%d_%d)-[:E]->(g%d_%d)", x, y-1, x, y)
			}
		}
	}
	return newStatementExecutor(tb, "grid", b.String())
}

// newStatementExecutor is an executor over a graph one statement creates.
func newStatementExecutor(tb testing.TB, namespace, statement string) *StorageExecutor {
	tb.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(tb), namespace))
	_, err := exec.Execute(context.Background(), statement, nil)
	require.NoError(tb, err)
	return exec
}

// The statements of #1212 on its graph, and a length-bounded SHORTEST over
// an unbounded quantified path pattern on a 150-step chain and a 40 × 40
// grid. An operation that takes longer than benchmarkSelectedPathBudget is
// skipped, so a build that doesn't finish them still runs the others.
func BenchmarkShortestUnboundedQuantified(b *testing.B) {
	mentions, ctx := newMentionGraphExecutor(b)
	chain, grid := newChainExecutor(b, 150), newGridExecutor(b, 40)
	cases := []struct {
		name  string
		exec  *StorageExecutor
		query string
	}{
		{"plus_length_le_1", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) WHERE length(p) <= 1 RETURN count(*)"},
		{"bounded_1_2", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = SHORTEST 1 (s)(()-[r:REL]-()){1,2}(t) RETURN count(*)"},
		{"bounded_1_4", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = SHORTEST 1 (s)(()-[r:REL]-()){1,4}(t) RETURN count(*)"},
		{"plus", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) RETURN count(*)"},
		{"plus_k2", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = SHORTEST 2 (s)(()-[r:REL]-())+(t) RETURN count(*)"},
		{"all_shortest", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = ALL SHORTEST (s)(()-[r:REL]-())+(t) RETURN count(*)"},
		{"plus_to_thing", mentions, "MATCH (s:Doc {id: 'd10'}), (t:Thing {id: 'o3'}) MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) RETURN count(*)"},
		{"varlength_plus", mentions, "MATCH (s:Doc {id: 'd10'}) MATCH p = SHORTEST 1 (s)-[:REL]-+(t) RETURN count(*)"},
		{"chain_depth_150", chain, "MATCH (s:C {i: 0}) MATCH p = SHORTEST 1 (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth RETURN count(*)"},
		{"chain_k2_depth_150", chain, "MATCH (s:C {i: 75}) MATCH p = SHORTEST 2 (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth RETURN count(*)"},
		{"grid_depth_6", grid, "MATCH (s:G {x: 0, y: 0}) MATCH p = SHORTEST 1 (s)(()-[:E]-())+(t) WHERE length(p) <= $depth RETURN count(*)"},
		{"grid_k2_depth_6", grid, "MATCH (s:G {x: 20, y: 20}) MATCH p = SHORTEST 2 (s)(()-[:E]-())+(t) WHERE length(p) <= $depth RETURN count(*)"},
		{"grid_directed_unbounded", grid, "MATCH (s:G {x: 0, y: 0}) MATCH p = SHORTEST 1 (s)(()-[:E]->())+(t) RETURN count(*)"},
	}
	for _, c := range cases {
		params := map[string]interface{}{"depth": int64(150)}
		if strings.HasPrefix(c.name, "grid") {
			params["depth"] = int64(6)
		}
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				deadline, cancel := context.WithTimeout(ctx, benchmarkSelectedPathBudget)
				_, err := c.exec.Execute(deadline, c.query, params)
				exceeded := errors.Is(deadline.Err(), context.DeadlineExceeded)
				cancel()
				if exceeded {
					b.Skipf("an operation took longer than %v", benchmarkSelectedPathBudget)
				}
				require.NoError(b, err)
			}
		})
	}
}

// benchmarkSelectedPathBudget is the longest one operation of
// BenchmarkShortestUnboundedQuantified may take.
const benchmarkSelectedPathBudget = 5 * time.Second
