package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// internalCountMixedShape pairs the rows a pattern returns with the same
// pattern's count. The rows are the visible read's own answer, so a count that
// disagrees with len(rows) is the defect: the store counters count the hidden
// nodes and the relationships touching them, which no MATCH row contains.
type internalCountMixedShape struct {
	name  string
	rows  string
	count string
	want  int64
}

func internalCountMixedShapes() []internalCountMixedShape {
	return []internalCountMixedShape{
		{
			name:  "labelled node",
			rows:  "MATCH (n:Nor177Count) RETURN n.id AS v ORDER BY v",
			count: "MATCH (n:Nor177Count) RETURN count(n) AS v",
			want:  3,
		},
		{
			name:  "labelled node count star",
			rows:  "MATCH (n:Nor177Count) RETURN n.id AS v ORDER BY v",
			count: "MATCH (n:Nor177Count) RETURN count(*) AS v",
			want:  3,
		},
		{
			name:  "labelled start",
			rows:  "MATCH (a:Nor177Count)-[r:NOR177_LINKED]->() RETURN r.w AS v ORDER BY v",
			count: "MATCH (a:Nor177Count)-[r:NOR177_LINKED]->() RETURN count(r) AS v",
			want:  1,
		},
		{
			name:  "labelled end",
			rows:  "MATCH ()-[r:NOR177_LINKED]->(b:Nor177Count) RETURN r.w AS v ORDER BY v",
			count: "MATCH ()-[r:NOR177_LINKED]->(b:Nor177Count) RETURN count(r) AS v",
			want:  1,
		},
		{
			name:  "unlabelled endpoints",
			rows:  "MATCH ()-[r:NOR177_LINKED]->() RETURN r.w AS v ORDER BY v",
			count: "MATCH ()-[r:NOR177_LINKED]->() RETURN count(r) AS v",
			want:  1,
		},
		{
			name:  "labelled start count star",
			rows:  "MATCH (a:Nor177Count)-[r:NOR177_LINKED]->() RETURN r.w AS v ORDER BY v",
			count: "MATCH (a:Nor177Count)-[r:NOR177_LINKED]->() RETURN count(*) AS v",
			want:  1,
		},
		{
			name:  "undirected labelled start",
			rows:  "MATCH (a:Nor177Count)-[r:NOR177_LINKED]-(b) RETURN r.w AS v ORDER BY v",
			count: "MATCH (a:Nor177Count)-[r:NOR177_LINKED]-(b) RETURN count(r) AS v",
			want:  2,
		},
	}
}

// TestMixedInternalLabelCountsMatchVisibleRows is the NOR177 count contract for
// a scope whose nodes carry an ordinary and an internal label at once: every
// count shape answers the rows the same pattern returns, so a hidden node and
// both relationships touching it stay out of the number. Each parser
// configuration answers every shape.
func TestMixedInternalLabelCountsMatchVisibleRows(t *testing.T) {
	ctx := context.Background()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177mixed"))
			internalCountFixture(t, ctx, exec)

			for _, shape := range internalCountMixedShapes() {
				t.Run(shape.name, func(t *testing.T) {
					rows, err := exec.Execute(ctx, shape.rows, nil)
					require.NoError(t, err, shape.rows)
					require.Equal(t, int(shape.want), len(rows.Rows), shape.rows)

					result, err := exec.Execute(ctx, shape.count, nil)
					require.NoError(t, err, shape.count)
					require.Equal(t, [][]interface{}{{shape.want}}, result.Rows, shape.count)
				})
			}
		})
	}
}

// TestInternalLabelAbsentKeepsCounterRoute pins the other side of the
// eligibility proof: a user database that holds no internal-label node reads
// exactly what its counters count, so the count owners still answer from them.
// The proof is the label inventory, not a row scan, and it is per scope: the
// same queried label answers from the counters in a scope without internal
// labels and declines in one that has them.
func TestInternalLabelAbsentKeepsCounterRoute(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177plain"))
	for _, statement := range []string{
		`CREATE (:Nor177Plain {id: 1}), (:Nor177Plain {id: 2}), (:Nor177Plain {id: 3})`,
		`CREATE (:Nor177Plain {id: 4})-[:NOR177_PLAIN {w: 1}]->(:Nor177Plain {id: 5})`,
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}

	nodePattern, err := exec.parseNodePattern(ctx, "(n:Nor177Plain)")
	require.NoError(t, err)
	count, ok, err := exec.nodeCountFromStore(ctx, nodePattern, returnItem{expr: "count(n)"})
	require.NoError(t, err)
	require.True(t, ok, "no internal label in the scope: the counters describe the read")
	require.Equal(t, int64(5), count)

	matches := exec.parseTraversalPattern(ctx, "(:Nor177Plain)-[r:NOR177_PLAIN]->()")
	count, ok, err = exec.relationshipCountFromStore(ctx, matches, returnItem{expr: "count(r)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), count)

	// One internal-label node anywhere in the scope is enough to make the
	// counters count rows this read hides, so neither count owner answers.
	_, err = exec.Execute(ctx, "CREATE (:Nor177Plain:_internal {id: 6})", nil)
	require.NoError(t, err)
	_, ok, err = exec.nodeCountFromStore(ctx, nodePattern, returnItem{expr: "count(n)"})
	require.NoError(t, err)
	require.False(t, ok, "the counters no longer describe the read")
	_, ok, err = exec.relationshipCountFromStore(ctx, matches, returnItem{expr: "count(r)"})
	require.NoError(t, err)
	require.True(t, ok, "the hidden node has no relationship of the counted type")

	// The rows the read returns are unchanged by the eligibility decision.
	result, err := exec.Execute(ctx, "MATCH (n:Nor177Plain) RETURN count(n) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)
}

// TestTransactionStagedInternalLabelCountsVisibleRows pins the eligibility proof
// inside an explicit transaction: the counters the transaction reads are the
// committed ones plus its own staged deltas, so a staged node carrying an
// internal label is exactly as hidden from its rows as a committed one, and the
// count is left to those rows.
func TestTransactionStagedInternalLabelCountsVisibleRows(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	namespaced := storage.NewNamespacedEngine(base, "nor177tx")
	for i := 0; i < 3; i++ {
		_, err := namespaced.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("p%d", i)), Labels: []string{"Nor177TxNode"}})
		require.NoError(t, err)
	}

	tx, err := base.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("nor177tx"))
	txStore := &transactionStorageWrapper{
		tx:             tx,
		underlying:     namespaced,
		namespace:      "nor177tx",
		separator:      ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	exec := NewStorageExecutor(txStore)

	result, err := exec.Execute(ctx, "MATCH (n:Nor177TxNode) RETURN count(n) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)

	// The staged node carries the counted label and an internal one, so no row
	// of the pattern reads it whatever the counters add for it.
	_, err = txStore.CreateNode(ctx, &storage.Node{ID: storage.NodeID("nor177tx:hidden"), Labels: []string{"Nor177TxNode", "_internal"}})
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:Nor177TxNode) RETURN count(n) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	rows, err := exec.Execute(ctx, "MATCH (n:Nor177TxNode) RETURN n AS v", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows.Rows), "the staged hidden node is not a row either")
}

func TestUnrelatedInternalMetadataKeepsBufferedCountFastPath(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			fixture := openTypedCountScope(t, t.TempDir(), buffered)
			exec := fixture.admin
			_, err := exec.Execute(t.Context(), "CREATE (:VisibleCount {id:1})-[:VISIBLE_COUNT]->(:VisibleCount {id:2}), (:_Metadata)", nil)
			require.NoError(t, err)
			reader, ok := exec.storage.(storage.CountStoreReadEngine)
			require.True(t, ok)
			for _, request := range []storage.CountStoreReadRequest{
				{Kind: storage.CountStoreNodesByLabel, Name: "VisibleCount", HiddenLabelPrefix: "_"},
				{Kind: storage.CountStoreRelationshipsByType, Types: []string{"VISIBLE_COUNT"}, HiddenLabelPrefix: "_"},
			} {
				answer, err := reader.ReadCountStore(t.Context(), request)
				require.NoError(t, err)
				require.True(t, answer.DescribesRows, "unrelated metadata must not force a population scan")
			}
			_, err = exec.Execute(t.Context(), "MATCH (n:VisibleCount {id:1}) SET n:_internal", nil)
			require.NoError(t, err)
			for query, want := range map[string]int64{
				"MATCH (n:VisibleCount) RETURN count(n)":         1,
				"MATCH ()-[r:VISIBLE_COUNT]->() RETURN count(r)": 0,
			} {
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{want}}, result.Rows)
			}
		})
	}
}
