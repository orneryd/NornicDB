//go:build nornic_copycount

package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The explicit-transaction predicate scan (NOR-160): a row whose candidate
// holds only immutable values costs no copy; a row whose candidate holds a
// list costs exactly one, and rejected candidates cost none.
func TestNOR160PipelineScanCopiesOnlyRowsThatCanShare(t *testing.T) {
	for _, c := range []struct {
		name   string
		value  func(index int) any
		copies int
	}{
		{"immutable", func(index int) any { return int64(index) }, 0},
		{"list", func(index int) any { return []any{int64(index)} }, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scan"))
			const count = 8
			rows := make([]any, 0, count)
			for index := 0; index < count; index++ {
				rows = append(rows, map[string]any{"g": int64(index % 2), "v": c.value(index)})
			}
			_, err := exec.Execute(ctx, "UNWIND $rows AS row CREATE (n:P) SET n = row", map[string]any{"rows": rows})
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			read := func() [][]any {
				result, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.g = 0 RETURN n.v AS v", nil)
				require.NoError(t, err)
				return result.Rows
			}
			read() // fills the transaction's projected label cache

			var got [][]any
			nodes, _ := storage.CountStructuralCopies(func() { got = read() })
			require.Len(t, got, count/2)
			total := 0
			for id, copies := range nodes {
				require.Equal(t, c.copies, copies, "node %s", id)
				total += copies
			}
			require.Equal(t, c.copies*count/2, total, "copies: %v", nodes)
			for _, row := range got {
				require.NotEmpty(t, fmt.Sprint(row[0]))
			}
		})
	}
}
