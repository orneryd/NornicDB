package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFilteredCountReadsComplexProjectedPropertiesInEveryTransactionMode(t *testing.T) {
	tests := []struct {
		name      string
		predicate string
		want      int64
	}{
		{name: "list subscript", predicate: "node.tags[0] = 't0'", want: 2},
		{name: "list head", predicate: "head(node.tags) = 't0'", want: 2},
		{name: "numeric list subscript", predicate: "node.nums[0] = 1", want: 1},
		{name: "temporal comparison", predicate: "node.ts >= datetime('2026-09-13T00:00:00Z')", want: 2},
		{name: "temporal accessor", predicate: "node.ts.year = 2026", want: 4},
	}

	for _, mode := range []struct {
		name     string
		explicit bool
	}{
		{name: "autocommit"},
		{name: "explicit transaction", explicit: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "filtered_count_complex"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, `UNWIND range(1, 4) AS i
CREATE (:FCProbe {
  id: i,
  tags: ['t' + toString(i % 2), 'x'],
  nums: [i, i + 1],
  ts: datetime('2026-09-1' + toString(i) + 'T10:00:00Z')
})`, nil)
			require.NoError(t, err)
			if mode.explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					matching, err := exec.Execute(ctx, "MATCH (node:FCProbe) WHERE "+test.predicate+" RETURN node.id AS id", nil)
					require.NoError(t, err)
					require.Len(t, matching.Rows, int(test.want), "the non-aggregate filter is the semantic control")

					result, err := exec.Execute(ctx, "MATCH (node:FCProbe) WHERE "+test.predicate+" RETURN count(node) AS count", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
				})
			}
		})
	}
}
