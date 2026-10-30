package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH468_CollectedListSubscriptAndSlice(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "inline collected-list slice",
			query: "MATCH (i:I) WITH i ORDER BY i.sku RETURN collect(i.sku)[0..2] AS firsttwo",
			want:  [][]interface{}{{[]interface{}{"a1", "b2"}}},
		},
		{
			name:  "with collected-list slice and subscript",
			query: "MATCH (i:I) WITH i ORDER BY i.sku WITH collect(i.sku) AS l RETURN l[0..2] AS firsttwo, l[0] AS first",
			want:  [][]interface{}{{[]interface{}{"a1", "b2"}, "a1"}},
		},
		{
			name:  "literal slice control",
			query: "RETURN [1,2,3,4][1..3] AS sl",
			want:  [][]interface{}{{[]interface{}{int64(2), int64(3)}}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh468")
			for index, sku := range []string{"a1", "b2", "c3"} {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(sku), Labels: []string{"I"},
					Properties: map[string]interface{}{"sku": sku, "price": float64(index) + 1.25, "qty": int64(index)},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			for _, testCase := range cases {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, testCase.query, nil)
					require.NoError(t, err)
					require.Equal(t, testCase.want, result.Rows)
				})
			}

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
