package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH466_MapProjectionSelectorsAndComputedFields(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "selected properties",
			query: "MATCH (i:I {sku:'a1'}) RETURN i {.sku, .price} AS m",
			want:  [][]interface{}{{map[string]interface{}{"sku": "a1", "price": float64(10.5)}}},
		},
		{
			name:  "selected and computed property",
			query: "MATCH (i:I {sku:'a1'}) RETURN i {.sku, double: i.qty * 2} AS m",
			want:  [][]interface{}{{map[string]interface{}{"sku": "a1", "double": int64(6)}}},
		},
		{
			name:  "all properties",
			query: "MATCH (i:I {sku:'a1'}) RETURN i {.*} AS m",
			want:  [][]interface{}{{map[string]interface{}{"sku": "a1", "price": float64(10.5), "qty": int64(3)}}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh466")
			_, err := store.CreateNode(&storage.Node{
				ID: "item-a1", Labels: []string{"I"},
				Properties: map[string]interface{}{"sku": "a1", "price": float64(10.5), "qty": int64(3)},
			})
			require.NoError(t, err)
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
