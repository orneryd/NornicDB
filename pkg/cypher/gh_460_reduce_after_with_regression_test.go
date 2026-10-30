package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH460_ReduceAfterWithUsesLocalAndInheritedValues(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "standalone reduce control",
			query: "RETURN 1 AS a, reduce(s = 0, x IN [1,2,3] | s + x) AS r",
			want:  [][]interface{}{{int64(1), int64(6)}},
		},
		{
			name:  "reduce after collected WITH",
			query: "MATCH (p:P) WITH collect(p.name) AS names RETURN reduce(s = 0, x IN [1,2,3] | s + x) AS r",
			want:  [][]interface{}{{int64(6)}},
		},
		{
			name:  "aggregate and reduce after WITH",
			query: "MATCH (p:P) WITH collect(p.name) AS names RETURN size(names) AS n, reduce(s = 0, x IN [1,2,3] | s + x) AS r",
			want:  [][]interface{}{{int64(4), int64(6)}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh460")
			for index, name := range []string{"Ann", "Bob", "Cid", "Dee"} {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(name), Labels: []string{"P"},
					Properties: map[string]interface{}{"name": name, "ordinal": index},
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
