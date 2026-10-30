package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH467_NestedMapAccessAndKeysAfterWith(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		params map[string]interface{}
		want   [][]interface{}
	}{
		{
			name:  "nested map property",
			query: "WITH {a: 1, b: {c: 2}} AS m RETURN m.a AS a, m.b.c AS c",
			want:  [][]interface{}{{int64(1), int64(2)}},
		},
		{
			name:  "map keys and nested map",
			query: "WITH {a: 1, b: {c: 2}} AS m RETURN keys(m) AS k, m.b AS b",
			want:  [][]interface{}{{[]interface{}{"a", "b"}, map[string]interface{}{"c": int64(2)}}},
		},
		{
			name:   "parameter nested map control",
			query:  "RETURN $m.b.c AS c",
			params: map[string]interface{}{"m": map[string]interface{}{"b": map[string]interface{}{"c": int64(2)}}},
			want:   [][]interface{}{{int64(2)}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh467"))
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			for _, testCase := range cases {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, testCase.query, testCase.params)
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
