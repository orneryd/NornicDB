package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH452Comment_OrderedWithWhereCollect(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "bare UNWIND RETURN descending",
			query: "UNWIND [3,1,2] AS x RETURN x ORDER BY x DESC",
			want:  [][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
		},
		{
			name:  "bare UNWIND RETURN ascending",
			query: "UNWIND [3,1,2] AS x RETURN x ORDER BY x",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name:  "ordered WITH collect",
			query: "UNWIND [3,1,2] AS x WITH x ORDER BY x RETURN collect(x) AS l",
			want:  [][]interface{}{{[]interface{}{int64(1), int64(2), int64(3)}}},
		},
		{
			name: "comment ordered projection filter collect",
			query: `UNWIND [3, 1, 2] AS i
WITH i, i * i AS sq ORDER BY i
WITH i, sq WHERE sq > 1
RETURN collect(sq) AS values`,
			want: [][]interface{}{{[]interface{}{int64(4), int64(9)}}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh452")
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
