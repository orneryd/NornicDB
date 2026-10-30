package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH453_ListComprehensionWhereAndProjectionCases(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "gh-453-filter-then-project-range",
			query: "RETURN [x IN range(1,5) WHERE x % 2 = 1 | x * 10] AS l",
			want:  [][]interface{}{{[]interface{}{int64(10), int64(30), int64(50)}}},
		},
		{
			name:  "gh-453-filter-range-without-projection",
			query: "RETURN [x IN range(1,5) WHERE x % 2 = 1] AS l",
			want:  [][]interface{}{{[]interface{}{int64(1), int64(3), int64(5)}}},
		},
		{
			name:  "gh-453-filter-literal-list-then-project",
			query: "RETURN [x IN [1,2,3] WHERE x > 1 | x * 10] AS l",
			want:  [][]interface{}{{[]interface{}{int64(20), int64(30)}}},
		},
		{
			name:  "gh-453-projection-only-control",
			query: "RETURN [x IN range(1,5) | x * 10] AS l",
			want:  [][]interface{}{{[]interface{}{int64(10), int64(20), int64(30), int64(40), int64(50)}}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh453"))
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
