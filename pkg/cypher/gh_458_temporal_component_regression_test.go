package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH458_TemporalComponentsRemainTypedThroughWith(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "date year and month",
			query: "RETURN date('2026-09-19').year AS y, date('2026-09-19').month AS m",
			want:  [][]interface{}{{int64(2026), int64(9)}},
		},
		{
			name:  "duration days",
			query: "RETURN duration({days: 2}).days AS dd",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name:  "date component after with",
			query: "WITH date('2026-09-19') AS dt RETURN dt.year AS y",
			want:  [][]interface{}{{int64(2026)}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh458"))
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
