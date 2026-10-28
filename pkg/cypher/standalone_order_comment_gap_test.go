package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestStandaloneOrderCommentGapPublic(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, gap := range []string{"/*c*/", " //c\n"} {
		for _, tc := range []struct{ name, query, want string }{
			{"standalone", "UNWIND [7] AS call ORDER" + gap + "BY call DESC SKIP 0 LIMIT 1 RETURN call", "UNWIND [7] AS call WITH * ORDER" + gap + "BY call DESC SKIP 0 LIMIT 1 RETURN call"},
			{"projection", "UNWIND [7] AS call RETURN call ORDER" + gap + "BY call DESC SKIP 0 LIMIT 1", "UNWIND [7] AS call RETURN call ORDER" + gap + "BY call DESC SKIP 0 LIMIT 1"},
		} {
			t.Run(fmt.Sprintf("%s/%q", tc.name, gap), func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, []string{"call"}, result.Columns, tc.query)
				require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, tc.query)
				got, rewrite := desugarStandaloneOrderClauses(tc.query)
				require.Equal(t, tc.want, got, tc.query)
				if tc.name == "projection" {
					require.Nil(t, rewrite)
					allocs := testing.AllocsPerRun(100, func() {
						if got, rewrite := desugarStandaloneOrderClauses(tc.query); got != tc.query || rewrite != nil {
							panic("unexpected comment order rewrite")
						}
					})
					require.Zero(t, allocs)
				}
			})
		}
	}
}

func TestStandaloneOrderCompoundAdmissionBounds(t *testing.T) {
	for _, tc := range []struct {
		text      string
		end, want int
	}{
		{"ORDER/*c*/BY x", len("ORDER/*c*/BY x"), standaloneStageOrder},
		{"ORDER //c\nBY x", len("ORDER //c\nBY x"), standaloneStageOrder},
		{"ORDER/*c*/BY x", len("ORDER/*c*/B"), standaloneStageItems},
		{"ORDER/*c*/BYthing", len("ORDER/*c*/BYthing"), standaloneStageItems},
		{"ORDER/*unfinished", len("ORDER/*unfinished"), standaloneStageItems},
		{"ORDER // BY", len("ORDER // BY"), standaloneStageItems},
	} {
		require.Equal(t, tc.want, standaloneClauseKind(tc.text, "ORDER", len("ORDER"), tc.end), tc.text)
	}
}
