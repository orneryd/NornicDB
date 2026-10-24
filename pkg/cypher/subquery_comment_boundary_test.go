package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSubqueryCommentBoundaryWithNormalizationSetting(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(enabled)
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
					exec, _ := newTestExecutor(t)
					if explicit {
						base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, base.Close()) })
						tx, err := base.BeginTransaction()
						require.NoError(t, err)
						t.Cleanup(func() { _ = tx.Rollback() })
						exec = NewStorageExecutor(&transactionStorageWrapper{
							tx: tx, underlying: base, separator: ":",
							mutatedNodeIDs: make(map[string]struct{}),
						})
					}
					for _, gap := range []string{" ", " /*gap*/ ", " //gap\n ", "/* first */ /* second */"} {
						for _, tc := range []struct {
							kind, body string
							want       interface{}
						}{
							{"COUNT", "LIMIT 1 RETURN 1 AS x", int64(1)},
							{"EXISTS", "RETURN 1 AS x", true},
							{"COLLECT", "RETURN 1 AS x", []interface{}{int64(1)}},
						} {
							query := fmt.Sprintf("RETURN (%s%s{ %s }) AS v", tc.kind, gap, tc.body)
							t.Run(query, func(t *testing.T) {
								result, err := exec.Execute(context.Background(), query, nil)
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
							})
						}
					}
				})
			}
		})
	}
}

func TestSubqueryCommentBoundaryLexicalControls(t *testing.T) {
	for _, expr := range []string{"COUNT /*gap*/ { RETURN 1 AS x }", "COUNT //gap\n { RETURN 1 AS x }", "COUNT /* ' // */ { RETURN 1 AS x }"} {
		found := findSubqueryExpressions(expr)
		require.Len(t, found, 1, expr)
		require.Equal(t, "COUNT", found[0].kind)
	}
	for _, expr := range []string{"'COUNT /*gap*/ { RETURN 1 AS x }'", "`COUNT` /*gap*/ { RETURN 1 AS x }", "n:COUNT /*gap*/ { RETURN 1 AS x }", "COUNT /*gap*/ {.a}", "'//' + n:COUNT /*gap*/ { RETURN 1 AS x }"} {
		require.Empty(t, findSubqueryExpressions(expr), expr)
	}
}
