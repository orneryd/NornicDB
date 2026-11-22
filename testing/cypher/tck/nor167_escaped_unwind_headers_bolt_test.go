package tck

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNOR167EscapedUnwindHeadersBolt checks the three source-escaped alias
// forms over Bolt, in autocommit and explicit transactions, on both parsers.
func TestNOR167EscapedUnwindHeadersBolt(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			driver, shutdown := startConformanceServer(t)
			defer shutdown()
			for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := t.Context()
					backend := newDifferentialBackend(t, driver, "nornic", mode)
					require.NoError(t, backend.Reset(ctx))
					escapedIdentifier := `a\u0062`
					for _, tc := range []struct {
						query string
						rows  [][]any
					}{
						{"UNWIND ['value'] AS `" + escapedIdentifier + "` RETURN `" + escapedIdentifier + "`", [][]any{{"value"}}},
						{"UNWIND [] AS `" + escapedIdentifier + "` RETURN `" + escapedIdentifier + "`", nil},
						{"RETURN 1 AS `" + escapedIdentifier + "`", [][]any{{int64(1)}}},
					} {
						assert.True(t, strings.Contains(tc.query, `\u0062`), "query must contain the literal Unicode escape: %q", tc.query)
						result, err := backend.Execute(ctx, tc.query, nil)
						if !assert.NoError(t, err, tc.query) {
							continue
						}
						assert.Equal(t, []string{"ab"}, result.Columns, tc.query)
						if tc.rows == nil {
							assert.Empty(t, result.Rows, tc.query)
						} else {
							assert.Equal(t, tc.rows, result.Rows, tc.query)
						}
						t.Logf("statement=%q columns=%v rows=%v", tc.query, result.Columns, result.Rows)
					}
				})
			}
		})
	}
}
