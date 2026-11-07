package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNormalizeArityPublicExecute(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{"RETURN normalize() AS v", "MATCH (n) RETURN normalize() AS v", "MATCH (n) WITH normalize() AS v RETURN v"} {
				t.Run(query, func(t *testing.T) {
					require.NotPanics(t, func() {
						_, err := setupTestExecutor(t).Execute(t.Context(), query, nil)
						require.ErrorContains(t, err, "Insufficient parameters for function 'normalize'")
						var diagnostic interface {
							BoltErrorCode() string
							BoltErrorDetail() string
						}
						require.ErrorAs(t, err, &diagnostic)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", diagnostic.BoltErrorCode())
						require.Equal(t, "FunctionParameterCount", diagnostic.BoltErrorDetail())
					})
				})
			}
			for _, query := range []string{"RETURN normalize('Å') AS v", "RETURN normalize('Å', NFC) AS v", "RETURN normalize('Å', NFKC) AS v"} {
				result, err := setupTestExecutor(t).Execute(t.Context(), query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{"Å"}}, result.Rows, query)
			}
		})
	}
}
