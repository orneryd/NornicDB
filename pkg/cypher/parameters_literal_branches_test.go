package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementMissingParametersAfterCRLineComment(t *testing.T) {
	for name, statement := range map[string]string{
		"CR":      "RETURN 1 // $ignored\rRETURN $real AS value",
		"LF":      "RETURN 1 // $ignored\nRETURN $real AS value",
		"CRLF":    "RETURN 1 // $ignored\r\nRETURN $real AS value",
		"block":   "RETURN 1 /* $ignored */ + $real AS value",
		"literal": "RETURN '$ignored' AS literal, $real AS value",
	} {
		t.Run(name, func(t *testing.T) {
			missing := statementMissingParameters(statement, func(string) bool { return false })
			require.Equal(t, []string{"real"}, missing)
		})
	}
}

type customLiteral struct{ s string }

func (c customLiteral) String() string { return c.s }
