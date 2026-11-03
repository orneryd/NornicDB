package cypher

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

// Expectations were captured from both original entry points before migration.
// Scalar leaves deliberately reject parenthesized and compound expressions;
// literal-led source admission keeps the existing broader operand contract.
func TestStaticLiteralPolicies(t *testing.T) {
	raw, err := os.ReadFile("testdata/literal_type_policies.json")
	require.NoError(t, err)
	var rows []struct {
		Input  string `json:"input"`
		Scalar string `json:"scalar"`
		Led    string `json:"led"`
	}
	require.NoError(t, json.Unmarshal(raw, &rows))
	require.Len(t, rows, 41)
	for _, row := range rows {
		t.Run(row.Input, func(t *testing.T) {
			require.Equal(t, row.Scalar, staticLiteralType(t.Context(), row.Input, scalarLiteralOnly), "scalar leaf")
			require.Equal(t, row.Led, staticLiteralType(t.Context(), row.Input, literalLedExpression), "literal-led source")
		})
	}
}

var literalTypeBenchmarkResult string

func BenchmarkStaticLiteralPolicies(b *testing.B) {
	for _, scalar := range []bool{true, false} {
		mode := "led"
		if scalar {
			mode = "scalar"
		}
		for _, input := range []string{"42", "(42)", "'x'", "1+2", "$p", "[1,2]"} {
			b.Run(mode+"/"+input, func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if scalar {
						literalTypeBenchmarkResult = staticLiteralType(b.Context(), input, scalarLiteralOnly)
					} else {
						literalTypeBenchmarkResult = staticLiteralType(b.Context(), input, literalLedExpression)
					}
				}
			})
		}
	}
}
