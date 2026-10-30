package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Arity is a declared signature rule; empty input cannot suppress admission.
func TestCanonicalBoundProcedureArity(t *testing.T) {
	for _, prefix := range []string{"", "UNWIND [] AS ignored ", "UNWIND [0] AS ignored "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+prefix, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, mode+prefix+"CALL db.labels(1) YIELD label RETURN label", nil)
				require.Error(t, err)
				code, detail, message := boundRuleError(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
				require.Equal(t, "InvalidNumberOfArguments", detail)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError: procedure db.labels accepts at most 0 arguments, got 1", message)
				require.Nil(t, result)
			})
		}
	}
}
