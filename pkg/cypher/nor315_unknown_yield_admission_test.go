package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An invalid output name is a declaration error. EXPLAIN must reject it, and
// execution modes must reject it before calling the registered handler.
func TestNOR315UnknownYieldRejectedBeforeInvocationInEveryMode(t *testing.T) {
	const name = "nor315.yieldAdmission"
	calls := nor315Register(t, name, nor315EmptyHandler)
	forEachParser(t, func(t *testing.T) {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+"registered procedure", func(t *testing.T) {
				exec := newCallScopeExecutor(t)
				before := calls.Load()
				_, err := exec.Execute(t.Context(), "CALL "+name+"() YIELD value RETURN value", nil)
				require.NoError(t, err)
				require.Greater(t, calls.Load(), before, "the valid handler control really executes")
				before = calls.Load()
				result, err := exec.Execute(t.Context(), mode+"CALL "+name+"() YIELD missing RETURN missing", nil)
				require.Error(t, err, "every mode must reject an output absent from the declaration")
				require.Nil(t, result)
				require.Equal(t, before, calls.Load(), "invalid YIELD must be rejected before executing the handler")
			})
			t.Run(mode+"built in procedure", func(t *testing.T) {
				exec := newCallScopeExecutor(t)
				result, err := exec.Execute(t.Context(), mode+"CALL db.labels() YIELD missing RETURN missing", nil)
				require.Error(t, err)
				require.Nil(t, result)
			})
		}
	})
}
