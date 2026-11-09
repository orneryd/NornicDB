package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwindRewriteUsesStatementEffects(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		for _, tc := range []struct {
			query    string
			eligible bool
		}{
			{"MATCH (n:Doc) RETURN count(n) AS c", true},
			{"MATCH (n:Doc) CALL kp.suppress(n) RETURN count(n) AS c", false},
			{"MATCH (n:Doc) CALL effect_fixture.unknown() RETURN count(n) AS c", false},
		} {
			t.Run(tc.query, func(t *testing.T) {
				eligible, err := exec.canApplySetBasedUnwindRewrite(t.Context(), tc.query, []interface{}{int64(1), int64(2)})
				require.NoError(t, err)
				require.Equal(t, tc.eligible, eligible)
			})
		}
	})
}
