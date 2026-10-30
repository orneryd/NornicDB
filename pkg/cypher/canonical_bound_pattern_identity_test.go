package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). Entity-kind constraints consume the admitted
// pattern identity. A fresh bare-CALL declaration is distinct from a weak input;
// an explicit/legacy import reuses the existing declaration and its type.
func TestCanonicalBoundPatternIdentityInputs(t *testing.T) {
	for _, open := range []string{"CALL { ", "CALL (value) { ", "CALL (*) { ", "CALL { WITH value "} {
		for _, input := range []string{"WITH 7 AS value ", "WITH 7 AS value UNWIND [] AS noRows ", "WITH 7 AS value UNWIND [0] AS oneRow "} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(fmt.Sprintf("%s/%s/%s", open, input, mode), func(t *testing.T) {
					executor, ctx := newUnitExecutor(t)
					_, err := executor.Execute(ctx, "CREATE (:PatternIdentity)", nil)
					require.NoError(t, err)
					result, err := executor.Execute(ctx, mode+input+open+"MATCH (value:PatternIdentity) RETURN count(*) AS x } RETURN x", nil)
					if open != "CALL { " {
						require.Error(t, err, "a reused scalar ID is not a node")
						return
					}
					require.NoError(t, err)
					require.Equal(t, []string{"x"}, result.Columns)
					if mode == "EXPLAIN " || strings.Contains(input, "UNWIND []") {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					}
				})
			}
		}
	}
}
