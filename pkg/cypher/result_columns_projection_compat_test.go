package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The semantic callers retain their source-name contract while execution's
// projection plan is the only reader. Names remain case-sensitive and quoted
// aliases are decoded by the same canonical alias owner as execution.
func TestResultHeaderSemanticProjectionCompatibility(t *testing.T) {
	for _, tc := range []struct {
		clause string
		names  []string
	}{
		{"RETURN *", nil},
		{"RETURN DISTINCT * ORDER BY x SKIP 1 LIMIT 2", nil},
		{"RETURN *, x AS y", []string{"*", "y"}},
		{"RETURN DISTINCT *, x AS `Kept Name` ORDER BY x SKIP 1 LIMIT 2", []string{"*", "Kept Name"}},
		{"RETURN x AS MiXeD, x + 1", []string{"MiXeD", "x + 1"}},
		{"RETURN x AS `Kept Name`", []string{"Kept Name"}},
		{"RETURN x AS `tick``Name`", []string{"tick`Name"}},
		{"RETURN `MiXeD Name`", []string{"MiXeD Name"}},
		{"RETURN DISTINCT $age + avg(person.age) - 1000 ORDER BY person.age SKIP 1 LIMIT 2", []string{"$age + avg(person.age) - 1000"}},
	} {
		t.Run(tc.clause, func(t *testing.T) {
			columns := pipelineReturnSourceColumns(t.Context(), tc.clause)
			if tc.names == nil {
				require.Nil(t, columns)
				return
			}
			require.ElementsMatch(t, tc.names, columns)
		})
	}
}
