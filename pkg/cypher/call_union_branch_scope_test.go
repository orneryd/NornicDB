package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCallUnionBranchLocalBindingsAfterNestedCall(t *testing.T) {
	for _, parser := range []struct {
		name  string
		enter func() func()
	}{
		{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser},
	} {
		t.Run(parser.name, func(t *testing.T) {
			defer parser.enter()()
			for _, test := range callscopetest.TransitionRegressions() {
				if !strings.HasPrefix(test.Name, "union-branch/") {
					continue
				}
				t.Run(test.Name, func(t *testing.T) {
					executor := newCallScopeExecutor(t)
					result, err := executor.Execute(t.Context(), test.Query, nil)
					require.NoError(t, err, test.Query)
					require.Equal(t, test.Columns, result.Columns)
					require.Equal(t, test.Rows, result.Rows, "each UNION branch owns its local MATCH binding")
				})
			}
		})
	}
}
