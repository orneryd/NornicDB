package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Fabric's fragment hook reaches this owner with canonicalized record bindings;
// it must enforce the same compile-time contract on an empty input stream.
func TestProjectionTypeScopeOwner(t *testing.T) {
	for _, query := range []string{
		"UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v",
		"MATCH (n:ReviewNoNodes) WITH n.v * 1.0 AS x CALL (*) { WITH count(*) AS c RETURN [1,2][x] AS v } RETURN v",
	} {
		t.Run(query, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			require.Error(t, exec.validateMatchSemanticScopes(query, false))
		})
	}
}
