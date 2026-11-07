package cypher

import (
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNestedScopeMembershipBoundaries(t *testing.T) {
	runCallScopeExecuteCases(t, []callScopeExecuteCase{
		{name: "scalar-cannot-shadow-scoped-import", query: "WITH 1 AS a CALL (a) { RETURN COUNT { WITH 9 AS a RETURN a AS v } AS value } RETURN a,value", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "nested-return-rejects-duplicate-columns", query: "CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "scalar-star-call-excludes-captured-constant", query: "WITH 1 AS a RETURN COUNT { CALL (*) { WITH 0 AS marker RETURN a AS value } RETURN value AS item } AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "scalar-listed-call-can-import-captured-constant", query: "WITH 1 AS a RETURN COUNT { CALL (a) { WITH 0 AS marker RETURN a AS value } RETURN value AS item } AS v", columns: []string{"v"}, rows: [][]interface{}{{int64(1)}}},
		{name: "scoped-read-adds-member", query: "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * UNION RETURN 2 AS z } RETURN a,z", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "projection-read-adds-member", query: "WITH 1 AS a CALL (a) { RETURN *,a+1 AS v } RETURN a,v", columns: []string{"a", "v"}, rows: [][]interface{}{{int64(1), int64(2)}}},
		{name: "scalar-enclosing-is-constant", query: "WITH 1 AS a RETURN COLLECT { RETURN * } AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "scalar-unchanged-projection-is-constant", query: "WITH 1 AS a RETURN COLLECT { WITH a RETURN * } AS v", code: "Neo.ClientError.Statement.SyntaxError"},
	})
}

func TestContextInputScalarShadowIsRejectedBeforeExecution(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		ctx = withValueBindings(ctx, map[string]interface{}{"a": int64(1)})
		query := "WITH a CALL (a) { RETURN COUNT { WITH 9 AS a RETURN a AS v } AS value } RETURN a,value"
		for attempt := 0; attempt < 2; attempt++ {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		}
	})
}
