package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A composite statement's variable scopes are checked on the statement
// without the USE clauses the planner reads, wherever they stand.
func TestSingleGraphStatement(t *testing.T) {
	p := NewFabricPlanner(nil)
	for _, tt := range []struct{ query, want string }{
		{"RETURN 'USE comp' AS s", "RETURN 'USE comp' AS s"},
		{"USE comp WITH 1 AS x CALL (x) { USE comp.remote RETURN x AS y } RETURN y", "WITH 1 AS x CALL (x) { RETURN x AS y } RETURN y"},
		{"USE comp UNWIND [1] AS x CALL { WITH x USE comp.one RETURN x AS y } RETURN y", "UNWIND [1] AS x CALL { WITH x RETURN x AS y } RETURN y"},
		{"USE comp CALL { USE comp.a RETURN 1 AS v UNION ALL USE comp.b RETURN 2 AS v } RETURN v", "CALL { RETURN 1 AS v UNION ALL RETURN 2 AS v } RETURN v"},
		{"USE comp.a RETURN 1 AS v UNION USE comp.b RETURN 2 AS v", "RETURN 1 AS v UNION RETURN 2 AS v"},
		{"MATCH (n) WITH n USE comp.one RETURN n", "MATCH (n) WITH n RETURN n"},
		{"USE comp CALL { USE comp.one CALL { USE comp.remote RETURN 1 AS z } RETURN z } RETURN z", "CALL { CALL { RETURN 1 AS z } RETURN z } RETURN z"},
	} {
		got, err := p.SingleGraphStatement(tt.query)
		require.NoError(t, err, tt.query)
		require.Equal(t, tt.want, got, tt.query)
	}
}
