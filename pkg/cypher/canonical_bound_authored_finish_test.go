package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Coordinate mapping of normalized source belongs to the credentials accepted-
// view product at composition. This checks authored bytes without normalization,
// through the actual public handler/context and all three execution modes.
func TestCanonicalBoundAuthoredFinish(t *testing.T) {
	t.Setenv("NORNICDB_CYPHER_QUERY_NORMALIZATION", "false")
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	var authored string
	var calls int
	var actualSource string
	var finishStart, finishEnd int
	require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{
		Name: "custom.authoredSource", MaxArgs: 0,
		Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(ctx context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		calls++
		program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
		actualSource, finishStart, finishEnd = "", -1, -1
		if program != nil {
			actualSource = program.syntax.Source()
			clauses := program.root.program.statement.Clauses
			finish := clauses[len(clauses)-1]
			finishStart, finishEnd = finish.Start, finish.End
		}
		return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
	}))
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		t.Run(mode+"source", func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			authored = mode + " \n/* header */\n CALL custom.authoredSource() YIELD value WHERE value = 1\n/* before terminator */ \t FINISH /* after terminator */ \n"
			before := calls
			result, err := exec.Execute(ctx, authored, nil)
			require.NoError(t, err)
			require.Empty(t, result.Columns)
			require.Empty(t, result.Rows)
			if mode == "EXPLAIN " {
				require.Equal(t, before, calls)
			} else {
				require.Equal(t, before+1, calls)
				require.Equal(t, authored, actualSource, "the installed program must retain authored bytes")
				start := strings.Index(authored, "FINISH")
				require.Equal(t, start, finishStart)
				require.Equal(t, start+len("FINISH"), finishEnd)
			}
		})
		t.Run(mode+"position", func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			// The bad token is on authored line 3, column 6. Comments and odd
			// spacing around FINISH must not move this public grammar diagnostic.
			bad := mode + "/* header */\nCALL custom.authoredSource() YIELD value\nWHERE @ true\n/* before terminator */ \t FINISH /* after terminator */\n"
			_, err := exec.Execute(ctx, bad, nil)
			require.Error(t, err)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
			require.Contains(t, err.Error(), "line 3:6")
		})
	}
}
