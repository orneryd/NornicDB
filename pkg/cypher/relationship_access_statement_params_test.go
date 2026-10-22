package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipAccessProgramParameterFrameIsolation(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	const parentQuery = "MATCH ()-[s:T]->() RETURN s.p"
	const childQuery = "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p"
	parent, parentStatement := compileRelationshipStatement(base, parentQuery, nil, map[string]interface{}{"x": "parent"})
	parentProgram := parentStatement.program
	first, firstStatement := compileRelationshipStatement(parent, childQuery, nil, map[string]interface{}{"x": "first"})
	second, secondStatement := compileRelationshipStatement(parent, childQuery, nil, map[string]interface{}{"x": "second"})
	require.NotSame(t, parentProgram, firstStatement.program)
	require.Same(t, firstStatement.program, secondStatement.program)
	require.Same(t, parentProgram, parentStatement.program)
	require.Equal(t, "parent", getParamsFromContext(parent)["x"])

	var workers sync.WaitGroup
	for _, scope := range []struct {
		ctx  context.Context
		want string
	}{{first, "first"}, {second, "second"}} {
		for worker := 0; worker < 8; worker++ {
			workers.Add(1)
			go func(ctx context.Context, want string) {
				defer workers.Done()
				for iteration := 0; iteration < 16; iteration++ {
					if actual := parameterRowValues(ctx)["$x"]; actual != want {
						t.Errorf("shared immutable program changed parameter scope: got %v, want %s", actual, want)
						return
					}
				}
			}(scope.ctx, scope.want)
		}
	}
	workers.Wait()
	require.Equal(t, "parent", parameterRowValues(parent)["$x"])
	cancel()
	require.ErrorIs(t, first.Err(), context.Canceled)
	require.ErrorIs(t, second.Err(), context.Canceled)
}

func TestRelationshipAccessProgramParameterValueFreshness(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "statement_parameter_values"))
	for _, query := range []string{
		"CREATE ()-[:T {p:'x'}]->(), ()-[:T {p:'y'}]->()",
		"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)",
	} {
		_, err := e.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	const body = "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS p"
	for _, query := range []string{body, "CALL apoc.cypher.run($statement,{x:$x}) YIELD value RETURN value.p AS p"} {
		for _, value := range []string{"x", "y", "x"} {
			t.Run(fmt.Sprintf("%s/%s", query, value), func(t *testing.T) {
				result, err := e.Execute(context.Background(), "PROFILE "+query, map[string]interface{}{"x": value, "statement": body})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{value}}, result.Rows)
				assertRelationshipSuppliedPlan(t, result, 1)
			})
		}
	}
}
