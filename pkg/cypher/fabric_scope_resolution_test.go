package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// A CALL scope clause is resolved when the statement is checked, as Neo4j
// compiles it, before anything plans, runs or is enlisted, on one database
// and on a composite alike: a variable it names that the query doesn't
// define is a SyntaxError reported before a missing parameter, EXPLAIN
// reports it too, no write ahead of the CALL happens, and an explicit
// transaction has no participant afterwards. Words Neo4j reads as variable
// names there (null, NULL, true, nan, NaN, Infinity) are rejected when
// nothing defines them and import the variable when the query defines it.
func TestFabricScopeClauseResolvedBeforeExecution(t *testing.T) {
	exec, driver := fabricRemoteImportExecutor(t)
	const label = "FabricScopeResolution"
	defer func() {
		_, _ = neo4j.ExecuteQuery(context.Background(), driver, "MATCH (n:"+label+") DETACH DELETE n", nil, neo4j.EagerResultTransformer)
	}()
	for _, tc := range []struct{ name, scope, body string }{
		{"undefined_then_missing_parameter", "y", "RETURN $p AS z"},
		{"empty_item_then_missing_parameter", "x,", "RETURN $p AS z"},
		{"undefined", "y", "RETURN 1 AS z"},
		{"null", "null", "RETURN 1 AS z"},
		{"NULL", "NULL", "RETURN 1 AS z"},
		{"true", "true", "RETURN 1 AS z"},
		{"nan", "nan", "RETURN 1 AS z"},
		{"NaN", "NaN", "RETURN 1 AS z"},
		{"Infinity", "Infinity", "RETURN 1 AS z"},
		{"quoted_null", "`null`", "RETURN 1 AS z"},
	} {
		reference := "WITH 1 AS x CALL (" + tc.scope + ") { " + tc.body + " } RETURN z"
		want := fabricMatrixNeo4j(t, driver, reference, nil)
		require.Error(t, want.err, "Neo4j: %s", reference)
		explain := fabricMatrixNeo4j(t, driver, "EXPLAIN "+reference, nil)
		require.Error(t, explain.err, "Neo4j: EXPLAIN %s", reference)
		t.Run(tc.name+"/single", func(t *testing.T) {
			result, err := exec.Execute(context.Background(), reference, nil)
			requireFabricMatrixOutcome(t, want, result, err, reference)
			result, err = exec.Execute(context.Background(), "EXPLAIN "+reference, nil)
			requireFabricMatrixOutcome(t, explain, result, err, "EXPLAIN "+reference)
		})
		for _, destination := range []string{"remote", "http", "one"} {
			t.Run(tc.name+"/"+destination, func(t *testing.T) {
				query := "USE comp WITH 1 AS x CALL (" + tc.scope + ") { USE comp." + destination + " " + tc.body + " } RETURN z"
				fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
					result, err := runner.Execute(context.Background(), query, nil)
					requireFabricMatrixOutcome(t, want, result, err, query)
					if runner.txContext != nil {
						if tx, ok := runner.txContext.tx.(*fabric.FabricTransaction); ok {
							require.Empty(t, tx.Participants(), "a rejected statement enlists nothing")
						}
					}
				})
				result, err := exec.Execute(context.Background(), "EXPLAIN "+query, nil)
				requireFabricMatrixOutcome(t, explain, result, err, "EXPLAIN "+query)
			})
		}
	}
	// A write ahead of the CALL never happens, on any constituent.
	for _, outer := range []string{"one", "remote", "http"} {
		for _, inner := range []string{"one", "remote", "http"} {
			t.Run("write_before/"+outer+"/"+inner, func(t *testing.T) {
				run := fmt.Sprintf("scope-resolution-%d", time.Now().UnixNano())
				params := map[string]interface{}{"run": run}
				query := "USE comp CALL { USE comp." + outer + " CREATE (:" + label + " {run: $run}) RETURN 1 AS w } CALL (y) { USE comp." + inner + " RETURN 1 AS z } RETURN w, z"
				fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
					_, err := runner.Execute(context.Background(), query, params)
					require.ErrorContains(t, err, "Variable `y` not defined", "Fabric query: %s", query)
				})
				remote, err := neo4j.ExecuteQuery(context.Background(), driver, "MATCH (n:"+label+" {run: $run}) RETURN count(n) AS c", params, neo4j.EagerResultTransformer)
				require.NoError(t, err)
				require.EqualValues(t, 0, remote.Records[0].Values[0], "remote writes")
				local, err := exec.Execute(context.Background(), "USE comp CALL { USE comp.one MATCH (n:"+label+" {run: $run}) RETURN count(n) AS c } RETURN c", params)
				require.NoError(t, err)
				require.Equal(t, fabricTypedRows([][]interface{}{{int64(0)}}), fabricTypedRows(local.Rows), "local writes")
			})
		}
	}
	// A defined word imports its variable on one database too.
	for _, word := range []string{"null", "NULL", "true", "nan", "NaN", "Infinity"} {
		reference := "WITH 1 AS `" + word + "` CALL (" + word + ") { RETURN `" + word + "` + 1 AS y } RETURN y"
		want := fabricMatrixNeo4j(t, driver, reference, nil)
		require.NoError(t, want.err, "Neo4j: %s", reference)
		t.Run("defined/"+word+"/single", func(t *testing.T) {
			result, err := exec.Execute(context.Background(), reference, nil)
			requireFabricMatrixOutcome(t, want, result, err, reference)
		})
	}
}
