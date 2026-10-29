package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A failed public composite statement must not publish its earlier local write.
// Observe persistence with a separate executor, never its participant's view.
func TestCompositeAutocommitReleaseRollback(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			manager := newCompositeNativeManager(t, nil)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			ctx := context.Background()
			for _, name := range []string{"one", "two"} {
				require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: name}))
			}
			require.NoError(t, manager.CreateCompositeDatabase(ctx, "comp", []multidb.ConstituentRef{
				{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
				{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
			}))
			composite, err := manager.GetStorage("comp")
			require.NoError(t, err)
			executor := NewStorageExecutor(composite)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			physical, err := manager.GetStorage("one")
			require.NoError(t, err)
			observer := NewStorageExecutor(physical)
			shapes := []struct {
				name, query string
				writes      int64
			}{
				{"call", `CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE comp.two RETURN 1 / $d AS z } RETURN w, z`, 1},
				{"union", `CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS z UNION ALL USE comp.two RETURN 1 / $d AS z } RETURN z`, 1},
				{"nested", `CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { CALL { USE comp.two RETURN 1 / $d AS z } RETURN z } RETURN w, z`, 1},
				{"second_write", `CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one CREATE (:AcAtom {run: $run, v: w / $d}) RETURN 1 AS z } RETURN z`, 2},
				{"dynamic", `CALL { USE comp.one CALL apoc.cypher.doIt('CREATE (:AcAtom {run: $run}) RETURN 1 AS w', {run: $run}) YIELD value RETURN value.w AS w } CALL { USE comp.two RETURN 1 / $d AS z } RETURN w, z`, 1},
			}
			for _, shape := range shapes {
				t.Run(shape.name, func(t *testing.T) {
					for _, success := range []bool{true, false} {
						run := shape.name + "-failure"
						divisor, expected := int64(0), int64(0)
						if success {
							run = shape.name + "-success"
							divisor = 1
							expected = shape.writes
						}
						_, executionErr := executor.Execute(ctx, shape.query, map[string]interface{}{"d": divisor, "run": run})
						if success {
							require.NoError(t, executionErr)
						} else {
							require.Error(t, executionErr)
						}
						result, readErr := observer.Execute(ctx, `MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c`, map[string]interface{}{"run": run})
						require.NoError(t, readErr)
						require.Equal(t, [][]interface{}{{expected}}, result.Rows)
					}
				})
			}
		})
	}
}
