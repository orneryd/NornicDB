package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newDynamicModeExecutor(t *testing.T, composite bool) (*StorageExecutor, storage.Engine, storage.Engine) {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	for _, name := range []string{"mode_one", "mode_two"} {
		require.NoError(t, manager.CreateDatabase(name))
	}
	require.NoError(t, manager.CreateCompositeDatabase("mode_comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "mode_one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "mode_two", Type: "local", AccessMode: "read_write"},
	}))
	database := "mode_one"
	if composite {
		database = "mode_comp"
	}
	store, err := manager.GetStorage(database)
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	one, err := manager.GetStorage("mode_one")
	require.NoError(t, err)
	two, err := manager.GetStorage("mode_two")
	require.NoError(t, err)
	return exec, one, two
}

// Each level passes evaluated values to the next. The expression form also
// binds them as row values, so parameter-only prediction cannot implement it.
func dynamicModeBody(shape string, depth int, write bool) (string, map[string]interface{}) {
	body := "RETURN 1 AS value"
	if write {
		body = "CREATE (:U5DynamicMode) RETURN 1 AS value"
	}
	values := map[string]interface{}{}
	if shape == "nested" {
		depth++
	}
	for i := 0; i < depth; i++ {
		values = map[string]interface{}{"inner": body, "innerParams": values}
		statement, args, prefix := "$inner", "$innerParams", ""
		switch shape {
		case "inline":
			args = "{inner:$innerParams.inner,innerParams:$innerParams.innerParams}"
		case "expression":
			prefix = "WITH $inner AS text,$innerParams AS arguments "
			statement, args = "coalesce(text,'RETURN 0 AS value')", "coalesce(arguments,{})"
		}
		body = prefix + "CALL apoc.cypher.run(" + statement + "," + args + ") YIELD value RETURN value"
	}
	return body, values
}

func dynamicModeCall(shape, statement, values string) string {
	prefix := ""
	query, args := "$"+statement, "$"+values
	switch shape {
	case "inline":
		args = "{inner:$" + values + ".inner,innerParams:$" + values + ".innerParams}"
	case "expression":
		prefix = "WITH $" + statement + " AS text,$" + values + " AS arguments "
		query, args = "coalesce(text,'RETURN 0 AS value')", "coalesce(arguments,{})"
	}
	return prefix + "CALL apoc.cypher.run(" + query + "," + args + ") YIELD value"
}

func TestDynamicStatementModeMatrix(t *testing.T) {
	failure := registerAdmissionMatrixReject(t)
	for _, shape := range []string{"param", "inline", "nested", "expression"} {
		for _, depth := range []int{0, 1, 3, 17} {
			for _, write := range []bool{false, true} {
				for _, route := range []string{"native", "fabric", "union"} {
					for _, explicit := range []bool{false, true} {
						for _, reject := range []bool{false, true} {
							t.Run(fmt.Sprintf("%s/depth=%d/write=%v/%s/explicit=%v/failure=%v", shape, depth, write, route, explicit, reject), func(t *testing.T) {
								exec, one, two := newDynamicModeExecutor(t, route != "native")
								body, values := dynamicModeBody(shape, depth, write)
								read, readValues := dynamicModeBody(shape, depth, false)
								params := map[string]interface{}{"statement": body, "values": values, "readStatement": read, "readValues": readValues}
								call := dynamicModeCall(shape, "statement", "values")
								guard := " CALL u5matrix.reject() YIELD rejected"
								query := call
								if route != "union" && reject {
									query += guard
								}
								query += " RETURN value"
								if route != "native" {
									query = "CALL { USE mode_comp.one " + query + " } RETURN value"
								}
								if route == "union" {
									right := dynamicModeCall(shape, "readStatement", "readValues")
									if reject {
										right += guard
									}
									query += " UNION ALL CALL { USE mode_comp.two " + right + " RETURN value } RETURN value"
								}
								ctx := context.Background()
								if explicit {
									_, err := exec.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
								}
								result, err := exec.Execute(ctx, query, params)
								if reject {
									require.ErrorContains(t, err, failure.Error())
								} else {
									require.NoError(t, err)
									want := 1
									if route == "union" {
										want = 2
									}
									require.Len(t, result.Rows, want)
								}
								if explicit {
									_, err = exec.Execute(ctx, "COMMIT", nil)
									if reject {
										require.Error(t, err)
									} else {
										require.NoError(t, err)
									}
								}
								for i, store := range []storage.Engine{one, two} {
									nodes, err := store.GetNodesByLabel("U5DynamicMode")
									require.NoError(t, err)
									want := 0
									if i == 0 && write && !reject {
										want = 1
									}
									require.Len(t, nodes, want)
								}
							})
						}
					}
				}
			}
		}
	}
}

func TestDynamicPromotionReadsOwnWritesAndRejectsSecondWriter(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, secondWriter := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%v/secondWriter=%v", explicit, secondWriter), func(t *testing.T) {
				exec, one, two := newDynamicModeExecutor(t, true)
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				query := "CALL { USE mode_comp.one WITH $statement AS text CALL apoc.cypher.run(text,{}) YIELD value WITH value MATCH (n:U5DynamicMode) RETURN count(n) AS found } RETURN found"
				if secondWriter {
					query += " UNION ALL CALL { USE mode_comp.two CALL apoc.cypher.run($statement,{}) YIELD value RETURN 1 AS found } RETURN found"
				}
				result, err := exec.Execute(ctx, query, map[string]interface{}{"statement": "CREATE (:U5DynamicMode) RETURN 1 AS value"})
				if secondWriter {
					require.Error(t, err)
					require.True(t, strings.Contains(err.Error(), "Writing to more than one database"), "%v", err)
				} else {
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				}
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					if secondWriter {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}
				for i, store := range []storage.Engine{one, two} {
					nodes, err := store.GetNodesByLabel("U5DynamicMode")
					require.NoError(t, err)
					want := 0
					if i == 0 && !secondWriter {
						want = 1
					}
					require.Len(t, nodes, want)
				}
			})
		}
	}
}
