package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NornicDB keeps its documented implicit-import extension. A local declaration
// belongs to its branch, while a sibling reads the outer binding. Both the
// immutable input facts and execution must preserve that distinction.
func TestResultHeaderImplicitBranchInputs(t *testing.T) {
	for _, name := range []string{"value", "long name", "", "WHERE", "a`b"} {
		written := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		if name == "value" {
			written = name
		}
		for _, union := range []string{"UNION", "UNION ALL"} {
			for _, reverse := range []bool{false, true} {
				for _, empty := range []bool{false, true} {
					t.Run(fmt.Sprintf("name=%q/%s/reverse=%v/empty=%v", name, union, reverse, empty), func(t *testing.T) {
						local := "UNWIND [2] AS " + written + " RETURN " + written + " AS x"
						outer := "RETURN " + written + " AS x"
						inputs := [][]string{{}, {name}}
						if reverse {
							local, outer = outer, local
							inputs[0], inputs[1] = inputs[1], inputs[0]
						}
						prefix := "WITH 1 AS " + written
						if empty {
							prefix = "UNWIND [] AS " + written
						}
						query := prefix + " CALL { " + local + " " + union + " " + outer + " } RETURN x ORDER BY x"
						executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
						executor.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
							require.Len(t, statement.Clauses, 3)
							call := statement.Clauses[1].Call
							require.NotNil(t, call)
							require.Equal(t, []string{name}, call.Imports)
							require.Len(t, call.Statement.Branches, 2)
							for index, branch := range call.Statement.Branches {
								branch.Incoming.Visit(func(actual []string) { require.ElementsMatch(t, inputs[index], actual) }, func() { t.Fatal("branch inputs unknown") })
							}
						}, func() { t.Fatal("statement facts declined") }, func(reason resultmetadata.Unresolved) { t.Fatalf("facts unresolved: %v", reason.Err) })
						result, err := executor.Execute(context.Background(), query, nil)
						require.NoError(t, err)
						require.Equal(t, []string{"x"}, result.Columns)
						if empty {
							require.Empty(t, result.Rows)
						} else {
							require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
						}
					})
				}
			}
		}
	}
}

func TestResultHeaderImplicitInputFirstUse(t *testing.T) {
	for _, query := range []string{
		"WITH 1 AS value CALL { UNWIND [value] AS first UNWIND [2] AS value RETURN value AS x } RETURN x",
		"WITH 1 AS value CALL { WITH value WITH 2 AS value RETURN value AS x } RETURN x",
	} {
		t.Run(query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
			result, err := executor.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		})
	}
}

// Explicit legacy WITH imports expire when projected away. The documented
// implicit-import extension retains only the other outer names read by a
// branch. Scoped imports retain their names independently of both policies.
func TestResultHeaderUnionBranchScopeContexts(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, sibling := range []string{"legacy", "implicit", "scoped", "none"} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("scoped=%v/sibling=%s/reverse=%v", scoped, sibling, reverse), func(t *testing.T) {
					left := "WITH value UNWIND [1,2] AS seed WITH count(*) AS total RETURN total AS x"
					right := "RETURN value AS x"
					inputs := [][]string{{"value"}, {"value"}}
					retained := [][]string{{}, {"value"}}
					switch sibling {
					case "legacy":
						right, retained[1] = "WITH value RETURN value AS x", nil
					case "scoped":
						right = "CALL (value) { RETURN value AS x } RETURN x"
					case "none":
						right, inputs[1], retained[1] = "RETURN 11 AS x", nil, nil
					}
					frame := "CALL { "
					var callRetained []string
					if scoped {
						frame, callRetained = "CALL (value) { ", []string{"value"}
						inputs, retained = [][]string{{"value"}, {"value"}}, [][]string{{"value"}, {"value"}}
					}
					leftIndex := 0
					if reverse {
						left, right, leftIndex = right, left, 1
						inputs[0], inputs[1] = inputs[1], inputs[0]
						retained[0], retained[1] = retained[1], retained[0]
					}
					query := "WITH 7 AS value " + frame + left + " UNION ALL " + right + " } RETURN x ORDER BY x"
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "branch-context"))
					executor.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
						call := statement.Clauses[1].Call
						require.NotNil(t, call)
						call.Retained.Visit(func(actual []string) { require.ElementsMatch(t, callRetained, actual) }, func() { t.Fatal("unknown CALL context") })
						require.NotNil(t, call.Statement)
						// A UNION envelope publishes columns. Its branch scopes
						// cannot be mistaken for a combined retained context.
						call.Statement.Incoming.Visit(func(actual []string) { require.Empty(t, actual) }, func() { t.Fatal("unknown UNION envelope") })
						call.Statement.Retained.Visit(func(actual []string) { require.Empty(t, actual) }, func() { t.Fatal("unknown UNION envelope") })
						require.Len(t, call.Statement.Branches, 2)
						for index, branch := range call.Statement.Branches {
							branch.Incoming.Visit(func(actual []string) { require.ElementsMatch(t, inputs[index], actual) }, func() { t.Fatal("unknown branch input") })
							branch.Retained.Visit(func(actual []string) { require.ElementsMatch(t, retained[index], actual) }, func() { t.Fatal("unknown branch retention") })
							branch.Columns.Visit(func(actual []string) { require.Equal(t, []string{"x"}, actual) }, func() { t.Fatal("unknown branch columns") })
						}
						after := []string{"total"}
						if scoped {
							after = append(after, "value")
						}
						call.Statement.Branches[leftIndex].Clauses[2].After.Visit(func(actual []string) { require.ElementsMatch(t, after, actual) }, func() { t.Fatal("unknown projected scope") })
					}, func() { t.Fatal("branch context declined") }, func(reason resultmetadata.Unresolved) { t.Fatalf("branch context unresolved: %v", reason.Err) })
				})
			}
		}
	}
}
