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
						executor.StatementFacts(query, resultmetadata.KnownScope(nil), false).Visit(func(statement resultmetadata.Statement) {
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
