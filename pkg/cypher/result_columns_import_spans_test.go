package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Scope and byte spans must survive the same clause boundaries, regardless of
// the position of USE or comments standing where whitespace would stand.
func TestStatementFactsImportSourceBoundaries(t *testing.T) {
	for _, name := range []string{"x", "__fabric_import_0"} {
		for _, importing := range []string{"WITH *", "WITH " + name, "WITH/* import */ " + name, "WITH // import\n " + name, "/* decoy USE fake */ WITH " + name} {
			for _, bodyTail := range []string{
				" USE comp.remote RETURN " + name + " AS value",
				" UNWIND [" + name + "] AS item WITH collect(item) AS keys USE comp.remote RETURN keys AS value",
			} {
				query := "USE comp UNWIND [1,2] AS " + name + " CALL { " + importing + bodyTail + " } RETURN value"
				t.Run(query, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "import-spans"))
					exec.StatementFacts(query, resultmetadata.KnownScope(nil), false).Visit(func(statement resultmetadata.Statement) {
						var call *resultmetadata.Call
						for _, clause := range statement.Clauses {
							if clause.Call != nil {
								call = clause.Call
							}
						}
						require.NotNil(t, call)
						require.Equal(t, resultmetadata.ImportNone, call.Mode)
						require.Equal(t, []string{name}, call.Imports)
						call.Statement.Incoming.Visit(func(names []string) { require.Equal(t, []string{name}, names) }, func() { t.Fatal("import scope is unknown") })
						var selected bool
						previousEnd := call.Statement.Start
						for _, clause := range call.Statement.Clauses {
							require.GreaterOrEqual(t, clause.Start, previousEnd, "overlapping original-source clause spans")
							require.LessOrEqual(t, clause.End, len(query))
							previousEnd = clause.End
							if clause.Kind == resultmetadata.ClauseUse {
								selected = true
								require.Equal(t, "USE comp.remote", strings.TrimSpace(query[clause.Start:clause.End]))
								clause.Before.Visit(func(names []string) {
									expected := []string{name}
									if strings.Contains(bodyTail, "collect(item)") {
										expected = []string{"keys"}
									}
									require.Equal(t, expected, names)
								}, func() { t.Fatal("graph selection lost the projected scope") })
							}
						}
						require.True(t, selected, "USE must be its own source clause")
						statement.Columns.Visit(func(names []string) { require.Equal(t, []string{"value"}, names) }, func() { t.Fatal("known program has unknown columns") })
					}, func() { t.Fatal("owner has no continuation") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Reason, reason.Position, reason.Err) })
				})
			}
		}
	}
}
