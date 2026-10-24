package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmptyQuotedIncomingScope(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
	for _, presentFirst := range []bool{false, true} {
		query := "RETURN `` /* absent first */"
		if presentFirst {
			query = "RETURN `` /* present first */"
		}
		for _, present := range []bool{presentFirst, !presentFirst} {
			var incoming []string
			if present {
				incoming = []string{""}
			}
			known, unresolved := false, false
			executor.StatementFacts(query, resultmetadata.KnownScope(incoming), false).Visit(func(statement resultmetadata.Statement) {
				known = true
				statement.Columns.Visit(func(columns []string) { require.Equal(t, []string{""}, columns) }, func() { t.Fatal("known statement has unknown columns") })
			}, func() { t.Fatal("graph statement declined") }, func(reason resultmetadata.Unresolved) {
				unresolved = true
				require.Equal(t, resultmetadata.ReasonUndefinedVariable, reason.Reason)
			})
			require.Equal(t, present, known)
			require.Equal(t, !present, unresolved)
		}
	}
}
