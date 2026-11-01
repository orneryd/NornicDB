package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The retained qualification failures cover execution. This independently
// checks that obtaining their headers uses native admission without executing
// schema creation or inspecting existing records to discover the header.
func TestStatementEntrySchemaPreparationMetadata(t *testing.T) {
	declarations := []string{
		"CREATE CONSTRAINT owner_key FOR (n:Owner) REQUIRE (n.a, n.b) IS NODE KEY",
		"CREATE CONSTRAINT owner_rel_key FOR ()-[r:OwnerRel]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT owner_unique FOR (n:OwnerUnique) REQUIRE (n.a, n.b) IS UNIQUE",
		"CREATE CONSTRAINT owner_temporal FOR (n:OwnerTime) REQUIRE (n.a, n.from, n.to) IS TEMPORAL NO OVERLAP",
	}
	for _, declaration := range declarations {
		for _, prefix := range []string{"", "/* owner */ CYPHER 5 ", "EXPLAIN ", "PROFILE ", "CYPHER 25 EXPLAIN "} {
			query := prefix + declaration
			t.Run(query, func(t *testing.T) {
				engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner_metadata")
				executor := NewStorageExecutor(engine)
				beforeIndexes, beforeConstraints := engine.GetSchema().SchemaObjectCounts()
				columns, err := executor.StatementResultColumns(query, RawSource)
				require.NoError(t, err)
				require.Empty(t, columns)
				executor.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(
					func(statement resultmetadata.Statement) {
						statement.Columns.Visit(func(fields []string) { require.Empty(t, fields) }, func() { t.Fatal("unknown schema command header") })
					}, func() { t.Fatal("native schema command declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
				afterIndexes, afterConstraints := engine.GetSchema().SchemaObjectCounts()
				require.Equal(t, beforeIndexes, afterIndexes)
				require.Equal(t, beforeConstraints, afterConstraints)
			})
		}
	}
}

func TestStatementEntrySharedRejections(t *testing.T) {
	for _, body := range []string{
		"CREATE CONSTRAINT old_owner ON (n:Owner) ASSERT n.x IS UNIQUE",
		"RETURN EXISTS { CREATE (n:Owner) RETURN n } AS value",
		"RETURN COUNT { CREATE (n:Owner) RETURN n } AS value",
		"RETURN COLLECT { CREATE (n:Owner) RETURN n } AS value",
	} {
		for _, prefix := range []string{"", "CYPHER 5 ", "EXPLAIN ", "PROFILE "} {
			query := prefix + body
			t.Run(query, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner_rejection"))
				_, metadataErr := executor.StatementResultColumns(query, RawSource)
				_, executionErr := executor.Execute(t.Context(), query, nil)
				require.Error(t, metadataErr)
				require.Error(t, executionErr)
				require.Equal(t, statusText(executionErr), statusText(metadataErr))
			})
		}
	}
	for _, prefix := range []string{"EXPLAIN PROFILE ", "PROFILE EXPLAIN ", "CYPHER 5 EXPLAIN PROFILE "} {
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		query := prefix + "RETURN 1 AS value"
		_, metadataErr := executor.StatementResultColumns(query, RawSource)
		_, executionErr := executor.Execute(t.Context(), query, nil)
		require.Error(t, metadataErr)
		require.Error(t, executionErr)
		require.Equal(t, statusText(executionErr), statusText(metadataErr))
	}
}

func TestStatementEntryBareShowDatabasesConsumer(t *testing.T) {
	for _, query := range []string{"SHOW DATABASES", "/* prefix */ SHOW /* gap */ DATABASES", "CYPHER 5 SHOW DATABASES", "PROFILE SHOW DATABASES", "SHOW DATABASES;"} {
		require.True(t, IsShowDatabasesQuery(query), query)
	}
	for _, query := range []string{"RETURN 'SHOW DATABASES' AS text", "SHOW DATABASES YIELD name RETURN name", "SHOW DEFAULT DATABASE", "SHOW ALIASES", "SHOW DATABASES WHERE name = 'x'"} {
		require.False(t, IsShowDatabasesQuery(query), query)
	}
}

func TestStatementEntryIncompleteBodies(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		for _, source := range []string{"", "/* no query */", ";", "EXPLAIN", "PROFILE", "CYPHER 25", "RETURN 1; EXPLAIN", "RETURN 1; PROFILE", "RETURN 1; CYPHER 25"} {
			_, metadataErr := executor.StatementResultColumns(source, RawSource)
			require.Error(t, metadataErr, source)
			_, executionErr := executor.Execute(t.Context(), source, nil)
			require.Error(t, executionErr, source)
		}
	})
}
