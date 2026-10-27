package cypher

import (
	"context"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The expected headers are Neo4j 5.26.30's. When a reference is configured,
// verify them live as well; the public regression still runs without it.
func TestExplainYieldKeepsIncomingColumns(t *testing.T) {
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for _, tc := range []struct {
		name    string
		query   string
		columns []string
	}{
		{"with", "WITH 'row' AS value CALL db.labels() YIELD label RETURN *", []string{"label", "value"}},
		{"yield_alias", "WITH 'row' AS value CALL db.labels() YIELD label AS kind RETURN *", []string{"kind", "value"}},
		{"empty_unwind", "UNWIND [] AS value CALL db.labels() YIELD label RETURN *", []string{"label", "value"}},
		{"empty_match", "MATCH (value:ExplainYieldMissing) CALL db.labels() YIELD label RETURN *", []string{"label", "value"}},
		{"with_drops_old_name", "WITH 1 AS old WITH old + 1 AS value CALL db.labels() YIELD label RETURN *", []string{"label", "value"}},
		{"with_star", "WITH 1 AS z WITH *, 2 AS a CALL db.labels() YIELD label RETURN *", []string{"a", "label", "z"}},
		{"with_after_yield", "WITH 'row' AS value CALL db.labels() YIELD label WITH value AS kept RETURN *", []string{"kept"}},
		{"two_calls", "WITH 'row' AS value CALL db.labels() YIELD label CALL db.labels() YIELD label AS other RETURN *", []string{"label", "other", "value"}},
		{"distinct", "WITH 'row' AS value CALL db.labels() YIELD label RETURN DISTINCT *", []string{"label", "value"}},
		{"star_and_item", "WITH 'row' AS value CALL db.labels() YIELD label RETURN *, value AS copy", []string{"label", "value", "copy"}},
		{"star_replaces_name", "WITH 'row' AS value CALL db.labels() YIELD label RETURN *, 2 AS value", []string{"label", "value"}},
		{"explicit_return", "WITH 'row' AS value CALL db.labels() YIELD label RETURN value", []string{"value"}},
		{"standalone_call", "CALL db.labels() YIELD label RETURN *", []string{"label"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if reference != nil {
				t.Run("neo4j", func(t *testing.T) {
					result, err := neo4j.ExecuteQuery(ctx, reference, "EXPLAIN "+tc.query, nil, neo4j.EagerResultTransformer)
					require.NoError(t, err)
					require.Equal(t, tc.columns, result.Keys)
					require.Empty(t, result.Records)
				})
			}
			for _, transaction := range []bool{false, true} {
				name := "auto_commit"
				if transaction {
					name = "explicit_transaction"
				}
				t.Run(name, func(t *testing.T) {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "explain-yield")
					exec := NewStorageExecutor(store)
					if transaction {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() {
							_, err := exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						}()
					}
					result, err := exec.Execute(ctx, "EXPLAIN "+tc.query, nil)
					require.NoError(t, err)
					require.Empty(t, result.Rows, "EXPLAIN must not execute the procedure")
					require.Equal(t, tc.columns, result.Columns)
					require.Equal(t, tc.columns, exec.StatementColumns(tc.query), "empty/error response metadata uses the same header owner")
				})
			}
		})
	}
}

func TestStatementColumnsPreservesUnknownYieldScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "explain-yield-fallback"))
	// LOAD CSV is unsupported by execution. Every projection has the same
	// deliberate owner error; it cannot succeed with a guessed partial header.
	prefix := "LOAD CSV FROM 'file:///rows.csv' AS row CALL db.labels() YIELD label AS kind "
	for _, projection := range []string{"RETURN *", "RETURN *, 1 AS extra", "RETURN kind, 1 AS extra"} {
		columns, err := exec.StatementResultColumns(prefix+projection, RawSource)
		require.Error(t, err)
		require.Nil(t, columns)
	}
}
