package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The nodes and relationships a statement reads from storage count against
// its limit while it holds them, on every route that reads a label, the
// store or a type in bulk (#1343): a statement over the limit fails with the
// limit's error and gives its memory back; within the limit it returns what
// it returns with no limit.
func TestEntityReadsCountAgainstTheLimit(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	for _, setup := range []string{
		"UNWIND range(1, 40000) AS i CREATE (:Bulk {p: i % 2})",
		"UNWIND range(1, 20000) AS i CREATE (:Linked {i: i})-[:LINK]->(:Linked {i: -i})",
	} {
		_, err := executor.Execute(ctx, setup, nil)
		require.NoError(t, err, setup)
	}

	for _, test := range []struct{ name, query string }{
		// Procedures that read the whole store.
		{"db.labels", "CALL db.labels() YIELD label RETURN count(*) AS n"},
		{"db.propertyKeys", "CALL db.propertyKeys() YIELD propertyKey RETURN count(*) AS n"},
		{"db.schema.nodeTypeProperties", "CYPHER 5 CALL db.schema.nodeTypeProperties() YIELD nodeType RETURN count(*) AS n"},
		// An algorithm's input label.
		{"apoc algorithm input", "CALL apoc.algo.wcc('Bulk') YIELD node RETURN count(*) AS n"},
		// MERGE's label scan, no index.
		{"merge candidates", "MERGE (n:Bulk {p: 0}) RETURN count(*) AS n"},
		// A relationship type's relationships, counted in both directions.
		{"relationship type", "MATCH (a)-[:LINK]-(b) RETURN count(*) AS n"},
		// The label scan of an unbounded single-node read, counted.
		{"label scan", "MATCH (n:Bulk) WHERE n.p = 1 RETURN count(*) AS n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ConfigureTransactionMemory(0, TransactionMemoryLimits{})
			want, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
			_, err = executor.Execute(ctx, test.query, nil)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
			requirePoolsEmpty(t, "nornic")

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 256 << 20})
			got, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, want.Rows, got.Rows)
			requirePoolsEmpty(t, "nornic")
		})
	}
}
