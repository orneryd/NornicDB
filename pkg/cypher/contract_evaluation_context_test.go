package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Expression evaluation is preparation too: outer node/edge validation polls
// cannot stand in for compiling entries, counting adjacency or checking IN values.
func TestContractEvaluationPreparationCancellation(t *testing.T) {
	for _, phase := range []string{"compile entries", "node entries", "edge entries", "count DDL", "count COMMIT", "IN values", "stored capture", "namespace projection"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			b, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) *ExecuteResult {
				t.Helper()
				r, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
				return r
			}
			run("CREATE (:P {k:20})")
			run("MATCH (n:P) UNWIND range(1,20) AS i CREATE (n)-[:R {k:i}]->(:Q)")
			if phase == "stored capture" {
				// Reopen through the public API to make the physical reader cold.
				// A warm cache has its own collection owner and only one poll in read.
				require.NoError(t, b.Close())
				b, err = storage.NewBadgerEngine(dir)
				require.NoError(t, err)
				e = NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			}
			owner := "ValidateConstraintContractExpressionsWithContext"
			pattern, expression := "(n:P)", strings.Repeat("n.k > 0\n", 20)
			switch phase {
			case "node entries":
				owner = "validateConstraintContractForNodeEngine"
			case "edge entries":
				owner, pattern, expression = "validateConstraintContractForEdgeEngine", "()-[r:R]-()", strings.Repeat("r.k > 0\n", 20)
			case "count DDL", "count COMMIT", "stored capture", "namespace projection":
				owner, expression = "countMatchingPatternEdges", "COUNT { (n)-[:R]->(:Q) } <= 20"
				if phase == "stored capture" {
					owner = "nativeAdjacencyReader).read.func1"
				} else if phase == "namespace projection" {
					owner = "materializedValuesWithContext"
				}
			case "IN values":
				owner, expression = "evaluatePropertyInExpressionWithContext", "n.k IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]"
			}
			ddl := "CREATE CONSTRAINT prepare_contract FOR " + pattern + " REQUIRE {\n" + expression + "\n}"
			query := ddl
			if phase == "count COMMIT" {
				run(ddl)
				run("BEGIN")
				run("MATCH (n:P) SET n.ok = 1")
				query = "COMMIT"
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			if phase == "namespace projection" {
				ctx.via = "toUserEdgesInNamespace"
			}
			_, err = e.Execute(ctx, query, nil)
			t.Logf("phase=%s owner=%s hits=%d execute=%v", phase, owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			if phase == "count COMMIT" {
				_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
				require.Nil(t, run("MATCH (n:P) RETURN n.ok").Rows[0][0])
				run("BEGIN")
				run("MATCH (n:P) SET n.ok = 1")
				run("COMMIT")
			} else {
				require.Empty(t, b.GetSchemaForNamespace("test").GetAllConstraintContracts())
				run(ddl)
			}
		})
	}
}
