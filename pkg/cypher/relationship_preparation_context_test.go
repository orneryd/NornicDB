package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every relationship validator is preparation before publication, including
// the cold derived equality index and its committed-holder collection.
func TestRelationshipCommitPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"validateEdgeConstraints", "checkEdgeUniqueness", "relationshipUniqueCandidatesWithContext", "relationshipUniqueCandidatesWithContext.func1", "checkEdgeTemporalConstraint", "checkEdgeCardinality", "checkEdgePolicy"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) *ExecuteResult {
				t.Helper()
				r, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
				return r
			}
			run("CREATE (:A {k:0}), (:B {k:0})")
			run("MATCH (a:A), (b:B) UNWIND range(1,20) AS i CREATE (a)-[:R {k:i, start:1, end:2}]->(b)")
			prepare := func() {
				run("BEGIN")
				run("MATCH (a:A), (b:B) UNWIND range(21,40) AS i CREATE (a)-[:R {k:i, start:1, end:2}]->(b)")
			}
			prepare()
			// Install through the public schema API after staging, so COMMIT
			// is the first use of the equality cache, not a warmed retry.
			c := storage.Constraint{Name: "rel_prep", EntityType: storage.ConstraintEntityRelationship, Label: "R"}
			switch owner {
			case "checkEdgeTemporalConstraint":
				c.Type, c.Properties = storage.ConstraintTemporal, []string{"k", "start", "end"}
			case "checkEdgeCardinality":
				c.Type, c.Direction, c.MaxCount = storage.ConstraintCardinality, "OUTGOING", 100
			case "checkEdgePolicy":
				c.Type, c.PolicyMode, c.SourceLabel, c.TargetLabel = storage.ConstraintPolicy, "ALLOWED", "A", "B"
			default:
				c.Type, c.Properties = storage.ConstraintUnique, []string{"k"}
			}
			require.NoError(t, b.GetSchemaForNamespace("test").AddConstraint(c))
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			_, err = e.Execute(ctx, "COMMIT", nil)
			t.Logf("owner=%s hits=%d commit=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			require.Len(t, run("MATCH ()-[r:R]->() RETURN r.k").Rows, 20, "preparation must not publish")
			prepare()
			run("COMMIT")
			require.Len(t, run("MATCH ()-[r:R]->() RETURN r.k").Rows, 40)
		})
	}
}
