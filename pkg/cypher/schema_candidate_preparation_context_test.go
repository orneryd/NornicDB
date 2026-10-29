package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSchemaCandidatePreparationCancellation(t *testing.T) {
	for _, owner := range []string{"SeekableCompositeIndexesForLabelWithContext", "uniqueMergeKeysWithContext", "ConstraintPropertyIndexWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			schema := b.GetSchemaForNamespace("test")
			for i := 0; i < 20; i++ {
				require.NoError(t, schema.AddCompositeIndex(fmt.Sprintf("foreign_%d", i), fmt.Sprintf("Other%d", i), []string{"k"}))
				if owner == "uniqueMergeKeysWithContext" {
					require.NoError(t, schema.AddUniqueConstraint(fmt.Sprintf("unique_%d", i), "P", fmt.Sprintf("key%d", i)))
				}
			}
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) *ExecuteResult {
				t.Helper()
				r, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
				return r
			}
			run("CREATE (:P {k:1})")
			query := "MERGE (n:P {k:1}) RETURN n.k"
			if owner == "ConstraintPropertyIndexWithContext" {
				query = "CREATE CONSTRAINT p_owned FOR (n:P) REQUIRE n.k IS UNIQUE"
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			result, err := e.Execute(ctx, query, nil)
			t.Logf("owner=%s hits=%d execute=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			if result != nil {
				require.Empty(t, result.Rows)
			}
			if owner == "ConstraintPropertyIndexWithContext" {
				require.False(t, schema.HasConstraintNamed("p_owned"))
			}
			run(query)
			require.Len(t, run("MATCH (n:P) RETURN n.k").Rows, 1)
		})
	}
}
