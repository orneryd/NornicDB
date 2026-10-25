package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDDLIndexFillPreparationCancellation(t *testing.T) {
	for _, query := range []string{
		"CREATE INDEX fill_p FOR (n:P) ON (n.k, n.l)",
		"CREATE CONSTRAINT fill_p FOR (n:P) REQUIRE n.k IS UNIQUE",
		"CREATE CONSTRAINT fill_p FOR (n:P) REQUIRE (n.k, n.l) IS NODE KEY",
		"CREATE CONSTRAINT fill_p FOR (n:P) REQUIRE { n.k IS UNIQUE }",
		"CREATE CONSTRAINT fill_p FOR (n:P) REQUIRE { (n.k, n.l) IS NODE KEY }",
	} {
		t.Run(query, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			_, err = e.Execute(context.Background(), "UNWIND range(1,12) AS i CREATE (:P {k:i, l:1})", nil)
			require.NoError(t, err)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: "BackfillCompositeIndexWithContext", cancel: cancel}
			_, err = e.Execute(ctx, query, nil)
			require.GreaterOrEqual(t, ctx.hits, 2, "cancel inside the entry-fill owner")
			require.ErrorIs(t, err, context.Canceled)
			schema := b.GetSchemaForNamespace("test")
			require.False(t, schema.HasConstraintNamed("fill_p"))
			_, exists := schema.GetCompositeIndex("fill_p")
			require.False(t, exists, "failed fill must remove the partial index")
			_, err = e.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			result, err := e.Execute(context.Background(), "MATCH (n:P {k:12, l:1}) RETURN n.k", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
		})
	}
}
