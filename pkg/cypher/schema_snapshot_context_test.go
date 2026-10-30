package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSchemaSnapshotPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"StageSchemaChangesWithContext", "cloneSchemaIndexValues", "copySliceWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) {
				t.Helper()
				_, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
			}
			run("UNWIND range(1,12) AS i CREATE (:P {k:i,l:1})")
			run("BEGIN")
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			_, err = e.Execute(ctx, "CREATE INDEX stage_p FOR (n:P) ON (n.k,n.l)", nil)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			_, exists := b.GetSchemaForNamespace("test").GetCompositeIndex("stage_p")
			require.False(t, exists)
			run("BEGIN")
			run("CREATE INDEX stage_p FOR (n:P) ON (n.k,n.l)")
			run("COMMIT")
			_, exists = b.GetSchemaForNamespace("test").GetCompositeIndex("stage_p")
			require.True(t, exists)
		})
	}
}
