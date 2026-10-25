package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCDCPagePreparationCancellation(t *testing.T) {
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetChangeCapture("test", storage.ChangeCaptureFull)
	e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
	start := cdcID(t, e, "db.cdc.current")
	_, err = e.Execute(context.Background(), "UNWIND range(1,20) AS i CREATE (:P {k:i})", nil)
	require.NoError(t, err)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &ddlPreparationContext{Context: parent, owner: "ChangeEventsWithContext.func1", cancel: cancel}
	result, err := e.Execute(ctx, "CALL db.cdc.query($from, [])", map[string]interface{}{"from": start})
	t.Logf("owner=%s hits=%d execute=%v", ctx.owner, ctx.hits, err)
	require.GreaterOrEqual(t, ctx.hits, 2, "cancel during storage page collection")
	require.ErrorIs(t, err, context.Canceled)
	if result != nil {
		require.Empty(t, result.Rows)
	}
	require.Len(t, cdcRows(t, e, start), 20, "read cancellation must retain the complete log")
}
