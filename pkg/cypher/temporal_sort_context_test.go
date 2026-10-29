package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalDDLSortPreparationCancellation(t *testing.T) {
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for i := 0; i < 20; i++ {
		start := time.Date(2026, 1, 1, 0, i*2, 0, 0, time.UTC)
		_, err := b.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:f%d", i)), Labels: []string{"Fact"}, Properties: map[string]interface{}{"record": int64(1), "start": start, "end": start.Add(time.Minute)}})
		require.NoError(t, err)
	}
	e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &ddlPreparationContext{Context: parent, owner: "sortIndexCandidatesWithContext", cancel: cancel}
	query := "CREATE CONSTRAINT fact_t FOR (n:Fact) REQUIRE (n.record,n.start,n.end) IS TEMPORAL NO OVERLAP"
	_, err = e.Execute(ctx, query, nil)
	require.GreaterOrEqual(t, ctx.hits, 2, "cancel in the interval sort, after collection")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, b.GetSchemaForNamespace("test").HasConstraintNamed("fact_t"))
	_, err = e.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.True(t, b.GetSchemaForNamespace("test").HasConstraintNamed("fact_t"))
}
