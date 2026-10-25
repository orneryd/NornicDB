package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalLookupPreparationCancellation(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
	run := func(q string) *ExecuteResult {
		t.Helper()
		r, err := e.Execute(context.Background(), q, nil)
		require.NoError(t, err, q)
		return r
	}
	run("CREATE CONSTRAINT fact_t FOR (n:Fact) REQUIRE (n.record,n.start,n.end) IS TEMPORAL NO OVERLAP")
	run("UNWIND range(1,12) AS i CREATE (:Fact {record:[i],start:datetime('2026-01-01T00:00:00Z'),end:datetime('2026-02-01T00:00:00Z')})")
	query := "CALL db.temporal.asOf('Fact','record',[1],'start','end',datetime('2026-01-15T00:00:00Z')) YIELD node RETURN node.record AS record"
	require.Len(t, run(query).Rows, 1)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &ddlPreparationContext{Context: parent, owner: "currentTemporalNodeByScanInNamespace", cancel: cancel}
	_, err = e.Execute(ctx, query, nil)
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, run(query).Rows, 1)
}
