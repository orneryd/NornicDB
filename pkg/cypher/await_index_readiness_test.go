package cypher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// An index procedure must not report completion while SHOW INDEXES still
// reports the same index as POPULATING during startup.
func TestAwaitIndexProceduresDoNotReturnDuringStartupPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := t.Context()
		store := newTestBadgerNamespacedStore(t)
		writer := NewStorageExecutor(store)
		_, err := writer.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
		require.NoError(t, err)

		service := search.NewService(store, 3, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		reader := NewStorageExecutor(store)
		reader.SetSearchService(service)

		shown, err := reader.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"docs", "POPULATING"}}, shown.Rows)
		for _, statement := range []string{
			"CALL db.awaitIndexes(0)",
			"CALL db.awaitIndex('docs', 0)",
		} {
			_, err := reader.Execute(ctx, statement, nil)
			require.ErrorContains(t, err, "online", statement)
		}
		for _, statement := range []string{
			"CALL db.awaitIndexes(120)",
			"CALL db.awaitIndex('docs', 120)",
		} {
			waitCtx, cancel := context.WithCancel(ctx)
			cancelAfter := time.AfterFunc(20*time.Millisecond, cancel)
			_, err := reader.Execute(waitCtx, statement, nil)
			cancelAfter.Stop()
			cancel()
			require.ErrorIs(t, err, context.Canceled, statement)
		}

		require.NoError(t, service.BuildIndexes(ctx))
		shown, err = reader.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"docs", "ONLINE"}}, shown.Rows)
		for _, statement := range []string{
			"CALL db.awaitIndexes(0)",
			"CALL db.awaitIndex('docs', 0)",
		} {
			result, err := reader.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
			require.Empty(t, result.Columns)
			require.Empty(t, result.Rows)
		}
		_, err = writer.Execute(ctx, "DROP INDEX docs", nil)
		require.NoError(t, err)
		_, err = reader.Execute(ctx, "CALL db.awaitIndex('docs', 0)", nil)
		require.ErrorContains(t, err, "No such index 'docs'")
	})
}

func TestAwaitIndexProceduresReportFailedPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := t.Context()
		store := newTestBadgerNamespacedStore(t)
		writer := NewStorageExecutor(store)
		_, err := writer.Execute(ctx, "CREATE (:Doc {emb:[1.0,0.0,0.0]}), (:Doc {emb:[0.0,1.0,0.0]})", nil)
		require.NoError(t, err)
		_, err = writer.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
		require.NoError(t, err)

		service := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		require.ErrorContains(t, service.BuildIndexes(ctx), "index memory budget exceeded")
		reader := NewStorageExecutor(store)
		reader.SetSearchService(service)
		shown, err := reader.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"docs", "FAILED"}}, shown.Rows)

		for _, statement := range []string{
			"CALL db.awaitIndexes(0)",
			"CALL db.awaitIndex('docs', 0)",
		} {
			_, err := reader.Execute(ctx, statement, nil)
			require.ErrorContains(t, err, "FAILED", statement)
			require.ErrorContains(t, err, "index memory budget exceeded", statement)
		}
	})
}

// A newly published index may use the storage fallback when its dimensions
// differ from the search service. Awaiting it must start a lazy service build;
// otherwise the index has no population record and remains POPULATING forever.
func TestAwaitIndexesWarmsUnservedLazyVectorIndex(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := t.Context()
		store := newTestBadgerNamespacedStore(t)
		service := search.NewService(store, 3, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		var warmed atomic.Int32
		buildErr := make(chan error, 1)
		service.SetLazyWarming(true, func() {
			warmed.Add(1)
			buildErr <- service.BuildIndexes(service.LifetimeContext())
		})
		exec := NewStorageExecutor(store)
		exec.SetSearchService(service)
		_, err := exec.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:4,`vector.similarity_function`:'cosine'}}", nil)
		require.NoError(t, err)
		state, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"docs", "POPULATING"}}, state.Rows)
		require.Zero(t, warmed.Load())

		result, err := exec.Execute(ctx, "CALL db.awaitIndexes(1)", nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns)
		require.Empty(t, result.Rows)
		require.EqualValues(t, 1, warmed.Load())
		require.NoError(t, <-buildErr)
		state, err = exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"docs", "ONLINE"}}, state.Rows)
	})
}
