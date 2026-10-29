package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// A database selection starts its own result/lookup runtime, while an issued
// view retains its transaction's runtime. Both retain the configured behavior
// and the shared syntax-verdict owner, including the selected query parameters.
func TestExecutorCloneScopesPreserveDatabaseRuntime(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	one := storage.NewNamespacedEngine(baseStore, "clone_one")
	two := storage.NewNamespacedEngine(baseStore, "clone_two")
	base := NewStorageExecutor(one, QueryCachePolicy{MaxEntries: 17, TTL: 17 * time.Second})
	base.shellParams = map[string]interface{}{"selected": int64(5)}
	same := base.cloneWithStorage(one)
	selected := base.cloneWithStorage(two, selectedDatabaseStorage)
	require.Same(t, base.cache, same.cache)
	require.NotSame(t, base.cache, selected.cache)
	require.Same(t, base.StatementCaches(), selected.StatementCaches())
	require.Same(t, base.planCache, same.planCache)
	require.NotSame(t, base.planCache, selected.planCache)
	require.Same(t, base.nodeLookupCacheMu, same.nodeLookupCacheMu)
	require.NotSame(t, base.nodeLookupCacheMu, selected.nodeLookupCacheMu)
	maxEntries, ttl := same.QueryCachePolicy()
	require.Equal(t, 17, maxEntries)
	require.Equal(t, 17*time.Second, ttl)
	selected.shellParams["selected"] = int64(9)
	require.Equal(t, int64(5), base.shellParams["selected"])
	result, err := selected.Execute(context.Background(), "RETURN $selected AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9)}}, result.Rows)
	result, err = same.Execute(context.Background(), "RETURN $selected AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)
}

func TestStatementModeBindingUsesIssuedStorageMembership(t *testing.T) {
	owner := NewStorageExecutor(newTestMemoryEngine(t))
	_, err := owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if owner.HasActiveTransaction() {
			_, rollbackErr := owner.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, rollbackErr)
		}
	})
	decorated := storage.NewTracedEngine(owner.storage)
	state := &resolvedStatementMode{}
	bound, release := owner.cloneWithStorage(decorated).bindResolvedStatementMode(state)
	rebuilt := NewStorageExecutor(decorated)
	require.Same(t, state, rebuilt.resolvedStatementModeOf(context.Background()), "reconstruction retains the live reporting owner")
	require.Same(t, state, bound.resolvedStatementModeOf(context.Background()))
	release()
	require.Nil(t, rebuilt.resolvedStatementModeOf(context.Background()), "ending the scope restores the issued object's previous reporting owner")
	require.Nil(t, resolvedStatementModeResult(nil, true))
	result := &ExecuteResult{Metadata: map[string]interface{}{"existing": int64(7)}}
	reported := resolvedStatementModeResult(result, true)
	require.Equal(t, "w", reported.Metadata[resolvedStatementModeMetadata])
	require.NotContains(t, result.Metadata, resolvedStatementModeMetadata, "result framing leaves the incoming result unchanged")
	require.Equal(t, int64(7), reported.Metadata["existing"])
}

// The issued fragment is a removable view of the participant's database-local
// runtime. Constructing another database runtime here discards the native
// owner's caches, while sharing the controller's runtime crosses databases.
func TestFabricParticipantFragmentViewReusesIsolatedRuntime(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	e := f.compositeSession(t)
	ctx := context.Background()
	_, err := e.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if e.HasActiveTransaction() {
			_, err := e.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
	})
	_, err = e.Execute(ctx, "CALL { USE comp.one CREATE (:AcAtom {run:$run}) RETURN 1 AS w } CALL { USE comp.two MATCH (n:AcAtom) RETURN count(n) AS c } RETURN w,c", map[string]interface{}{"run": t.Name()})
	require.NoError(t, err)
	one := e.txContext.fabricLocalTxExec["local:one"]
	two := e.txContext.fabricLocalTxExec["local:two"]
	require.NotNil(t, one)
	require.NotNil(t, two)
	require.NotSame(t, e.cache, one.cache)
	require.NotSame(t, one.cache, two.cache)
	require.NotSame(t, one.nodeLookupCacheMu, two.nodeLookupCacheMu)
	require.NotSame(t, one.planCache, two.planCache)
	require.NotSame(t, one.analyzer, two.analyzer)
	require.NotSame(t, one.queryStatistics, two.queryStatistics)
	wrapper := one.txContext.storageWrapper
	statement := wrapper.statement.Load()
	require.NotNil(t, statement)
	view, viewCtx, err := statement.on(ctx, "one", e, wrapper)
	require.NoError(t, err)
	require.Same(t, wrapper, view.storage)
	require.Nil(t, view.txContext, "a fragment is never the BEGIN/terminal controller")
	require.Same(t, one.cache, view.cache)
	require.Same(t, wrapper.txNodeLookupCacheMu, view.nodeLookupCacheMu)
	require.Same(t, one.planCache, view.planCache)
	require.Same(t, one.queryStatistics, view.queryStatistics)
	require.Same(t, wrapper, view.getStorage(viewCtx))
	_, err = e.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.Zero(t, f.writes(t, "one", t.Name()))
}

func TestFabricScalarRecordViewKeepsBindingsPrivate(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	base := f.exec
	base.cache = NewSmartQueryCache(8)
	query := "WITH x AS x RETURN x + $delta AS v"
	params := map[string]interface{}{"delta": int64(10)}
	sentinel := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(-1)}}}
	base.cache.Put(query, params, sentinel, time.Hour)
	identity := &graphIdentityScope{database: "outer"}
	base.graphIdentityScope = identity
	base.fabricRecordBindings = map[string]interface{}{"x": int64(-100)}
	runner := &cypherFabricExecutor{base: base, autoCommit: true}
	for _, row := range []int64{1, 5, 2} {
		columns, rows, handled, err := runner.ExecuteRecordQuery(context.Background(), "one", query, params, map[string]interface{}{"x": row})
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, []string{"v"}, columns)
		require.Equal(t, [][]interface{}{{row + 10}}, rows)
		require.Same(t, identity, base.graphIdentityScope)
		require.Equal(t, int64(-100), base.fabricRecordBindings["x"])
	}
	cached, found := base.cache.Get(query, params)
	require.True(t, found)
	require.Equal(t, sentinel.Rows, cached.Rows, "internal records neither consult nor replace outer result-cache entries")
	_, rows, handled, err := runner.ExecuteRecordQuery(context.Background(), "two", query, map[string]interface{}{"delta": int64(20)}, map[string]interface{}{"x": int64(3)})
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, [][]interface{}{{int64(23)}}, rows)
	for _, graphQuery := range []string{"WITH 1 AS seed RETURN COUNT { (n:AcAtom) } AS c", "WITH $r AS r RETURN startNode(r) AS n"} {
		_, _, handled, err := runner.ExecuteRecordQuery(context.Background(), "one", graphQuery, nil, nil)
		require.NoError(t, err)
		require.False(t, handled, "graph-dependent expressions retain the native dispatch owner")
	}
}
