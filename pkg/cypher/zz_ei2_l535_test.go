package cypher

// Batch 2 takeover L535(b)(c) (research only, untracked): the #1753 round-9
// reviewer's reproductions with the FAILED population built the way the final
// build fails one (a service vector budget the population exceeds).

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzei2FailedPopulation(t *testing.T) (*StorageExecutor, storage.Engine, *search.Service) {
	t.Helper()
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]}), (:Doc {id:'b', emb:[0.0,1.0,0.0]})", nil)
	require.NoError(t, err)
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
	t.Cleanup(func() { _ = svc.Close() })
	t.Logf("ZZEI2 L535 setup: service build err=%v", svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	t.Logf("ZZEI2 L535 setup: CREATE VECTOR INDEX -> err=%v", err)
	state, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
	require.NoError(t, err)
	t.Logf("ZZEI2 L535 setup: SHOW VECTOR INDEXES -> %v", state.Rows)
	require.Equal(t, [][]interface{}{{"docs", "FAILED"}}, state.Rows, "setup must build a FAILED population")
	_, qerr := exec.Execute(ctx, fmt.Sprintf(incarnationQuery, "docs"), nil)
	t.Logf("ZZEI2 L535 setup: query of the FAILED index -> err=%v", qerr)
	return exec, store, svc
}

func zzei2ShowAndQuery(t *testing.T, exec *StorageExecutor) (any, any, error) {
	shown, err := exec.Execute(context.Background(), "SHOW VECTOR INDEXES YIELD name,state,failureMessage RETURN name,state,failureMessage", nil)
	require.NoError(t, err)
	rows, qerr := exec.Execute(context.Background(), fmt.Sprintf(incarnationQuery, "docs"), nil)
	var got any
	if rows != nil {
		got = rows.Rows
	}
	return shown.Rows, got, qerr
}

func TestZZEI2L535bRestartAfterFailedPopulation(t *testing.T) {
	for _, mode := range []string{"new-service-build-fails", "new-service-build-succeeds"} {
		t.Run(mode, func(t *testing.T) {
			exec, store, svc := zzei2FailedPopulation(t)
			require.NoError(t, svc.Close())
			opts := &search.ServiceOptions{VectorMemoryMaxBytes: 30}
			if mode == "new-service-build-succeeds" {
				opts = nil
			}
			next := search.NewService(store, 3, opts)
			t.Cleanup(func() { _ = next.Close() })
			buildErr := next.BuildIndexes(context.Background())
			exec.SetSearchService(next)
			shown, got, qerr := zzei2ShowAndQuery(t, exec)
			t.Logf("ZZEI2 L535b %s: rebuild err=%v; SHOW=%v; query err=%v rows=%v", mode, buildErr, shown, qerr, got)
		})
	}
}

func TestZZEI2L535cRejectedRepeatedRecreate(t *testing.T) {
	exec, store, svc := zzei2FailedPopulation(t)
	ctx := context.Background()
	previous, _, _ := zzei2ShowAndQuery(t, exec)
	for _, q := range []string{"BEGIN", "DROP INDEX docs", "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions, "DROP INDEX docs", "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	other := NewStorageExecutor(store)
	other.SetSearchService(svc)
	for _, q := range []string{"CREATE VECTOR INDEX other FOR (n:Other) ON (n.vec)" + incarnationVectorOptions, "DROP INDEX other"} {
		_, err := other.Execute(ctx, q, nil)
		t.Logf("ZZEI2 L535c competing schema commit %q -> err=%v", q, err)
	}
	_, commitErr := exec.Execute(ctx, "COMMIT", nil)
	shown, got, qerr := zzei2ShowAndQuery(t, other)
	require.ErrorContains(t, commitErr, "Transaction Conflict")
	require.Equal(t, previous, shown)
	require.ErrorContains(t, qerr, "FAILED")
	t.Logf("ZZEI2 L535c COMMIT err=%v; after: SHOW=%v; query err=%v rows=%v", commitErr, shown, qerr, got)
	// control: one DROP + CREATE (not repeated) rejected the same way
	exec2, store2, svc2 := zzei2FailedPopulation(t)
	for _, q := range []string{"BEGIN", "DROP INDEX docs", "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions} {
		_, err := exec2.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	other2 := NewStorageExecutor(store2)
	other2.SetSearchService(svc2)
	for _, q := range []string{"CREATE VECTOR INDEX other FOR (n:Other) ON (n.vec)" + incarnationVectorOptions, "DROP INDEX other"} {
		_, _ = other2.Execute(ctx, q, nil)
	}
	_, commitErr2 := exec2.Execute(ctx, "COMMIT", nil)
	shown2, got2, qerr2 := zzei2ShowAndQuery(t, other2)
	t.Logf("ZZEI2 L535c control single recreate: COMMIT err=%v; after: SHOW=%v; query err=%v rows=%v", commitErr2, shown2, qerr2, got2)
}
