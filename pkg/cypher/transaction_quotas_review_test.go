package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionQuotaUnlimitedDisjointMergeCommits(t *testing.T) {
	exec, engine, _ := quotaExecutor(t, nil)
	_, err := exec.Execute(context.Background(), "CREATE CONSTRAINT review_key IF NOT EXISTS FOR (n:ReviewMerge) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	const writers = 16
	executors := make([]*StorageExecutor, writers)
	for i := range executors {
		executors[i] = NewStorageExecutor(engine)
		_, err = executors[i].Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		_, err = executors[i].Execute(context.Background(), "MERGE (:ReviewMerge {id: $id})", map[string]any{"id": i})
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for _, e := range executors {
		wg.Add(1)
		go func(e *StorageExecutor) {
			defer wg.Done()
			_, err := e.Execute(context.Background(), "COMMIT", nil)
			errs <- err
		}(e)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "disjoint unlimited MERGEs must not manufacture admission conflicts")
	}
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, writers, count)
}

func TestTransactionQuotaExplicitCommitUsesCallerContext(t *testing.T) {
	exec, engine, _ := quotaExecutor(t, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 2}})
	_, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "CREATE (:Review)", nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The context reaches the explicit commit owner, including the script path.
	_, err = exec.handleCommit(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, exec.HasActiveTransaction())
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
}

// Both branches use this exact benchmark. One executor per worker avoids
// serializing through a shared Cypher session; IDs and unique MERGE keys are disjoint.
func BenchmarkTransactionQuotaParallel(b *testing.B) {
	for _, merge := range []bool{false, true} {
		b.Run(fmt.Sprintf("merge=%v", merge), func(b *testing.B) {
			base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			manager, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(b, err)
			b.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
			require.NoError(b, manager.CreateDatabase("parallel"))
			engine, err := manager.GetStorage("parallel")
			require.NoError(b, err)
			if merge {
				_, err = NewStorageExecutor(engine).Execute(context.Background(), "CREATE CONSTRAINT bench_key IF NOT EXISTS FOR (n:ParallelQuota) REQUIRE n.id IS UNIQUE", nil)
				require.NoError(b, err)
			}
			query := "CREATE (:ParallelQuota {id: $id})"
			if merge {
				query = "MERGE (:ParallelQuota {id: $id})"
			}
			var id atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				exec := NewStorageExecutor(engine)
				for pb.Next() {
					_, err := exec.Execute(context.Background(), query, map[string]any{"id": id.Add(1)})
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
