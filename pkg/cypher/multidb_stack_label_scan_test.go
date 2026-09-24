package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestMultidbStack_LabelScanScalesWithLabelNotStore reproduces a regression
// that only appears on the server storage stack: a NornicDB Bolt/HTTP server
// always serves a database-scoped engine through
// multidb.DatabaseManager.GetStorage, which wraps the namespaced engine in
// an unexported sizeTrackingEngine. That wrapper embeds only storage.Engine,
// so it must forward every optional storage interface explicitly; before it
// forwarded storage.ProjectedLabelNodeReader, collectNodesWithStreaming's
// label-indexed fast path in match_multi.go silently fell through to a
// whole-namespace storage.StreamNodes scan for every label-scoped MATCH. An
// in-process StorageExecutor built directly over storage.NamespacedEngine
// (skipping multidb) never exercises this, which is why the regression only
// showed up against a running server (upstream reference: eshu#7014 cause A).
//
// This test builds the exact same stack the server builds
// (DatabaseManager -> GetStorage -> sizeTrackingEngine) and measures
// `MATCH (r:Repository) RETURN count(r)` at 0, 2000 and 20000 unrelated
// decoy nodes. With the fix, wall time stays flat because the scan is
// label-indexed; without it, wall time scales with total store size (see
// the "before" timings in the fix's commit message).
func TestMultidbStack_LabelScanScalesWithLabelNotStore(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	manager, err := multidb.NewDatabaseManager(base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager: %v", err)
	}
	const dbName = "nornic"
	dbStore, err := manager.GetStorage(dbName)
	if err != nil {
		t.Fatalf("GetStorage(%q): %v", dbName, err)
	}
	if _, ok := dbStore.(storage.ProjectedLabelNodeReader); !ok {
		t.Fatalf("server storage stack for %q does not forward storage.ProjectedLabelNodeReader; the label-scan regression is unfixed", dbName)
	}

	// Disable the query-result cache (maxEntries=0): the whole point of this
	// repro is to measure the storage scan, and a warm cache would return
	// the prior result in ~1us regardless of engine behavior.
	exec := NewStorageExecutorWithQueryCachePolicy(dbStore, 0, 0)
	ctx := context.Background()

	// 20 Repository nodes among a growing population of unrelated nodes.
	for i := 0; i < 20; i++ {
		if _, err := exec.Execute(ctx, fmt.Sprintf("CREATE (:Repository {id: 'r%d'})", i), nil); err != nil {
			t.Fatalf("seed Repository %d: %v", i, err)
		}
	}

	measure := func(n int) time.Duration {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (r:Repository) RETURN count(r) AS total", nil)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("count at n=%d: %v", n, err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatalf("count at n=%d: expected one row/one column, got %#v", n, result.Rows)
		}
		got := result.Rows[0][0]
		if fmt.Sprintf("%v", got) != "20" {
			t.Fatalf("count at n=%d: expected 20, got %#v", n, got)
		}
		return elapsed
	}

	const batch = 1000
	created := 0
	for _, target := range []int{0, 2000, 20000} {
		for created < target {
			batchSize := batch
			if target-created < batchSize {
				batchSize = target - created
			}
			for i := 0; i < batchSize; i++ {
				if _, err := exec.Execute(ctx, fmt.Sprintf("CREATE (:Unrelated {uid: 'u%d'})", created+i), nil); err != nil {
					t.Fatalf("seed Unrelated %d: %v", created+i, err)
				}
			}
			created += batchSize
		}
		elapsed := measure(target)
		t.Logf("label count at %d unrelated nodes: %s", target, elapsed)
	}
}
