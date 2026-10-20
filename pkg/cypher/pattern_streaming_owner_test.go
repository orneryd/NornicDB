package cypher

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Generate scan rows on demand so storage setup and storage snapshots cannot
// hide buffering in the public query pipeline.
type patternPhaseScanEngine struct {
	*storage.MemoryEngine
	size    int
	visited atomic.Int64
}

func (s *patternPhaseScanEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*storage.Node) error) error {
	if label != "Item" {
		return s.MemoryEngine.StreamNodesByLabelProjectedInScope(scope, label, properties, visit)
	}
	for i := 0; i < s.size; i++ {
		s.visited.Add(1)
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("%sitem%d", scope, i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"k": int64(i), "bucket": int64(1)}}
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func TestPatternReadStreamsBeforeFullScan(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:Item) RETURN n.k AS k",
		"MATCH (n:Item {bucket:inspection.value()}) RETURN n.k AS k",
		"MATCH (n:Item) WITH n MATCH (n) RETURN n.k AS k",
	} {
		t.Run(query, func(t *testing.T) {
			var calls atomic.Int64
			withPluginFunctions(t, map[string]interface{}{"inspection.value": func() int64 { calls.Add(1); return 1 }})
			// Initialize the parser and exact query path on a separate tiny
			// fixture. The budget below measures streaming, not cold startup.
			warmStore := &patternPhaseScanEngine{MemoryEngine: newTestMemoryEngine(t), size: 1}
			warm := NewStorageExecutor(storage.NewNamespacedEngine(warmStore, "phase_stream"))
			_, warmErr := warm.Execute(context.Background(), query, nil)
			require.NoError(t, warmErr)
			calls.Store(0)
			store := &patternPhaseScanEngine{MemoryEngine: newTestMemoryEngine(t), size: 50000}
			exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "phase_stream"))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			run := executeStreamed(ctx, exec, query, 8)
			defer func() { cancel(); <-run.stream.Done() }()
			select {
			case <-run.stream.Started():
			default:
				t.Fatalf("read did not stream: %v", run.err)
			}
			runtime.ReadMemStats(&after)
			require.LessOrEqual(t, store.visited.Load(), int64(64), "first row must precede the full scan")
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20), "unconsumed rows must not be buffered")
			require.Less(t, after.Mallocs-before.Mallocs, uint64(20000), "allocations must follow consumed rows")
			rows, done := run.stream.Next(2)
			require.False(t, done)
			require.Len(t, rows, 2)
			require.LessOrEqual(t, store.visited.Load(), int64(64))
			if query == "MATCH (n:Item {bucket:inspection.value()}) RETURN n.k AS k" {
				require.Equal(t, int64(1), calls.Load())
			} else {
				require.Zero(t, calls.Load())
			}
		})
	}
}

func TestEmptyPatternReadsHaveStatistics(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, query := range []string{
			"MATCH (n:Source) RETURN n",
			"MATCH (n:Source) RETURN n.id",
			"MATCH (n:Source {id:$id}) RETURN n.id",
			"MATCH (n:Source {id:$id}) RETURN n.id LIMIT 2",
			"MATCH (n:Source) WHERE n.id = $id RETURN n.id",
			"UNWIND [] AS wanted MATCH (n:Source {id:wanted}) RETURN n.id",
		} {
			t.Run(fmt.Sprintf("transaction=%v/%s", explicit, query), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "phase_stats"))
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer exec.Execute(ctx, "ROLLBACK", nil)
				}
				result, err := exec.Execute(ctx, query, map[string]interface{}{"id": int64(1)})
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Empty(t, result.Rows)
				require.NotNil(t, result.Stats)
				require.Equal(t, &QueryStats{}, result.Stats)
			})
		}
	}
}
