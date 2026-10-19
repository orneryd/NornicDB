package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The context arranges cancellation inside real library work, without a
// production test hook, sleeping query, or parser-compilation measurement.
// Library tests separately count every work unit and enumerate every loop.
type louvainPhaseContext struct {
	context.Context
	phase string
	end func()
	once sync.Once
	mu sync.Mutex
	armed bool
}

func (ctx *louvainPhaseContext) Err() error {
	var callers [48]uintptr
	count := runtime.Callers(1, callers[:])
	frames := runtime.CallersFrames(callers[:count])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "/pkg/louvain.") && strings.Contains(frame.Function, ctx.phase) {
			ctx.once.Do(func() {
				ctx.mu.Lock()
				ctx.armed = true
				ctx.mu.Unlock()
				ctx.end()
			})
			break
		}
		if !more { break }
	}
	return ctx.Context.Err()
}

func (ctx *louvainPhaseContext) wasArmed() bool {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.armed
}

func TestStatementStopsLouvainInsideSharedKernel(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bounded_louvain")
	const population, clusterSize = 256, 8
	for node := 0; node < population; node++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint(node)), Labels: []string{"BoundLouvain"}})
		require.NoError(t, err)
	}
	for start := 0; start < population; start += clusterSize {
		for left := start; left < start+clusterSize; left++ {
			for right := left+1; right < start+clusterSize; right++ {
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("%d-%d", left, right)), StartNode: storage.NodeID(fmt.Sprint(left)), EndNode: storage.NodeID(fmt.Sprint(right)), Type: "LINK"}))
			}
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	// Prepare the catalog and warm parser syntax before a test deadline exists.
	_, err := exec.Execute(context.Background(), "CALL gds.graph.project('bound-louvain','BoundLouvain','LINK')", nil)
	require.NoError(t, err)
	queries := []string{
		"CALL gds.louvain.stream('bound-louvain') YIELD nodeId,communityId RETURN communityId",
		"CALL apoc.algo.louvain('BoundLouvain') YIELD node,community RETURN community",
	}
	for queryIndex, query := range queries {
		for _, phase := range []string{"louvain.run", ").moveNodes", ").neighbours", ").colourClasses", "louvain.sortInts", ").modularity", ").aggregate"} {
			for _, mode := range []string{"client_cancel", "deadline", "terminate"} {
				t.Run(fmt.Sprintf("procedure=%d/%s/%s", queryIndex, phase, mode), func(t *testing.T) {
					var parent context.Context
					var cancel context.CancelFunc
					if mode == "deadline" { parent, cancel = context.WithTimeout(context.Background(), 2*time.Second) } else { parent, cancel = context.WithCancel(context.Background()) }
					defer cancel()
					ctx := &louvainPhaseContext{Context: parent, phase: phase}
					ctx.end = func() {
						switch mode {
						case "client_cancel": cancel()
						case "deadline": <-parent.Done()
						case "terminate": terminateStatement(query)
						}
					}
					result, err := exec.Execute(ctx, query, nil)
					require.True(t, ctx.wasArmed(), "must end the statement during the requested graph phase, not preparation")
					require.Error(t, err, "a cancelled kernel must not return partial rows: %v", result)
					switch mode {
					case "client_cancel": require.ErrorIs(t, err, context.Canceled)
					case "deadline": require.ErrorIs(t, err, context.DeadlineExceeded)
					case "terminate": code, _ := nornicerrors.Neo4jStatus(err); require.Equal(t, "Neo.ClientError.Transaction.Terminated", code, "%v", err)
					}
				})
			}
		}
	}
}
