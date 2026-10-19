package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// boundedWorkClock is the row count a stage's statement evaluates, read
// through the test function bound.tick(x), which returns x. At row trigger
// it ends the statement: it waits for the context's deadline, cancels the
// context (a client cancelling), or terminates the statement's transaction
// (TERMINATE TRANSACTIONS). Rows evaluated after that are counted apart. A
// stage's workers may tick at once.
type boundedWorkClock struct {
	ctx     context.Context
	end     func()
	trigger int
	mu      sync.Mutex
	ended   bool
	rows    int
	after   int
}

func (c *boundedWorkClock) tick(x interface{}) interface{} {
	c.mu.Lock()
	if c.ended {
		c.after++
		c.mu.Unlock()
		return x
	}
	c.rows++
	reached := c.rows == c.trigger
	c.mu.Unlock()
	if reached {
		c.end()
		c.mu.Lock()
		c.ended = true
		c.mu.Unlock()
	}
	return x
}

func (c *boundedWorkClock) counts() (rows, after int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rows, c.after
}

// terminateStatement terminates the running transaction whose statement is
// query, as TERMINATE TRANSACTIONS does.
func terminateStatement(query string) {
	for _, tx := range runningTransactions.snapshot() {
		tx.mu.Lock()
		running := tx.query == query
		tx.mu.Unlock()
		if running {
			runningTransactions.terminate(tx.id())
		}
	}
}

// TestStatementStopsEachStageWithinBoundedWork runs one statement per
// pipeline stage through Execute (DISTINCT, ORDER BY, aggregation,
// projection, UNWIND, traversal, WHERE). Each evaluates bound.tick once per
// row it handles. Once the statement's deadline passes, its client cancels
// it or TERMINATE TRANSACTIONS stops it, the statement returns that error
// having evaluated at most a few polling intervals of rows more, never the
// rest of its input. Each statement runs twice, so both the first
// (text-read) and the compiled evaluation are held to the bound.
func TestStatementStopsEachStageWithinBoundedWork(t *testing.T) {
	const (
		people  = 20000
		trigger = 200
		// Rows a stage may evaluate after its context ended: a few probe
		// intervals (pipelineCancellationInterval), against the
		// people - trigger rows it would evaluate if it never looked.
		bound = 2048
	)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bounded")
	for i := 0; i < people; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("p-%d", i)),
			Labels:     []string{"P"},
			Properties: map[string]interface{}{"v": int64(i), "k": int64(i % 50)},
		})
		require.NoError(t, err)
	}
	for i := 0; i+1 < people; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("next-%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("p-%d", i)),
			EndNode:   storage.NodeID(fmt.Sprintf("p-%d", i+1)),
			Type:      "NEXT",
		}))
	}
	exec := NewStorageExecutor(store)

	var clock *boundedWorkClock
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "bound.tick" {
			return func(x interface{}) interface{} { return clock.tick(x) }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })

	stages := []struct{ name, cypher string }{
		{"distinct", "MATCH (n:P) RETURN DISTINCT bound.tick(n.v) AS v"},
		{"sort", "MATCH (n:P) RETURN n.v AS v ORDER BY bound.tick(n.v) DESC"},
		{"aggregation", "MATCH (n:P) RETURN n.k AS k, collect(bound.tick(n.v)) AS vs"},
		{"projection", "MATCH (n:P) RETURN bound.tick(n.v) AS v, n.k AS k"},
		{"unwind", fmt.Sprintf("UNWIND range(1, %d) AS x WITH bound.tick(x) AS y RETURN count(y) AS c", people)},
		{"traversal", "MATCH (a:P)-[:NEXT]->(b:P) WHERE bound.tick(b.v) IS NOT NULL RETURN count(b) AS c"},
		{"filter", "MATCH (n:P) WHERE bound.tick(n.v) IS NOT NULL RETURN n.v AS v"},
	}
	// Each mode ends the statement at the trigger row and names the error
	// it must then return.
	modes := []struct {
		name  string
		start func() (context.Context, context.CancelFunc)
		end   func(ctx context.Context, cancel context.CancelFunc, query string)
		check func(t *testing.T, err error)
	}{
		{"deadline",
			func() (context.Context, context.CancelFunc) { return context.WithTimeout(context.Background(), 2*time.Second) },
			func(ctx context.Context, _ context.CancelFunc, _ string) { <-ctx.Done() },
			func(t *testing.T, err error) { require.ErrorIs(t, err, context.DeadlineExceeded) }},
		{"client_cancel",
			func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			func(_ context.Context, cancel context.CancelFunc, _ string) { cancel() },
			func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) }},
		{"terminate",
			func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			func(_ context.Context, _ context.CancelFunc, query string) { terminateStatement(query) },
			func(t *testing.T, err error) {
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Transaction.Terminated", code, "%v", err)
			}},
	}
	nonce := 0
	for _, stage := range stages {
		for _, mode := range modes {
			for _, pass := range []string{"first", "compiled"} {
				t.Run(stage.name+"/"+mode.name+"/"+pass, func(t *testing.T) {
					ctx, cancel := mode.start()
					defer cancel()
					clock = &boundedWorkClock{ctx: ctx, trigger: trigger, end: func() { mode.end(ctx, cancel, stage.cypher) }}
					nonce++
					result, err := exec.Execute(ctx, stage.cypher, map[string]interface{}{"nonce": nonce})
					require.Error(t, err, "the statement returned a result after it was ended: %v", result)
					mode.check(t, err)
					rows, after := clock.counts()
					require.GreaterOrEqual(t, rows, trigger, "the statement stopped before it was ended")
					require.LessOrEqual(t, after, bound, "rows evaluated after the statement was ended")
				})
			}
		}
	}
}
