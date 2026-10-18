package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineParallelFilterCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parallel_filter_budget"))
	previous := GetParallelConfig()
	defer SetParallelConfig(previous)
	config := previous
	config.Enabled = true
	config.MinBatchSize = 1
	config.MaxWorkers = 4
	SetParallelConfig(config)
	items := make([]interface{}, 1025)
	for i := range items {
		items[i] = int64(i)
	}
	nodes := make([]*storage.Node, 10000)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprint(i)), Properties: map[string]interface{}{"items": items}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ctx = withPipelineCancellation(withExpressionFailureSlot(ctx))
	deadline, _ := ctx.Deadline()
	var id atomic.Uint64
	done := make(chan []*storage.Node, 1)
	go func() {
		id.Store(pipelineCurrentGoroutine())
		done <- executor.filterNodes(ctx, nodes, "n", "size([x IN n.items | x+1])>0")
	}()
	select {
	case result := <-done:
		require.Nil(t, result)
	case <-time.After(time.Second):
		t.Fatal("parallel filter workers did not exit")
	}
	require.ErrorIs(t, getExpressionFailure(ctx), context.DeadlineExceeded)
	require.Less(t, time.Since(deadline), 250*time.Millisecond)
	assertPipelineQueryExited(t, id.Load())
	healthy, release := context.WithTimeout(context.Background(), time.Second)
	defer release()
	require.Len(t, executor.filterNodes(healthy, nodes[:2], "n", "size([x IN n.items | x+1])>0"), 2)
}

func TestPipelineParallelCallTailCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parallel_call_budget"))
	seed := &ExecuteResult{Columns: []string{"v"}, Rows: make([][]interface{}, 1025)}
	for i := range seed.Rows {
		seed.Rows[i] = []interface{}{int64(i)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ctx = withPipelineCancellation(withExpressionFailureSlot(ctx))
	deadline, _ := ctx.Deadline()
	var id atomic.Uint64
	done := make(chan error, 1)
	go func() {
		id.Store(pipelineCurrentGoroutine())
		_, _, err := executor.executeCallTailParallel(ctx, seed, "RETURN reduce(s=0,x IN range(1,1000000000) | s+x+v) AS out", []int{0}, []string{"out"})
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("parallel CALL workers did not exit")
	}
	require.Less(t, time.Since(deadline), 250*time.Millisecond)
	assertPipelineQueryExited(t, id.Load())
}

func TestPipelineParallelCallTailErrorOrder(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parallel_error_order"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{"text"}, {int64(0)}, {int64(1)}}}
	_, handled, err := executor.executeCallTailParallel(ctx, seed, "RETURN 1/v AS out", []int{0}, []string{"out"})
	require.True(t, handled)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code, "parallel completion order must not replace the first row's error")
}

func TestPipelineParallelFragmentCancellation(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "parallel_fragment_budget")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ctx = withPipelineCancellation(withExpressionFailureSlot(ctx))
	parent := pipelineCancellationForContext(ctx)
	deadline, _ := ctx.Deadline()
	done := make(chan error, 4)
	var ids [4]atomic.Uint64
	for i := range ids {
		go func() {
			ids[i].Store(pipelineCurrentGoroutine())
			executor := NewStorageExecutor(engine)
			adapter := cypherFabricExecutor{base: executor}
			_, _, err := adapter.executeFragmentQuery(ctx, executor, "RETURN reduce(s=0,x IN range(1,1000000000) | s+x) AS total", nil)
			done <- err
		}()
	}
	for range ids {
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(time.Second):
			t.Fatal("fragment worker did not exit")
		}
	}
	require.Less(t, time.Since(deadline), 250*time.Millisecond)
	require.NoError(t, parent.err)
	require.Zero(t, parent.remaining, "fragment workers mutated their parent's probe")
	for i := range ids {
		assertPipelineQueryExited(t, ids[i].Load())
	}
}
