package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The stream's own edge cases: a threshold below 1 is 1, and a cancelled
// context stops a statement waiting for demand or for the consumer to take
// a batch.
func TestResultStreamStopsOnCancelledContext(t *testing.T) {
	stream := NewResultStream(0, nil)
	require.Equal(t, 1, stream.threshold)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, stream.emit(ctx, []interface{}{1}))
	require.True(t, stream.emit(ctx, []interface{}{2}))
	require.True(t, stream.streamed())
	require.False(t, stream.emit(ctx, []interface{}{3}), "waiting for demand")
	stream.want = 1
	require.False(t, stream.emit(ctx, []interface{}{4}), "waiting to hand over a batch")
}

func TestVisitNodeListStopsAndFails(t *testing.T) {
	nodes := []*storage.Node{{ID: "a"}, {ID: "b"}}
	visited := 0
	require.NoError(t, visitNodeList(nodes, func(*storage.Node) error {
		visited++
		return storage.ErrIterationStopped
	}))
	require.Equal(t, 1, visited)
	failure := errors.New("visit failed")
	require.ErrorIs(t, visitNodeList(nodes, func(*storage.Node) error { return failure }), failure)
}

// A streamed MATCH reads an indexed seed or a property-filtered label scan
// as it does without a stream, and fails on a scan error.
func TestResultStreamMatchSeeds(t *testing.T) {
	exec, engine := newStreamTestExecutor(t, 1)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Item {k: 5, s: 'x'}), (:Item {k: 5, s: 'x'}), (:Item {k: 5, s: 'y'}), (:Item {k: 6, s: 'x'})", nil)
	require.NoError(t, err)
	streamedRows := func(query string) [][]interface{} {
		run := executeStreamed(ctx, exec, query, 1)
		<-run.stream.Started()
		rows, done := run.stream.Next(-1)
		require.True(t, done, query)
		<-run.stream.Done()
		require.NoError(t, run.err, query)
		return rows
	}
	require.Len(t, streamedRows("MATCH (n:Item {k: 5, s: 'x'}) RETURN n.s AS s"), 2)
	_, err = exec.Execute(ctx, "CREATE INDEX item_k FOR (n:Item) ON (n.k)", nil)
	require.NoError(t, err)
	require.Len(t, streamedRows("MATCH (n:Item {k: 5, s: 'x'}) RETURN n.s AS indexed"), 2)

	engine.failAfter.Store(3)
	run := executeStreamed(ctx, exec, "MATCH (n:Item) RETURN n.k AS k", 1)
	<-run.stream.Started()
	_, done := run.stream.Next(-1)
	require.True(t, done)
	<-run.stream.Done()
	require.ErrorContains(t, run.err, "scan failed")
}

// The RETURN's argument checks fail a streamed statement: on rows it
// receives whole, and on each row as it arrives.
func TestResultStreamChecksReturnArguments(t *testing.T) {
	exec, _ := newStreamTestExecutor(t, 20)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "MATCH (n:Item) SET n.v = n.k + 1", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {k: 99, v: 'text'})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:Item) RETURN range(1, n.v) AS r",
		"MATCH (n:Item) CALL { WITH n RETURN n.v AS v } RETURN range(1, v) AS r",
	} {
		run := executeStreamed(ctx, exec, query, 1)
		select {
		case <-run.stream.Started():
			_, done := run.stream.Next(-1)
			require.True(t, done, query)
		case <-run.stream.Done():
		}
		<-run.stream.Done()
		require.Error(t, run.err, query)
	}
}

// A streamed RETURN whose row source declines its shape declines the
// statement before any row was handed on, and fails it after.
func TestResultStreamDeclineBeforeAndAfterStreaming(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	clauses := []pipelineClause{{kind: pipelineClauseReturn, text: "RETURN x"}}
	declineAfter := func(rows int) pipelineRowSource {
		return func(yield func(pipelineRow) bool) bool {
			for i := 0; i < rows; i++ {
				if !yield(pipelineRow{"x": int64(i)}) {
					return true
				}
			}
			return false
		}
	}
	scope := map[string]struct{}{"x": {}}
	ctx := context.Background()
	columns, _, handled, err := exec.pipelineStreamReturn(ctx, NewResultStream(1, nil), nil, declineAfter(0), clauses, clauses, 0, scope, false)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, columns)

	_, _, handled, err = exec.pipelineStreamReturn(ctx, NewResultStream(1, nil), nil, declineAfter(2), clauses, clauses, 0, scope, false)
	require.True(t, handled)
	require.ErrorContains(t, err, "after it had sent rows")
}

type queryLimitChecker interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
}

type fakeQueryLimitChecker struct{ limits interface{} }

func (fakeQueryLimitChecker) CheckQueryRate() error { return nil }
func (fakeQueryLimitChecker) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return ctx, func() {}, nil
}
func (f fakeQueryLimitChecker) GetQueryLimits() interface{} { return f.limits }

type fakeMaxResults int64

func (f fakeMaxResults) GetMaxResults() int64 { return int64(f) }

type fakeLimitedEngine struct {
	storage.Engine
	checker queryLimitChecker
}

func (f fakeLimitedEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return f.checker
}

func TestMaxResultsReadsTheDatabaseLimit(t *testing.T) {
	for _, tc := range []struct {
		engine storage.Engine
		want   int64
	}{
		{newTestMemoryEngine(t), 0},
		{fakeLimitedEngine{}, 0},
		{fakeLimitedEngine{checker: fakeQueryLimitChecker{}}, 0},
		{fakeLimitedEngine{checker: fakeQueryLimitChecker{limits: struct{}{}}}, 0},
		{fakeLimitedEngine{checker: fakeQueryLimitChecker{limits: fakeMaxResults(7)}}, 7},
	} {
		require.Equal(t, tc.want, (&StorageExecutor{storage: tc.engine}).maxResults())
	}
}

// A streamed MATCH whose pattern can't be evaluated fails, and one whose
// context is cancelled stops its scan.
func TestStreamedNodeMatchSourceFailures(t *testing.T) {
	exec, _ := newStreamTestExecutor(t, 30)
	run := executeStreamed(context.Background(), exec, "MATCH (n:Item {k: 1 / 0}) RETURN n.k AS k", 1)
	<-run.stream.Done()
	require.Error(t, run.err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	template := exec.pipelineNodeMatchTemplateFor("MATCH (n:Item)")
	source := exec.pipelineStreamedNodeMatchSource(withExpressionFailureSlot(ctx), pipelineRowsSource([]pipelineRow{{}}), template, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, streamScan: true})
	yielded := 0
	require.False(t, source(func(pipelineRow) bool { yielded++; return true }))
	require.Zero(t, yielded)
}

// Cancelling a streamed RETURN over rows it received whole stops it too.
func TestResultStreamCancelOverMaterializedRows(t *testing.T) {
	exec, _ := newStreamTestExecutor(t, 50)
	ctx, cancel := context.WithCancel(context.Background())
	run := executeStreamed(ctx, exec, "MATCH (n:Item) CALL { WITH n RETURN n.k AS j } RETURN j", 1)
	<-run.stream.Started()
	_, done := run.stream.Next(1)
	require.False(t, done)
	cancel()
	<-run.stream.Done()
	require.ErrorIs(t, run.err, context.Canceled)
}
