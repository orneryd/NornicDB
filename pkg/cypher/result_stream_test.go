package cypher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingScanEngine counts the nodes its label scans hand out, and can
// make them fail.
type countingScanEngine struct {
	*storage.MemoryEngine
	visited atomic.Int64
	// failAfter makes a scan fail after that many nodes (0: never).
	failAfter atomic.Int64
}

func (c *countingScanEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*storage.Node) error) error {
	seen := int64(0)
	return c.MemoryEngine.StreamNodesByLabelProjectedInScope(scope, label, properties, func(node *storage.Node) error {
		c.visited.Add(1)
		seen++
		if limit := c.failAfter.Load(); limit > 0 && seen > limit {
			return errors.New("scan failed")
		}
		return visit(node)
	})
}

type streamedExecution struct {
	stream *ResultStream
	result *ExecuteResult
	err    error
}

// executeStreamed runs query with a result stream of the given threshold
// on its own goroutine, as Bolt does, and returns once the statement
// started streaming or finished.
func executeStreamed(ctx context.Context, exec *StorageExecutor, query string, threshold int) *streamedExecution {
	run := &streamedExecution{stream: NewResultStream(threshold, nil)}
	go func() {
		defer run.stream.Finish()
		run.result, run.err = exec.Execute(WithResultStream(ctx, run.stream), query, nil)
	}()
	select {
	case <-run.stream.Started():
	case <-run.stream.Done():
	}
	return run
}

func newStreamTestExecutor(t *testing.T, items int) (*StorageExecutor, *countingScanEngine) {
	t.Helper()
	engine := &countingScanEngine{MemoryEngine: newTestMemoryEngine(t)}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
	_, err := exec.Execute(context.Background(), fmt.Sprintf("UNWIND range(0, %d) AS i CREATE (:Item {k: i})", items-1), nil)
	require.NoError(t, err)
	return exec, engine
}

// A streamed read produces rows, and reads the label, only as far as they
// are consumed (#939); every row arrives once, and Execute returns the
// statement's outcome without the rows.
func TestResultStreamReadsOnlyWhatIsConsumed(t *testing.T) {
	exec, engine := newStreamTestExecutor(t, 5000)
	engine.visited.Store(0)
	run := executeStreamed(context.Background(), exec, "MATCH (n:Item) RETURN n.k AS k", 10)
	select {
	case <-run.stream.Started():
	default:
		t.Fatalf("statement did not stream: %v", run.err)
	}
	require.Equal(t, []string{"k"}, run.stream.Columns())
	first, done := run.stream.Next(20)
	require.False(t, done)
	require.Len(t, first, 20)
	require.Less(t, engine.visited.Load(), int64(2500), "the scan ran ahead of the rows consumed")

	rest, done := run.stream.Next(-1)
	require.True(t, done)
	seen := make(map[int64]bool)
	for _, row := range append(first, rest...) {
		k := row[0].(int64)
		require.False(t, seen[k], "row %d twice", k)
		seen[k] = true
	}
	require.Len(t, seen, 5000)
	<-run.stream.Done()
	require.NoError(t, run.err)
	require.Empty(t, run.result.Rows)
	require.True(t, run.stream.streamed())
}

// Statements the stream doesn't take, or that end within the threshold,
// return their whole result; a subquery's or a UNION branch's RETURN is not
// the statement's. Every statement returns all its rows once.
func TestResultStreamLeavesOtherStatementsWhole(t *testing.T) {
	exec, _ := newStreamTestExecutor(t, 300)
	for _, tc := range []struct {
		query    string
		rows     int
		streamed bool
	}{
		{"MATCH (n:Item) RETURN n.k AS k", 300, true},
		{"MATCH (n:Item) WITH n RETURN n.k AS k", 300, true},
		{"UNWIND range(1, 1000) AS i RETURN i", 1000, true},
		{"MATCH (n:Item) CALL { WITH n RETURN n.k AS j } RETURN j AS k", 300, true},
		{"MATCH (n:Item) RETURN n.k AS k ORDER BY k", 300, false},
		{"MATCH (n:Item) RETURN count(n) AS c", 1, false},
		{"MATCH (n:Item) WHERE n.k < 5 RETURN n.k AS k", 5, false},
		{"MATCH (n:Item) RETURN DISTINCT n.k % 7 AS k", 7, false},
		{"MATCH (n:Item) RETURN n.k AS k LIMIT 150", 150, false},
		{"MATCH (n:Item) SET n.seen = true RETURN n.k AS k", 300, false},
		{"MATCH (`odd name`:Item) RETURN `odd name`.k AS k", 300, false},
		{"MATCH (n:Item) RETURN n.k AS k UNION ALL MATCH (n:Item) RETURN n.k AS k", 600, false},
	} {
		run := executeStreamed(context.Background(), exec, tc.query, 100)
		var got int
		select {
		case <-run.stream.Started():
			streamed, done := run.stream.Next(-1)
			require.True(t, done, tc.query)
			got = len(streamed)
			<-run.stream.Done()
			require.True(t, tc.streamed, "%s streamed", tc.query)
		default:
			<-run.stream.Done()
			require.False(t, tc.streamed, "%s did not stream", tc.query)
			if run.result != nil {
				got = len(run.result.Rows)
			}
		}
		require.NoError(t, run.err, tc.query)
		require.Equal(t, tc.rows, got, tc.query)
	}
}

// The streamed columns are the ones the client named, as without a
// stream.
func TestResultStreamColumnsAreTheClients(t *testing.T) {
	exec, _ := newStreamTestExecutor(t, 50)
	const query = "MATCH (n:Item)   RETURN n.k  +  1, n.k*2 AS doubled"
	// Streamed first: a streamed result is not cached, and a cached one
	// is never streamed.
	run := executeStreamed(context.Background(), exec, query, 10)
	<-run.stream.Started()
	rows, done := run.stream.Next(-1)
	require.True(t, done)
	require.Len(t, rows, 50)
	whole, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, whole.Columns, run.stream.Columns())
	require.Len(t, whole.Rows, 50)
}

// An error after the stream started is Execute's error, after the rows
// before it; cancelling the context stops the statement.
func TestResultStreamErrorsAndCancellation(t *testing.T) {
	exec, _ := newStreamTestExecutor(t, 1)
	run := executeStreamed(context.Background(), exec, "UNWIND range(1, 100) AS i RETURN 10 / (i - 50) AS x", 10)
	<-run.stream.Started()
	rows, done := run.stream.Next(-1)
	require.True(t, done)
	require.Len(t, rows, 49)
	<-run.stream.Done()
	require.Error(t, run.err)
	require.Contains(t, run.err.Error(), "/ by zero")

	ctx, cancel := context.WithCancel(context.Background())
	run = executeStreamed(ctx, exec, "UNWIND range(1, 100000) AS i RETURN i", 10)
	<-run.stream.Started()
	_, done = run.stream.Next(5)
	require.False(t, done)
	cancel()
	<-run.stream.Done()
	require.True(t, errors.Is(run.err, context.Canceled), "err = %v", run.err)
}
