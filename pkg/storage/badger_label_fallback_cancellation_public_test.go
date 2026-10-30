package storage_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func openPausedLabelFallbackEngine(t *testing.T, foreignNodes, scopedNodes int) (*storage.BadgerEngine, *labelBackfillTestGate) {
	t.Helper()
	dir := t.TempDir()
	initial, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true})
	require.NoError(t, err)

	nodes := make([]*storage.Node, 0, foreignNodes+scopedNodes)
	for i := 0; i < foreignNodes; i++ {
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("foreign:%08d", i))})
	}
	for i := 0; i < scopedNodes; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:%08d", i))}
		if i == scopedNodes-1 {
			node.Labels = []string{"Present"}
		}
		nodes = append(nodes, node)
	}
	for start := 0; start < len(nodes); start += 2_000 {
		end := start + 2_000
		if end > len(nodes) {
			end = len(nodes)
		}
		require.NoError(t, initial.BulkCreateNodes(nodes[start:end]))
	}
	require.NoError(t, initial.Close())

	raw, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, raw.DropPrefix([]byte{0x03}))
	require.NoError(t, raw.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{0x10, 0x05})
	}))
	require.NoError(t, raw.Close())

	gate := newLabelBackfillTestGate()
	logger := slog.New(&labelBackfillCompletionHandler{next: slog.NewTextHandler(io.Discard, nil), gate: gate})
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true, Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() {
		gate.release()
		_ = engine.Close()
	})
	select {
	case <-gate.started:
	case <-time.After(30 * time.Second):
		t.Fatal("label index rebuild did not start after its ready marker was removed")
	}
	select {
	case <-gate.paused:
	case <-time.After(5 * time.Minute):
		t.Fatal("label index rebuild did not reach the index-clear test hook")
	}
	return engine, gate
}

func TestReview1667CancelledMissingLabelScan(t *testing.T) {
	engine, _ := openPausedLabelFallbackEngine(t, 0, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := engine.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Missing",
		ApplyDecayFilter: true,
	}, func(*storage.Node) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestReview1667CancellationDuringBodyScan(t *testing.T) {
	engine, _ := openPausedLabelFallbackEngine(t, 0, 3)
	ctx, cancel := context.WithCancel(context.Background())
	seen := 0

	err := engine.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Present",
		ApplyDecayFilter: true,
	}, func(*storage.Node) error {
		seen++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, seen)
}

func TestReview1667DeadlineLargeScopedStore(t *testing.T) {
	engine, _ := openPausedLabelFallbackEngine(t, 50_000, 50_000)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()

	err := engine.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Missing",
		ApplyDecayFilter: true,
	}, func(*storage.Node) error { return nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Cancel only when the statement reaches the actual storage owner. Extra
// executor polls cannot turn this into a pre-cancelled query test.
type statementOwnerCancelContext struct {
	context.Context
	cancel  context.CancelFunc
	owner   string
	reached bool
}

func (c *statementOwnerCancelContext) Err() error {
	pcs := make([]uintptr, 48)
	n := runtime.Callers(1, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, c.owner) {
			c.reached = true
			c.cancel()
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestLabelStatementContextDuringRebuild(t *testing.T) {
	engine, _ := openPausedLabelFallbackEngine(t, 0, 8)
	for _, query := range []string{"MATCH (n:Present) RETURN n.value", "MATCH (n:Missing) RETURN count(n)"} {
		t.Run(query, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &statementOwnerCancelContext{Context: parent, cancel: cancel, owner: "newLabelIndexReader"}
			executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
			_, err := executor.Execute(ctx, query, nil)
			require.True(t, ctx.reached, "statement must deliver its context to the candidate owner")
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
