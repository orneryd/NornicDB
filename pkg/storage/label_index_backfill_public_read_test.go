package storage_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type labelBackfillTestGate struct {
	started      chan struct{}
	paused       chan struct{}
	resume       chan struct{}
	completed    chan struct{}
	startOnce    sync.Once
	pauseOnce    sync.Once
	resumeOnce   sync.Once
	completeOnce sync.Once
}

func newLabelBackfillTestGate() *labelBackfillTestGate {
	return &labelBackfillTestGate{
		started:   make(chan struct{}),
		paused:    make(chan struct{}),
		resume:    make(chan struct{}),
		completed: make(chan struct{}),
	}
}

func (g *labelBackfillTestGate) markStarted() {
	g.startOnce.Do(func() { close(g.started) })
}

func (g *labelBackfillTestGate) pause() {
	g.pauseOnce.Do(func() {
		close(g.paused)
		<-g.resume
	})
}

func (g *labelBackfillTestGate) release() {
	g.resumeOnce.Do(func() { close(g.resume) })
}

type labelBackfillCompletionHandler struct {
	next slog.Handler
	gate *labelBackfillTestGate
}

func (h *labelBackfillCompletionHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level == slog.LevelDebug || h.next.Enabled(ctx, level)
}

func (h *labelBackfillCompletionHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "label index backfill started" {
		h.gate.markStarted()
	}
	if record.Message == "label index backfill index cleared" {
		h.gate.pause()
		return nil
	}
	if record.Message == "label index backfill completed" {
		h.gate.completeOnce.Do(func() { close(h.gate.completed) })
	}
	if record.Level == slog.LevelDebug {
		return nil
	}
	return h.next.Handle(ctx, record)
}

func (h *labelBackfillCompletionHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &labelBackfillCompletionHandler{next: h.next.WithAttrs(attrs), gate: h.gate}
}

func (h *labelBackfillCompletionHandler) WithGroup(name string) slog.Handler {
	return &labelBackfillCompletionHandler{next: h.next.WithGroup(name), gate: h.gate}
}

func TestPublicCypherReadsCompleteLabelsDuringBackfill(t *testing.T) {
	dir := t.TempDir()
	initial, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = initial.Close() })

	const firstIndexBatch = 50_000
	nodes := make([]*storage.Node, 0, 100_003)
	for i := 0; i < firstIndexBatch-1; i++ {
		nodes = append(nodes, &storage.Node{
			ID: storage.NodeID(fmt.Sprintf("test:a-padding-%05d", i)),
		})
	}
	sourceID := storage.NodeID("test:b-source")
	nodes = append(nodes, &storage.Node{
		ID:         sourceID,
		Labels:     []string{"Source"},
		Properties: map[string]interface{}{"key": int64(2)},
	})
	for i := 0; i < firstIndexBatch; i++ {
		nodes = append(nodes, &storage.Node{
			ID: storage.NodeID(fmt.Sprintf("test:c-padding-%05d", i)),
		})
	}
	var targetIDs []storage.NodeID
	for i := 0; i < 3; i++ {
		id := storage.NodeID(fmt.Sprintf("test:z-target-%05d", i))
		targetIDs = append(targetIDs, id)
		nodes = append(nodes, &storage.Node{
			ID:         id,
			Labels:     []string{"Target"},
			Properties: map[string]interface{}{"key": int64(i)},
		})
	}
	for start := 0; start < len(nodes); start += 2_000 {
		end := start + 2_000
		if end > len(nodes) {
			end = len(nodes)
		}
		require.NoError(t, initial.BulkCreateNodes(nodes[start:end]))
	}
	err = initial.CreateEdge(&storage.Edge{
		ID:        "test:source-target",
		StartNode: sourceID,
		EndNode:   targetIDs[2],
		Type:      "LINK",
	})
	require.NoError(t, err)
	require.NoError(t, initial.Close())

	// Remove the ready marker. The large stored graph remains authoritative.
	raw, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, raw.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{0x10, 0x05})
	}))
	nodeCount := 0
	require.NoError(t, raw.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Rewind()
		for it.ValidForPrefix([]byte{0x01}) {
			nodeCount++
			it.Next()
		}
		return nil
	}))
	require.Equal(t, len(nodes), nodeCount, "the large store must be present before the rebuild starts")
	require.ErrorIs(t, raw.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte{0x10, 0x05})
		return err
	}), badger.ErrKeyNotFound)
	require.NoError(t, raw.Close())

	gate := newLabelBackfillTestGate()
	logger := slog.New(&labelBackfillCompletionHandler{
		next: slog.NewTextHandler(io.Discard, nil),
		gate: gate,
	})
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:   dir,
		LowMemory: true,
		Logger:    logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		gate.release()
		if engine != nil {
			_ = engine.Close()
		}
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

	executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
	ctx := context.Background()
	run := func(query string) *cypher.ExecuteResult {
		t.Helper()
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result
	}

	result := run("MATCH (n:Target) RETURN n")
	require.Len(t, result.Rows, len(targetIDs), "MATCH must scan stored nodes while the label index is incomplete")

	result = run("MATCH (n:Target) RETURN count(n)")
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, len(targetIDs), result.Rows[0][0], "the label count path must stay complete during rebuild")

	result = run("MATCH (n:Target) WHERE n.key = 2 RETURN count(n)")
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 1, result.Rows[0][0], "filtered counts must use the complete label scan")

	result = run("MATCH (n:Target) WHERE n.key = 2 RETURN n")
	require.Len(t, result.Rows, 1, "planner label seeks must use the complete fallback")

	result = run("MATCH (s:Source) WHERE EXISTS { MATCH (t:Target) WHERE t.key = s.key } RETURN count(s)")
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 1, result.Rows[0][0], "EXISTS must see labels while the index is rebuilding")

	result = run("MATCH (s:Source)-[r:LINK]->(t:Target) RETURN s.key, count(t)")
	require.Len(t, result.Rows, 1, "aggregate label membership must use the complete fallback")
	require.EqualValues(t, 2, result.Rows[0][0])
	require.EqualValues(t, 1, result.Rows[0][1])

	found, err := engine.HasLabelBatch(targetIDs, "Target")
	require.NoError(t, err)
	for _, id := range targetIDs {
		require.True(t, found[id], "point label membership must read the stored body for %s", id)
	}

	gate.release()
	select {
	case <-gate.completed:
	case <-time.After(10 * time.Minute):
		t.Fatal("label index rebuild did not finish after the test hook was released")
	}
	require.NoError(t, engine.Close())
	engine = nil

	reopened, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedExecutor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(reopened, "test"))
	reopenedResult, err := reopenedExecutor.Execute(ctx, "MATCH (n:Target) RETURN n", nil)
	require.NoError(t, err)
	require.Len(t, reopenedResult.Rows, len(targetIDs), "the complete label index must survive reopen")
}
