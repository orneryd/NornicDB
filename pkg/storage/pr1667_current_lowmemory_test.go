package storage

// PR #1667 / NOR-71 (folded NOR-142) regression coverage: the native on-disk
// LowMemory label-index rebuild at the reported 60,000-node scale.
//
// NOR-142 recorded that a LowMemory rebuild aborted with Badger's "Txn is too
// big to fit into one request" after node 13,105 of 60,000, that the release
// base then returned a partial label count, and that the PR #1667 scan
// fallback kept reads complete while the index itself stayed permanently
// incomplete. This file pins the rebuild outcome on the current native disk
// configuration. It adds no production seam: it uses the real
// NewBadgerEngineWithOptions disk configuration, the existing label-index test
// helpers, and the existing maintenance pause point.
//
// Covered contracts:
//   - 60,000 labeled nodes seeded through bounded commits rebuild without
//     exceeding Badger's per-request transaction limit,
//   - the readiness marker never exists while a rebuild chunk is still
//     uncommitted, and is durable only once the rebuild finished,
//   - the exact complete label set (count, IDs, labels, bodies) reads back
//     after close/reopen, with no rebuild scheduled from the durable marker,
//   - the default memory profile completes the same population and namespace
//     isolation stays correct,
//   - cancelling a rebuild at its chunk pause publishes no readiness and the
//     restart rebuilds the complete index.
//
// Durations are recorded as diagnostics on this host only; they are not a
// benchmark claim and no neutral-cost comparison is made here.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testlatch"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// pr1667ReportedNodeCount is the population NOR-142 reported.
	pr1667ReportedNodeCount = 60_000
	// pr1667InterruptedNodeCount exceeds one LowMemory rebuild transaction, so
	// an interruption covers a held chunk rather than a full tiny rebuild. It is
	// deliberately below the reported population to bound the focused run.
	pr1667InterruptedNodeCount = 30_000
	// pr1667SeedCommitSize bounds each seeding commit.
	pr1667SeedCommitSize = 2_000
	pr1667Label          = "BackfillRecord"
	pr1667SharedLabel    = "Shared"
)

// pr1667LogCapture keeps the engine's structured rebuild diagnostics so a
// failing assertion can report the rebuild's own error text instead of only a
// missing marker.
type pr1667LogCapture struct {
	mu      sync.Mutex
	records []string
}

func (c *pr1667LogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *pr1667LogCapture) Handle(_ context.Context, record slog.Record) error {
	var line strings.Builder
	line.WriteString(record.Level.String())
	line.WriteString(" ")
	line.WriteString(record.Message)
	record.Attrs(func(attr slog.Attr) bool {
		line.WriteString(" ")
		line.WriteString(attr.Key)
		line.WriteString("=")
		line.WriteString(attr.Value.String())
		return true
	})
	c.mu.Lock()
	c.records = append(c.records, line.String())
	c.mu.Unlock()
	return nil
}

func (c *pr1667LogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *pr1667LogCapture) WithGroup(string) slog.Handler      { return c }

// rebuildDiagnostics returns the captured label-index rebuild lines, so a
// failed readiness assertion can state why the rebuild stopped.
func (c *pr1667LogCapture) rebuildDiagnostics() string {
	c.mu.Lock()
	lines := append([]string(nil), c.records...)
	c.mu.Unlock()
	var out strings.Builder
	for _, line := range lines {
		if strings.Contains(line, "label index backfill") {
			out.WriteString("\n  ")
			out.WriteString(line)
		}
	}
	if out.Len() == 0 {
		return "\n  (no label-index rebuild diagnostics were emitted)"
	}
	return out.String()
}

// pr1667Engine opens the real on-disk engine with the requested memory profile
// and test-owned fixture cleanup.
func pr1667Engine(t *testing.T, capture *pr1667LogCapture, dir string, lowMemory bool) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:   dir,
		LowMemory: lowMemory,
		Logger:    slog.New(capture),
	}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	return engine
}

// pr1667SeededID is the stored ID of seeded node index in namespace.
func pr1667SeededID(namespace string, index int) NodeID {
	return NodeID(fmt.Sprintf("%s:lowmem-%06d", namespace, index))
}

// pr1667SeedLabelNodes creates count single-label nodes in bounded commits and
// returns how long the seeding took. Every node carries its sequence number so
// a later read proves the index entry still resolves to that node's own body.
func pr1667SeedLabelNodes(t *testing.T, engine *BadgerEngine, namespace, label string, count, commitSize int) time.Duration {
	t.Helper()
	start := time.Now()
	for offset := 0; offset < count; offset += commitSize {
		end := min(offset+commitSize, count)
		batch := make([]*Node, 0, end-offset)
		for i := offset; i < end; i++ {
			batch = append(batch, &Node{
				ID:         pr1667SeededID(namespace, i),
				Labels:     []string{label},
				Properties: map[string]any{"seq": int64(i)},
			})
		}
		require.NoError(t, engine.BulkCreateNodes(t.Context(), batch), "seeding nodes %d..%d as one bounded commit", offset, end)
	}
	return time.Since(start)
}

// pr1667RequireEngineLabelSet asserts that label resolves to exactly the seeded
// nodes of namespace: count, IDs, exact label list and body.
func pr1667RequireEngineLabelSet(t *testing.T, engine *BadgerEngine, namespace, label string, count int) {
	t.Helper()
	nodes, err := engine.GetNodesByLabel(context.Background(), label)
	require.NoError(t, err)
	require.Len(t, nodes, count, "label %q must resolve to every stored node of namespace %q", label, namespace)
	seen := make(map[NodeID]int64, len(nodes))
	for _, node := range nodes {
		require.Equal(t, []string{label}, node.Labels, "node %q must keep exactly its stored label", node.ID)
		seq, ok := node.Properties["seq"].(int64)
		require.True(t, ok, "node %q must expose its seeded sequence property", node.ID)
		seen[node.ID] = seq
	}
	for i := 0; i < count; i++ {
		id := pr1667SeededID(namespace, i)
		seq, ok := seen[id]
		require.True(t, ok, "label %q must include seeded node %q", label, id)
		require.Equal(t, int64(i), seq, "node %q must carry its own body", id)
	}
}

// pr1667RequireNamespacedLabelSet asserts the same contract through the
// namespaced reader, which strips the namespace prefix from returned IDs.
func pr1667RequireNamespacedLabelSet(t *testing.T, store *NamespacedEngine, namespace, label string, count int) {
	t.Helper()
	nodes, err := store.GetNodesByLabel(context.Background(), label)
	require.NoError(t, err)
	require.Len(t, nodes, count, "namespaced label %q in %q must resolve to its own nodes only", label, namespace)
	seen := make(map[NodeID]int64, len(nodes))
	for _, node := range nodes {
		require.Equal(t, []string{label}, node.Labels, "node %q must keep exactly its stored label", node.ID)
		seq, ok := node.Properties["seq"].(int64)
		require.True(t, ok, "node %q must expose its seeded sequence property", node.ID)
		seen[node.ID] = seq
	}
	for i := 0; i < count; i++ {
		id := NodeID(fmt.Sprintf("lowmem-%06d", i))
		seq, ok := seen[id]
		require.True(t, ok, "namespace %q label %q must include node %q", namespace, label, id)
		require.Equal(t, int64(i), seq, "node %q must carry its own body", id)
	}
}

// pr1667Ready reports the stored readiness selection without polling.
func pr1667Ready(t *testing.T, engine *BadgerEngine) bool {
	t.Helper()
	ready, err := engine.indexBackfillReady(startupLabelIndex)
	require.NoError(t, err)
	return ready
}

// pr1667RequireMarkerAbsent asserts that no readiness marker exists yet, which
// is what keeps a later restart from trusting a partial index.
func pr1667RequireMarkerAbsent(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	err := engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(labelIndexReadyKey)
		return err
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "no readiness marker may exist before the rebuild's final batch is durable")
}

// pr1667WaitBackfill waits for the scheduled rebuild goroutine to finish.
func pr1667WaitBackfill(t *testing.T, capture *pr1667LogCapture, done <-chan struct{}, timeout time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(timeout):
		t.Fatalf("the label-index rebuild did not finish within %s:%s", timeout, capture.rebuildDiagnostics())
		return 0
	}
}

// pr1667ChunkGate holds the existing maintenance pause point
// (pauseIndexBackfillChunk) so a test can inspect the store before a rebuild
// chunk commits. It is the production pause seam, armed by the test only.
type pr1667ChunkGate struct {
	paused  chan int
	release chan struct{}
	dead    *testlatch.Latch
	mu      sync.Mutex
	chunks  int
}

func newPR1667ChunkGate() *pr1667ChunkGate {
	return &pr1667ChunkGate{
		paused:  make(chan int, 1),
		release: make(chan struct{}),
		dead:    testlatch.New(),
	}
}

// hook blocks the rebuild before each chunk commit until the test releases it.
func (g *pr1667ChunkGate) hook(point string) {
	if point != pauseIndexBackfillChunk {
		return
	}
	g.mu.Lock()
	g.chunks++
	chunk := g.chunks
	g.mu.Unlock()
	select {
	case g.paused <- chunk:
	case <-g.dead.Done():
		return
	}
	select {
	case <-g.release:
	case <-g.dead.Done():
	}
}

// arm installs the gate as the storage maintenance pause hook. The engine whose
// rebuild it must hold is opened after this call.
func (g *pr1667ChunkGate) arm() func() { return setMaintenancePauseHook(g.hook) }

// releaseOne lets the rebuild proceed past one held chunk.
func (g *pr1667ChunkGate) releaseOne() {
	select {
	case g.release <- struct{}{}:
	case <-g.dead.Done():
	}
}

// stop unblocks a held rebuild so engine cleanup cannot wait on this gate.
func (g *pr1667ChunkGate) stop() {
	g.dead.Release()
}

func (g *pr1667ChunkGate) observedChunks() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.chunks
}

// pr1667RunReportedScaleRebuild seeds the NOR-142 population, removes only the
// derived label index and readiness marker, and verifies the startup rebuild.
func pr1667RunReportedScaleRebuild(t *testing.T, lowMemory bool) {
	t.Helper()
	dir := t.TempDir()
	capture := &pr1667LogCapture{}

	engine := pr1667Engine(t, capture, dir, lowMemory)
	seedDuration := pr1667SeedLabelNodes(t, engine, "test", pr1667Label, pr1667ReportedNodeCount, pr1667SeedCommitSize)
	pr1667RequireEngineLabelSet(t, engine, "test", pr1667Label, pr1667ReportedNodeCount)
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)
	require.NoError(t, engine.Close())
	t.Logf("seed: %d nodes in %s (%d nodes per commit, low_memory=%t)",
		pr1667ReportedNodeCount, seedDuration, pr1667SeedCommitSize, lowMemory)

	reopened := pr1667Engine(t, capture, dir, lowMemory)
	worker := pendingLabelIndexBackfill(t, reopened)
	require.NotNil(t, worker, "removing the readiness marker must schedule the label-index rebuild")
	rebuildDuration := pr1667WaitBackfill(t, capture, worker.done.Done(), 10*time.Minute)
	if !pr1667Ready(t, reopened) {
		visible, readErr := reopened.GetNodesByLabel(context.Background(), pr1667Label)
		t.Fatalf("the %d-node rebuild (low_memory=%t) must complete and mark the index ready: the label index reports %d nodes (read error: %v)%s",
			pr1667ReportedNodeCount, lowMemory, len(visible), readErr, capture.rebuildDiagnostics())
	}
	requireLabelIndexReadyMarker(t, reopened, labelIndexSchemeVersion)
	pr1667RequireEngineLabelSet(t, reopened, "test", pr1667Label, pr1667ReportedNodeCount)
	require.NoError(t, reopened.Close())
	t.Logf("rebuild: %d nodes in %s (low_memory=%t)", pr1667ReportedNodeCount, rebuildDuration, lowMemory)

	again := pr1667Engine(t, capture, dir, lowMemory)
	assert.Nil(t, pendingLabelIndexBackfill(t, again), "the durable readiness marker must not schedule another rebuild")
	pr1667RequireEngineLabelSet(t, again, "test", pr1667Label, pr1667ReportedNodeCount)
	require.NoError(t, again.Close())
}

// pr1667RunReadinessChunkContract holds every rebuild chunk at the existing
// pause point and asserts that no readiness marker exists while a chunk is
// uncommitted, then that readiness and the complete index are published only
// once the rebuild finished.
func pr1667RunReadinessChunkContract(t *testing.T, lowMemory bool, count int) {
	t.Helper()
	dir := t.TempDir()
	capture := &pr1667LogCapture{}

	seedEngine := pr1667Engine(t, capture, dir, lowMemory)
	pr1667SeedLabelNodes(t, seedEngine, "test", pr1667Label, count, pr1667SeedCommitSize)
	wipeLabelIndexAndMarker(t, seedEngine, prefixLabelIndex)
	require.NoError(t, seedEngine.Close())

	gate := newPR1667ChunkGate()
	restore := gate.arm()
	defer func() {
		gate.stop()
		restore()
	}()

	reopened := pr1667Engine(t, capture, dir, lowMemory)
	worker := pendingLabelIndexBackfill(t, reopened)
	require.NotNil(t, worker, "removing the readiness marker must schedule the label-index rebuild")

	deadline := time.NewTimer(5 * time.Minute)
	defer deadline.Stop()
waitChunks:
	for {
		select {
		case chunk := <-gate.paused:
			// The held chunk has not committed: no readiness may exist yet, even
			// when earlier chunks are already durable.
			require.False(t, pr1667Ready(t, reopened),
				"readiness must stay unpublished while rebuild chunk %d is uncommitted:%s", chunk, capture.rebuildDiagnostics())
			pr1667RequireMarkerAbsent(t, reopened)
			if visible, err := reopened.GetNodesByLabel(context.Background(), pr1667Label); err == nil {
				// Diagnostic only: the durable index prefix an ordinary label read
				// sees while the rebuild is still incomplete.
				t.Logf("rebuild chunk %d uncommitted: label index reports %d of %d nodes", chunk, len(visible), count)
			}
			gate.releaseOne()
		case <-worker.done.Done():
			break waitChunks
		case <-deadline.C:
			t.Fatalf("the rebuild did not finish while its chunks were released:%s", capture.rebuildDiagnostics())
		}
	}

	require.True(t, pr1667Ready(t, reopened),
		"the finished %d-node rebuild (low_memory=%t) must mark the index ready:%s",
		count, lowMemory, capture.rebuildDiagnostics())
	require.Positive(t, gate.observedChunks(),
		"the %d-node rebuild (low_memory=%t) must have run through its chunk pause:%s",
		count, lowMemory, capture.rebuildDiagnostics())
	requireLabelIndexReadyMarker(t, reopened, labelIndexSchemeVersion)
	pr1667RequireEngineLabelSet(t, reopened, "test", pr1667Label, count)
	require.NoError(t, reopened.Close())
	gate.stop()
	restore()

	again := pr1667Engine(t, capture, dir, lowMemory)
	assert.Nil(t, pendingLabelIndexBackfill(t, again), "the durable readiness marker must not schedule another rebuild")
	pr1667RequireEngineLabelSet(t, again, "test", pr1667Label, count)
	require.NoError(t, again.Close())
	t.Logf("rebuild chunk pauses observed: %d (low_memory=%t, nodes=%d)", gate.observedChunks(), lowMemory, count)
}

// pr1667RunCancellationContract cancels a rebuild while one of its chunks is
// held at the pause point, then proves the restart publishes the complete index
// instead of trusting anything from the cancelled run.
func pr1667RunCancellationContract(t *testing.T, lowMemory bool, count int) {
	t.Helper()
	dir := t.TempDir()
	capture := &pr1667LogCapture{}

	seedEngine := pr1667Engine(t, capture, dir, lowMemory)
	pr1667SeedLabelNodes(t, seedEngine, "test", pr1667Label, count, pr1667SeedCommitSize)
	wipeLabelIndexAndMarker(t, seedEngine, prefixLabelIndex)
	require.NoError(t, seedEngine.Close())

	gate := newPR1667ChunkGate()
	restore := gate.arm()
	defer func() {
		gate.stop()
		restore()
	}()

	interrupted := pr1667Engine(t, capture, dir, lowMemory)
	worker := pendingLabelIndexBackfill(t, interrupted)
	require.NotNil(t, worker, "removing the readiness marker must schedule the label-index rebuild")

	select {
	case chunk := <-gate.paused:
		require.Equal(t, 1, chunk)
		require.False(t, pr1667Ready(t, interrupted),
			"the interrupted rebuild must not publish readiness:%s", capture.rebuildDiagnostics())
		pr1667RequireMarkerAbsent(t, interrupted)
		if partial, err := interrupted.GetNodesByLabel(context.Background(), pr1667Label); err == nil {
			// Diagnostic only: this is the index state a restart would trust if
			// readiness had been published.
			t.Logf("interrupted rebuild: label index holds %d of %d nodes", len(partial), count)
		}
	case <-worker.done.Done():
		t.Fatalf("the rebuild finished before reaching its chunk pause, so nothing could be interrupted:%s",
			capture.rebuildDiagnostics())
	case <-time.After(2 * time.Minute):
		t.Fatalf("the rebuild never reached its first chunk pause:%s", capture.rebuildDiagnostics())
	}

	// Signal the exact cancellation owner before releasing the held batch.
	// This is the same stop signal Close uses, without relying on a
	// scheduler delay to guess whether that signal has occurred.
	require.NotNil(t, worker.stop)
	worker.stop()
	gate.releaseOne()
	require.NoError(t, interrupted.Close())
	gate.stop()
	restore()

	resumed := pr1667Engine(t, capture, dir, lowMemory)
	resumedWorker := pendingLabelIndexBackfill(t, resumed)
	require.NotNil(t, resumedWorker, "the cancelled rebuild must not have published readiness; the restart must schedule it again")
	pr1667WaitBackfill(t, capture, resumedWorker.done.Done(), 10*time.Minute)
	require.True(t, pr1667Ready(t, resumed),
		"the restarted rebuild must complete and mark the index ready:%s", capture.rebuildDiagnostics())
	requireLabelIndexReadyMarker(t, resumed, labelIndexSchemeVersion)
	pr1667RequireEngineLabelSet(t, resumed, "test", pr1667Label, count)
	require.NoError(t, resumed.Close())

	again := pr1667Engine(t, capture, dir, lowMemory)
	assert.Nil(t, pendingLabelIndexBackfill(t, again), "the durable readiness marker must not schedule another rebuild")
	pr1667RequireEngineLabelSet(t, again, "test", pr1667Label, count)
	require.NoError(t, again.Close())
}

func TestPR1667CurrentLowMemoryLabelRebuild(t *testing.T) {
	for _, profile := range []struct {
		name      string
		lowMemory bool
	}{
		{name: "lowmemory_rebuilds_the_reported_60k_population", lowMemory: true},
		{name: "default_profile_control_rebuilds_the_same_population", lowMemory: false},
	} {
		t.Run(profile.name, func(t *testing.T) {
			pr1667RunReportedScaleRebuild(t, profile.lowMemory)
		})
	}

	t.Run("namespace_isolation_survives_the_rebuild", func(t *testing.T) {
		dir := t.TempDir()
		capture := &pr1667LogCapture{}
		const (
			alphaCount = 40
			betaCount  = 25
		)

		engine := pr1667Engine(t, capture, dir, true)
		pr1667SeedLabelNodes(t, engine, "alpha", pr1667SharedLabel, alphaCount, pr1667SeedCommitSize)
		pr1667SeedLabelNodes(t, engine, "beta", pr1667SharedLabel, betaCount, pr1667SeedCommitSize)
		pr1667RequireNamespacedLabelSet(t, NewNamespacedEngine(engine, "alpha"), "alpha", pr1667SharedLabel, alphaCount)
		pr1667RequireNamespacedLabelSet(t, NewNamespacedEngine(engine, "beta"), "beta", pr1667SharedLabel, betaCount)
		wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)
		require.NoError(t, engine.Close())

		reopened := pr1667Engine(t, capture, dir, true)
		worker := pendingLabelIndexBackfill(t, reopened)
		require.NotNil(t, worker, "removing the readiness marker must schedule the label-index rebuild")
		pr1667WaitBackfill(t, capture, worker.done.Done(), time.Minute)
		require.True(t, pr1667Ready(t, reopened),
			"the namespaced rebuild must complete and mark the index ready:%s", capture.rebuildDiagnostics())
		requireLabelIndexReadyMarker(t, reopened, labelIndexSchemeVersion)

		pr1667RequireNamespacedLabelSet(t, NewNamespacedEngine(reopened, "alpha"), "alpha", pr1667SharedLabel, alphaCount)
		pr1667RequireNamespacedLabelSet(t, NewNamespacedEngine(reopened, "beta"), "beta", pr1667SharedLabel, betaCount)
		// The unscoped reader sees both namespaces and nothing foreign.
		unscoped, err := reopened.GetNodesByLabel(context.Background(), pr1667SharedLabel)
		require.NoError(t, err)
		assert.Len(t, unscoped, alphaCount+betaCount)
		// A namespace that owns no nodes of the rebuilt label stays empty.
		empty, err := NewNamespacedEngine(reopened, "gamma").GetNodesByLabel(context.Background(), pr1667SharedLabel)
		require.NoError(t, err)
		assert.Empty(t, empty)
		require.NoError(t, reopened.Close())

		again := pr1667Engine(t, capture, dir, true)
		assert.Nil(t, pendingLabelIndexBackfill(t, again), "the durable readiness marker must not schedule another rebuild")
		pr1667RequireNamespacedLabelSet(t, NewNamespacedEngine(again, "alpha"), "alpha", pr1667SharedLabel, alphaCount)
		pr1667RequireNamespacedLabelSet(t, NewNamespacedEngine(again, "beta"), "beta", pr1667SharedLabel, betaCount)
		require.NoError(t, again.Close())
	})

	t.Run("readiness_is_published_only_by_the_final_batch", func(t *testing.T) {
		pr1667RunReadinessChunkContract(t, true, pr1667InterruptedNodeCount)
	})

	t.Run("cancellation_at_a_chunk_pause_resumes_to_a_complete_index", func(t *testing.T) {
		pr1667RunCancellationContract(t, true, pr1667InterruptedNodeCount)
	})
}
