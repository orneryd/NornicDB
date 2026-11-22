//go:build closeout_l2_e46

package storage

// E46: an update of a node while an embedding clear runs on it; the store
// rebuilt from snapshot plus journal must not bring the cleared vectors back.
//
// The fixture is the pin's own (package storage test helpers, no build tag):
// newWriteBehindTestEngine, rrvNode/rrvVectors/rrvWorkerMeta/rrvRead,
// takePeriodicSnapshot, crashClose, RecoverIntoEngine. The interleaving is
// placed as in the entry: the update starts inside the clear's per-node
// journal callback, after the reset record is appended and before the store
// write.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// l2E46Interleave runs between() inside the clear's journal callback, after
// the wrapper appended the node's reset record and before clear().
type l2E46Interleave struct {
	*BadgerEngine
	between func(context.Context)
}

// GetInnerEngine keeps the wrapper unwrappable as the native store, as the
// pin's own interleave wrapper does.
func (e *l2E46Interleave) GetInnerEngine() Engine { return e.BadgerEngine }

func (e *l2E46Interleave) clearEmbeddingsForPrefix(idPrefix string, journal func(NodeID, func() error) error) (int, error) {
	ctx := context.Background()
	return e.BadgerEngine.clearEmbeddingsForPrefix(idPrefix, func(id NodeID, clear func() error) error {
		write := func() error {
			if e.between != nil {
				e.between(ctx)
			}
			return clear()
		}
		if journal != nil {
			return journal(id, write)
		}
		return write()
	})
}

type l2E46Outcome struct {
	cleared        int
	clearErr       error
	window         string // "inside" (update committed inside the window), "lock-wait", "timeout", "none"
	updateErr      error
	liveChunks     int
	liveMeta       map[string]any
	rebuiltChunks  int
	rebuiltMeta    map[string]any
	rebuiltNeeds   bool
	replayFailed   int
	replayApplied  int
}

// l2E46Run: create test:shape with 2 chunk vectors of 8 dims and worker
// metadata; periodic snapshot; clear with (interleave=true) or without an
// update in the window (the update then runs before the clear); crash;
// rebuild an empty store from the snapshot plus the journal.
func l2E46Run(t *testing.T, setFlag bool, interleave bool) l2E46Outcome {
	t.Helper()
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true})
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	wal := engine.bufferedCommitWAL
	interpose := &l2E46Interleave{BadgerEngine: engine}
	journal := NewWALEngine(interpose, wal)

	_, err := journal.CreateNode(context.Background(), &Node{
		ID:              rrvNode,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "alpha", "flag": false},
		ChunkEmbeddings: rrvVectors(2, 3),
		EmbedMeta:       rrvWorkerMeta(2),
	})
	require.NoError(t, err)
	snapshot := takePeriodicSnapshot(t, journal, dir)

	update := func(ctx context.Context) error {
		node, err := journal.GetNode(ctx, rrvNode)
		if err != nil {
			return err
		}
		node = CopyNode(node)
		if setFlag {
			node.Properties["flag"] = true
		}
		return journal.UpdateNode(ctx, NodeUpdateRequest{After: node})
	}

	out := l2E46Outcome{window: "none"}
	updateOutcome := make(chan error, 1)
	started := false
	if interleave {
		waits := fixtureowner.ObserveHookSignal(t, SetUniqueKeyLockWaitHookForTesting, 8)
		interpose.between = func(ctx context.Context) {
			if started {
				return
			}
			started = true
			go func() { updateOutcome <- update(ctx) }()
			select {
			case err := <-updateOutcome:
				out.window = "inside"
				out.updateErr = err
			case <-waits:
				out.window = "lock-wait"
			case <-time.After(5 * time.Second):
				// Neither committed nor reported waiting on the node lock:
				// the clear proceeds; the update result is collected below.
				out.window = "timeout"
			}
		}
	} else {
		require.NoError(t, update(context.Background()))
	}

	out.cleared, out.clearErr = journal.ClearAllEmbeddingsForPrefix("test:")
	if interleave && started && out.window != "inside" {
		select {
		case err := <-updateOutcome:
			out.updateErr = err
		case <-time.After(60 * time.Second):
			t.Fatalf("the update started inside the clear did not finish within 60 s after the clear")
		}
	}

	live := rrvRead(t, engine)
	out.liveChunks, out.liveMeta = len(live.ChunkEmbeddings), live.EmbedMeta
	crashClose(t, engine, wal)

	destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	result, _, err := RecoverIntoEngine(context.Background(), destination, filepath.Join(dir, "wal"), snapshot)
	require.NoError(t, err)
	out.replayFailed, out.replayApplied = result.Failed, result.Applied

	got := rrvRead(t, destination)
	out.rebuiltChunks, out.rebuiltMeta = len(got.ChunkEmbeddings), got.EmbedMeta
	out.rebuiltNeeds = NodeNeedsEmbedding(got)
	return out
}

// l2E46Defect: the clear succeeded and the live node has no vectors, but the
// rebuilt node has vectors back or does not need embedding.
func l2E46Defect(o l2E46Outcome) bool {
	if o.liveChunks != 0 {
		// Live and rebuilt must at least agree.
		return o.rebuiltChunks != o.liveChunks
	}
	return o.rebuiltChunks != 0 || !o.rebuiltNeeds
}

func l2E46Report(t *testing.T, id string, o l2E46Outcome) {
	t.Helper()
	if o.clearErr != nil || o.cleared != 1 {
		t.Fatalf("harness: clear returned %d, %v (want 1, nil)", o.cleared, o.clearErr)
	}
	if o.replayFailed != 0 {
		t.Logf("replay reported %d failed entries", o.replayFailed)
	}
	vals := []any{id, o.cleared, o.window, o.updateErr, o.liveChunks, o.liveMeta, o.rebuiltChunks, o.rebuiltMeta, o.rebuiltNeeds, o.replayApplied, o.replayFailed}
	format := "L2RESULT %s %s cleared=%d window=%s updateErr=%v live_chunks=%d live_meta=%v rebuilt_chunks=%d rebuilt_meta=%v rebuilt_needs_embedding=%v replay_applied=%d replay_failed=%d"
	if l2E46Defect(o) {
		t.Logf(format, append([]any{vals[0], "DEFECT"}, vals[1:]...)...)
		t.Errorf("rebuilt node holds %d chunk vectors (needs embedding %v) while the live node holds %d", o.rebuiltChunks, o.rebuiltNeeds, o.liveChunks)
		return
	}
	t.Logf(format, append([]any{vals[0], "OK"}, vals[1:]...)...)
}

func TestL2_e46_Repro(t *testing.T) {
	t.Run("a_no_change", func(t *testing.T) {
		l2E46Report(t, "E46/a", l2E46Run(t, false, true))
	})
	t.Run("b_flag_true", func(t *testing.T) {
		l2E46Report(t, "E46/b", l2E46Run(t, true, true))
	})
}

// Control: the same update committed before the clear (no interleaving);
// the rebuilt node must have no vectors and need embedding.
func TestL2_e46_Control(t *testing.T) {
	t.Run("a_no_change", func(t *testing.T) {
		l2E46Report(t, "E46-control/a", l2E46Run(t, false, false))
	})
	t.Run("b_flag_true", func(t *testing.T) {
		l2E46Report(t, "E46-control/b", l2E46Run(t, true, false))
	})
}

// Cause: the entry's cause is that the update is accepted between the reset
// record (appended by WALEngine.ClearAllEmbeddingsForPrefix's callback) and
// the store write. At the pin BadgerEngine.clearEmbeddingsForPrefix
// (pkg/storage/badger_stats.go, inMutationTransaction + LockNodeForWrite)
// holds the node's write lock across both. This logs which branch the
// update took ("inside" = accepted in the window, the entry's cause;
// "lock-wait" = held off by the node lock) for both variants, without
// asserting.
func TestL2_e46_Cause(t *testing.T) {
	for _, setFlag := range []bool{false, true} {
		o := l2E46Run(t, setFlag, true)
		t.Logf("CAUSE setFlag=%v window=%s updateErr=%v live_chunks=%d rebuilt_chunks=%d rebuilt_meta=%v", setFlag, o.window, o.updateErr, o.liveChunks, o.rebuiltChunks, o.rebuiltMeta)
	}
}
