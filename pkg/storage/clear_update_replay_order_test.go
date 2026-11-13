package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Report 46: an ordinary update of a node that an embedding clear is
// resetting is accepted between the reset record the clear journals for that
// node and the store write that removes its vectors. The clear reports
// success and the live node has no managed vectors, but a rebuild of an empty
// store from the snapshot the clear started from plus the journal replays
// reset → update and gives the node the old vectors back, marked as current,
// so it is never embedded again.
//
// The test starts the update of the node from inside the clear's per-node
// journal callback: the earliest point after the wrapper appended that node's
// reset record, before the store write removes the vectors. The update must
// not be accepted there: a node's reset record and the removal it describes
// are one ordered unit with the node's other accepted writes, so the journal
// keeps the order the store applied. The update is then accepted after the
// reset, and the rebuilt store must hold no managed vectors, need embedding,
// and hold the update's property.

// cruInterleave runs the node's update at the first point of the clear that
// follows the reset record.
type cruInterleave struct {
	*BadgerEngine
	update  func(context.Context) error
	between func(context.Context)
}

// GetInnerEngine keeps the wrapper unwrappable as the native store, so the
// journal wrapper still grants native publication admission.
func (e *cruInterleave) GetInnerEngine() Engine { return e.BadgerEngine }

func (e *cruInterleave) clearEmbeddingsForPrefix(idPrefix string, journal func(NodeID, func() error) error) (int, error) {
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

type cruLab struct {
	dir       string
	engine    *BadgerEngine
	wal       *WAL
	journal   *WALEngine
	interpose *cruInterleave
}

func cruOpen(t *testing.T, dir string) *cruLab {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true})
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	interpose := &cruInterleave{BadgerEngine: engine, update: func(context.Context) error { return nil }}
	return &cruLab{dir: dir, engine: engine, wal: engine.bufferedCommitWAL, journal: NewWALEngine(interpose, engine.bufferedCommitWAL), interpose: interpose}
}

func (l *cruLab) create(t *testing.T) {
	t.Helper()
	_, err := l.journal.CreateNode(context.Background(), &Node{
		ID:              rrvNode,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "alpha", "flag": false},
		ChunkEmbeddings: rrvVectors(2, 3),
		EmbedMeta:       rrvWorkerMeta(2),
	})
	require.NoError(t, err)
}

// snapshotPeriodic takes the snapshot automatic compaction takes, and
// truncates the journal as it does.
func (l *cruLab) snapshotPeriodic(t *testing.T) string {
	t.Helper()
	l.journal.snapshotDir = filepath.Join(l.dir, "snapshots")
	require.NoError(t, os.MkdirAll(l.journal.snapshotDir, 0o755))
	require.NoError(t, l.journal.createSnapshotAndCompact(context.Background()))
	entries, err := os.ReadDir(l.journal.snapshotDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	return filepath.Join(l.journal.snapshotDir, entries[0].Name())
}

// TestClearEmbeddingsDoesNotResurrectVectorsAnUpdateCarriedBack reproduces
// report 46 and asserts the corrected order.
func TestClearEmbeddingsDoesNotResurrectVectorsAnUpdateCarriedBack(t *testing.T) {
	variants := []struct {
		name     string
		apply    func(node *Node)
		wantFlag bool
	}{
		{name: "the update changes nothing", apply: func(node *Node) {}},
		{name: "the update changes a property", apply: func(node *Node) { node.Properties["flag"] = true }, wantFlag: true},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			dir := t.TempDir()
			lab := cruOpen(t, dir)
			lab.create(t)
			snapshot := lab.snapshotPeriodic(t)

			lab.interpose.update = func(ctx context.Context) error {
				node, err := lab.journal.GetNode(ctx, rrvNode)
				if err != nil {
					return err
				}
				node = CopyNode(node)
				variant.apply(node)
				return lab.journal.UpdateNode(ctx, NodeUpdateRequest{After: node})
			}
			waits := fixtureowner.ObserveHookSignal(t, SetUniqueKeyLockWaitHookForTesting, 8)
			var updateInsideReset bool
			updateOutcome := make(chan error, 1)
			lab.interpose.between = func(ctx context.Context) {
				go func() { updateOutcome <- lab.interpose.update(ctx) }()
				select {
				case err := <-updateOutcome:
					require.NoError(t, err)
					updateInsideReset = true
				case <-waits:
					// The canonical write-lock owner observed the update
					// waiting behind this node's reset. No elapsed-time
					// assumption establishes the interleaving.
				case <-time.After(5 * time.Second):
					t.Fatal("update neither completed nor reached the node write-lock wait")
				}
			}

			cleared, err := lab.journal.ClearAllEmbeddingsForPrefix("test:")
			require.NoError(t, err)
			require.Equal(t, 1, cleared)
			require.False(t, updateInsideReset,
				"an update of the node was accepted between its reset record and the removal that record describes")
			if !updateInsideReset {
				if err := <-updateOutcome; err != nil {
					// The update lost the node to the reset: the store answers a
					// write of one node from an older snapshot with a definite
					// conflict, never with an accepted but unapplied write.
					require.ErrorIs(t, err, ErrConflict)
					var accepted *AcceptedCommitError
					require.False(t, errors.As(err, &accepted), "the refused update is not left accepted")
				}
			}

			require.NoError(t, lab.interpose.update(t.Context()), "a fresh update after reset succeeds")
			live := rrvRead(t, lab.engine)
			require.Equal(t, variant.wantFlag, live.Properties["flag"])
			require.Empty(t, live.ChunkEmbeddings, "the clear removed the live node's managed vectors")
			crashClose(t, lab.engine, lab.wal)

			destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
			require.NoError(t, err)
			require.Zero(t, result.Failed, "%v", result.Errors)

			got := rrvRead(t, destination)
			requireSameStoredMap(t, live.Properties, got.Properties, "properties")
			require.Empty(t, got.ChunkEmbeddings, "the rebuilt node holds the vectors the clear removed")
			require.True(t, NodeNeedsEmbedding(got), "the rebuilt node needs embedding; metadata %v", got.EmbedMeta)
		})
	}
}
