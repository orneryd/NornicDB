package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// L561: the journal refuses the per-node embedding reset record (journal
// closed) while the store still works.
func TestEI4R1795ClearJournalFailure(t *testing.T) {
	lab := rrvOpen(t, t.TempDir())
	lab.create("alpha", rrvVectors(2, 1), rrvWorkerMeta(2), nil)
	require.NoError(t, lab.wal.Close())
	count, err := lab.journal.ClearAllEmbeddingsForPrefix("test:")
	got, gerr := lab.engine.GetNode(context.Background(), rrvNode)
	require.NoError(t, gerr)
	t.Logf("CLEARFAIL cleared=%d err=%v remaining-vector-chunks=%d", count, err, len(got.ChunkEmbeddings))
	if err == nil {
		require.Equal(t, 1, count, "must not report a successful clear when the journal rejected it")
		require.Empty(t, got.ChunkEmbeddings)
	}
}

// L563: an ordinary update of the same node scheduled between the clear's
// journal record and its store write (the reviewer's scheduling point, on the
// head's journal callback).
type ei4ClearHook struct {
	*BadgerEngine
	hook func()
}

func (h *ei4ClearHook) GetInnerEngine() Engine { return h.BadgerEngine }

func (h *ei4ClearHook) clearEmbeddingsForPrefix(prefix string, journal func(NodeID, func() error) error) (int, error) {
	return h.BadgerEngine.clearEmbeddingsForPrefix(prefix, func(id NodeID, clear func() error) error {
		return journal(id, func() error { h.hook(); return clear() })
	})
}

func TestEI4R1795ClearJournalOrderWithNoopNodeWrite(t *testing.T) {
	for _, change := range []string{"noop", "property"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			lab := rrvOpen(t, t.TempDir())
			lab.create("alpha", rrvVectors(2, 1), rrvWorkerMeta(2), nil)
			snapshot := lab.snapshotPeriodic()
			inner := &ei4ClearHook{BadgerEngine: lab.engine}
			journal := NewWALEngine(inner, lab.wal)
			inner.hook = func() {
				n := lab.node()
				if change == "property" {
					n.Properties["flag"] = true
				}
				err := journal.UpdateNode(ctx, NodeUpdateRequest{After: n})
				t.Logf("hooked update err=%v", err)
			}
			count, err := journal.ClearAllEmbeddingsForPrefix("test:")
			t.Logf("clear count=%d err=%v live chunks=%d", count, err, len(lab.node().ChunkEmbeddings))
			require.NoError(t, err)
			crashClose(t, lab.engine, lab.wal)
			dest := newWriteBehindTestEngine(t, 0, writeBehindTestOptions{dataDir: t.TempDir(), synchronous: true})
			result, _, err := RecoverIntoEngine(ctx, dest, filepath.Join(lab.dir, "wal"), snapshot)
			require.NoError(t, err)
			require.Zero(t, result.Failed, "%v", result.Errors)
			n, err := dest.GetNode(ctx, rrvNode)
			require.NoError(t, err)
			t.Logf("CLEARORDER %s count=%d rebuilt vectors=%v metadata=%v eligible=%v", change, count, rrvBits(n.ChunkEmbeddings), n.EmbedMeta, NodeNeedsEmbedding(n))
			require.Empty(t, n.ChunkEmbeddings, "a successful clear must not restore the old vectors after a concurrent update")
		})
	}
}

// L562: a client write that supplies inline managed vectors (unchanged text)
// while an embedding worker attempt for the same node is in flight; the
// worker then stores its vectors beside the node. A periodic snapshot, then a
// rebuild from snapshot plus journal.
func TestEI4InlineClientWriteDuringWorkerAttempt(t *testing.T) {
	for _, snap := range []string{"periodic", "full"} {
		t.Run(snap, func(t *testing.T) {
			ctx := context.Background()
			lab := rrvOpen(t, t.TempDir())
			lab.create("alpha", nil, nil, nil)
			workerView := lab.node() // the worker's read at the start of its attempt
			client := lab.node()
			client.ChunkEmbeddings, client.EmbedMeta = rrvVectors(2, 1), rrvWorkerMeta(2)
			require.NoError(t, lab.journal.UpdateNode(ctx, NodeUpdateRequest{After: client}))
			afterClient := rrvRead(t, lab.engine)
			workerView.ChunkEmbeddings, workerView.EmbedMeta = rrvVectors(3, 100), rrvWorkerMeta(3)
			werr := lab.journal.UpdateNodeEmbeddingSidecar(ctx, workerView)
			live := rrvRead(t, lab.engine)
			snapshot := ""
			if snap == "periodic" {
				snapshot = lab.snapshotPeriodic()
			} else {
				snapshot = lab.snapshotFull()
			}
			crashClose(t, lab.engine, lab.wal)
			dst := newWriteBehindTestEngine(t, 0, writeBehindTestOptions{dataDir: t.TempDir(), synchronous: true})
			result, _, err := RecoverIntoEngine(ctx, dst, filepath.Join(lab.dir, "wal"), snapshot)
			require.NoError(t, err)
			require.Zero(t, result.Failed, "%v", result.Errors)
			got := rrvRead(t, dst)
			t.Logf("INLINEWORKER %s workerErr=%v afterClient=%v live=%v liveMeta=%v rebuilt=%v rebuiltMeta=%v needs=%v", snap, werr, rrvBits(afterClient.ChunkEmbeddings), rrvBits(live.ChunkEmbeddings), live.EmbedMeta, rrvBits(got.ChunkEmbeddings), got.EmbedMeta, NodeNeedsEmbedding(got))
			if len(got.ChunkEmbeddings) > 0 {
				require.Equal(t, rrvBits(live.ChunkEmbeddings), rrvBits(got.ChunkEmbeddings), "a rebuild must not present superseded vectors as current")
			}
		})
	}
}

// L559: with asynchronous writes, a vector refresh by the embedding worker
// after a snapshot, then an ordinary update journaled as a buffered commit;
// a rebuild from the snapshot plus the journal.
func TestEI4AsyncRebuildAfterVectorRefresh(t *testing.T) {
	for _, async := range []bool{true, false} {
		for _, compact := range []bool{false, true} {
			name := map[bool]string{true: "async", false: "sync"}[async] + map[bool]string{true: "/periodic-snapshot", false: "/snapshot-no-truncate"}[compact]
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !async, manualDrain: true})
				engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
				lab := &rrvLab{t: t, dir: dir, engine: engine, wal: engine.bufferedCommitWAL, journal: NewWALEngine(engine, engine.bufferedCommitWAL)}
				lab.create("alpha", rrvVectors(2, 1), rrvWorkerMeta(2), nil)
				if async {
					require.NoError(t, engine.FlushWriteBehind(ctx))
				}
				var snapshot string
				if compact {
					snapshot = lab.snapshotPeriodic()
				} else {
					snapshot = filepath.Join(dir, "snap.nds")
					require.NoError(t, SaveStreamingSnapshot(ctx, engine, snapshot, SnapshotOptions{Sequence: lab.wal.Sequence()}))
				}
				lab.sidecar(func(n *Node) { n.ChunkEmbeddings, n.EmbedMeta = rrvVectors(3, 100), map[string]any{"has_embedding": true, "chunk_count": 3, "embedding_model": "model-B"} })
				n := lab.node()
				n.Properties["flag"] = true
				require.NoError(t, lab.journal.UpdateNode(ctx, NodeUpdateRequest{After: n}))
				if async {
					require.NoError(t, engine.FlushWriteBehind(ctx))
				}
				live := rrvRead(t, engine)
				ops := map[OperationType]int{}
				if entries, err := ReadWALEntriesFromDir(ctx, filepath.Join(dir, "wal")); err == nil {
					for _, e := range entries {
						ops[e.Operation]++
					}
				}
				crashClose(t, engine, lab.wal)
				dst := newWriteBehindTestEngine(t, 0, writeBehindTestOptions{dataDir: t.TempDir(), synchronous: true})
				result, _, err := RecoverIntoEngine(ctx, dst, filepath.Join(dir, "wal"), snapshot)
				require.NoError(t, err)
				got := rrvRead(t, dst)
				t.Logf("ASYNCREBUILD %s journal=%v result=%+v live=%v liveMeta=%v rebuilt=%v rebuiltMeta=%v flag=%v needs=%v", name, ops, result, rrvBits(live.ChunkEmbeddings), live.EmbedMeta, rrvBits(got.ChunkEmbeddings), got.EmbedMeta, got.Properties["flag"], NodeNeedsEmbedding(got))
				if len(got.ChunkEmbeddings) > 0 {
					require.Equal(t, rrvBits(live.ChunkEmbeddings), rrvBits(got.ChunkEmbeddings), "rebuilt vectors must be the live ones")
				}
			})
		}
	}
}
