package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newReadSnapshotTestEngine(t testing.TB, writeBehind bool) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         writeBehind,
		WriteBehindInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func snapshotNodeText(t *testing.T, snapshot ReadSnapshotView) map[NodeID]string {
	t.Helper()
	out := map[NodeID]string{}
	require.NoError(t, snapshot.IterateNodes(func(node *Node) bool {
		text, _ := node.Properties["text"].(string)
		if _, seen := out[node.ID]; seen {
			t.Errorf("node %s visited twice", node.ID)
		}
		out[node.ID] = text
		return true
	}))
	return out
}

func snapshotEdgeNotes(t *testing.T, snapshot ReadSnapshotView) map[EdgeID]string {
	t.Helper()
	out := map[EdgeID]string{}
	require.NoError(t, snapshot.StreamEdges(context.Background(), func(edge *Edge) error {
		note, _ := edge.Properties["note"].(string)
		if _, seen := out[edge.ID]; seen {
			t.Errorf("edge %s visited twice", edge.ID)
		}
		out[edge.ID] = note
		return nil
	}))
	return out
}

// TestReadSnapshotSeesOnePoint: a node and its relationship changed together
// in one transaction are seen together, before or after, by every read of
// one snapshot, whichever read runs first.
func TestReadSnapshotSeesOnePoint(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "old"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"note": "old"}}))

	snapshot, err := engine.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	defer snapshot.Close()

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "new"}}))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"note": "new"}}))
	require.NoError(t, tx.Commit())

	require.Equal(t, "old", snapshotEdgeNotes(t, snapshot)["test:r"])
	require.Equal(t, "old", snapshotNodeText(t, snapshot)["test:a"])

	later, err := engine.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	defer later.Close()
	require.Equal(t, "new", snapshotNodeText(t, later)["test:a"])
	require.Equal(t, "new", snapshotEdgeNotes(t, later)["test:r"])
}

// TestReadSnapshotIncludesWriteBehind: acknowledged write-behind entries are
// part of the snapshot, and a flush while the snapshot is open neither loses
// nor duplicates them.
func TestReadSnapshotIncludesWriteBehind(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, true)
	for _, id := range []NodeID{"test:a", "test:b"} {
		bufferedCreate(t, engine, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "buffered"}})
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"note": "buffered"}}))
	require.NoError(t, tx.Commit())
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the writes must still be buffered")

	snapshot, err := engine.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	defer snapshot.Close()
	require.NoError(t, engine.FlushWriteBehind())

	require.Equal(t, map[NodeID]string{"test:a": "buffered", "test:b": "buffered"}, snapshotNodeText(t, snapshot))
	require.Equal(t, map[EdgeID]string{"test:r": "buffered"}, snapshotEdgeNotes(t, snapshot))
}

// TestReadSnapshotIsScopedByNamespace: a namespaced engine's snapshot reads
// only its database, with user-visible IDs.
func TestReadSnapshotIsScopedByNamespace(t *testing.T) {
	base := newReadSnapshotTestEngine(t, false)
	one, two := NewNamespacedEngine(base, "one"), NewNamespacedEngine(base, "two")
	for _, engine := range []*NamespacedEngine{one, two} {
		_, err := engine.CreateNode(&Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"text": engine.Namespace()}})
		require.NoError(t, err)
	}
	snapshot, err := OpenReadSnapshot(context.Background(), one)
	require.NoError(t, err)
	defer snapshot.Close()
	require.Equal(t, map[NodeID]string{"n": "one"}, snapshotNodeText(t, snapshot))
}

type snapshotlessEngine struct{ Engine }

// TestReadSnapshotUnsupportedEngine: an engine without the capability, or a
// wrapper over one, reports ErrReadSnapshotUnsupported.
func TestReadSnapshotUnsupportedEngine(t *testing.T) {
	base := newReadSnapshotTestEngine(t, false)
	for _, engine := range []any{snapshotlessEngine{Engine: base}, NewNamespacedEngine(snapshotlessEngine{Engine: base}, "one"), NewWALEngine(snapshotlessEngine{Engine: base}, nil)} {
		_, err := OpenReadSnapshot(context.Background(), engine)
		require.ErrorIs(t, err, ErrReadSnapshotUnsupported)
		require.False(t, SupportsReadSnapshot(context.Background(), engine))
	}
	require.True(t, SupportsReadSnapshot(context.Background(), NewNamespacedEngine(NewWALEngine(base, nil), "one")))
}

// BenchmarkReadSnapshotDiskRetention measures what a snapshot held open under
// a write stream keeps on disk: every version written after it opened, which
// compaction must keep until it closes (a version above the discard
// timestamp is never dropped). A store of 8,000 nodes, 8 KiB of
// incompressible text each, is overwritten in full four times (256 MiB)
// while a snapshot is open, enough to flush the memtables. After each phase
// the levels are compacted and the SST bytes on disk are reported: without a
// snapshot, with the snapshot open, after it is closed, and after the same
// stream once more once it is closed.
func BenchmarkReadSnapshotDiskRetention(b *testing.B) {
	const nodes, valueBytes, rounds = 8000, 8 << 10, 4
	r := rand.New(rand.NewSource(1))
	text := func() string {
		buf := make([]byte, valueBytes/2)
		_, _ = r.Read(buf)
		return hex.EncodeToString(buf)
	}
	for range b.N {
		b.StopTimer()
		engine := newReadSnapshotTestEngine(b, false)
		overwrite := func() {
			for range rounds {
				for i := range nodes {
					require.NoError(b, engine.UpdateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": text()}}))
				}
			}
		}
		for i := range nodes {
			_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": text()}})
			require.NoError(b, err)
		}
		dir := engine.db.DB.Opts().Dir
		sstBytes := func() int64 {
			require.NoError(b, engine.db.DB.Flatten(1))
			var total int64
			require.NoError(b, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() || filepath.Ext(path) != ".sst" {
					return err
				}
				info, err := entry.Info()
				if err == nil {
					total += info.Size()
				}
				return err
			}))
			return total
		}
		// Control: the same stream with no snapshot open.
		overwrite()
		unheld := sstBytes()
		snapshot, err := engine.OpenReadSnapshot(context.Background())
		require.NoError(b, err)
		b.StartTimer()
		overwrite()
		b.StopTimer()
		held := sstBytes()
		snapshot.Close()
		// The discard timestamp advances on a commit (managedBadgerDB).
		require.NoError(b, engine.UpdateNode(&Node{ID: "test:n0", Labels: []string{"Doc"}, Properties: map[string]any{"text": text()}}))
		released := sstBytes()
		// The levels are already flat: the retained versions go when later
		// compactions rewrite their tables, as the write stream goes on.
		overwrite()
		streamed := sstBytes()
		b.ReportMetric(float64(nodes*valueBytes*rounds), "overwritten-B")
		b.ReportMetric(float64(unheld), "sst-no-snapshot-B")
		b.ReportMetric(float64(held), "sst-snapshot-open-B")
		b.ReportMetric(float64(released), "sst-after-close-B")
		b.ReportMetric(float64(streamed), "sst-after-close-and-stream-B")
	}
}
