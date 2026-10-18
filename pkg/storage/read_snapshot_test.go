package storage

import (
	"context"
	"fmt"
	"strings"
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
// a write stream keeps on disk: the versions it can still see, which
// compaction must keep until it closes. Each round overwrites every value of
// a 4,000-node store (4 KiB of text each) once while a snapshot is open,
// compacts, and reports the store's size with the snapshot open and after it
// is closed and compacted again.
func BenchmarkReadSnapshotDiskRetention(b *testing.B) {
	const nodes, valueBytes = 4000, 4 << 10
	text := func(round int) string { return strings.Repeat(fmt.Sprint(round%10), valueBytes) }
	for range b.N {
		b.StopTimer()
		engine := newReadSnapshotTestEngine(b, false)
		for i := range nodes {
			_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": text(0)}})
			require.NoError(b, err)
		}
		compact := func() int64 {
			require.NoError(b, engine.db.DB.Flatten(1))
			for engine.db.DB.RunValueLogGC(0.01) == nil {
			}
			lsm, vlog := engine.db.DB.Size()
			return lsm + vlog
		}
		baseline := compact()
		snapshot, err := engine.OpenReadSnapshot(context.Background())
		require.NoError(b, err)
		b.StartTimer()
		for i := range nodes {
			require.NoError(b, engine.UpdateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": text(1)}}))
		}
		b.StopTimer()
		held := compact()
		snapshot.Close()
		// The discard timestamp advances on a commit (managedBadgerDB).
		require.NoError(b, engine.UpdateNode(&Node{ID: "test:n0", Labels: []string{"Doc"}, Properties: map[string]any{"text": text(1)}}))
		released := compact()
		b.ReportMetric(float64(nodes*valueBytes), "overwritten-B")
		b.ReportMetric(float64(held-baseline), "held-growth-B")
		b.ReportMetric(float64(released-baseline), "released-growth-B")
	}
}
