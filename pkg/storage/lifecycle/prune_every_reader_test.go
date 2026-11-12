package lifecycle

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// A prune keeps, for every key, the version each registered reader can see,
// not only the oldest reader's. A reader registered before a key existed sees
// none of that key's records; it must not stop newer readers from protecting
// the record they see (review r4509519740 on #1768). This holds for every
// history kind, with retention and TTL, across delete and recreate, and for a
// manual prune with the automatic lifecycle off.

import (
	"context"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type everyReaderFixture struct {
	t       *testing.T
	engine  *storage.BadgerEngine
	manager *MVCCLifecycleManager
	opts    storage.MVCCPruneOptions
}

func newEveryReaderFixture(t *testing.T) *everyReaderFixture {
	return newEveryReaderFixtureWith(t, 100, true, storage.MVCCPruneOptions{MaxVersionsPerKey: 1})
}

// newEveryReaderFixtureWith builds an engine keeping retention versions per
// key, with its lifecycle enabled or not (production disables it when
// retention keeps at most one version, but a manual prune still runs).
func newEveryReaderFixtureWith(t *testing.T, retention int, enabled bool, opts storage.MVCCPruneOptions) *everyReaderFixture {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, EngineOptions: storage.EngineOptions{
		RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: retention},
	}}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	config := DefaultLifecycleConfig()
	config.Enabled = enabled
	config.MaxVersionsPerKey = retention
	manager := NewMVCCLifecycleManager(config, engine)
	engine.SetLifecycleController(manager)
	return &everyReaderFixture{t: t, engine: engine, manager: manager, opts: opts}
}

// write creates or updates id to v and returns the version it committed.
func (f *everyReaderFixture) write(id storage.NodeID, v int) storage.MVCCVersion {
	f.t.Helper()
	node := &storage.Node{ID: id, Properties: map[string]any{"v": v}}
	if _, err := f.engine.GetNodeCurrentHead(context.Background(), id); err != nil {
		_, err := f.engine.CreateNode(f.t.Context(), node)
		require.NoError(f.t, err)
	} else {
		require.NoError(f.t, f.engine.UpdateNode(f.t.Context(), storage.NodeUpdateRequest{After: node}))
	}
	head, err := f.engine.GetNodeCurrentHead(context.Background(), id)
	require.NoError(f.t, err)
	return head.Version
}

func (f *everyReaderFixture) reader(namespace string, version storage.MVCCVersion) {
	f.t.Helper()
	release := f.engine.RegisterSnapshotReader(storage.SnapshotReaderInfo{Namespace: namespace, SnapshotVersion: version, StartTime: time.Now()})
	f.t.Cleanup(release)
}

func (f *everyReaderFixture) prune() int64 {
	f.t.Helper()
	deleted, err := f.manager.RunPruneNow(context.Background(), f.opts)
	require.NoError(f.t, err)
	return deleted
}

func (f *everyReaderFixture) requireVisible(id storage.NodeID, version storage.MVCCVersion, want int) {
	f.t.Helper()
	node, err := f.engine.GetNodeVisibleAt(f.t.Context(), id, version)
	require.NoError(f.t, err, "a registered reader's version was pruned")
	require.EqualValues(f.t, want, node.Properties["v"])
}

func TestManagerPruneKeepsEveryReaderVisibleVersion(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts storage.MVCCPruneOptions
	}{
		{"retention", storage.MVCCPruneOptions{MaxVersionsPerKey: 1}},
		{"retention and TTL", storage.MVCCPruneOptions{MaxVersionsPerKey: 1, MinRetentionAge: time.Millisecond}},
	} {
		t.Run("reader older than the key does not hide a newer reader/"+mode.name, func(t *testing.T) {
			f := newEveryReaderFixtureWith(t, 100, true, mode.opts)
			before := f.write("dba:other", 0)
			f.reader("dba", before) // registered before dba:n exists
			n := storage.NodeID("dba:n")
			var at2 storage.MVCCVersion
			for v := 0; v <= 6; v++ {
				version := f.write(n, v)
				if v == 2 {
					at2 = version
					f.reader("dba", at2)
				}
			}
			time.Sleep(5 * time.Millisecond) // past the TTL
			require.Positive(t, f.prune(), "versions no reader sees are pruned")
			f.requireVisible(n, at2, 2)
		})
	}

	t.Run("several readers at different versions", func(t *testing.T) {
		f := newEveryReaderFixture(t)
		f.reader("dba", f.write("dba:other", 0))
		n := storage.NodeID("dba:n")
		seen := map[int]storage.MVCCVersion{}
		for v := 0; v <= 8; v++ {
			version := f.write(n, v)
			if v == 1 || v == 3 || v == 5 {
				seen[v] = version
				f.reader("dba", version)
			}
		}
		f.prune()
		for v, version := range seen {
			f.requireVisible(n, version, v)
		}
	})

	t.Run("an as-of reader without a database protects every database", func(t *testing.T) {
		f := newEveryReaderFixture(t)
		f.reader("", f.write("dba:other", 0))
		n := storage.NodeID("dba:n")
		var at2 storage.MVCCVersion
		for v := 0; v <= 6; v++ {
			version := f.write(n, v)
			if v == 2 {
				at2 = version
				f.reader("", at2)
			}
		}
		f.prune()
		f.requireVisible(n, at2, 2)
	})

	t.Run("tombstone and recreate", func(t *testing.T) {
		f := newEveryReaderFixture(t)
		f.reader("dba", f.write("dba:other", 0))
		n := storage.NodeID("dba:n")
		var at2 storage.MVCCVersion
		for v := 0; v <= 2; v++ {
			at2 = f.write(n, v)
		}
		f.reader("dba", at2)
		require.NoError(t, f.engine.DeleteNode(t.Context(), n))
		for v := 10; v <= 13; v++ {
			f.write(n, v)
		}
		f.prune()
		f.requireVisible(n, at2, 2)
		current, err := f.engine.GetNode(t.Context(), n)
		require.NoError(t, err)
		require.EqualValues(t, 13, current.Properties["v"])
	})

	t.Run("relationship and both adjacency histories", func(t *testing.T) {
		f := newEveryReaderFixture(t)
		f.reader("dba", f.write("dba:other", 0))
		start, end := storage.NodeID("dba:s"), storage.NodeID("dba:t")
		f.write(start, 0)
		f.write(end, 0)
		e := storage.EdgeID("dba:e")
		link := func(v int) *storage.Edge {
			return &storage.Edge{ID: e, StartNode: start, EndNode: end, Type: "LINK", Properties: map[string]any{"v": v}}
		}
		require.NoError(t, f.engine.CreateEdge(t.Context(), link(0)))
		for v := 1; v <= 2; v++ {
			require.NoError(t, f.engine.UpdateEdge(t.Context(), storage.EdgeUpdateRequest{After: link(v)}))
		}
		head, err := f.engine.GetEdgeCurrentHead(context.Background(), e)
		require.NoError(t, err)
		at2 := head.Version
		f.reader("dba", at2)
		for v := 3; v <= 6; v++ {
			require.NoError(t, f.engine.UpdateEdge(t.Context(), storage.EdgeUpdateRequest{After: link(v)}))
		}
		// Adjacency history: the relationship leaves and comes back twice.
		for round := 0; round < 2; round++ {
			require.NoError(t, f.engine.DeleteEdge(t.Context(), e))
			require.NoError(t, f.engine.CreateEdge(t.Context(), link(10+round)))
		}
		f.prune()
		edge, err := f.engine.GetEdgeVisibleAt(t.Context(), e, at2)
		require.NoError(t, err, "the reader's relationship version was pruned")
		require.EqualValues(t, 2, edge.Properties["v"])
		out, err := f.engine.ReadAdjacentEdgesVisibleAt(t.Context(), storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: start}, at2)
		require.NoError(t, err)
		require.Len(t, out, 1, "the reader's outgoing adjacency was pruned")
		in, err := f.engine.ReadAdjacentEdgesVisibleAt(t.Context(), storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: end}, at2)
		require.NoError(t, err)
		require.Len(t, in, 1, "the reader's incoming adjacency was pruned")
	})

	t.Run("readers in another database protect nothing here", func(t *testing.T) {
		f := newEveryReaderFixture(t)
		n := storage.NodeID("dba:n")
		for v := 0; v <= 6; v++ {
			version := f.write(n, v)
			if v == 2 {
				f.reader("dbb", version)
			}
		}
		// Six archived versions; retention keeps one.
		require.EqualValues(t, 5, f.prune())
	})

	t.Run("a reader older than every record protects none of them", func(t *testing.T) {
		f := newEveryReaderFixture(t)
		f.reader("dba", f.write("dba:other", 0))
		n := storage.NodeID("dba:n")
		for v := 0; v <= 6; v++ {
			f.write(n, v)
		}
		require.EqualValues(t, 5, f.prune())
	})
}

// Retention keeping at most one version turns the automatic lifecycle off,
// but a manual prune still runs (POST .../mvcc/prune). It protects every open
// transaction exactly as with the lifecycle on.
func TestManualPruneWithLifecycleOffProtectsOpenTransactions(t *testing.T) {
	t.Run("retention 1", func(t *testing.T) {
		f := newEveryReaderFixtureWith(t, 1, false, storage.MVCCPruneOptions{})
		n, other := storage.NodeID("dba:n"), storage.NodeID("dbb:n")
		var at2 storage.MVCCVersion
		for v := 0; v <= 2; v++ {
			at2 = f.write(n, v)
			f.write(other, v)
		}
		tx, err := f.engine.BeginTransaction(context.Background())
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, tx.SetNamespace("dba"))
		for v := 3; v <= 6; v++ {
			f.write(n, v)
			f.write(other, v)
		}
		// dba keeps v2..v5 for the transaction (2 of 6 archived go); dbb has
		// no reader and keeps one (5 go).
		require.EqualValues(t, 2+5, f.prune())
		f.requireVisible(n, at2, 2)
		node, err := tx.GetNode(context.Background(), n)
		require.NoError(t, err)
		require.EqualValues(t, 2, node.Properties["v"], "the open transaction keeps its view")
	})

	t.Run("retention 0", func(t *testing.T) {
		f := newEveryReaderFixtureWith(t, 0, false, storage.MVCCPruneOptions{})
		start, end := storage.NodeID("dba:s"), storage.NodeID("dba:t")
		f.write(start, 0)
		f.write(end, 0)
		e := storage.EdgeID("dba:e")
		require.NoError(t, f.engine.CreateEdge(t.Context(), &storage.Edge{ID: e, StartNode: start, EndNode: end, Type: "LINK"}))
		tx, err := f.engine.BeginTransaction(context.Background())
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, tx.SetNamespace("dba"))
		require.NoError(t, f.engine.DeleteEdge(t.Context(), e))
		require.NoError(t, f.engine.DeleteNode(t.Context(), start))
		require.NoError(t, f.manager.TriggerPruneNow(context.Background()))
		// The open transaction still reads the relationship and its endpoint.
		edge, err := tx.GetEdge(context.Background(), e)
		require.NoError(t, err)
		require.Equal(t, start, edge.StartNode)
		node, err := tx.GetNode(context.Background(), start)
		require.NoError(t, err)
		require.Equal(t, start, node.ID)
	})
}
