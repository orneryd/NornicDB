package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type writeBehindTestOptions struct {
	dataDir string
	wal     bool
	// Existing callers keep the background write-behind worker by default.
	synchronous bool
	manualDrain bool
	lowMemory   bool
}

func newWriteBehindTestEngine(t testing.TB, interval time.Duration, options ...writeBehindTestOptions) *BadgerEngine {
	t.Helper()
	var opts writeBehindTestOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.dataDir == "" {
		opts.dataDir = t.TempDir()
	}
	if opts.wal {
		t.Cleanup(config.WithWALEnabled())
	}
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             opts.dataDir,
		LowMemory:           opts.lowMemory,
		WriteBehind:         !opts.synchronous,
		WriteBehindInterval: interval,
	}))
	if engine != nil {
		fixtureowner.OwnChecked(t, engine)
	}
	require.NoError(t, err)
	if opts.manualDrain && engine.writeBehind != nil {
		engine.writeBehind.SetDelayBounds(interval, interval)
		engine.writeBehind.Close()
	}
	if opts.wal {
		walCfg := DefaultWALConfig()
		walCfg.Dir = filepath.Join(opts.dataDir, "wal")
		walCfg.BatchSyncInterval = time.Hour
		wal, err := NewWAL(walCfg.Dir, walCfg)
		if wal != nil {
			fixtureowner.OwnChecked(t, wal, engine)
		}
		require.NoError(t, err)
		engine.SetBufferedCommitWAL(wal)
	}
	return engine
}

func bufferedWrite(t testing.TB, engine *BadgerEngine, write func(*BadgerTransaction) error) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, write(tx))
	require.NoError(t, tx.Commit())
}

func bufferedCreate(t testing.TB, engine *BadgerEngine, node *Node) {
	t.Helper()
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(node); return err })
}

func TestWriteBehind_BufferedCommitVisibleBeforeFlush(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	require.Greater(t, engine.writeBehind.PendingOps(), 0, "commit must stay buffered before flush")

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"], "acknowledged write visible through the overlay")
}

func TestWriteBehind_FlushLandsInBadger(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"])

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestWriteBehind_MultipleCommitsApplyInOrder(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	// Updating the same node in a later buffered commit must shadow the
	// earlier version (a second create of the same ID now correctly
	// conflicts via the overlay).
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	err = tx.UpdateGraphNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(2)}}, ClientNodeEdits)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"], "newest commit shadows the older one")

	require.NoError(t, engine.FlushWriteBehind())
	got, err = engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
}

func TestWriteBehind_BufferedDeleteHidesAndPersists(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})
	require.NoError(t, engine.FlushWriteBehind())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:n1", CascadeEdges))
	require.NoError(t, tx.Commit())

	_, err = engine.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "buffered delete hides the node")

	require.NoError(t, engine.FlushWriteBehind())
	_, err = engine.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "delete persists after flush")
}

// TestWriteBehind_ScansSeeBufferedWrites reproduces the Northwind seed
// verification failure: label scans, label streams and edge-type reads must
// see acknowledged-but-unflushed writes through the overlay immediately, not
// only after the background flush lands them.
func TestWriteBehind_ScansSeeBufferedWrites(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	for i := 0; i < 5; i++ {
		bufferedCreate(t, engine, &Node{
			ID:         NodeID("test:o" + strconv.Itoa(i)),
			Labels:     []string{"Order"},
			Properties: map[string]any{"v": int64(i)},
		})
	}
	for i := 0; i < 3; i++ {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		err = tx.CreateEdge(&Edge{
			ID:        EdgeID("test:p" + strconv.Itoa(i)),
			StartNode: "test:o0",
			EndNode:   "test:o1",
			Type:      "PURCHASED",
		})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	// Nothing has flushed: every read path below must see the buffered data.
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	nodes, err := engine.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, nodes, 5, "GetNodesByLabel must merge the overlay")

	// The derived-count fast paths must include buffered deltas too.
	labelCount, err := engine.NodeCountByLabel("Order")
	require.NoError(t, err)
	require.Equal(t, int64(5), labelCount, "NodeCountByLabel must include buffered deltas")
	nsLabelCount, err := engine.NodeCountByLabelInNamespace("test", "Order")
	require.NoError(t, err)
	require.Equal(t, int64(5), nsLabelCount, "NodeCountByLabelInNamespace must include buffered deltas")

	allNodes, err := engine.AllNodes()
	require.NoError(t, err)
	foundOrder := 0
	for _, n := range allNodes {
		for _, label := range n.Labels {
			if label == "Order" {
				foundOrder++
			}
		}
	}
	require.Equal(t, 5, foundOrder, "AllNodes must merge the overlay")

	var streamed int
	err = engine.StreamNodesByLabelProjected("Order", nil, func(n *Node) error {
		streamed++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 5, streamed, "StreamNodesByLabelProjected must merge the overlay")

	edges, err := engine.GetEdgesByType("PURCHASED")
	require.NoError(t, err)
	require.Len(t, edges, 3, "GetEdgesByType must merge the overlay")

	edgeCount, err := engine.EdgeCountByType("PURCHASED")
	require.NoError(t, err)
	require.Equal(t, int64(3), edgeCount, "EdgeCountByType must include buffered deltas")

	allEdges, err := engine.AllEdges()
	require.NoError(t, err)
	purchased := 0
	for _, e := range allEdges {
		if e.Type == "PURCHASED" {
			purchased++
		}
	}
	require.Equal(t, 3, purchased, "AllEdges must merge the overlay")

	var streamedEdges int
	err = engine.StreamEdgesByType(context.Background(), "PURCHASED", func(e *Edge) error {
		streamedEdges++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, streamedEdges, "StreamEdgesByType must merge the overlay")

	// A buffered delete of a node also hides it from label scans.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:o0", CascadeEdges))
	require.NoError(t, tx.Commit())

	nodes, err = engine.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, nodes, 4, "buffered delete must hide the node from label scans")

	require.NoError(t, engine.FlushWriteBehind())
	nodes, err = engine.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, nodes, 4, "delete persists through the flush")
}

func TestWriteBehind_ExplicitBeginDrainsBufferedWrites(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	// The storage layer's BeginTransaction does not drain (autocommit uses it);
	// the explicit-transaction admission path drains via FlushWriteBehind.
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode("test:n1")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.NoError(t, tx.Rollback())
}

func TestWriteBehind_CloseDrainsAndPersists(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             dir,
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(7)}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(7), got.Properties["v"], "Close must persist acknowledged buffered writes")
}

// TestWriteBehind_DetachDeleteAcrossBatchesDeletesEdgesOnce reproduces the
// Northwind wipe/reseed corruption: a bounded DETACH DELETE wipe spans many
// statements, and each relationship is reachable from BOTH endpoints'
// adjacency indexes. Without the write-behind tombstone overlay, the second
// endpoint's batch re-deletes relationships the first batch already deleted
// (and re-applies their derived-count deltas), and the replay later grinds
// through tombstoned-head fallback iterators.
func TestWriteBehind_DetachDeleteAcrossBatchesDeletesEdgesOnce(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	// a -> b and b -> c: deleting a and b in separate buffered statements
	// reaches the a->b relationship from both endpoints.
	bufferedCreate(t, engine, &Node{ID: "test:a", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:b", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:c", Labels: []string{"N"}})
	for _, e := range []*Edge{
		{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"},
		{ID: "test:bc", StartNode: "test:b", EndNode: "test:c", Type: "R"},
	} {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.CreateEdge(e))
		require.NoError(t, tx.Commit())
	}
	// Land the creates so the delete path reads committed records, exactly
	// like a wipe of previously persisted data.
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, int64(2), mustEdgeCount(t, engine))

	// Wipe batch 1: delete a, which detaches a->b.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
	require.NoError(t, tx.Commit())

	// The buffered delete must hide the relationship immediately.
	require.True(t, engine.writeBehind.EdgeDeleted("test:ab"))
	require.Equal(t, int64(-1), engine.writeBehind.EdgeTypeCountDelta("R"))
	edges, err := engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Len(t, edges, 1, "buffered detach delete must hide the relationship from type scans")

	// Wipe batch 2: delete b. b->c goes, but a->b was already deleted and
	// must not be counted or replayed again.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:b", CascadeEdges))
	require.NoError(t, tx.Commit())

	require.Equal(t, int64(-2), engine.writeBehind.EdgeTypeCountDelta("R"),
		"each relationship must be deleted exactly once across wipe batches")
	require.True(t, engine.writeBehind.EdgeDeleted("test:ab"))
	require.True(t, engine.writeBehind.EdgeDeleted("test:bc"))

	edges, err = engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Empty(t, edges, "both relationships hidden before the flush")

	// The flush lands both batches: counts must not underflow and the
	// type is empty.
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.LastErr())
	require.Zero(t, mustEdgeCount(t, engine), "wipe must not double-delete the cached total edge count")
	count, err := engine.EdgeCountByType("R")
	require.NoError(t, err)
	require.Zero(t, count, "wipe must not double-delete derived edge-type counts")
	edges, err = engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Empty(t, edges)

	// Reseed and confirm the round trip converges.
	bufferedCreate(t, engine, &Node{ID: "test:a2", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:b2", Labels: []string{"N"}})
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:a2b2", StartNode: "test:a2", EndNode: "test:b2", Type: "R"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, int64(1), mustEdgeCount(t, engine), "reseeded relationship counted exactly once")
	count, err = engine.EdgeCountByType("R")
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "reseeded relationship counted exactly once")
}

// mustEdgeCount returns the engine's cached total edge count.
func mustEdgeCount(t *testing.T, engine *BadgerEngine) int64 {
	t.Helper()
	n, err := engine.EdgeCount()
	require.NoError(t, err)
	return n
}

// TestWriteBehind_DetachDeleteAfterFlushStillDeletesOnce covers the
// interleaving the Northwind wipe hits when the flusher lands a wipe batch's
// generation before the next bounded batch runs: the second batch must see
// the already-applied relationship as gone through Badger, not re-delete it.
func TestWriteBehind_DetachDeleteAfterFlushStillDeletesOnce(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:a", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:b", Labels: []string{"N"}})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())

	// Wipe batch 1: delete a (detaches a->b). Flush immediately: the
	// generation is applied and removed from the buffer's overlay.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())

	// Wipe batch 2: delete b. The relationship is already tombstoned in
	// Badger; the batch must not delete it again.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:b", CascadeEdges))
	require.NoError(t, tx.Commit())
	require.Equal(t, int64(0), engine.writeBehind.EdgeTypeCountDelta("R"),
		"already-applied relationship must not be deleted twice")
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.LastErr())
	require.Zero(t, mustEdgeCount(t, engine))
	count, err := engine.EdgeCountByType("R")
	require.NoError(t, err)
	require.Zero(t, count)
}

// TestWriteBehind_PropertyIndexBufferedGenerations reproduces the Pokec
// batched-load loss: a property-index seek must find nodes whose commits are
// acknowledged but not yet flushed, or MATCH (n:User {id: $x}) CREATE …
// silently produces nothing. The buffer attaches to the schema as its
// pending-write source.
// AcrossGenerations keeps the pending index
// correct when generations land while a later generation still holds a
// newer version of the same node.

// KeepsUnchangedValueOnBufferedUpdate is the
// #1043 follow-up: a buffered update that keeps a flushed node's indexed
// value (MATCH (n:User {id: 1}) SET n.v = …) must leave the node findable by
// that value. The committed entry is superseded by the buffered version, so
// the pending index has to list it, or the next MATCH by id finds nothing
// and its SET silently does nothing.
func TestWriteBehind_PropertyIndexBufferedGenerations(t *testing.T) {
	type lookup struct {
		value   int64
		present bool
		message string
	}
	type step struct {
		action      string
		id, version int64
		lookups     []lookup
	}
	for _, tc := range []struct {
		name        string
		withVersion bool
		steps       []step
	}{
		{name: "SeesBufferedNodes", steps: []step{
			{lookups: []lookup{{1, true, "index lookup must see buffered nodes before the flush"}, {2, false, ""}}},
			{action: "flush", lookups: []lookup{{1, true, ""}}},
			{action: "update", id: 2, lookups: []lookup{{1, false, "buffered rewrite must supersede the committed index entry"}, {2, true, ""}}},
			{action: "delete", lookups: []lookup{{2, false, "buffered delete must hide the node from index seeks"}}},
			{action: "flush", lookups: []lookup{{2, false, ""}}},
		}},
		{name: "AcrossGenerations", steps: []step{
			{action: "flush", lookups: []lookup{{1, true, ""}}},
			{action: "update", id: 7, lookups: []lookup{{7, true, ""}, {1, false, ""}}},
			{action: "delete", lookups: []lookup{{7, false, ""}}},
			{action: "create", id: 9, lookups: []lookup{{9, true, ""}}},
			{action: "flush", lookups: []lookup{{9, true, ""}, {7, false, ""}}},
		}},
		{name: "KeepsUnchangedValueOnBufferedUpdate", withVersion: true, steps: []step{
			{action: "flush"},
			{action: "update", id: 1, version: 1, lookups: []lookup{{1, true, "a buffered update that keeps the indexed value must stay findable by it"}}},
			{action: "update", id: 1, version: 2, lookups: []lookup{{1, true, "a second buffered update of the same node must stay findable too"}}},
			{action: "flush", lookups: []lookup{{1, true, ""}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour)
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddPropertyIndex("user_id_idx", "User", []string{"id"}))
			node := func(id, version int64) *Node {
				props := map[string]any{"id": id}
				if tc.withVersion {
					props["v"] = version
				}
				return &Node{ID: "test:u1", Labels: []string{"User"}, Properties: props}
			}
			bufferedCreate(t, engine, node(1, 0))
			for _, step := range tc.steps {
				switch step.action {
				case "": // Observe the initial buffered generation.
				case "flush":
					require.NoError(t, engine.FlushWriteBehind())
				case "create":
					bufferedCreate(t, engine, node(step.id, step.version))
				case "update":
					bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
						return tx.UpdateGraphNode(node(step.id, step.version), ClientNodeEdits)
					})
				case "delete":
					bufferedWrite(t, engine, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode("test:u1", CascadeEdges) })
				default:
					t.Fatalf("unknown property-index test action %q", step.action)
				}
				for _, check := range step.lookups {
					got := schema.PropertyIndexLookup("User", "id", check.value)
					if check.present {
						require.Equal(t, []NodeID{"test:u1"}, got, check.message)
					} else {
						require.Empty(t, got, check.message)
					}
				}
			}
			if tc.withVersion {
				got, err := engine.GetNode("test:u1")
				require.NoError(t, err)
				require.Equal(t, int64(2), got.Properties["v"], "the last buffered update must be the one applied")
			}
		})
	}
}

func TestWriteBehind_DisabledCommitIsSynchronous(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	require.Nil(t, engine.writeBehind)
	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "disabled engine commits synchronously")
}
