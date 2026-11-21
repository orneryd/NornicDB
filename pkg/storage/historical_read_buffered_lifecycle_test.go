package storage

// N152-86: a historical read answers the state an acknowledged write-behind
// commit established, not only the state a drain already landed. The buffer
// allocates the commit's MVCC version at ACK time (bufferedCommit.version) and
// the drain replays that commit under exactly that version, so one cutoff
// answers the same graph before and after the drain, while a view opened at a
// cutoff never promotes a later ACK into itself.
//
// The workload here is the lifecycle those rules must survive, read through the
// public engine contract: a buffered update with a relationship move, a
// buffered delete, a buffered recreation, and one pinned view held across a
// drain and later ACKs. Every buffered commit asserts AcknowledgedBuffered, so
// a drained apply is never mistaken for the reproduction.

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// historicalBufferedEngine is the retained-history write-behind fixture. The
// background drain is stopped, so every ACK stays unflushed until the test
// flushes it, and retention keeps the superseded bodies a cutoff before the ACK
// still resolves.
func historicalBufferedEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
		EngineOptions:       EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}},
	}))
	require.NoError(t, err)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	engine.writeBehind.Close()
	return engine
}

// historicalBufferedWrite runs one buffered commit and asserts the write-behind
// buffer acknowledged it.
func historicalBufferedWrite(t *testing.T, engine *BadgerEngine, write func(*BadgerTransaction) error) {
	t.Helper()
	tx := bufferedWrite(t, engine, write)
	require.True(t, tx.AcknowledgedBuffered(), "the write must be acknowledged through the write-behind buffer")
}

// historicalBufferedNodeUpdate rewrites a node's labels and properties in one
// acknowledged buffered commit.
func historicalBufferedNodeUpdate(t *testing.T, engine *BadgerEngine, id NodeID, labels []string, properties map[string]any) {
	t.Helper()
	historicalBufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		current, err := tx.GetNode(context.Background(), id)
		if err != nil {
			return err
		}
		return tx.UpdateGraphNode(context.Background(), NodeUpdateRequest{
			After:  &Node{ID: current.ID, Labels: labels, Properties: properties},
			Policy: ClientNodeEdits,
		})
	})
}

// historicalBufferedEdgeUpdate rewrites a relationship's endpoints and
// properties in one acknowledged buffered commit: the move and the update share
// the commit the adjacency overlay has to honor.
func historicalBufferedEdgeUpdate(t *testing.T, engine *BadgerEngine, id EdgeID, start, end NodeID, properties map[string]any) {
	t.Helper()
	historicalBufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		current, err := tx.GetEdge(context.Background(), id)
		if err != nil {
			return err
		}
		return tx.UpdateEdge(context.Background(), EdgeUpdateRequest{After: &Edge{
			ID:         current.ID,
			Type:       current.Type,
			StartNode:  start,
			EndNode:    end,
			Properties: properties,
		}})
	})
}

func historicalBufferedNodeCreate(t *testing.T, engine *BadgerEngine, node *Node) {
	t.Helper()
	historicalBufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(context.Background(), node)
		return err
	})
}

func historicalBufferedEdgeCreate(t *testing.T, engine *BadgerEngine, edge *Edge) {
	t.Helper()
	historicalBufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(context.Background(), edge)
	})
}

func historicalBufferedNodeDelete(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	historicalBufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.DeleteGraphNode(context.Background(), id, RejectConnected)
	})
}

func historicalBufferedEdgeDelete(t *testing.T, engine *BadgerEngine, id EdgeID) {
	t.Helper()
	historicalBufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.DeleteEdge(context.Background(), id)
	})
}

// historicalFlushed drains the acknowledged commits into the store. The
// fixture's background worker is stopped, so the flush must be uncancellable: a
// cancellable waiter only wakes a worker that no longer runs.
func historicalFlushed(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the acknowledged commits must still be undrained")
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	require.Zero(t, engine.writeBehind.PendingOps(), "the drain must land every acknowledged commit")
}

// requireHistoricalNodeAt asserts the node's point read at the view's cutoff.
func requireHistoricalNodeAt(t *testing.T, historical Engine, id NodeID, property string, want any, wantLabels []string) {
	t.Helper()
	node, err := historical.GetNode(t.Context(), id)
	require.NoError(t, err, "node %s must answer at the cutoff", id)
	require.Equal(t, want, node.Properties[property], "node %s property %s at the cutoff", id, property)
	if wantLabels != nil {
		require.Equal(t, wantLabels, node.Labels, "node %s labels at the cutoff", id)
	}
}

func requireHistoricalNodeAbsent(t *testing.T, historical Engine, id NodeID) {
	t.Helper()
	_, err := historical.GetNode(t.Context(), id)
	require.ErrorIs(t, err, ErrNotFound, "node %s must be absent at the cutoff", id)
}

// requireHistoricalEdgeAt asserts the relationship's point read, including the
// endpoints the cutoff holds.
func requireHistoricalEdgeAt(t *testing.T, historical Engine, id EdgeID, start, end NodeID, property string, want any) {
	t.Helper()
	edge, err := historical.GetEdge(t.Context(), id)
	require.NoError(t, err, "relationship %s must answer at the cutoff", id)
	require.Equal(t, start, edge.StartNode, "relationship %s start node at the cutoff", id)
	require.Equal(t, end, edge.EndNode, "relationship %s end node at the cutoff", id)
	require.Equal(t, want, edge.Properties[property], "relationship %s property %s at the cutoff", id, property)
}

func requireHistoricalEdgeAbsent(t *testing.T, historical Engine, id EdgeID) {
	t.Helper()
	_, err := historical.GetEdge(t.Context(), id)
	require.ErrorIs(t, err, ErrNotFound, "relationship %s must be absent at the cutoff", id)
}

// requireHistoricalAdjacent asserts exactly the relationships one directional
// adjacency read lists at the view's cutoff.
func requireHistoricalAdjacent(t *testing.T, historical Engine, node NodeID, direction AdjacentEdgeDirection, want ...EdgeID) {
	t.Helper()
	edges, err := historical.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Direction: direction, Node: node})
	require.NoError(t, err)
	got := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		got = append(got, edge.ID)
	}
	require.ElementsMatch(t, want, got, "adjacent relationships of %s at the cutoff", node)
}

// TestHistoricalRead_BufferedWriteBehindLifecycle walks one graph through the
// write-behind lifecycle and asserts, at every stage, the same point and
// adjacency answer for the same cutoff before and after the drain.
func TestHistoricalRead_BufferedWriteBehindLifecycle(t *testing.T) {
	engine := historicalBufferedEngine(t)
	start, far, moved := NodeID("test:start"), NodeID("test:far"), NodeID("test:moved")
	link, reborn := EdgeID("test:link"), EdgeID("test:reborn")

	// The retained base state: three nodes and one relationship, stored
	// synchronously so the fixture starts with committed history.
	for _, id := range []NodeID{start, far, moved} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Live"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
		ID: link, Type: "LINK", StartNode: start, EndNode: far, Properties: map[string]any{"w": int64(1)},
	}))
	seededCutoff := time.Now().UTC()

	t.Run("buffered update and move", func(t *testing.T) {
		historicalBufferedNodeUpdate(t, engine, start, []string{"Live", "Moved"}, map[string]any{"v": int64(2)})
		historicalBufferedEdgeUpdate(t, engine, link, moved, far, map[string]any{"w": int64(2)})
		cutoff := time.Now().UTC()
		require.Greater(t, engine.writeBehind.PendingOps(), 0, "the acknowledged commits must still be undrained")

		// The state the ACK established: the updated node, and the moved
		// relationship under its new start node only.
		updated := func(t *testing.T, historical Engine) {
			t.Helper()
			requireHistoricalNodeAt(t, historical, start, "v", int64(2), []string{"Live", "Moved"})
			requireHistoricalEdgeAt(t, historical, link, moved, far, "w", int64(2))
			requireHistoricalAdjacent(t, historical, start, AdjacentOutgoing)
			requireHistoricalAdjacent(t, historical, moved, AdjacentOutgoing, link)
			requireHistoricalAdjacent(t, historical, far, AdjacentIncoming, link)
		}
		// The state the cutoff before the ACK holds.
		seeded := func(t *testing.T, historical Engine) {
			t.Helper()
			requireHistoricalNodeAt(t, historical, start, "v", int64(1), []string{"Live"})
			requireHistoricalEdgeAt(t, historical, link, start, far, "w", int64(1))
			requireHistoricalAdjacent(t, historical, start, AdjacentOutgoing, link)
			requireHistoricalAdjacent(t, historical, moved, AdjacentOutgoing)
		}

		t.Run("before the drain", func(t *testing.T) {
			updated(t, historicalOpen(t, engine, cutoff))
			seeded(t, historicalOpen(t, engine, seededCutoff))
		})
		historicalFlushed(t, engine)
		t.Run("after the drain", func(t *testing.T) {
			updated(t, historicalOpen(t, engine, cutoff))
			seeded(t, historicalOpen(t, engine, seededCutoff))
		})

		// The live graph agrees: the ACK, not the drain, is the change.
		current, err := engine.GetNode(t.Context(), start)
		require.NoError(t, err)
		require.Equal(t, int64(2), current.Properties["v"])
	})

	var deletedCutoff time.Time
	t.Run("buffered delete", func(t *testing.T) {
		historicalBufferedEdgeDelete(t, engine, link)
		historicalBufferedNodeDelete(t, engine, start)
		deletedCutoff = time.Now().UTC()
		require.Greater(t, engine.writeBehind.PendingOps(), 0, "the acknowledged commits must still be undrained")

		deleted := func(t *testing.T, historical Engine) {
			t.Helper()
			requireHistoricalNodeAbsent(t, historical, start)
			requireHistoricalEdgeAbsent(t, historical, link)
			requireHistoricalAdjacent(t, historical, start, AdjacentOutgoing)
			requireHistoricalAdjacent(t, historical, moved, AdjacentOutgoing)
			requireHistoricalAdjacent(t, historical, far, AdjacentIncoming)
			// Only the current live incarnation answers a read: the ACKed
			// delete ends it, so the cutoff before that delete cannot answer
			// the incarnation it ended either.
			requireHistoricalNodeAbsent(t, historicalOpen(t, engine, seededCutoff), start)
		}

		t.Run("before the drain", func(t *testing.T) {
			deleted(t, historicalOpen(t, engine, deletedCutoff))
		})
		historicalFlushed(t, engine)
		t.Run("after the drain", func(t *testing.T) {
			deleted(t, historicalOpen(t, engine, deletedCutoff))
		})
	})

	t.Run("buffered recreation", func(t *testing.T) {
		historicalBufferedNodeCreate(t, engine, &Node{ID: start, Labels: []string{"Reborn"}, Properties: map[string]any{"v": int64(3)}})
		historicalBufferedEdgeCreate(t, engine, &Edge{
			ID: reborn, Type: "LINK", StartNode: start, EndNode: far, Properties: map[string]any{"w": int64(3)},
		})
		cutoff := time.Now().UTC()
		require.Greater(t, engine.writeBehind.PendingOps(), 0, "the acknowledged commits must still be undrained")

		recreated := func(t *testing.T, historical Engine) {
			t.Helper()
			requireHistoricalNodeAt(t, historical, start, "v", int64(3), []string{"Reborn"})
			requireHistoricalEdgeAt(t, historical, reborn, start, far, "w", int64(3))
			requireHistoricalAdjacent(t, historical, start, AdjacentOutgoing, reborn)
			requireHistoricalAdjacent(t, historical, far, AdjacentIncoming, reborn)
			requireHistoricalEdgeAbsent(t, historical, link)
			// The delete stage's cutoff still belongs to the ended
			// incarnation, whatever the later ACKed recreation added.
			atDeleted := historicalOpen(t, engine, deletedCutoff)
			requireHistoricalNodeAbsent(t, atDeleted, start)
			requireHistoricalEdgeAbsent(t, atDeleted, reborn)
			requireHistoricalAdjacent(t, atDeleted, start, AdjacentOutgoing)
		}

		t.Run("before the drain", func(t *testing.T) {
			recreated(t, historicalOpen(t, engine, cutoff))
		})
		historicalFlushed(t, engine)
		t.Run("after the drain", func(t *testing.T) {
			recreated(t, historicalOpen(t, engine, cutoff))
		})
	})
}

// TestHistoricalRead_PinnedBufferedViewIsStable proves a view opened at a cutoff
// keeps that cutoff: the ACKs that follow it, and the drain that lands them,
// change neither its point reads nor its adjacency.
func TestHistoricalRead_PinnedBufferedViewIsStable(t *testing.T) {
	engine := historicalBufferedEngine(t)
	node, far, late := NodeID("test:node"), NodeID("test:far"), NodeID("test:late")
	link := EdgeID("test:link")

	for _, id := range []NodeID{node, far} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Live"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
		ID: link, Type: "LINK", StartNode: node, EndNode: far, Properties: map[string]any{"w": int64(1)},
	}))

	historicalBufferedNodeUpdate(t, engine, node, []string{"Live", "Moved"}, map[string]any{"v": int64(2)})
	historicalBufferedEdgeUpdate(t, engine, link, node, far, map[string]any{"w": int64(2)})
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the pinned view must be opened over acknowledged, undrained commits")
	pinned := historicalOpen(t, engine, time.Now().UTC())

	atPinnedCutoff := func(t *testing.T) {
		t.Helper()
		requireHistoricalNodeAt(t, pinned, node, "v", int64(2), []string{"Live", "Moved"})
		requireHistoricalEdgeAt(t, pinned, link, node, far, "w", int64(2))
		requireHistoricalAdjacent(t, pinned, node, AdjacentOutgoing, link)
		requireHistoricalNodeAbsent(t, pinned, late)
	}
	atPinnedCutoff(t)

	// Later ACKs: a newer value, a deleted relationship and a node that did not
	// exist at the pinned cutoff.
	historicalBufferedNodeUpdate(t, engine, node, []string{"Live", "Moved"}, map[string]any{"v": int64(3)})
	historicalBufferedEdgeDelete(t, engine, link)
	historicalBufferedNodeCreate(t, engine, &Node{ID: late, Labels: []string{"Late"}, Properties: map[string]any{"v": int64(3)}})
	later := historicalOpen(t, engine, time.Now().UTC())

	atLaterCutoff := func(t *testing.T) {
		t.Helper()
		requireHistoricalNodeAt(t, later, node, "v", int64(3), nil)
		requireHistoricalEdgeAbsent(t, later, link)
		requireHistoricalAdjacent(t, later, node, AdjacentOutgoing)
		requireHistoricalNodeAt(t, later, late, "v", int64(3), []string{"Late"})
	}

	t.Run("before the drain", func(t *testing.T) {
		atPinnedCutoff(t)
		atLaterCutoff(t)
	})
	historicalFlushed(t, engine)
	t.Run("after the drain", func(t *testing.T) {
		atPinnedCutoff(t)
		atLaterCutoff(t)
	})

	// The live graph moved on, so the pinned answers above are not the current
	// ones by accident.
	current, err := engine.GetNode(t.Context(), node)
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Properties["v"])
}
