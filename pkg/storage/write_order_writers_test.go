package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// noDanglingRelationships fails when a stored relationship names an endpoint
// that does not exist, after the buffer drained.
func noDanglingRelationships(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	require.NoError(t, engine.FlushWriteBehind(nil))
	engine.invalidateCachesAfterRestore()
	edges, err := engine.AllEdges()
	require.NoError(t, err)
	for _, e := range edges {
		for _, id := range []NodeID{e.StartNode, e.EndNode} {
			_, err := engine.GetNode(id)
			require.NoError(t, err, "relationship %s keeps its endpoint %s", e.ID, id)
		}
	}
}

// A relationship create and a delete of its endpoint, the one validated
// before the other commits, never leave the relationship without its
// endpoint: whichever commits second sees the first, buffered or not.
func TestRelationshipEndpointsHoldAcrossOrderedCommits(t *testing.T) {
	createEdge := func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"})
	}
	deleteNode := func(tx *BadgerTransaction) error { return tx.DeleteGraphNode("test:a", CascadeEdges) }
	for _, c := range []struct {
		name string
		// staged is validated first and committed last.
		staged         func(*BadgerTransaction) error
		stagedImplicit bool
		between        func(*BadgerEngine) error
	}{
		{"buffered create, direct delete between", createEdge, true, func(e *BadgerEngine) error { return e.DeleteNode("test:a") }},
		{"buffered delete, direct create between", deleteNode, true, func(e *BadgerEngine) error {
			return e.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"})
		}},
		{"buffered create, buffered delete between", createEdge, true, func(e *BadgerEngine) error { return runPublicationTransaction(e, true, deleteNode, nil) }},
		{"buffered delete, buffered create between", deleteNode, true, func(e *BadgerEngine) error { return runPublicationTransaction(e, true, createEdge, nil) }},
		{"explicit create, buffered delete between", createEdge, false, func(e *BadgerEngine) error { return runPublicationTransaction(e, true, deleteNode, nil) }},
		{"explicit delete, buffered create between", deleteNode, false, func(e *BadgerEngine) error { return runPublicationTransaction(e, true, createEdge, nil) }},
		{"explicit create, direct delete between", createEdge, false, func(e *BadgerEngine) error { return e.DeleteNode("test:a") }},
		{"explicit delete, direct create between", deleteNode, false, func(e *BadgerEngine) error {
			return e.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			t.Cleanup(func() { _ = engine.Close() })
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, engine.FlushWriteBehind(nil))
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			if c.stagedImplicit {
				require.NoError(t, tx.SetImplicit(true))
			}
			require.NoError(t, c.staged(tx))
			require.NoError(t, c.between(engine))
			if err := tx.Commit(); err != nil {
				require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidEdge), "unexpected commit error: %v", err)
			}
			noDanglingRelationships(t, engine)
		})
	}
}

// Hiding or showing an entity on demand right after an acknowledged,
// unflushed commit of it finds the entity, reports its acknowledged body,
// and keeps both across the flush and a restart.
func TestManualSuppressionOrdersAfterBufferedCommits(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		for _, buffered := range []string{"create", "update"} {
			t.Run(kind+"/"+buffered, func(t *testing.T) {
				dir := t.TempDir()
				engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
				_, err = engine.CreateNode(listenerOrderNode("initial"))
				require.NoError(t, err)
				if buffered == "update" || kind == "node" {
					if kind == "edge" {
						require.NoError(t, engine.CreateEdge(listenerOrderEdge("initial")))
					}
				}
				require.NoError(t, engine.FlushWriteBehind(nil))
				var notified any
				engine.OnNodeUpdated(func(n *Node) { notified = n.Properties["v"] })
				engine.OnEdgeUpdated(func(e *Edge) { notified = e.Properties["v"] })
				require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
					switch {
					case kind == "node" && buffered == "create":
						if err := tx.DeleteGraphNode("test:n", CascadeEdges); err != nil {
							return err
						}
						_, err := tx.CreateNode(&Node{ID: "test:fresh", Properties: map[string]any{"v": "acknowledged"}})
						return err
					case kind == "node":
						return tx.UpdateGraphNode(NodeUpdateRequest{After: listenerOrderNode("acknowledged"), Policy: ClientNodeEdits})
					case buffered == "create":
						return tx.CreateEdge(listenerOrderEdge("acknowledged"))
					default:
						return tx.UpdateEdge(EdgeUpdateRequest{After: listenerOrderEdge("acknowledged")})
					}
				}, nil))
				id := "test:n"
				if kind == "node" && buffered == "create" {
					id = "test:fresh"
				}
				if kind == "edge" {
					id = "test:r"
				}
				notified = nil
				if kind == "node" {
					require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, string(id), true))
					// Showing it again reports it on both branches.
					require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, string(id), false))
					require.Equal(t, "acknowledged", notified, "the update reported the acknowledged body")
					require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, string(id), true))
				} else {
					require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, string(id), true))
					if notified != nil {
						require.Equal(t, "acknowledged", notified, "the update reported the acknowledged body")
					}
				}
				require.NoError(t, engine.Close())

				reopened, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = reopened.Close() })
				require.True(t, reopened.HasManualSuppressions(), "the suppression survives")
				var v any
				if kind == "node" {
					n, err := reopened.GetStoredNode(NodeID(id))
					require.NoError(t, err)
					v = n.Properties["v"]
				} else {
					e, err := reopened.GetStoredEdge(EdgeID(id))
					require.NoError(t, err)
					v = e.Properties["v"]
				}
				require.Equal(t, "acknowledged", v, "the acknowledged body is stored")
			})
		}
	}
}

// A node's embeddings stored right after an acknowledged, unflushed update of
// it survive that update's flush.
func TestEmbeddingSidecarOrdersAfterBufferedCommits(t *testing.T) {
	dir := t.TempDir()
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	_, err = engine.CreateNode(listenerOrderNode("initial"))
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(nil))
	require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(NodeUpdateRequest{After: listenerOrderNode("acknowledged"), Policy: ClientNodeEdits})
	}, nil))
	embedded := listenerOrderNode("acknowledged")
	embedded.ChunkEmbeddings = [][]float32{{1, 2, 3}}
	embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(embedded))
	require.NoError(t, engine.Close())

	reopened, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	n, err := reopened.GetNode("test:n")
	require.NoError(t, err)
	require.Equal(t, "acknowledged", n.Properties["v"])
	require.Equal(t, [][]float32{{1, 2, 3}}, n.ChunkEmbeddings, "the embeddings survive the earlier update's flush")
}

// Deleting a relationship right after an acknowledged, unflushed relabel of
// its endpoint takes the relationship out of the endpoint's newest label
// counts.
func TestRelationshipDeleteCountsTheNewestEndpointLabels(t *testing.T) {
	for _, route := range []string{"direct", "explicit"} {
		t.Run(route, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			t.Cleanup(func() { _ = engine.Close() })
			_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"Old"}})
			require.NoError(t, err)
			_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"End"}})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
			require.NoError(t, engine.FlushWriteBehind(nil))
			require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
				return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:a", Labels: []string{"New"}}, Policy: ClientNodeEdits})
			}, nil))
			if route == "direct" {
				require.NoError(t, engine.DeleteEdge("test:r"))
			} else {
				require.NoError(t, runPublicationTransaction(engine, false, func(tx *BadgerTransaction) error { return tx.DeleteEdge("test:r") }, nil))
			}
			require.NoError(t, engine.FlushWriteBehind(nil))
			for _, label := range []string{"Old", "New"} {
				count, err := engine.EdgeCountByStartLabelInNamespace("test", label, "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(0), count, fmt.Sprintf("LINK relationships starting at %s", label))
			}
		})
	}
}

// A direct relationship create right after acknowledged, unflushed commits
// finds what they wrote: endpoints created in the buffer exist, and a
// relationship created there already exists.
func TestDirectRelationshipCreateSeesAcknowledgedCommits(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	t.Cleanup(func() { _ = engine.Close() })
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "test:a"}, {ID: "test:b"}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}), "the acknowledged endpoints exist")
	require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: "test:s", StartNode: "test:b", EndNode: "test:a", Type: "LINK"})
	}, nil))
	require.ErrorIs(t, engine.CreateEdge(&Edge{ID: "test:s", StartNode: "test:b", EndNode: "test:a", Type: "LINK", Properties: map[string]any{"v": "direct"}}), ErrAlreadyExists, "the acknowledged relationship exists")
	require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode("test:b", CascadeEdges) }, nil))
	require.ErrorIs(t, engine.CreateEdge(&Edge{ID: "test:t", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}), ErrNotFound, "the acknowledged delete removed the endpoint")
	noDanglingRelationships(t, engine)
}
