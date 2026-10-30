package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// visibleOnlyEngine hides every stored-read capability, so the package
// helpers must fall back to the visible Engine reads.
type visibleOnlyEngine struct{ Engine }

// unwrappingEngine adds no visibility of its own; StoredReader unwraps it.
type unwrappingEngine struct {
	Engine
	inner Engine
}

func (u unwrappingEngine) GetInnerEngine() Engine { return u.inner }

// idCheck sorts the IDs a stored read returned, failing on its error; its
// methods take a read's two results directly.
type idCheck struct{ t *testing.T }

func idsOf(t *testing.T) idCheck { return idCheck{t} }

func (c idCheck) nodes(nodes []*Node, err error) []string {
	c.t.Helper()
	require.NoError(c.t, err)
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, string(node.ID))
	}
	sort.Strings(ids)
	return ids
}

func (c idCheck) edges(edges []*Edge, err error) []string {
	c.t.Helper()
	require.NoError(c.t, err)
	ids := make([]string, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, string(edge.ID))
	}
	sort.Strings(ids)
	return ids
}

// Every stored reader returns what storage holds, hidden entities included,
// within its database scope; an engine without stored reads falls back to
// what its visible reads return.
func TestStoredReaderSurfaceIncludesHiddenEntities(t *testing.T) {
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	walEngine := NewWALEngine(base, wal)
	t.Cleanup(func() { _ = wal.Close() })
	for _, node := range []*Node{
		{ID: "ns:a", Labels: []string{"A"}, Properties: map[string]any{"id": "a"}},
		{ID: "ns:b", Labels: []string{"B"}},
		{ID: "ns:h", Labels: []string{"A"}, Properties: map[string]any{"id": "h"}},
		{ID: "other:z", Labels: []string{"A"}},
		{ID: "ns:iso", Labels: []string{"C"}},
	} {
		_, err := base.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "ns:r", StartNode: "ns:a", EndNode: "ns:b", Type: "LINK"},
		{ID: "ns:x", StartNode: "ns:a", EndNode: "ns:b", Type: "LINK"},
	} {
		require.NoError(t, base.CreateEdge(edge))
	}
	require.NoError(t, base.SetManualSuppression(StoredEntityNodeKind, "ns:h", true))
	require.NoError(t, base.SetManualSuppression(StoredEntityEdgeKind, "ns:x", true))
	ctx := context.Background()

	t.Run("physical readers", func(t *testing.T) {
		for name, engine := range map[string]Engine{
			"badger":    base,
			"wal":       walEngine,
			"unwrapped": unwrappingEngine{Engine: visibleOnlyEngine{base}, inner: base},
		} {
			t.Run(name, func(t *testing.T) {
				node, err := GetStoredNode(engine, "ns:h")
				require.NoError(t, err)
				require.Equal(t, NodeID("ns:h"), node.ID)
				edge, err := GetStoredEdge(engine, "ns:x")
				require.NoError(t, err)
				require.Equal(t, EdgeID("ns:x"), edge.ID)
				require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(GetStoredOutgoingEdges(engine, "ns:a")))
				require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(GetStoredIncomingEdges(engine, "ns:b")))
				require.Equal(t, []string{"ns:a", "ns:h", "other:z"}, idsOf(t).nodes(GetStoredNodesByLabel(engine, "A")))
				var streamed []*Node
				require.NoError(t, StreamStoredNodesByLabelProjected(engine, "A", []string{"id"}, func(node *Node) error {
					streamed = append(streamed, node)
					return nil
				}))
				require.Equal(t, []string{"ns:a", "ns:h", "other:z"}, idsOf(t).nodes(streamed, nil))
				var edges []*Edge
				require.NoError(t, StreamStoredEdgesByType(nil, engine, "LINK", func(edge *Edge) error {
					edges = append(edges, edge)
					return nil
				}))
				require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(edges, nil))
			})
		}
	})

	t.Run("scoped readers", func(t *testing.T) {
		for name, reader := range map[string]ScopedStoredEntityReader{"badger": base.BadgerEngine, "wal": walEngine} {
			t.Run(name, func(t *testing.T) {
				require.Equal(t, []string{"ns:a", "ns:h"}, idsOf(t).nodes(reader.GetStoredNodesByLabelInScope("ns:", "A")))
				var nodes []*Node
				require.NoError(t, reader.StreamStoredNodesByLabelProjectedInScope("ns:", "A", []string{"id"}, func(node *Node) error {
					nodes = append(nodes, node)
					return nil
				}))
				require.Equal(t, []string{"ns:a", "ns:h"}, idsOf(t).nodes(nodes, nil))
				var edges []*Edge
				require.NoError(t, reader.StreamStoredEdgesByTypeInScope(ctx, "ns:", "LINK", func(edge *Edge) error {
					edges = append(edges, edge)
					return nil
				}))
				require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(edges, nil))
			})
		}
	})

	t.Run("namespaced readers", func(t *testing.T) {
		for name, inner := range map[string]Engine{"badger": base, "wal": walEngine} {
			t.Run(name, func(t *testing.T) {
				ns := NewNamespacedEngine(inner, "ns")
				node, err := ns.GetStoredNode("h")
				require.NoError(t, err)
				require.Equal(t, NodeID("h"), node.ID)
				edge, err := ns.GetStoredEdge("x")
				require.NoError(t, err)
				require.Equal(t, EdgeID("x"), edge.ID)
				require.Equal(t, []string{"r", "x"}, idsOf(t).edges(ns.GetStoredOutgoingEdges("a")))
				require.Equal(t, []string{"r", "x"}, idsOf(t).edges(ns.GetStoredIncomingEdges("b")))
				require.Equal(t, []string{"a", "h"}, idsOf(t).nodes(ns.GetStoredNodesByLabel("A")))
				var nodes []*Node
				require.NoError(t, ns.StreamStoredNodesByLabelProjected("A", []string{"id"}, func(node *Node) error {
					nodes = append(nodes, node)
					return nil
				}))
				require.Equal(t, []string{"a", "h"}, idsOf(t).nodes(nodes, nil))
				var edges []*Edge
				require.NoError(t, ns.StreamStoredEdgesByType(ctx, "LINK", func(edge *Edge) error {
					edges = append(edges, edge)
					return nil
				}))
				require.Equal(t, []string{"r", "x"}, idsOf(t).edges(edges, nil))
				require.ErrorIs(t, ns.StreamStoredNodesByLabelProjected("A", nil, nil), ErrInvalidData)
				require.ErrorIs(t, ns.StreamStoredEdgesByType(ctx, "LINK", nil), ErrInvalidData)
			})
		}
	})

	t.Run("visible-only fallback", func(t *testing.T) {
		engine := visibleOnlyEngine{base}
		_, ok := StoredReader(engine)
		require.False(t, ok)
		_, err := GetStoredNode(engine, "ns:h")
		require.ErrorIs(t, err, ErrNotFound)
		_, err = GetStoredEdge(engine, "ns:x")
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, []string{"ns:r"}, idsOf(t).edges(GetStoredOutgoingEdges(engine, "ns:a")))
		require.Equal(t, []string{"ns:r"}, idsOf(t).edges(GetStoredIncomingEdges(engine, "ns:b")))
		require.Equal(t, []string{"ns:a", "other:z"}, idsOf(t).nodes(GetStoredNodesByLabel(engine, "A")))
		var edges []*Edge
		require.NoError(t, StreamStoredEdgesByType(ctx, engine, "", func(edge *Edge) error {
			edges = append(edges, edge)
			return nil
		}))
		require.Equal(t, []string{"ns:r"}, idsOf(t).edges(edges, nil))
		count, err := NewNamespacedEngine(engine, "ns").NodeCountByLabelInNamespace("ns", "A")
		require.NoError(t, err)
		require.EqualValues(t, 1, count, "the fallback count keeps its namespace and visibility")
	})

	t.Run("transaction", func(t *testing.T) {
		tx, err := base.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.SetNamespace("ns"))
		node, err := tx.GetStoredNode("ns:h")
		require.NoError(t, err)
		require.Equal(t, NodeID("ns:h"), node.ID)
		edge, err := tx.GetStoredEdge("ns:x")
		require.NoError(t, err)
		require.Equal(t, EdgeID("ns:x"), edge.ID)
		require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(tx.GetStoredOutgoingEdges("ns:a")))
		require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(tx.GetStoredIncomingEdges("ns:b")))
		require.Equal(t, []string{"ns:a", "ns:h"}, idsOf(t).nodes(tx.GetStoredNodesByLabel("A")))
		var edges []*Edge
		require.NoError(t, tx.StreamStoredEdgesByType(ctx, "LINK", func(edge *Edge) error {
			edges = append(edges, edge)
			return nil
		}))
		require.Equal(t, []string{"ns:r", "ns:x"}, idsOf(t).edges(edges, nil))

		_, err = tx.CreateNode(&Node{ID: "ns:n", Labels: []string{"A"}, Properties: map[string]any{"id": "n"}})
		require.NoError(t, err)
		require.NoError(t, tx.DeleteEdge("ns:r"))
		require.NoError(t, tx.CreateEdge(&Edge{ID: "ns:y", StartNode: "ns:a", EndNode: "ns:b", Type: "LINK"}))
		require.NoError(t, tx.DeleteGraphNode("ns:iso", CascadeEdges))
		summary := tx.PendingChanges(false)
		require.Equal(t, "ns", summary.Namespace)
		require.Positive(t, summary.Operations)
		require.Nil(t, summary.Nodes)
		changes := tx.PendingChanges(true)
		require.Contains(t, changes.Nodes, NodeID("ns:n"))
		require.Contains(t, changes.Edges, EdgeID("ns:r"))
		require.Nil(t, changes.Edges["ns:r"], "a deleted relationship is a nil final image")
		require.NotNil(t, changes.Edges["ns:y"])
		require.Contains(t, changes.Nodes, NodeID("ns:iso"))
		require.Nil(t, changes.Nodes["ns:iso"], "a deleted node is a nil final image")

		image, err := tx.StoredNodeImage(&Node{ID: "n", Labels: []string{"A"}, Properties: map[string]any{"id": "n"}})
		require.NoError(t, err)
		require.Equal(t, NodeID("ns:n"), image.ID)
		require.Equal(t, "n", image.Properties["id"])
		edgeImage, err := tx.StoredEdgeImage(&Edge{ID: "y", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"w": int64(2)}})
		require.NoError(t, err)
		require.Equal(t, EdgeID("ns:y"), edgeImage.ID)
		require.Equal(t, NodeID("ns:a"), edgeImage.StartNode)
		require.EqualValues(t, 2, edgeImage.Properties["w"])

		require.NoError(t, tx.Rollback())
		_, err = tx.StoredNodeImage(&Node{ID: "n"})
		require.Error(t, err, "a finished transaction has no staged image")
		_, err = tx.StoredEdgeImage(&Edge{ID: "y", StartNode: "a", EndNode: "b", Type: "LINK"})
		require.Error(t, err)
	})
}
