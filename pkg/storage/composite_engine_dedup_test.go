package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeEngine_GetNodesByLabelPropagatesConstituentError(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	readErr := errors.New("constituent label scan failed")
	composite := NewCompositeEngine(
		map[string]Engine{"failed": failingLabelScanEngine{Engine: backing, err: readErr}},
		nil,
		map[string]string{"failed": "read"},
	)
	nodes, err := composite.GetNodesByLabel(context.Background(), "Evidence")
	require.ErrorIs(t, err, readErr)
	require.Nil(t, nodes)
}

func TestCompositeEngine_StreamNodesByLabelProjected(t *testing.T) {
	first := newTestMemoryEngine(t)
	second := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	shared := NodeID(prefixTestID("shared-projected"))
	unique := NodeID(prefixTestID("unique-projected"))
	for _, engine := range []*MemoryEngine{first, second} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: shared, Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "shared", "unused": "hidden"}})
		require.NoError(t, err)
	}
	_, err := second.CreateNode(t.Context(), &Node{ID: unique, Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "unique", "unused": "hidden"}})
	require.NoError(t, err)
	composite := NewCompositeEngine(
		map[string]Engine{"first": first, "second": second}, nil,
		map[string]string{"first": "read", "second": "read"},
	)
	seen := make(map[NodeID]map[string]any)
	err = composite.StreamNodesByLabelProjected(context.Background(), "Evidence", []string{"asset_id"}, func(node *Node) error {
		seen[node.ID] = node.Properties
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[NodeID]map[string]any{
		shared: {"asset_id": "shared"},
		unique: {"asset_id": "unique"},
	}, seen)

	visited := 0
	err = composite.StreamNodesByLabelProjected(context.Background(), "Evidence", nil, func(*Node) error {
		visited++
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
	require.Equal(t, 1, visited)
}

func TestCompositeEngine_StreamNodesByLabelProjectedUnsupportedConstituent(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	unsupported := struct{ Engine }{backing}
	composite := NewCompositeEngine(
		map[string]Engine{"supported": backing, "unsupported": unsupported}, nil,
		map[string]string{"supported": "read", "unsupported": "read"},
	)
	visited := false
	err := composite.StreamNodesByLabelProjected(context.Background(), "Evidence", nil, func(*Node) error {
		visited = true
		return nil
	})
	require.ErrorIs(t, err, ErrNotImplemented)
	require.False(t, visited)
}

// Every readable source retains its own entity even when its stored local ID
// equals another source's ID. All seven original read paths share this fixture
// and assertion owner; node-only cases retain their one-node population.
func TestCompositeEngine_ReadsPreserveEqualIDsAcrossDatabases(t *testing.T) {
	node1 := NodeID(prefixTestID("node1"))
	node2 := NodeID(prefixTestID("node2"))
	edge := EdgeID(prefixTestID("edge1"))
	for _, test := range []struct {
		name     string
		edgeRead bool
		read     func(context.Context, *CompositeEngine) ([]string, error)
	}{
		{name: "TestCompositeEngine_Deduplication_GetNodesByLabel", read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.GetNodesByLabel(ctx, "Person")
			return nodeIDs(values), err
		}},
		{name: "TestCompositeEngine_Deduplication_AllNodes", read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.AllNodes(ctx)
			return nodeIDs(values), err
		}},
		{name: "TestCompositeEngine_Deduplication_GetEdgesByType", edgeRead: true, read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.GetEdgesByType(ctx, "KNOWS")
			return edgeIDs(values), err
		}},
		{name: "TestCompositeEngine_Deduplication_AllEdges", edgeRead: true, read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.AllEdges(ctx)
			return edgeIDs(values), err
		}},
		{name: "TestCompositeEngine_Deduplication_GetOutgoingEdges", edgeRead: true, read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Node: node1})
			return edgeIDs(values), err
		}},
		{name: "TestCompositeEngine_Deduplication_GetIncomingEdges", edgeRead: true, read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: node2})
			return edgeIDs(values), err
		}},
		{name: "TestCompositeEngine_Deduplication_GetEdgesBetween", edgeRead: true, read: func(ctx context.Context, c *CompositeEngine) ([]string, error) {
			values, err := c.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: node1, Other: node2})
			return edgeIDs(values), err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			first, second := newTestMemoryEngine(t), newTestMemoryEngine(t)
			for _, engine := range []*MemoryEngine{first, second} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: node1, Labels: []string{"Person"}})
				require.NoError(t, err)
				if test.edgeRead {
					_, err = engine.CreateNode(t.Context(), &Node{ID: node2, Labels: []string{"Person"}})
					require.NoError(t, err)
					require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: edge, StartNode: node1, EndNode: node2, Type: "KNOWS"}))
				}
			}
			composite := NewCompositeEngine(map[string]Engine{"db1": first, "db2": second}, map[string]string{"db1": "db1", "db2": "db2"}, map[string]string{"db1": "read_write", "db2": "read_write"})
			ids, err := test.read(context.Background(), composite)
			require.NoError(t, err)
			want := string(node1)
			if test.edgeRead {
				want = string(edge)
			}
			require.Equal(t, []string{want, want}, ids, "both constituent records must survive with their original local IDs")
		})
	}
}
