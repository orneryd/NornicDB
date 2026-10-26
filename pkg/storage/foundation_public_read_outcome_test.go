// Author: Lane #1644 (Codex). Exercise public read outcomes across the admitted
// constituent owner, with already-correct point/batch/stored paths as controls.
package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicConstituentReadOutcome(t *testing.T) {
	ctx := context.Background()
	version := storage.MVCCVersion{CommitSequence: 1}
	operations := []struct {
		name string
		read func(*storage.CompositeEngine) error
	}{
		{"point-node", func(c *storage.CompositeEngine) error { _, err := c.GetNode("n"); return err }},
		{"batch-nodes", func(c *storage.CompositeEngine) error { _, err := c.BatchGetNodes([]storage.NodeID{"n"}); return err }},
		{"qualified-point-node", func(c *storage.CompositeEngine) error { _, err := c.GetNode(storage.NodeID(storage.SourceNodeElementID("tenant", "n"))); return err }},
        {"qualified-between", func(c *storage.CompositeEngine) error { _, err := c.GetEdgesBetween(storage.NodeID(storage.SourceNodeElementID("tenant", "n")), "m"); return err }},
        {"qualified-historical-between", func(c *storage.CompositeEngine) error { _, err := c.GetEdgesBetweenVisibleAt(storage.NodeID(storage.SourceNodeElementID("tenant", "n")), "m", version); return err }},
        {"point-edge", func(c *storage.CompositeEngine) error { _, err := c.GetEdge("e"); return err }},
		{"all-nodes", func(c *storage.CompositeEngine) error { _, err := c.AllNodes(); return err }},
		{"all-edges", func(c *storage.CompositeEngine) error { _, err := c.AllEdges(); return err }},
		{"label-nodes", func(c *storage.CompositeEngine) error { _, err := c.GetNodesByLabel("Doc"); return err }},
		{"outgoing", func(c *storage.CompositeEngine) error { _, err := c.GetOutgoingEdges("n"); return err }},
		{"incoming", func(c *storage.CompositeEngine) error { _, err := c.GetIncomingEdges("n"); return err }},
		{"between", func(c *storage.CompositeEngine) error { _, err := c.GetEdgesBetween("n", "m"); return err }},
		{"typed-edges", func(c *storage.CompositeEngine) error { _, err := c.GetEdgesByType("R"); return err }},
		{"node-count", func(c *storage.CompositeEngine) error { _, err := c.NodeCount(); return err }},
		{"edge-count", func(c *storage.CompositeEngine) error { _, err := c.EdgeCount(); return err }},
		{"node-options", func(c *storage.CompositeEngine) error { return c.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{}, func(*storage.Node) error { return nil }) }},
		{"node-stream", func(c *storage.CompositeEngine) error { return c.StreamNodes(ctx, func(*storage.Node) error { return nil }) }},
		{"edge-stream", func(c *storage.CompositeEngine) error { return c.StreamEdges(ctx, func(*storage.Edge) error { return nil }) }},
		{"latest-node", func(c *storage.CompositeEngine) error { _, err := c.GetNodeLatestVisible("n"); return err }},
		{"historical-node", func(c *storage.CompositeEngine) error { _, err := c.GetNodeVisibleAt("n", version); return err }},
		{"historical-label", func(c *storage.CompositeEngine) error { _, err := c.GetNodesByLabelVisibleAt("Doc", version); return err }},
		{"node-head", func(c *storage.CompositeEngine) error { _, err := c.GetNodeCurrentHead("n"); return err }},
		{"stored-facts", func(c *storage.CompositeEngine) error { _, err := storage.ReadStoredMetadata(ctx, c, storage.StoredReadRequest{Nodes: []storage.NodeID{"n"}}); return err }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			cause := errors.New("constituent read admission failed")
			view := storage.NewCompositeEngine(map[string]storage.Engine{"a": base}, map[string]string{"a": "tenant"}, map[string]string{"a": "read"}).WithConstituentAccess(func(string, storage.Engine, bool) (storage.Engine, error) { return nil, cause })
			require.ErrorIs(t, operation.read(view), cause, "a failed source cannot become a successful empty read or a fabricated not-found")
		})
	}
	t.Run("empty-success-control", func(t *testing.T) {
		view := storage.NewCompositeEngine(nil, nil, nil)
		nodes, err := view.AllNodes()
		require.NoError(t, err)
		require.Empty(t, nodes)
	})
}
