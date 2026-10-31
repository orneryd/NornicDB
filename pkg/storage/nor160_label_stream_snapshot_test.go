// NOR-160 lane (Claude), 31 October 2026: the label stream and the label list
// read one snapshot each, including their node-cache hits.
package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A label stream reads the label index from its snapshot. A node it reaches
// after a concurrent commit must still be that snapshot's node: a commit that
// removes the label from a node not yet visited must not make the stream hand
// out a node without the label, or with the commit's values, from the node
// cache (NOR-160).
func TestNOR160LabelStreamReadsOneSnapshot(t *testing.T) {
	engine := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"P"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
		_, err = engine.GetNode(id) // the node cache holds both
		require.NoError(t, err)
	}

	var delivered []*Node
	committed := false
	done := make(chan error, 1)
	err := engine.StreamNodesByLabelProjected("P", nil, func(node *Node) error {
		if len(delivered) == 0 {
			go func() {
				done <- engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:b", Labels: []string{"Q"}, Properties: map[string]any{"v": int64(2)}}})
			}()
			select {
			case err := <-done:
				require.NoError(t, err)
				committed = true
			case <-time.After(2 * time.Second):
			}
		}
		delivered = append(delivered, node)
		return nil
	})
	require.NoError(t, err)
	if !committed {
		require.NoError(t, <-done)
	}
	t.Logf("commit landed during the stream: %v", committed)
	require.Len(t, delivered, 2)
	for _, node := range delivered {
		require.Contains(t, node.Labels, "P", "%s: a label stream handed out a node without its label", node.ID)
		require.Equal(t, int64(1), node.Properties["v"], "%s: a label stream mixed a later commit into its snapshot", node.ID)
	}
}
