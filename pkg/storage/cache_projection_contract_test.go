package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Each read route must detach selected nested values from both the durable
// cache and an acknowledged write-behind image. Reopening supplies an independent
// codec oracle after callers have reused every returned container.
func TestCacheProjectionNestedOwnership(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			dir := t.TempDir()
			b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			node := &Node{ID: "test:projection-contract", Labels: []string{"ProjectionContract"}, Properties: map[string]any{
				"left":    map[string]any{"values": []int64{1, 2}},
				"right":   map[string]any{"values": []int64{3, 4}},
				"omitted": "retained",
			}}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetImplicit(buffered))
			_, err = tx.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, tx.CommitContext(context.Background()))
			_, err = b.GetStoredNode(node.ID)
			require.NoError(t, err)
			selected := []string{"left", "right"}
			readers := []struct {
				name      string
				read      func() (*Node, error)
				projected bool
			}{
				{"stored", func() (*Node, error) { return b.GetStoredNode(node.ID) }, false},
				{"query", func() (*Node, error) { return b.GetQueryNode(node.ID) }, false},
				{"without-embeddings", func() (*Node, error) { return b.GetNodeWithoutEmbeddings(node.ID) }, false},
				{"projected", func() (*Node, error) { return b.GetNodeProjected(node.ID, selected) }, true},
				{"all-nodes", func() (*Node, error) {
					nodes, err := b.AllNodes()
					if err != nil {
						return nil, err
					}
					if len(nodes) != 1 {
						return nil, fmt.Errorf("expected one stored node, got %d", len(nodes))
					}
					return nodes[0], nil
				}, false},
			}
			for _, reader := range readers {
				t.Run(reader.name, func(t *testing.T) {
					got, err := reader.read()
					require.NoError(t, err)
					if reader.projected {
						require.NotContains(t, got.Properties, "omitted")
					}
					for _, key := range selected {
						got.Properties[key].(map[string]any)["values"].([]int64)[0] = 99
					}
					again, err := reader.read()
					require.NoError(t, err)
					require.Equal(t, []int64{1, 2}, again.Properties["left"].(map[string]any)["values"])
					require.Equal(t, []int64{3, 4}, again.Properties["right"].(map[string]any)["values"])
				})
			}
			require.NoError(t, b.FlushWriteBehind(nil))
			require.NoError(t, b.Close())
			b, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
			require.NoError(t, err)
			cold, err := b.GetStoredNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, node.Properties, cold.Properties)
		})
	}
}
