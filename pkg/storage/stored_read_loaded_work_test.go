// Author: Lane #1644 (Codex). Public pending stored reads exercise actual owned copy work.
package storage

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type loadedStoredCancelContext struct {
	context.Context
	cancel         context.CancelFunc
	checks, budget int
}

func (c *loadedStoredCancelContext) Err() error {
	c.checks++
	if c.checks > c.budget {
		c.cancel()
	}
	return c.Context.Err()
}

func TestStoredReadLoadedWorkCancellation(t *testing.T) {
	for _, shape := range []string{"labels", "properties", "vectors", "edge-properties"} {
		t.Run(shape, func(t *testing.T) {
			b := NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			node := &Node{ID: "d7:loaded", Labels: []string{"L"}, Properties: map[string]any{}}
			switch shape {
			case "labels":
				for i := 0; i < 4096; i++ {
					node.Labels = append(node.Labels, fmt.Sprintf("L%d", i))
				}
			case "properties":
				for i := 0; i < 4096; i++ {
					node.Properties[fmt.Sprintf("p%d", i)] = int64(i)
				}
			case "vectors":
				node.ChunkEmbeddings = make([][]float32, 4096)
				for i := range node.ChunkEmbeddings {
					node.ChunkEmbeddings[i] = []float32{float32(i)}
				}
			}
			_, err = tx.CreateNode(node)
			require.NoError(t, err)
			if shape == "edge-properties" {
				edge := &Edge{ID: "d7:edge", StartNode: node.ID, EndNode: node.ID, Type: "R", Properties: map[string]any{}}
				for i := 0; i < 4096; i++ {
					edge.Properties[fmt.Sprintf("p%d", i)] = int64(i)
				}
				require.NoError(t, tx.CreateEdge(edge))
			}
			for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
				t.Run(fmt.Sprint(mode), func(t *testing.T) {
					parent, cancel := context.WithCancel(context.Background())
					defer cancel()
					ctx := &loadedStoredCancelContext{Context: parent, cancel: cancel, budget: 128}
					request := StoredReadRequest{Mode: mode, Nodes: []NodeID{node.ID}, NodeProperties: []string{}}
					if shape == "edge-properties" {
						request.Nodes = nil
						request.Edges = []EdgeID{"d7:edge"}
						request.EdgeProperties = []string{}
					}
					result, err := ReadStored(ctx, tx, request)
					if shape == "labels" || mode == StoredFullMaterialization {
						require.ErrorIs(t, err, context.Canceled, "owned copy loops must honor cancellation, not complete hidden work before returning")
						require.Nil(t, result)
					} else {
						require.NoError(t, err, "excluded payload must not consume the copy/decoder budget")
						require.NotNil(t, result)
						require.LessOrEqual(t, ctx.checks, ctx.budget)
					}
				})
			}
		})
	}
}
