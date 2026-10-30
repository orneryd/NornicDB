package storage

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// These are the optional public directional protocols used by request readers.
// An older provider follows its genuine legacy entrance, reproducing the lost
// caller; the correction supplies the contextual protocol on maintained stores.
func TestDirectionalReadPreparationCancellation(t *testing.T) {
	for _, shape := range []string{"native", "wal", "namespace", "namespace-wal"} {
		for _, direction := range []string{"outgoing", "incoming", "both"} {
			stages := []string{"physical"}
			if shape == "namespace" || shape == "namespace-wal" {
				stages = append(stages, "namespace projection")
			}
			for _, stage := range stages {
				t.Run(shape+"/"+direction+"/"+stage, func(t *testing.T) {
					path := t.TempDir()
					b, err := NewBadgerEngine(path)
					require.NoError(t, err)
					_, err = b.CreateNode(&Node{ID: "test:anchor"})
					require.NoError(t, err)
					for i := 0; i < 20; i++ {
						id := NodeID(fmt.Sprintf("test:n%d", i))
						_, err = b.CreateNode(&Node{ID: id})
						require.NoError(t, err)
						require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:out%d", i)), StartNode: "test:anchor", EndNode: id, Type: "R"}))
						require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:in%d", i)), StartNode: id, EndNode: "test:anchor", Type: "R"}))
					}
					require.NoError(t, b.Close())
					b, err = NewBadgerEngine(path)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, b.Close()) })
					var engine Engine = b
					anchor := NodeID("test:anchor")
					if shape == "wal" || shape == "namespace-wal" {
						engine = NewWALEngine(engine, nil)
					}
					if shape == "namespace" || shape == "namespace-wal" {
						engine = NewNamespacedEngine(engine, "test")
						anchor = "anchor"
					}
					read := func(ctx context.Context) (out, in []*Edge, err error) {
						switch direction {
						case "outgoing":
							if reader, ok := engine.(interface {
								GetOutgoingEdgesWithContext(context.Context, NodeID) ([]*Edge, error)
							}); ok {
								out, err = reader.GetOutgoingEdgesWithContext(ctx, anchor)
							} else {
								out, err = engine.GetOutgoingEdges(anchor)
							}
						case "incoming":
							if reader, ok := engine.(interface {
								GetIncomingEdgesWithContext(context.Context, NodeID) ([]*Edge, error)
							}); ok {
								in, err = reader.GetIncomingEdgesWithContext(ctx, anchor)
							} else {
								in, err = engine.GetIncomingEdges(anchor)
							}
						default:
							if reader, ok := engine.(interface {
								GetAdjacentEdgesWithContext(context.Context, NodeID) ([]*Edge, []*Edge, error)
							}); ok {
								return reader.GetAdjacentEdgesWithContext(ctx, anchor)
							}
							return engine.(AdjacentEdgesEngine).GetAdjacentEdges(anchor)
						}
						return
					}
					ctx := preparationContext(t, "collectEdgesByIndexPrefix", 2)
					ctx.additionalOwner, ctx.via = "cachedAdjacency.func1", "readAdjacencyWithContext"
					if stage == "namespace projection" {
						ctx.owner, ctx.additionalOwner, ctx.via = "materializedValuesWithContext", "", "toUserEdgesInNamespace"
					}
					out, in, err := read(ctx)
					t.Logf("observations=%d read=%v outgoing=%d incoming=%d", ctx.hits, err, len(out), len(in))
					require.GreaterOrEqual(t, ctx.hits, 2)
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, out)
					require.Empty(t, in)
					out, in, err = read(context.Background())
					require.NoError(t, err)
					if direction != "incoming" {
						require.Len(t, out, 20)
					}
					if direction != "outgoing" {
						require.Len(t, in, 20)
					}
				})
			}
		}
	}
}
