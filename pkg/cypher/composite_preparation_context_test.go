package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeSourcePreparationCancellation(t *testing.T) {
	for _, operation := range []string{"label", "first", "all", "projected", "ids", "snapshot", "batch", "MATCH", "MATCH first", "options", "stream nodes", "stream edges", "chunks", "edge type", "parked", "consume marker", "mark clean", "prune versions", "rebuild heads", "prune temporal", "rebuild temporal", "trigger prune"} {
		t.Run(operation, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			engines := map[string]storage.Engine{}
			names, modes := map[string]string{}, map[string]string{}
			for i := 0; i < 20; i++ {
				name := fmt.Sprintf("source%d", i)
				e := storage.NewNamespacedEngine(b, name)
				_, err := e.CreateNode(&storage.Node{ID: "one", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(i)}})
				require.NoError(t, err)
				engines[name], names[name], modes[name] = e, name, "read_write"
			}
			c := storage.NewCompositeEngine(engines, names, modes)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: "getConstituentsForReadWithContext", cancel: cancel}
			callbacks := 0
			switch operation {
			case "label":
				_, err = c.GetNodesByLabelWithContext(ctx, "P")
			case "first":
				_, err = c.GetFirstNodeByLabelWithContext(ctx, "P")
			case "all":
				_, err = c.AllNodesWithContext(ctx)
			case "projected":
				err = c.StreamNodesByLabelProjectedWithContext(ctx, "P", []string{"k"}, func(*storage.Node) error { callbacks++; return nil })
			case "ids":
				err = c.ForEachNodeIDByLabelWithContext(ctx, "P", func(storage.NodeID) bool { callbacks++; return true })
			case "snapshot":
				_, err = c.GetNodesByLabelVisibleAtWithContext(ctx, "P", storage.MVCCVersion{})
			case "batch":
				_, err = c.BatchGetNodesWithContext(ctx, []storage.NodeID{"one"})
			case "options":
				err = c.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{}, func(*storage.Node) error { callbacks++; return nil })
			case "stream nodes":
				err = c.StreamNodes(ctx, func(*storage.Node) error { callbacks++; return nil })
			case "stream edges":
				err = c.StreamEdges(ctx, func(*storage.Edge) error { callbacks++; return nil })
			case "chunks":
				err = c.StreamNodeChunks(ctx, 2, func([]*storage.Node) error { callbacks++; return nil })
			case "edge type":
				err = c.StreamEdgesByType(ctx, "R", func(*storage.Edge) error { callbacks++; return nil })
			case "parked":
				_, err = c.StreamParkedEmbeddingFailures(ctx, func(storage.NodeID, map[string]any) error { callbacks++; return nil })
			case "consume marker":
				_, err = c.ConsumeCleanShutdownMarker(ctx)
			case "mark clean":
				err = c.MarkCleanShutdown(ctx)
			case "prune versions":
				_, err = c.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{})
			case "rebuild heads":
				err = c.RebuildMVCCHeads(ctx)
			case "prune temporal":
				_, err = c.PruneTemporalHistory(ctx, storage.TemporalPruneOptions{})
			case "rebuild temporal":
				err = c.RebuildTemporalIndexes(ctx)
			case "trigger prune":
				err = c.TriggerPruneNow(ctx)
			default:
				e := NewStorageExecutor(c)
				q := "MATCH (n:P) RETURN n.k"
				if operation == "MATCH first" {
					q += " LIMIT 1"
				}
				_, err = e.Execute(ctx, q, nil)
				// Composite statements must target one graph. These two cases
				// qualify that refusal, not native aggregate-read cancellation.
				require.ErrorContains(t, err, "require explicit graph targeting")
				require.Zero(t, ctx.hits)
				return
			}
			t.Logf("operation=%s owner=%s hits=%d read=%v", operation, ctx.owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2, "cancel inside source enumeration before any entity read")
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, callbacks)
			nodes, err := c.GetNodesByLabel("P")
			require.NoError(t, err)
			require.Len(t, nodes, 20)
		})
	}
}
