// Author: Codex, NOR-160 implementation sub-agent, 2026-10-26.
package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Keep these rows on both products: they distinguish complete point reads,
// projected-first reads, embedding hydration, adjacency and iterator ownership.
func BenchmarkNOR160CommittedSnapshotReads(b *testing.B) {
	engine := newTestMemoryEngine(b)
	writer, err := engine.BeginTransaction()
	require.NoError(b, err)
	for _, node := range []*Node{
		{ID: "test:a", Labels: []string{"Held"}, Properties: map[string]any{"value": int64(1)}},
		{ID: "test:b", Labels: []string{"Held"}, Properties: map[string]any{"value": int64(2)}},
		{ID: "test:embedded", Labels: []string{"Embedded"}, Properties: map[string]any{"value": int64(3)}, ChunkEmbeddings: [][]float32{{1, 2, 3}}, EmbedMeta: map[string]any{"chunk_count": int64(1)}},
	} {
		_, err := writer.CreateNode(node)
		require.NoError(b, err)
	}
	require.NoError(b, writer.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "COHELD", Properties: map[string]any{"value": int64(1)}}))
	require.NoError(b, writer.Commit())
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit"
		}
		b.Run(mode, func(b *testing.B) {
			rows := []string{"node", "query-node", "projected-first", "query-embedded", "full-embedded", "edge", "outgoing", "incoming", "label", "edge-stream", "projected-first-policy", "edge-policy", "outgoing-policy"}
			for _, row := range rows {
				b.Run(row, func(b *testing.B) {
					engine.SetDecayEnabled(strings.HasSuffix(row, "-policy"))
					defer engine.SetDecayEnabled(false)
					operation := strings.TrimSuffix(row, "-policy")
					tx, err := engine.BeginTransaction()
					require.NoError(b, err)
					require.NoError(b, tx.SetNamespace("test"))
					b.Cleanup(func() { _ = tx.Rollback() })
					var read func() error
					if explicit {
						switch operation {
						case "node":
							read = func() error { _, err := tx.GetNode("test:a"); return err }
						case "query-node":
							read = func() error { _, err := tx.GetQueryNode("test:a"); return err }
						case "projected-first":
							read = func() error { _, err := tx.GetNodeProjected("test:a", []string{"value"}); return err }
						case "query-embedded":
							read = func() error { _, err := tx.GetQueryNode("test:embedded"); return err }
						case "full-embedded":
							read = func() error { _, err := tx.GetNode("test:embedded"); return err }
						case "edge":
							read = func() error { _, err := tx.GetEdge("test:e"); return err }
						case "outgoing":
							read = func() error {
								_, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
								return err
							}
						case "incoming":
							read = func() error {
								_, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
								return err
							}
						case "label":
							read = func() error { _, err := tx.GetNodesByLabel("Held"); return err }
						case "edge-stream":
							read = func() error {
								return tx.StreamEdgesByType(context.Background(), "COHELD", func(*Edge) error { return nil })
							}
						}
					} else {
						switch operation {
						case "node":
							read = func() error { _, err := engine.GetNode("test:a"); return err }
						case "query-node":
							read = func() error { _, err := engine.GetQueryNode("test:a"); return err }
						case "projected-first":
							read = func() error { _, err := engine.GetNodeProjected("test:a", []string{"value"}); return err }
						case "query-embedded":
							read = func() error { _, err := engine.GetQueryNode("test:embedded"); return err }
						case "full-embedded":
							read = func() error { _, err := engine.GetNode("test:embedded"); return err }
						case "edge":
							read = func() error { _, err := engine.GetEdge("test:e"); return err }
						case "outgoing":
							read = func() error { _, err := engine.GetOutgoingEdges("test:a"); return err }
						case "incoming":
							read = func() error { _, err := engine.GetIncomingEdges("test:b"); return err }
						case "label":
							read = func() error { _, err := engine.GetNodesByLabel("Held"); return err }
						case "edge-stream":
							read = func() error {
								return engine.StreamEdgesByType(context.Background(), "COHELD", func(*Edge) error { return nil })
							}
						}
					}
					require.NoError(b, read())
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						if err := read(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
