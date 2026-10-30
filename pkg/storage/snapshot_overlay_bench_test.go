package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkSnapshotOverlayReads measures hot reads with an idle buffer and a
// small live generation. The same source is run on the unfixed and fixed heads.
func BenchmarkSnapshotOverlayReads(b *testing.B) {
	for _, pending := range []bool{false, true} {
		b.Run(fmt.Sprintf("pending=%v", pending), func(b *testing.B) {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()
			engine.writeBehind.Close()
			ids := make([]NodeID, 64)
			for i := range ids {
				ids[i] = NodeID(fmt.Sprintf("test:n%03d", i))
				if _, err := engine.CreateNode(&Node{ID: ids[i], Labels: []string{"L"}, Properties: map[string]any{"v": int64(i)}}); err != nil {
					b.Fatal(err)
				}
			}
			for i := 1; i < len(ids); i++ {
				if err := engine.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:r%03d", i)), StartNode: ids[0], EndNode: ids[i], Type: "REL"}); err != nil {
					b.Fatal(err)
				}
			}
			if pending {
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				if err = tx.SetImplicit(true); err != nil {
					b.Fatal(err)
				}
				if err = tx.SetNamespace("test"); err != nil {
					b.Fatal(err)
				}
				if err = tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: ids[1], Labels: []string{"L"}, Properties: map[string]any{"v": int64(100)}}, Policy: ClientNodeEdits}); err != nil {
					b.Fatal(err)
				}
				if err = tx.CreateEdge(&Edge{ID: "test:pending", StartNode: ids[0], EndNode: ids[1], Type: "REL"}); err != nil {
					b.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			reads := []struct {
				name string
				run  func() error
			}{
				{"point", func() error { _, err := engine.GetNode(ids[0]); return err }},
				{"projection", func() error { _, err := engine.GetNodeProjected(ids[0], []string{"v"}); return err }},
				{"batch", func() error { _, err := engine.BatchGetNodes(ids); return err }},
				{"adjacency", func() error { _, err := engine.GetOutgoingEdges(ids[0]); return err }},
				{"label", func() error { _, err := engine.GetNodesByLabel("L"); return err }},
				{"stream", func() error { return engine.StreamNodes(context.Background(), func(*Node) error { return nil }) }},
				{"counter", func() error { _, err := engine.EdgeCountByTypeInNamespace("test", "REL"); return err }},
			}
			for _, read := range reads {
				b.Run(read.name, func(b *testing.B) {
					if err := read.run(); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := read.run(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
