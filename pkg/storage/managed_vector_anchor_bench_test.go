package storage

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func BenchmarkManagedVectorAnchorFirstUse(b *testing.B) {
	for _, count := range []int{1, 128, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			cfg := DefaultWALConfig()
			cfg.SyncMode = "immediate"
			cfg.BatchSyncInterval = time.Hour
			wal, err := NewWAL(b.TempDir(), cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = wal.Close() })
			operations := make([]Operation, count)
			for i := range operations {
				vector := make([]float32, 2048)
				for j := range vector {
					vector[j] = float32(i + j)
				}
				before := &Node{ID: NodeID(fmt.Sprintf("test:anchor-profile-%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": strings.Repeat("a", 2000)}, ChunkEmbeddings: [][]float32{vector}}
				after := CopyNode(before)
				after.Properties["updated"] = true
				operations[i] = Operation{Type: OpUpdateNode, Node: after, OldNode: before, nodeAnchorEligible: true}
			}
			ctx := b.Context()
			admission := func() error { return nil }
			startSyncs := wal.Stats().TotalSyncs
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				wal.advanceManagedVectorGeneration()
				b.StartTimer()
				references, err := wal.managedReferences(ctx, operations, admission)
				if err != nil || len(references.entries) != count {
					b.Fatalf("references=%d err=%v", len(references.entries), err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(wal.Stats().TotalSyncs-startSyncs)/float64(b.N), "syncs/op")
		})
	}
}
