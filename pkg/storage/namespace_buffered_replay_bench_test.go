package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Measure the shared physical replay boundary separately from ACK preparation.
// Listener-free and subscribed stores exercise both notification dispositions.
func BenchmarkNamespaceBufferedReplay(b *testing.B) {
	for _, listeners := range []bool{false, true} {
		b.Run(map[bool]string{false: "no-listeners", true: "listeners"}[listeners], func(b *testing.B) {
			engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour, LowMemory: true})
			require.NoError(b, err)
			defer engine.Close()
			const batchSize = 32
			var nodes [batchSize]*Node
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID("replay_bench:" + string(rune('a'+i))), Labels: []string{"Item"}, Properties: map[string]any{"value": int64(0)}}
				_, err := engine.CreateNode(nodes[i])
				require.NoError(b, err)
			}
			if listeners {
				engine.OnNodeUpdated(func(*Node) {})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tx, err := engine.BeginTransaction()
				require.NoError(b, err)
				require.NoError(b, tx.SetImplicit(true))
				require.NoError(b, tx.SetNamespace("replay_bench"))
				for _, node := range nodes {
					node.Properties["value"] = int64(i + 1)
					require.NoError(b, tx.UpdateNode(node))
				}
				require.NoError(b, tx.Commit())
				b.StartTimer()
				require.NoError(b, engine.FlushWriteBehind())
			}
		})
	}
}
