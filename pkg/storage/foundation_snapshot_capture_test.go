// Author: Lane #1644 (Codex). The raw and strict snapshot frame consumers must
// resolve endpoint identity in the same captured dictionary as stored reads.
package storage

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoundationSnapshotCapturedEndpointIdentity(t *testing.T) {
	for _, properties := range []map[string]any{{"key": "raw"}, {"key": []any{int8(1), int8(2)}}} {
		for _, scoped := range []bool{false, true} {
			t.Run(fmtNameSnapshot(properties, scoped), func(t *testing.T) {
				base := NewMemoryEngine()
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				for _, id := range []NodeID{"tenant:a", "tenant:b"} {
					_, err := base.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, base.CreateEdge(&Edge{ID: "tenant:e", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R", Properties: properties}))
				// Substitute only the mutable dictionary. The captured persisted mapping
				// remains authoritative; the control proves that a live lookup differs.
				num, ok := base.idDict.lookupNodeNumID("tenant:a")
				require.True(t, ok)
				shard := &base.idDict.nodes[nodeReverseShard(num)]
				shard.mu.Lock()
				saved := shard.reverse[num]
				shard.reverse[num] = "foreign:substituted"
				shard.mu.Unlock()
				defer func() { shard.mu.Lock(); shard.reverse[num] = saved; shard.mu.Unlock() }()
				var issued Engine = base
				want := NodeID("tenant:a")
				if scoped {
					issued = NewNamespacedEngine(base, "tenant")
					want = "a"
				}
				var output bytes.Buffer
				require.NoError(t, WriteSnapshot(context.Background(), issued, &output, SnapshotOptions{}))
				visitor := &collectingSnapshotVisitor{}
				_, err := ReadSnapshot(bytes.NewReader(output.Bytes()), visitor)
				require.NoError(t, err)
				require.Len(t, visitor.edges, 1)
				require.Equal(t, want, visitor.edges[0].StartNode)
				live, ok := base.idDict.lookupNodeIDByNum(num)
				require.True(t, ok)
				require.Equal(t, NodeID("foreign:substituted"), live)
			})
		}
	}
}
func fmtNameSnapshot(properties map[string]any, scoped bool) string {
	if _, raw := properties["key"].(string); raw {
		if scoped {
			return "raw/scoped"
		}
		return "raw/native"
	}
	if scoped {
		return "strict/scoped"
	}
	return "strict/native"
}
