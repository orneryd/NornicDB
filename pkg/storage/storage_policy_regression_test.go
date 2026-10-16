package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingKeysCleanedOnDeleteAndInlineUpdate(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "inline-update"
		if remove {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t)
			chunks := make([][]float32, 16)
			for i := range chunks {
				chunks[i] = []float32{float32(i), 1, 2}
			}
			node := &Node{ID: "test:vectors", Properties: map[string]any{"content": strings.Repeat("x", 60*1024)}, ChunkEmbeddings: chunks}
			_, err := eng.CreateNode(node)
			require.NoError(t, err)
			count := func() int {
				n := 0
				require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
					opts := badger.DefaultIteratorOptions
					opts.Prefix = embeddingPrefix(node.ID)
					it := txn.NewIterator(opts)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						n++
					}
					return nil
				}))
				return n
			}
			require.Equal(t, 16, count(), "fixture stores all chunks separately")
			if remove {
				err = eng.DeleteNode(node.ID)
			} else {
				node.Properties = map[string]any{"content": "small"}
				node.ChunkEmbeddings = [][]float32{{1, 2, 3}}
				err = eng.UpdateNode(node)
			}
			require.NoError(t, err)
			require.Zero(t, count(), "no orphaned embedding keys survive")
		})
	}
}

func TestRelationshipVersionDecayUsesUpdatedTime(t *testing.T) {
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)
	schema := eng.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "version", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromVersion, Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "links", TargetEdgeType: "LINKS", IsEdge: true, ProfileRef: "version"}))
	now := time.Now()
	old := now.Add(-72 * time.Hour)
	edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINKS", CreatedAt: old, UpdatedAt: now}
	require.False(t, eng.filterEdgeByDecay(edge, now.UnixNano()), "recently updated edge remains visible")
	edge.UpdatedAt = old
	require.True(t, eng.filterEdgeByDecay(edge, now.UnixNano()), "stale update is suppressed")
}

func TestNamespacedEdgesPreserveSuppressionFlag(t *testing.T) {
	for _, update := range []bool{false, true} {
		name := "create"
		if update {
			name = "update"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t)
			eng.SetRevealAll(true)
			ns := NewNamespacedEngine(eng, "test")
			for _, id := range []NodeID{"a", "b"} {
				_, err := ns.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			edge := &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINKS", VisibilitySuppressed: !update}
			require.NoError(t, ns.CreateEdge(edge))
			if update {
				edge.VisibilitySuppressed = true
				edge.Properties = map[string]any{"edited": true}
				require.NoError(t, ns.UpdateEdge(edge))
			}
			got, err := eng.GetEdge("test:e")
			require.NoError(t, err)
			require.True(t, got.VisibilitySuppressed, "namespace translation must retain stored visibility metadata")
			require.Equal(t, EdgeID("e"), edge.ID, "caller IDs are unchanged")
		})
	}
}
