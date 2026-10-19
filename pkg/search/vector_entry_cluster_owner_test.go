package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorEntryClusterMarkerTokens(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	for _, reverse := range []bool{false, true} {
		for _, alias := range []string{nodeNamedVectorKey("other", "v"), nodePropertyVectorKey("other", "p"), nodeChunkVectorKey("other", 1)} {
			name := "forward/" + alias
			if reverse {
				name = "reverse/" + alias
			}
			t.Run(name, func(t *testing.T) {
				engine := newNamespacedEngine(t)
				svc := NewServiceWithDimensions(engine, vectorOwnerDims)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				svc.EnableClustering(nil, 1)
				svc.SetMinEmbeddingsForClustering(1)
				other := &storage.Node{ID: "other", Properties: map[string]any{"text": "victimtoken"}}
				switch alias {
				case nodeNamedVectorKey("other", "v"):
					other.NamedEmbeddings = map[string][]float32{"v": vectorOwnerAxis(0)}
				case nodePropertyVectorKey("other", "p"):
					other.Properties["p"] = vectorOwnerAxis(0)
				default:
					other.ChunkEmbeddings = [][]float32{vectorOwnerAxis(0), vectorOwnerAxis(1)}
				}
				marker := vectorOwnerChunkNode(alias, 2)
				marker.Properties["text"] = "markertoken"
				nodes := []*storage.Node{other, marker}
				if reverse {
					nodes[0], nodes[1] = nodes[1], nodes[0]
				}
				for _, node := range nodes {
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
					require.NoError(t, svc.IndexNode(node))
				}
				require.NoError(t, svc.TriggerClustering(context.Background()))
				checkTerms := func() {
					t.Helper()
					svc.clusterStateMu.RLock()
					defer svc.clusterStateMu.RUnlock()
					gen := svc.clusterGen
					require.NotNil(t, gen)
					for cluster, terms := range gen.terms {
						canonical := svc.deriveClusterTerms(gen.members(cluster))
						require.NotNil(t, canonical)
						require.Equal(t, canonical.counts, terms.counts, "live token deltas must equal canonical vector ownership")
					}
				}
				checkTerms()
				marker.Properties["text"] = "updatedtoken"
				require.NoError(t, engine.UpdateNode(marker))
				require.NoError(t, svc.IndexNode(marker))
				checkTerms()
				require.NoError(t, engine.DeleteNode(marker.ID))
				require.NoError(t, svc.RemoveNode(marker.ID))
				checkTerms()
				require.True(t, cgHits(t, svc, vectorOwnerAxis(0), "other"))
			})
		}
	}
}
