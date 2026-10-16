package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseBuildRelationshipCapacityErrorsRemainVisible(t *testing.T) {
	for _, traced := range []bool{false, true} {
		for _, kind := range []string{"oversized_payload", "aggregate_payload", "oversized_metadata", "aggregate_metadata", "mixed_payload", "within_budget", "unlimited"} {
			t.Run(fmt.Sprintf("traced=%t/%s", traced, kind), func(t *testing.T) {
				engine := newNamespacedEngine(t)
				var source storage.Engine = engine
				if traced {
					source = storage.NewTracedEngine(engine)
				}
				_, streaming := source.(storage.StreamingEngine)
				require.Equal(t, !traced, streaming, "fixture must cover streaming and AllEdges fallback")
				options := &ServiceOptions{VectorStorageMode: "memory"}
				edgeCount, wantEdges := 2, 1
				wantResident := int64(12)
				reject := true
				switch kind {
				case "oversized_payload":
					options.VectorMemoryMaxBytes = 8
					edgeCount, wantEdges, wantResident = 1, 0, 0
				case "aggregate_payload":
					options.VectorMemoryMaxBytes = 12
				case "oversized_metadata":
					options.MetadataMemoryMaxBytes = 75
					edgeCount, wantEdges, wantResident = 1, 0, 0
				case "aggregate_metadata":
					options.MetadataMemoryMaxBytes = 100
				case "mixed_payload":
					options.VectorMemoryMaxBytes = 24
					wantResident = 24
				case "within_budget":
					options.VectorMemoryMaxBytes = 24
					reject, wantEdges, wantResident = false, 2, 24
				case "unlimited":
					reject, wantEdges, wantResident = false, 2, 24
				}
				for _, id := range []storage.NodeID{"a", "b"} {
					node := &storage.Node{ID: id}
					if kind == "mixed_payload" && id == "a" {
						node.NamedEmbeddings = map[string][]float32{"v": {1, 0, 0}}
					}
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
				}
				for i := 0; i < edgeCount; i++ {
					require.NoError(t, engine.CreateEdge(&storage.Edge{
						ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "a", EndNode: "b", Type: "REL",
						Properties: map[string]any{"embedding": []float32{1, 0, 0}},
					}))
				}
				svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, "v2", options)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				svc.SetIndexFlags(false, true)
				err := svc.BuildIndexes(context.Background())
				if reject {
					assert.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
					assert.False(t, svc.IsReady(), "a rejected relationship must stop completion")
				} else {
					require.NoError(t, err)
					require.True(t, svc.IsReady())
				}
				assert.Len(t, svc.edgePropVector, wantEdges)
				assert.Len(t, svc.edgeTypes, wantEdges, "rejected edge metadata must not be installed")
				assert.Len(t, svc.indexCapacityByEdge, wantEdges)
				assert.Equal(t, wantResident, svc.vectorResidentBytes)
				if !reject {
					hits, err := svc.VectorQueryRelationships(context.Background(), []float32{1, 0, 0}, RelationshipVectorQuerySpec{
						Type: "REL", Property: "embedding", Similarity: "cosine", Limit: 2,
					})
					require.NoError(t, err)
					require.Len(t, hits, 2, "successful readiness must expose the complete relationship population")
					ids := make([]string, 0, len(hits))
					for _, hit := range hits {
						ids = append(ids, hit.ID)
						assert.InDelta(t, 1, hit.Score, 1e-6)
					}
					assert.ElementsMatch(t, []string{"r0", "r1"}, ids)
				}
			})
		}
	}
}
