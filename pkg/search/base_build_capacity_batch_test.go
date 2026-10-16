package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capacityBatchCountingIndex struct {
	bm25Index
	batchCalls  int
	singleCalls int
}

func (c *capacityBatchCountingIndex) Index(id, text string) {
	c.singleCalls++
	c.bm25Index.Index(id, text)
}
func (c *capacityBatchCountingIndex) IndexBatch(entries []FulltextBatchEntry) {
	c.batchCalls++
	c.bm25Index.IndexBatch(entries)
}

func TestBaseBuildCapacityUsesOneBatchOwner(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, kind := range []string{"oversized_text", "aggregate_text", "aggregate_metadata", "aggregate_vector", "full_batch_text", "within_budget", "unlimited"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, kind), func(t *testing.T) {
					engine := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					_, iterator := source.(NodeIterator)
					require.Equal(t, !traced, iterator, "fixture must select the intended real traversal")
					options := &ServiceOptions{}
					text := "xxx"
					nodeCount := 2
					reject := true
					wantDocs := 1
					wantResident := int64(3)
					wantVectors := 0
					switch kind {
					case "oversized_text":
						options.BM25MemoryMaxBytes = 4
						text = "xxxxx"
						nodeCount = 1
						wantDocs = 0
						wantResident = 0
					case "aggregate_text":
						options.BM25MemoryMaxBytes = 4
					case "full_batch_text":
						options.BM25MemoryMaxBytes = 4
						nodeCount = fulltextBuildBatchSize + 1
					case "aggregate_metadata":
						options.MetadataMemoryMaxBytes = 40
						text = "x"
						wantResident = 1
					case "aggregate_vector":
						options.VectorMemoryMaxBytes = 12
						text = ""
						wantDocs = 0
						wantResident = 0
						wantVectors = 1
					case "within_budget":
						options.BM25MemoryMaxBytes = 6
						reject = false
						wantDocs = 2
						wantResident = 6
					case "unlimited":
						reject = false
						wantDocs = 2
						wantResident = 0
					}
					for i := 0; i < nodeCount; i++ {
						n := &storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), Properties: map[string]any{"text": text}}
						if kind == "aggregate_vector" {
							n.NamedEmbeddings = map[string][]float32{"v": {1, 0, 0}}
						}
						_, err := engine.CreateNode(n)
						require.NoError(t, err)
					}
					svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, options)
					t.Cleanup(func() { require.NoError(t, svc.Close()) })
					if kind == "aggregate_metadata" {
						svc.SetIndexFlags(true, false)
					}
					counted := &capacityBatchCountingIndex{bm25Index: svc.fulltext()}
					svc.setFulltext(counted)
					err := svc.BuildIndexes(context.Background())
					if reject {
						assert.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
						assert.False(t, svc.IsReady(), "a rejected build must not publish ready")
					} else {
						require.NoError(t, err)
						require.True(t, svc.IsReady())
					}
					assert.Equal(t, wantDocs, svc.fulltext().Count(), "rejected entries must not enter fulltext")
					assert.Equal(t, wantVectors, svc.EmbeddingCount())
					if kind != "unlimited" {
						assert.Equal(t, wantResident, svc.bm25ResidentBytes)
					}
					if kind == "unlimited" {
						require.Greater(t, counted.batchCalls, 0, "preserve unlimited batch optimization")
						require.Zero(t, counted.singleCalls)
					} else {
						assert.Zero(t, counted.batchCalls, "configured budgets must be admitted before any IndexBatch")
					}
				})
			}
		}
	}
}
