package inference

import (
	"context"
	"testing"
	"time"

	configpkg "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Similarity results are node-level: IDs are owning node IDs and are never
// parsed, so a node whose own ID looks like a vector-entry key ("node-2-chunk-0")
// is its own target. The best score per target is kept, and the source itself
// is skipped.
func TestEngine_OnStoreBestOfChunks_DeduplicatesNodeResults(t *testing.T) {
	engine := New(&Config{
		SimilarityThreshold: 0.8,
		SimilarityTopK:      0, // exercises default top-k fallback
	})

	var calls []int
	engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
		calls = append(calls, k)
		switch int(embedding[0]) {
		case 1:
			return []SimilarityResult{
				{ID: "node-2", Score: 0.90},
				{ID: "node-1", Score: 0.99}, // self
				{ID: "node-2-chunk-0", Score: 0.84},
			}, nil
		case 2:
			return []SimilarityResult{
				{ID: "node-2", Score: 0.95}, // same node, higher score
				{ID: "node-4", Score: 0.70}, // below threshold
			}, nil
		default:
			return nil, nil
		}
	})

	suggestions, err := engine.OnStoreBestOfChunks(context.Background(), "node-1", [][]float32{
		{1},
		{2},
		nil,
	})
	require.NoError(t, err)
	require.Len(t, suggestions, 2)
	require.Equal(t, []int{10, 10}, calls)

	assert.Equal(t, "node-2", suggestions[0].TargetID)
	assert.Equal(t, "node-2-chunk-0", suggestions[1].TargetID)
	assert.Equal(t, "similarity", suggestions[0].Method)
	assert.Equal(t, "similarity", suggestions[1].Method)
}

func TestEngine_ExtensionGettersSettersAndCleanup(t *testing.T) {
	engine := New(nil)

	topo := &TopologyIntegration{}
	cluster := &ClusterIntegration{}
	kalman := &KalmanAdapter{}
	qc := &HeimdallQC{}
	cooldown := NewCooldownTableWithConfig(map[string]time.Duration{"custom": time.Second})
	evidence := NewEvidenceBufferWithConfig(map[string]EvidenceThreshold{
		"RELATES_TO": {MinCount: 2, MinScore: 0.4, MinSessions: 1, MaxAge: time.Minute},
	})
	meta := storage.NewEdgeMetaStore()
	nodeCfg := storage.NewNodeConfigStore()

	engine.SetTopologyIntegration(topo)
	engine.SetClusterIntegration(cluster)
	engine.SetKalmanAdapter(kalman)
	engine.SetHeimdallQC(qc)
	engine.SetCooldownTable(cooldown)
	engine.SetEvidenceBuffer(evidence)
	engine.SetEdgeMetaStore(meta)
	engine.SetNodeConfigStore(nodeCfg)

	assert.Same(t, topo, engine.GetTopologyIntegration())
	assert.Same(t, cluster, engine.GetClusterIntegration())
	assert.Same(t, kalman, engine.GetKalmanAdapter())
	assert.Same(t, qc, engine.GetHeimdallQC())
	assert.Same(t, cooldown, engine.GetCooldownTable())
	assert.Same(t, evidence, engine.GetEvidenceBuffer())
	assert.Same(t, meta, engine.GetEdgeMetaStore())
	assert.Same(t, nodeCfg, engine.GetNodeConfigStore())
	assert.Equal(t, time.Second, cooldown.GetLabelCooldown("custom"))
	assert.Equal(t, 2, evidence.GetThreshold("RELATES_TO").MinCount)

	restore := configpkg.WithEdgeProvenanceEnabled()
	defer restore()

	require.NoError(t, meta.Append(context.Background(), storage.EdgeMeta{
		Src:        "node-1",
		Dst:        "node-2",
		Label:      "RELATES_TO",
		SignalType: "similarity",
		Timestamp:  time.Now().Add(-2 * time.Hour),
	}))

	cooldown.RecordMaterializationAt("node-1", "node-2", "custom", time.Now().Add(-3*time.Second))
	evidence.AddEvidenceWithMetadata("node-1", "node-2", "RELATES_TO", 0.5, "similarity", "s1", map[string]interface{}{"k": "v"})
	evidence.entries[EvidenceKey{Src: "node-1", Dst: "node-2", Label: "RELATES_TO"}.String()].FirstTs = time.Now().Add(-2 * time.Minute)

	cooldownRemoved, evidenceRemoved, provenanceRemoved := engine.CleanupTier1WithProvenance(time.Hour)
	assert.Equal(t, 1, cooldownRemoved)
	assert.Equal(t, 1, evidenceRemoved)
	assert.Equal(t, 1, provenanceRemoved)
}

func TestEngine_ValidateSuggestionsWithHeimdall_Branches(t *testing.T) {
	t.Run("empty suggestions and nil QC short-circuit", func(t *testing.T) {
		engine := New(nil)
		got := engine.validateSuggestionsWithHeimdall(context.Background(), "src", []float32{1}, nil)
		require.Nil(t, got)
	})

	t.Run("review error fail-open returns original suggestions", func(t *testing.T) {
		engine := New(nil)
		engine.SetHeimdallQC(NewHeimdallQC(func(ctx context.Context, prompt string) (string, error) {
			return "", assert.AnError
		}, nil))

		in := []EdgeSuggestion{{SourceID: "src", TargetID: "dst", Type: "RELATES_TO", Confidence: 0.8}}
		got := engine.validateSuggestionsWithHeimdall(context.Background(), "src", []float32{1}, in)
		require.Equal(t, in, got)
	})

	t.Run("approved plus augmented suggestions with candidate-pool filtering", func(t *testing.T) {
		cleanupQC := configpkg.WithAutoTLPLLMQCEnabled()
		defer cleanupQC()
		cleanupAug := configpkg.WithAutoTLPLLMAugmentEnabled()
		defer cleanupAug()

		engine := New(nil)
		engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
			require.Equal(t, 20, k)
			return []SimilarityResult{
				{ID: "src", Score: 0.99},    // filtered: source node
				{ID: "t1", Score: 0.95},     // filtered: already suggested
				{ID: "cand-1", Score: 0.90}, // candidate
				{ID: "cand-2", Score: 0.88}, // candidate
				{ID: "cand-3", Score: 0.86}, // candidate
			}, nil
		})
		engine.SetHeimdallQC(NewHeimdallQC(func(ctx context.Context, prompt string) (string, error) {
			return `{"approved":[0],"additional":[{"target_id":"aug-1","type":"INSPIRED_BY","conf":0.7,"reason":"extra"}]}`, nil
		}, nil))

		in := []EdgeSuggestion{{SourceID: "src", TargetID: "t1", Type: "RELATES_TO", Confidence: 0.9}}
		got := engine.validateSuggestionsWithHeimdall(context.Background(), "src", []float32{1, 2, 3}, in)
		require.Len(t, got, 2)
		require.Equal(t, "t1", got[0].TargetID)
		require.Equal(t, "aug-1", got[1].TargetID)
		require.Equal(t, "src", got[1].SourceID, "augmented edges should be backfilled with source id")
	})
}
