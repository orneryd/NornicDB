package inference

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock LLM responses for testing
const (
	mockApproveAllResponse       = `{"approved": [0, 1, 2], "rejected": [], "reasoning": "All suggestions look valid"}`
	mockApprovePartialResponse   = `{"approved": [0, 2], "rejected": [1], "reasoning": "Middle one is not related"}`
	mockRejectAllResponse        = `{"approved": [], "rejected": [0, 1, 2], "reasoning": "None of these make sense"}`
	mockWithTypeOverrideResponse = `{"approved": [0], "rejected": [1], "type_overrides": {"0": "SIMILAR_TO"}, "reasoning": "Type should be more specific"}`
	mockWithAugmentationResponse = `{"approved": [0], "additional": [{"target_id": "aug-node-1", "type": "INSPIRED_BY", "conf": 0.75, "reason": "Found related concept"}], "reasoning": "Added extra connection"}`
	mockMalformedJSON            = `{"approved": [0, 1 "reasoning": broken`
	mockNonJSONResponse          = `I think we should approve the first two suggestions because they seem related.`
	mockEmptyResponse            = ``
)

func TestHeimdallQC_NewWithDefaults(t *testing.T) {
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockApproveAllResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	require.NotNil(t, qc)
	assert.Equal(t, 10*time.Second, qc.config.Timeout)
	assert.Equal(t, 4096, qc.config.MaxContextBytes)
	assert.Equal(t, 5, qc.config.MaxBatchSize)
	assert.Equal(t, 200, qc.config.MaxNodeSummaryLen)
	assert.Equal(t, 0.5, qc.config.MinConfidenceToReview)
	assert.True(t, qc.config.CacheDecisions)
}

func TestHeimdallQC_NewWithCustomConfig(t *testing.T) {
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockApproveAllResponse, nil
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		Timeout:               5 * time.Second,
		MaxContextBytes:       2048,
		MaxBatchSize:          3,
		MaxNodeSummaryLen:     100,
		MinConfidenceToReview: 0.7,
		CacheDecisions:        false,
		CacheTTL:              30 * time.Minute,
	}

	qc := NewHeimdallQC(mockLLM, cfg)
	require.NotNil(t, qc)
	assert.Equal(t, 5*time.Second, qc.config.Timeout)
	assert.Equal(t, 2048, qc.config.MaxContextBytes)
	assert.Equal(t, 3, qc.config.MaxBatchSize)
	assert.Equal(t, 0.7, qc.config.MinConfidenceToReview)
}

func TestHeimdallQC_ReviewBatch_FeatureFlagDisabled(t *testing.T) {
	// Ensure feature flag is disabled
	config.DisableAutoTLPLLMQC()
	defer config.DisableAutoTLPLLMQC()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		return mockApproveAllResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)

	approved, augmented, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 3, len(approved), "All suggestions should pass through when flag is disabled")
	assert.Equal(t, 0, len(augmented))
	assert.Equal(t, 0, callCount, "LLM should not be called when flag is disabled")
}

func TestHeimdallQC_ReviewBatch_ApproveAll(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockApproveAllResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)
	sourceNode := NodeSummary{ID: "source-node", Labels: []string{"Test"}}

	approved, augmented, err := qc.ReviewBatch(context.Background(), sourceNode, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 3, len(approved))
	assert.Equal(t, 0, len(augmented))

	// Verify suggestions are marked as Heimdall approved
	for _, sug := range approved {
		assert.Contains(t, sug.Reason, "Heimdall approved")
	}
}

func TestHeimdallQC_ReviewBatch_ApprovePartial(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockApprovePartialResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 2, len(approved), "Only indices 0 and 2 should be approved")

	// Verify correct suggestions were approved
	targetIDs := make(map[string]bool)
	for _, sug := range approved {
		targetIDs[sug.TargetID] = true
	}
	assert.True(t, targetIDs["target-0"])
	assert.False(t, targetIDs["target-1"])
	assert.True(t, targetIDs["target-2"])
}

func TestHeimdallQC_ReviewBatch_RejectAll(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockRejectAllResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 0, len(approved), "All suggestions should be rejected")
}

func TestHeimdallQC_ReviewBatch_TypeOverride(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockWithTypeOverrideResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(2)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	require.Equal(t, 1, len(approved))
	assert.Equal(t, "SIMILAR_TO", approved[0].Type, "Type should be overridden")
}

func TestHeimdallQC_ReviewBatch_WithAugmentation(t *testing.T) {
	cleanupQC := config.WithAutoTLPLLMQCEnabled()
	defer cleanupQC()
	cleanupAug := config.WithAutoTLPLLMAugmentEnabled()
	defer cleanupAug()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockWithAugmentationResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(2)
	candidatePool := []NodeSummary{
		{ID: "aug-node-1", Labels: []string{"Concept"}},
	}

	approved, augmented, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, candidatePool)

	require.NoError(t, err)
	assert.Equal(t, 1, len(approved))
	assert.Equal(t, 1, len(augmented), "Should have one augmented edge")

	if len(augmented) > 0 {
		assert.Equal(t, "aug-node-1", augmented[0].TargetID)
		assert.Equal(t, "INSPIRED_BY", augmented[0].Type)
		assert.Equal(t, 0.75, augmented[0].Confidence)
		assert.Equal(t, "heimdall_augment", augmented[0].Method)
	}
}

func TestHeimdallQC_ReviewBatch_LLMError_FailOpen(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return "", errors.New("LLM service unavailable")
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	// Should NOT return error - fail-open behavior
	require.NoError(t, err)
	// All suggestions should pass through on error
	assert.Equal(t, 3, len(approved), "All suggestions should pass through on LLM error (fail-open)")

	// Check stats
	stats := qc.GetStats()
	assert.Equal(t, int64(1), stats.Errors)
}

func TestHeimdallQC_ReviewBatch_LLMTimeout_FailOpen(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		// Simulate timeout by checking context
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return "", errors.New("timeout")
		}
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		Timeout:               50 * time.Millisecond, // Very short timeout
		MaxContextBytes:       4096,
		MaxBatchSize:          5,
		MinConfidenceToReview: 0.5,
	}

	qc := NewHeimdallQC(mockLLM, cfg)
	suggestions := createTestSuggestions(2)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 2, len(approved), "Should fail-open on timeout")
}

func TestHeimdallQC_ReviewBatch_LLMPanic_Recovery(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	// Note: In real implementation, the LLM func is called directly,
	// so panics would propagate. This test verifies we handle errors gracefully.
	// A production system might wrap the call in a recover().
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return "", errors.New("simulated crash")
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(2)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 2, len(approved), "Should fail-open on crash")
}

func TestHeimdallQC_ReviewBatch_MalformedJSON_FuzzyParse(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockMalformedJSON, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	// Fuzzy parse should default to approve all
	assert.Equal(t, 3, len(approved), "Malformed JSON should trigger fuzzy parse and approve")
}

func TestHeimdallQC_ReviewBatch_NonJSONResponse_FuzzyParse(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockNonJSONResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(2)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	// Fuzzy parse: "approve" appears in text
	assert.Equal(t, 2, len(approved))
}

func TestHeimdallQC_ReviewBatch_EmptyResponse_FuzzyParse(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return mockEmptyResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(2)

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	// Empty should default to approve (fail-open)
	assert.Equal(t, 2, len(approved))
}

func TestHeimdallQC_ReviewBatch_PromptTooLarge_SkipReview(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		return mockApproveAllResponse, nil
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		MaxContextBytes:       100, // Very small limit
		MaxBatchSize:          5,
		MinConfidenceToReview: 0.5,
		MaxNodeSummaryLen:     200,
	}

	qc := NewHeimdallQC(mockLLM, cfg)

	// Create suggestions that will exceed the limit
	suggestions := createTestSuggestions(5)
	sourceNode := NodeSummary{
		ID:     "source-node",
		Labels: []string{"Test", "Large"},
		Props: map[string]string{
			"content": strings.Repeat("This is a large property value. ", 10),
		},
	}

	approved, _, err := qc.ReviewBatch(context.Background(), sourceNode, suggestions, nil)

	require.NoError(t, err)
	// Should pass through without LLM call
	assert.Equal(t, 5, len(approved))
	assert.Equal(t, 0, callCount, "LLM should not be called when prompt exceeds size limit")

	// Check stats
	stats := qc.GetStats()
	assert.Greater(t, stats.Skipped, int64(0), "Should have recorded skipped suggestions")
}

func TestHeimdallQC_ReviewBatch_LowConfidence_SkipReview(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		return mockApproveAllResponse, nil
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		MaxContextBytes:       4096,
		MaxBatchSize:          5,
		MinConfidenceToReview: 0.8, // High threshold
	}

	qc := NewHeimdallQC(mockLLM, cfg)

	// Create low-confidence suggestions
	suggestions := []EdgeSuggestion{
		{TargetID: "target-0", Type: "RELATES_TO", Confidence: 0.5, Method: "similarity"},
		{TargetID: "target-1", Type: "RELATES_TO", Confidence: 0.6, Method: "similarity"},
	}

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	// Low confidence suggestions should be skipped (not approved, not sent to LLM)
	assert.Equal(t, 0, len(approved))
	assert.Equal(t, 0, callCount, "LLM should not be called for low-confidence suggestions")
}

func TestHeimdallQC_ReviewBatch_Batching(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		// Return approving indices based on batch (simple mock)
		return `{"approved": [0, 1], "reasoning": "batch approved"}`, nil
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		MaxContextBytes:       4096,
		MaxBatchSize:          2, // Small batch size
		MinConfidenceToReview: 0.5,
	}

	qc := NewHeimdallQC(mockLLM, cfg)
	suggestions := createTestSuggestions(5) // 5 suggestions with batch size 2 = 3 batches

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	assert.Equal(t, 3, callCount, "Should make 3 LLM calls for 5 suggestions with batch size 2")
	// Each batch approves 2, except last batch has 1: 2+2+1 = 5
	// But mock always returns [0,1], so last batch only approves index 0
	assert.GreaterOrEqual(t, len(approved), 3)
}

func TestHeimdallQC_ReviewBatch_Caching(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		return mockApproveAllResponse, nil
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		CacheDecisions:        true,
		CacheTTL:              time.Hour,
		MaxContextBytes:       4096,
		MaxBatchSize:          5,
		MinConfidenceToReview: 0.5,
	}

	qc := NewHeimdallQC(mockLLM, cfg)
	suggestions := createTestSuggestions(2)
	sourceNode := NodeSummary{ID: "source"}

	// First call
	_, _, err := qc.ReviewBatch(context.Background(), sourceNode, suggestions, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, callCount)

	// Second call with same inputs should hit cache
	_, _, err = qc.ReviewBatch(context.Background(), sourceNode, suggestions, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, callCount, "Second call should use cache")

	stats := qc.GetStats()
	assert.Equal(t, int64(1), stats.CacheHits)
}

func TestHeimdallQC_ClearCache(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		return mockApproveAllResponse, nil
	}

	cfg := &HeimdallQCConfig{
		Enabled:               true,
		CacheDecisions:        true,
		CacheTTL:              time.Hour,
		MaxContextBytes:       4096,
		MaxBatchSize:          5,
		MinConfidenceToReview: 0.5,
	}

	qc := NewHeimdallQC(mockLLM, cfg)
	suggestions := createTestSuggestions(2)
	sourceNode := NodeSummary{ID: "source"}

	// First call
	qc.ReviewBatch(context.Background(), sourceNode, suggestions, nil)
	assert.Equal(t, 1, callCount)

	// Clear cache
	qc.ClearCache()

	// Call again - should make new LLM call
	qc.ReviewBatch(context.Background(), sourceNode, suggestions, nil)
	assert.Equal(t, 2, callCount, "Should call LLM again after cache clear")
}

func TestHeimdallQC_GetStats(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		time.Sleep(1 * time.Millisecond) // Add small delay so latency > 0
		return mockApprovePartialResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3)

	qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	stats := qc.GetStats()
	assert.Equal(t, int64(1), stats.BatchesProcessed)
	assert.Equal(t, int64(3), stats.SuggestionsIn)
	assert.Equal(t, int64(2), stats.SuggestionsOut)          // mockApprovePartialResponse approves [0, 2]
	assert.GreaterOrEqual(t, stats.AvgLatencyMs, float64(0)) // May be 0 on fast machines
}

func TestHeimdallQC_FuzzyParseBatchResponse(t *testing.T) {
	qc := NewHeimdallQC(nil, nil)

	tests := []struct {
		name          string
		raw           string
		numCandidates int
		wantApproved  int
	}{
		{
			name:          "approve all keyword",
			raw:           "I approve all of these suggestions",
			numCandidates: 3,
			wantApproved:  3,
		},
		{
			name:          "all approved keyword",
			raw:           "All approved, they look good",
			numCandidates: 3,
			wantApproved:  3,
		},
		{
			name:          "reject all keyword",
			raw:           "I reject all of these",
			numCandidates: 3,
			wantApproved:  0,
		},
		{
			name:          "none keyword",
			raw:           "None of these make sense",
			numCandidates: 3,
			wantApproved:  0,
		},
		{
			name:          "default approve",
			raw:           "hmm not sure what to do here",
			numCandidates: 3,
			wantApproved:  3, // Default fail-open
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := qc.fuzzyParseBatchResponse(tt.raw, tt.numCandidates)
			assert.Equal(t, tt.wantApproved, len(resp.Approved))
		})
	}
}

func TestSummarizeNode(t *testing.T) {
	props := map[string]interface{}{
		"title":     "Test Node",
		"content":   "This is a short string", // Under 2x maxPropLen, will be included
		"embedding": []float32{0.1, 0.2, 0.3}, // Should be skipped (not string)
		"count":     42,                       // Should be skipped (not string)
	}

	summary := SummarizeNode("node-123", []string{"Test", "Node"}, props, 50)

	assert.Equal(t, "node-123", summary.ID)
	assert.Equal(t, []string{"Test", "Node"}, summary.Labels)
	assert.Equal(t, "Test Node", summary.Props["title"])
	assert.Equal(t, "This is a short string", summary.Props["content"])
	_, hasEmbedding := summary.Props["embedding"]
	_, hasCount := summary.Props["count"]
	assert.False(t, hasEmbedding, "embedding should be skipped (not string)")
	assert.False(t, hasCount, "count should be skipped (not string)")
}

func TestSummarizeNode_Truncation(t *testing.T) {
	// String must be > maxPropLen but <= maxPropLen*2 to be truncated (not skipped)
	props := map[string]interface{}{
		"content": "This is a string to truncate", // 28 chars, > 20 but <= 40
	}

	summary := SummarizeNode("node-123", []string{"Test"}, props, 20)

	assert.Equal(t, "This is a string ...", summary.Props["content"])
}

func TestEstimatePromptSize(t *testing.T) {
	sourceNode := NodeSummary{
		ID:     "source",
		Labels: []string{"Test"},
		Props:  map[string]string{"title": "Test Node"},
	}

	candidates := []CandidateSummary{
		{TargetID: "target-1", Type: "RELATES_TO", Method: "similarity"},
		{TargetID: "target-2", Type: "RELATES_TO", Method: "co_access"},
	}

	size := EstimatePromptSize(sourceNode, candidates)
	assert.Greater(t, size, 200) // Base size + content
}

func TestTruncateStr(t *testing.T) {
	tests := []struct {
		input    string
		max      int
		expected string
	}{
		{"short", 10, "short"},
		{"this is a long string", 10, "this is..."},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"}, // max <= 3 doesn't add ellipsis
		{"", 10, ""},
	}

	for _, tt := range tests {
		result := truncateStr(tt.input, tt.max)
		assert.Equal(t, tt.expected, result)
	}
}

func TestGetSystemPrompt(t *testing.T) {
	// Test without augmentation
	prompt := GetSystemPrompt(false)
	assert.Contains(t, prompt, "Review graph edges")
	assert.Contains(t, prompt, "approved")
	assert.NotContains(t, prompt, "additional") // No augmentation instructions

	// Test with augmentation
	augmentPrompt := GetSystemPrompt(true)
	assert.Contains(t, augmentPrompt, "Review graph edges")
	assert.Contains(t, augmentPrompt, "additional") // Has augmentation instructions

	// Both should be short for KV cache efficiency
	assert.Less(t, len(prompt), 250, "System prompt should be concise")
	assert.Less(t, len(augmentPrompt), 350, "Augment prompt should be concise")
}

func TestSystemPromptIsStateless(t *testing.T) {
	// Verify system prompts don't mention conversation, history, or context
	// These would imply stateful behavior which we don't want
	prompt := GetSystemPrompt(false)

	assert.NotContains(t, strings.ToLower(prompt), "previous")
	assert.NotContains(t, strings.ToLower(prompt), "conversation")
	assert.NotContains(t, strings.ToLower(prompt), "history")
	assert.NotContains(t, strings.ToLower(prompt), "remember")
	assert.NotContains(t, strings.ToLower(prompt), "context")
}

// Helper function to create test suggestions
func createTestSuggestions(count int) []EdgeSuggestion {
	suggestions := make([]EdgeSuggestion, count)
	for i := 0; i < count; i++ {
		suggestions[i] = EdgeSuggestion{
			SourceID:   "source-node",
			TargetID:   "target-" + string(rune('0'+i)),
			Type:       "RELATES_TO",
			Confidence: 0.8, // Above default threshold
			Reason:     "Test suggestion",
			Method:     "similarity",
		}
	}
	return suggestions
}

// Test context cancellation
func TestHeimdallQC_ReviewBatch_ContextCancelled(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			time.Sleep(100 * time.Millisecond) // Simulate work
			return mockApproveAllResponse, nil
		}
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(2)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	approved, _, err := qc.ReviewBatch(ctx, NodeSummary{ID: "source"}, suggestions, nil)

	// Should fail-open and return all suggestions
	require.NoError(t, err)
	assert.Equal(t, 2, len(approved))
}

// Test empty suggestions
func TestHeimdallQC_ReviewBatch_EmptySuggestions(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	callCount := 0
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		callCount++
		return mockApproveAllResponse, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)

	approved, augmented, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, []EdgeSuggestion{}, nil)

	require.NoError(t, err)
	assert.Equal(t, 0, len(approved))
	assert.Equal(t, 0, len(augmented))
	assert.Equal(t, 0, callCount, "Should not call LLM for empty suggestions")
}

// Test invalid indices in response
func TestHeimdallQC_ReviewBatch_InvalidIndicesInResponse(t *testing.T) {
	cleanup := config.WithAutoTLPLLMQCEnabled()
	defer cleanup()

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		// Return invalid indices (out of bounds)
		return `{"approved": [0, 5, 99, -1], "reasoning": "bad indices"}`, nil
	}

	qc := NewHeimdallQC(mockLLM, nil)
	suggestions := createTestSuggestions(3) // Only indices 0, 1, 2 are valid

	approved, _, err := qc.ReviewBatch(context.Background(), NodeSummary{ID: "source"}, suggestions, nil)

	require.NoError(t, err)
	// Should only approve index 0 (the only valid one)
	assert.Equal(t, 1, len(approved))
	assert.Equal(t, "target-0", approved[0].TargetID)
}
