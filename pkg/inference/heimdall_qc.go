// Package inference - Heimdall SLM Hybrid Review for Auto-TLP.
//
// This module provides LLM-based batch review for automatically inferred edges.
// It operates in hybrid mode:
//  1. TLP algorithms generate fast candidates
//  2. Heimdall reviews batch + can optionally suggest additional edges
//
// Designed for small instruction-tuned models:
//   - Simple, structured prompts
//   - Size limits to avoid context overflow
//   - Batch processing for efficiency
//   - Graceful degradation when nodes are too large
//
// KV Cache Optimization:
//
//	The system prompt is static and cached by the SLM's KV cache.
//	Only the dynamic content (nodes, suggestions) varies per call.
//	Use GetSystemPrompt() to configure your SLM's system message.
//
// Feature Flags:
//   - NORNICDB_AUTO_TLP_LLM_QC_ENABLED: Enable batch review of TLP suggestions
//   - NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED: Allow Heimdall to add new suggestions
//
// Usage:
//
//	qc := inference.NewHeimdallQC(heimdallFunc, nil)
//	engine.SetHeimdallQC(qc)
package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ============================================================================
// UNIFIED SLM INTERFACE - Shared by Bifrost commands AND Heimdall QC
// ============================================================================
//
// ARCHITECTURE:
//   Both Bifrost commands and Heimdall QC use the SAME SLM instance.
//   Each call is STATELESS - no context accumulation between calls.
//
// KV CACHE STRUCTURE:
//   [Static System Prefix] → Cached once, reused for all calls
//   [Dynamic User Content] → Varies per call (command or QC data)
//
// The SLM sees: SYSTEM_PROMPT + USER_CONTENT → OUTPUT (one-shot)
//
// ============================================================================

// HeimdallSystemPrompt is the static system prompt for Heimdall QC.
// This is cached in KV alongside Bifrost's command definitions.
// Ultra-concise for one-shot completion - no multi-turn conversation.
const HeimdallSystemPrompt = `Review graph edges. Output JSON only.
Format: {"approved":[indices],"rejected":[indices],"reasoning":"why"}
Approve if nodes are meaningfully related. Reject spam/duplicates.`

// HeimdallAugmentSystemPrompt includes augmentation capability.
const HeimdallAugmentSystemPrompt = `Review graph edges. Output JSON only.
Format: {"approved":[indices],"rejected":[indices],"reasoning":"why"}
Approve if nodes are meaningfully related. Reject spam/duplicates.
May add: "additional":[{"target_id":"id","type":"TYPE","conf":0.8,"reason":"why"}]`

// HeimdallFunc is the function signature for calling the SLM.
//
// IMPORTANT: Each call is STATELESS. No context accumulates.
// Uses the SAME heimdall.Generator as Bifrost commands (in-memory llama.cpp).
// System prompt is cached in KV, only user content varies per call.
//
// Example (using shared heimdall.Generator):
//
//	heimdallFunc := func(ctx context.Context, userContent string) (string, error) {
//	    prompt := inference.GetSystemPrompt(augmentEnabled) + "\n\n" + userContent
//	    return generator.Generate(ctx, prompt, heimdall.GenerateParams{
//	        MaxTokens: 256, Temperature: 0.1,
//	    })
//	}
type HeimdallFunc func(ctx context.Context, prompt string) (string, error)

// ============================================================================

// HeimdallQCConfig configures the Heimdall hybrid review system.
type HeimdallQCConfig struct {
	// Logger receives structured QC events.
	Logger *slog.Logger
	// Localizer renders QC operator prose. Nil preserves source English.
	Localizer *localization.Manager

	// Enabled controls whether QC is active (also requires feature flag)
	Enabled bool

	// Timeout for SLM response (default: 10s for batch)
	Timeout time.Duration

	// MaxContextBytes is the maximum prompt size in bytes
	// If a batch exceeds this, nodes are summarized or skipped
	// Default: 4096 (safe for most small models)
	MaxContextBytes int

	// MaxBatchSize limits how many suggestions per SLM call
	// Default: 5 (balance between efficiency and model capacity)
	MaxBatchSize int

	// MaxNodeSummaryLen truncates node properties to this length
	// Default: 200 characters per property
	MaxNodeSummaryLen int

	// MinConfidenceToReview skips TLP suggestions below this threshold
	// Default: 0.5 (don't waste SLM time on weak candidates)
	MinConfidenceToReview float64

	// CacheDecisions caches Heimdall decisions
	// Default: true
	CacheDecisions bool

	// CacheTTL is how long to cache decisions
	// Default: 1 hour
	CacheTTL time.Duration
}

// DefaultHeimdallQCConfig returns sensible defaults for small models.
func DefaultHeimdallQCConfig() *HeimdallQCConfig {
	return &HeimdallQCConfig{
		Enabled:               true,
		Timeout:               10 * time.Second,
		MaxContextBytes:       4096, // ~1000 tokens for small models
		MaxBatchSize:          5,
		MaxNodeSummaryLen:     200,
		MinConfidenceToReview: 0.5,
		CacheDecisions:        true,
		CacheTTL:              time.Hour,
	}
}

// HeimdallBatchRequest contains a batch of suggestions for review.
type HeimdallBatchRequest struct {
	// SourceNode is the newly created/accessed node
	SourceNode NodeSummary `json:"source"`

	// Candidates are TLP suggestions to review
	Candidates []CandidateSummary `json:"candidates"`

	// AllowAugment indicates if Heimdall can suggest additional edges
	AllowAugment bool `json:"allow_augment"`

	// CandidatePool are other nearby nodes Heimdall can consider (for augment)
	// Only populated if AllowAugment is true
	CandidatePool []NodeSummary `json:"candidate_pool,omitempty"`
}

// NodeSummary is a compact representation of a node for the prompt.
type NodeSummary struct {
	ID     string            `json:"id"`
	Labels []string          `json:"labels"`
	Props  map[string]string `json:"props"` // Summarized string props only
}

// CandidateSummary represents a TLP suggestion for review.
type CandidateSummary struct {
	TargetID   string            `json:"target_id"`
	Labels     []string          `json:"labels"`
	Props      map[string]string `json:"props"`
	Type       string            `json:"type"`
	Confidence float64           `json:"conf"`
	Method     string            `json:"method"`
}

// HeimdallBatchResponse is the SLM's batch decision.
type HeimdallBatchResponse struct {
	// Approved are indices of candidates to keep (0-based)
	Approved []int `json:"approved"`

	// Rejected are indices of candidates to skip (optional, for logging)
	Rejected []int `json:"rejected,omitempty"`

	// TypeOverrides maps candidate index to suggested type override
	TypeOverrides map[int]string `json:"type_overrides,omitempty"`

	// Additional are NEW edges Heimdall suggests (only if augment enabled)
	Additional []AugmentedEdge `json:"additional,omitempty"`

	// Reasoning is brief overall explanation
	Reasoning string `json:"reasoning,omitempty"`
}

// AugmentedEdge is a new edge suggested by Heimdall.
type AugmentedEdge struct {
	TargetID   string  `json:"target_id"`
	Type       string  `json:"type"`
	Confidence float64 `json:"conf"`
	Reason     string  `json:"reason"`
}

// HeimdallQC manages LLM-based hybrid review for Auto-TLP.
type HeimdallQC struct {
	config   *HeimdallQCConfig
	heimdall HeimdallFunc
	cache    map[string]cachedBatchDecision
	cacheMu  sync.RWMutex
	stats    HeimdallQCStats
}

type cachedBatchDecision struct {
	response  HeimdallBatchResponse
	expiresAt time.Time
}

// HeimdallQCStats tracks QC operations.
type HeimdallQCStats struct {
	mu               sync.Mutex
	BatchesProcessed int64
	SuggestionsIn    int64
	SuggestionsOut   int64
	Augmented        int64
	Skipped          int64 // Too large or below threshold
	Errors           int64
	CacheHits        int64
	AvgLatencyMs     float64
	totalLatencyMs   int64
}

// NewHeimdallQC creates a new Heimdall QC manager.
func NewHeimdallQC(heimdallFunc HeimdallFunc, cfg *HeimdallQCConfig) *HeimdallQC {
	if cfg == nil {
		cfg = DefaultHeimdallQCConfig()
	}

	return &HeimdallQC{
		config:   cfg,
		heimdall: heimdallFunc,
		cache:    make(map[string]cachedBatchDecision),
	}
}

// ReviewBatch reviews a batch of TLP suggestions with Heimdall.
// Returns approved suggestions (possibly with type overrides) + any augmented edges.
func (h *HeimdallQC) ReviewBatch(
	ctx context.Context,
	sourceNode NodeSummary,
	suggestions []EdgeSuggestion,
	candidatePool []NodeSummary, // For augmentation
) (approved []EdgeSuggestion, augmented []EdgeSuggestion, err error) {

	// Check feature flags
	if !config.IsAutoTLPLLMQCEnabled() || !h.config.Enabled {
		// QC disabled - return all suggestions as-is
		return suggestions, nil, nil
	}

	// Filter by minimum confidence
	var toReview []EdgeSuggestion
	var autoApproved []EdgeSuggestion
	for _, sug := range suggestions {
		if sug.Confidence >= h.config.MinConfidenceToReview {
			toReview = append(toReview, sug)
		} else {
			h.stats.mu.Lock()
			h.stats.Skipped++
			h.stats.mu.Unlock()
		}
	}

	if len(toReview) == 0 {
		return autoApproved, nil, nil
	}

	// Process in batches
	for i := 0; i < len(toReview); i += h.config.MaxBatchSize {
		end := i + h.config.MaxBatchSize
		if end > len(toReview) {
			end = len(toReview)
		}
		batch := toReview[i:end]

		batchApproved, batchAugmented, err := h.processBatch(ctx, sourceNode, batch, candidatePool)
		if err != nil {
			// On error, approve all in batch (fail-open)
			h.logEvent(ctx, slog.LevelWarn, localization.InferenceHeimdallBatchErrorEvent(err))
			approved = append(approved, batch...)
			continue
		}

		approved = append(approved, batchApproved...)
		augmented = append(augmented, batchAugmented...)
	}

	return approved, augmented, nil
}

// processBatch handles a single batch of suggestions.
func (h *HeimdallQC) processBatch(
	ctx context.Context,
	sourceNode NodeSummary,
	suggestions []EdgeSuggestion,
	candidatePool []NodeSummary,
) (approved []EdgeSuggestion, augmented []EdgeSuggestion, err error) {

	allowAugment := config.IsAutoTLPLLMAugmentEnabled()

	// Build candidates summary
	candidates := make([]CandidateSummary, 0, len(suggestions))
	for _, sug := range suggestions {
		candidates = append(candidates, CandidateSummary{
			TargetID:   sug.TargetID,
			Type:       sug.Type,
			Confidence: sug.Confidence,
			Method:     sug.Method,
			// Note: Labels and Props would be populated by caller if available
		})
	}

	// Build request
	req := HeimdallBatchRequest{
		SourceNode:   sourceNode,
		Candidates:   candidates,
		AllowAugment: allowAugment,
	}

	// Only include candidate pool if augment is enabled and pool is small enough
	if allowAugment && len(candidatePool) <= 10 {
		req.CandidatePool = candidatePool
	}

	// Build prompt and check size
	prompt := h.buildBatchPrompt(&req)
	if len(prompt) > h.config.MaxContextBytes {
		h.logPrintf(ctx, slog.LevelWarn, "[HEIMDALL] ⚠️ Prompt too large (%d > %d bytes), approving batch without review",
			len(prompt), h.config.MaxContextBytes)
		h.stats.mu.Lock()
		h.stats.Skipped += int64(len(suggestions))
		h.stats.mu.Unlock()
		return suggestions, nil, nil
	}

	// Check cache
	cacheKey := h.batchCacheKey(&req)
	if h.config.CacheDecisions {
		h.cacheMu.RLock()
		if cached, ok := h.cache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
			h.cacheMu.RUnlock()
			h.stats.mu.Lock()
			h.stats.CacheHits++
			h.stats.mu.Unlock()
			return h.applyBatchResponse(suggestions, candidatePool, &cached.response)
		}
		h.cacheMu.RUnlock()
	}

	// Call Heimdall
	callCtx, cancel := context.WithTimeout(ctx, h.config.Timeout)
	defer cancel()

	startTime := time.Now()
	rawResponse, err := h.heimdall(callCtx, prompt)
	latencyMs := time.Since(startTime).Milliseconds()

	// Update stats
	h.stats.mu.Lock()
	h.stats.BatchesProcessed++
	h.stats.SuggestionsIn += int64(len(suggestions))
	h.stats.totalLatencyMs += latencyMs
	h.stats.AvgLatencyMs = float64(h.stats.totalLatencyMs) / float64(h.stats.BatchesProcessed)
	h.stats.mu.Unlock()

	if err != nil {
		h.stats.mu.Lock()
		h.stats.Errors++
		h.stats.mu.Unlock()
		return nil, nil, fmt.Errorf("heimdall call failed: %w", err)
	}

	// Parse response
	response, err := h.parseBatchResponse(rawResponse, len(suggestions))
	if err != nil {
		h.logPrintf(ctx, slog.LevelWarn, "[HEIMDALL] ⚠️ Parse error: %v", err)
		// Fail-open: approve all
		return suggestions, nil, nil
	}

	// Cache the decision
	if h.config.CacheDecisions {
		h.cacheMu.Lock()
		h.cache[cacheKey] = cachedBatchDecision{
			response:  *response,
			expiresAt: time.Now().Add(h.config.CacheTTL),
		}
		h.cacheMu.Unlock()
	}

	h.logPrintf(ctx, slog.LevelInfo, "[HEIMDALL] ✅ Batch reviewed | in=%d approved=%d augmented=%d latency=%dms",
		len(suggestions), len(response.Approved), len(response.Additional), latencyMs)

	return h.applyBatchResponse(suggestions, candidatePool, response)
}

func (h *HeimdallQC) logEvent(ctx context.Context, level slog.Level, event localization.LogEvent) {
	logInferenceEvent(ctx, h.config.Logger, h.config.Localizer, level, event)
}

func (h *HeimdallQC) logPrintf(ctx context.Context, level slog.Level, format string, args ...any) {
	h.logEvent(ctx, level, localization.InferenceOperatorEvent(format, args...))
}

// GetSystemPrompt returns the appropriate static system prompt.
// Configure your SLM to cache this in KV cache - it never changes.
// Use augment=true when NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED is set.
func GetSystemPrompt(augment bool) string {
	if augment {
		return HeimdallAugmentSystemPrompt
	}
	return HeimdallSystemPrompt
}

// buildBatchPrompt creates the dynamic user content for one-shot completion.
// The system prompt (static, cached) tells the model WHAT to do.
// This function generates the DATA to process.
func (h *HeimdallQC) buildBatchPrompt(req *HeimdallBatchRequest) string {
	var sb strings.Builder

	// Source node (compact, data only)
	sb.WriteString(fmt.Sprintf("SRC:%s%v\n", req.SourceNode.ID, req.SourceNode.Labels))
	for k, v := range req.SourceNode.Props {
		sb.WriteString(fmt.Sprintf(" %s:%s\n", k, truncateStr(v, h.config.MaxNodeSummaryLen)))
	}

	// Candidates (numbered for index reference)
	sb.WriteString("EDGES:\n")
	for i, c := range req.Candidates {
		sb.WriteString(fmt.Sprintf("%d.%s→%s(%.0f%%)\n", i, c.TargetID, c.Type, c.Confidence*100))
	}

	// Candidate pool for augmentation (if enabled)
	if req.AllowAugment && len(req.CandidatePool) > 0 {
		sb.WriteString("POOL:")
		for _, n := range req.CandidatePool {
			sb.WriteString(fmt.Sprintf("%s,", n.ID))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// parseBatchResponse parses the SLM response.
func (h *HeimdallQC) parseBatchResponse(raw string, numCandidates int) (*HeimdallBatchResponse, error) {
	raw = strings.TrimSpace(raw)

	// Find JSON
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start == -1 || end == -1 || end <= start {
		// Try to extract approval from text
		return h.fuzzyParseBatchResponse(raw, numCandidates), nil
	}

	jsonStr := raw[start : end+1]

	var response HeimdallBatchResponse
	if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
		return h.fuzzyParseBatchResponse(raw, numCandidates), nil
	}

	return &response, nil
}

// fuzzyParseBatchResponse extracts decisions from non-JSON response.
func (h *HeimdallQC) fuzzyParseBatchResponse(raw string, numCandidates int) *HeimdallBatchResponse {
	lower := strings.ToLower(raw)

	// If "all" or "approve" appears, approve everything
	if strings.Contains(lower, "approve all") || strings.Contains(lower, "all approved") {
		approved := make([]int, numCandidates)
		for i := range approved {
			approved[i] = i
		}
		return &HeimdallBatchResponse{
			Approved:  approved,
			Reasoning: "Fuzzy parse: appears to approve all",
		}
	}

	// If "reject" or "none" appears, reject everything
	if strings.Contains(lower, "reject all") || strings.Contains(lower, "none") {
		return &HeimdallBatchResponse{
			Approved:  []int{},
			Reasoning: "Fuzzy parse: appears to reject all",
		}
	}

	// Default: approve all (fail-open)
	approved := make([]int, numCandidates)
	for i := range approved {
		approved[i] = i
	}
	return &HeimdallBatchResponse{
		Approved:  approved,
		Reasoning: "Fuzzy parse: defaulting to approve",
	}
}

// applyBatchResponse converts response to approved suggestions.
func (h *HeimdallQC) applyBatchResponse(
	suggestions []EdgeSuggestion,
	candidatePool []NodeSummary,
	response *HeimdallBatchResponse,
) (approved []EdgeSuggestion, augmented []EdgeSuggestion, err error) {

	// Build approved list
	approvedSet := make(map[int]bool)
	for _, idx := range response.Approved {
		if idx >= 0 && idx < len(suggestions) {
			approvedSet[idx] = true
		}
	}

	for i, sug := range suggestions {
		if approvedSet[i] {
			// Apply type override if present
			if newType, ok := response.TypeOverrides[i]; ok && newType != "" {
				sug.Type = newType
			}
			sug.Reason = fmt.Sprintf("%s (Heimdall approved)", sug.Reason)
			approved = append(approved, sug)
		}
	}

	// Process augmented edges
	for _, aug := range response.Additional {
		augmented = append(augmented, EdgeSuggestion{
			SourceID:   "", // Will be set by caller
			TargetID:   aug.TargetID,
			Type:       aug.Type,
			Confidence: aug.Confidence,
			Reason:     fmt.Sprintf("Heimdall augmented: %s", aug.Reason),
			Method:     "heimdall_augment",
		})
	}

	// Update stats
	h.stats.mu.Lock()
	h.stats.SuggestionsOut += int64(len(approved))
	h.stats.Augmented += int64(len(augmented))
	h.stats.mu.Unlock()

	return approved, augmented, nil
}

// batchCacheKey generates cache key for a batch request.
func (h *HeimdallQC) batchCacheKey(req *HeimdallBatchRequest) string {
	var ids []string
	ids = append(ids, req.SourceNode.ID)
	for _, c := range req.Candidates {
		ids = append(ids, c.TargetID)
	}
	return strings.Join(ids, ":")
}

// GetStats returns current QC statistics.
func (h *HeimdallQC) GetStats() HeimdallQCStats {
	h.stats.mu.Lock()
	defer h.stats.mu.Unlock()
	return HeimdallQCStats{
		BatchesProcessed: h.stats.BatchesProcessed,
		SuggestionsIn:    h.stats.SuggestionsIn,
		SuggestionsOut:   h.stats.SuggestionsOut,
		Augmented:        h.stats.Augmented,
		Skipped:          h.stats.Skipped,
		Errors:           h.stats.Errors,
		CacheHits:        h.stats.CacheHits,
		AvgLatencyMs:     h.stats.AvgLatencyMs,
	}
}

// ClearCache clears the decision cache.
func (h *HeimdallQC) ClearCache() {
	h.cacheMu.Lock()
	defer h.cacheMu.Unlock()
	h.cache = make(map[string]cachedBatchDecision)
}

// SummarizeNode creates a compact summary of a node for prompts.
// Truncates large properties and filters to string values only.
func SummarizeNode(id string, labels []string, props map[string]interface{}, maxPropLen int) NodeSummary {
	summary := NodeSummary{
		ID:     id,
		Labels: labels,
		Props:  make(map[string]string),
	}

	for k, v := range props {
		// Only include string-ish properties
		var strVal string
		switch val := v.(type) {
		case string:
			strVal = val
		case []byte:
			strVal = string(val)
		default:
			// Skip non-string properties (embeddings, etc.)
			continue
		}

		// Skip very long properties or embeddings
		if len(strVal) > maxPropLen*2 {
			continue
		}

		summary.Props[k] = truncateStr(strVal, maxPropLen)
	}

	return summary
}

// EstimatePromptSize estimates the byte size of a batch prompt.
func EstimatePromptSize(sourceNode NodeSummary, candidates []CandidateSummary) int {
	// Rough estimation: JSON overhead + content
	size := 200 // Base prompt text
	size += len(sourceNode.ID) + 50
	for k, v := range sourceNode.Props {
		size += len(k) + len(v) + 10
	}
	for _, c := range candidates {
		size += len(c.TargetID) + len(c.Type) + len(c.Method) + 50
		for k, v := range c.Props {
			size += len(k) + len(v) + 10
		}
	}
	return size
}

// truncateStr shortens a string to max length with ellipsis.
func truncateStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
