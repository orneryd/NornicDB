package search

import (
	"context"
	"math"
	"sort"

	"github.com/orneryd/nornicdb/pkg/localization"
)

const (
	// rerankMethodOriginal means the candidates keep their original order.
	rerankMethodOriginal = "original"
	// rerankMethodApplied means at least one valid provider result ordered the
	// complete eligible candidate set.
	rerankMethodApplied = "rerank"
)

// rerankOutcome is the one service-owned rerank result consumed by node,
// graph, explicit rerank and their protocol projections.
//
// Completeness contract: when reranking is enabled, TopK defines both the
// provider input and the final output population. Results contains each
// eligible candidate from that bounded prefix at most once, up to the
// requested result limit.
// Provider-ranked candidates come first in provider order; a valid unique
// subset is completed by omitted candidates from the same prefix in their
// original order. Candidates beyond TopK are not part of this outcome.
// Candidates explicitly below a configured minimum score are ineligible.
// The request minimum governs rerank output, and for explicit candidates every
// outcome, through selectEligibleRerankResults. An outcome may therefore be
// empty; that is an authoritative eligibility decision, not a failed search to
// replace with retrieval fallback.
// Invalid or empty provider output returns the bounded prefix in original
// order and reports rerank_failed. Consumers preserve this list order;
// scores are data, not a second ordering contract.
type rerankOutcome struct {
	Results           []RerankResult
	Method            string
	FallbackTriggered bool
	FallbackReason    SearchFallbackReason
	excludedIDs       []string
	failure           error
}

type rerankRunOptions struct {
	providerLimit int
	resultLimit   int
	minScore      float64
	memo          *rerankMemo
	// minimumGovernsCandidates applies minScore to an original-order outcome
	// too. Explicit candidates carry caller scores on the minimum's scale;
	// search candidates carry fused scores, which a rerank minimum doesn't
	// govern, so a search fallback is returned unfiltered with its reason.
	minimumGovernsCandidates bool
}

// runRerank is the single owner of provider invocation, validation, filtering,
// completeness, ordering and truthful outcome reporting.
func (s *Service) runRerank(ctx context.Context, reranker Reranker, query string, candidates []RerankCandidate, options rerankRunOptions) (outcome rerankOutcome) {
	if reranker == nil || !reranker.Enabled() {
		return originalrerankOutcome(candidates, options, SearchFallbackNone)
	}
	if len(candidates) == 0 {
		return originalrerankOutcome(candidates, options, SearchFallbackNone)
	}

	providerLimit := options.providerLimit
	if providerLimit <= 0 || providerLimit > len(candidates) {
		providerLimit = len(candidates)
	}
	// TopK has always capped both provider work and caller-visible rerank
	// output. Bound the canonical population here so validation, subset
	// completion, filtering, fallback, memo rebinding and every projection all
	// share that contract. A provider subset can only be completed from the
	// candidates it was eligible to rank.
	candidates = candidates[:providerLimit]

	var raw []RerankResult
	var pending []RerankCandidate
	var pendingPositions []int
	rankable := 0
	if options.memo == nil {
		allRankable := true
		for index := 0; index < providerLimit; index++ {
			if candidates[index].Content == "" {
				allRankable = false
				break
			}
		}
		if allRankable {
			pending = candidates[:providerLimit]
			rankable = providerLimit
		} else {
			pending = make([]RerankCandidate, 0, providerLimit)
			pendingPositions = make([]int, 0, providerLimit)
			for index := 0; index < providerLimit; index++ {
				if candidates[index].Content == "" {
					continue
				}
				rankable++
				pending = append(pending, candidates[index])
				pendingPositions = append(pendingPositions, index)
			}
		}
	} else {
		raw = make([]RerankResult, 0, providerLimit)
		pending = make([]RerankCandidate, 0, providerLimit)
		pendingPositions = make([]int, 0, providerLimit)
		for index := 0; index < providerLimit; index++ {
			candidate := candidates[index]
			if entry, ok := options.memo.get(query, candidate.ID); ok {
				rankable++
				if entry.returned {
					result := entry.bind(candidate, index)
					result.NewRank = int(entry.providerOrder) + 1
					raw = append(raw, result)
				}
				continue
			}
			if candidate.Content == "" {
				continue
			}
			rankable++
			pending = append(pending, candidate)
			pendingPositions = append(pendingPositions, index)
		}
	}
	if rankable == 0 {
		return originalrerankOutcome(candidates, options, SearchFallbackRerankNoCandidates)
	}

	if len(pending) > 0 {
		providerResults, err := reranker.Rerank(ctx, query, pending)
		if err == nil {
			err = validateRerankResults(providerResults, pending)
		}
		if err != nil {
			s.logPrintf("⚠️ Reranking failed (%s): %v", reranker.Name(), err)
			return failedrerankOutcome(candidates, options, reranker.Name(), err)
		}

		if options.memo == nil {
			raw = providerResults
		}
		var returned []bool
		if options.memo != nil {
			returned = make([]bool, len(pending))
		}
		for resultIndex := range providerResults {
			result := providerResults[resultIndex]
			pendingIndex := rerankResultCandidateIndex(result, pending)
			fullIndex := pendingIndex
			if pendingPositions != nil {
				fullIndex = pendingPositions[pendingIndex]
			}
			result.OriginalRank = fullIndex + 1
			result.ID = candidates[fullIndex].ID
			result.Content = candidates[fullIndex].Content
			result.BiScore = candidates[fullIndex].Score
			if options.memo != nil {
				returned[pendingIndex] = true
				entry := options.memo.put(query, candidates[fullIndex], result, true)
				result.NewRank = int(entry.providerOrder) + 1
				raw = append(raw, result)
			} else {
				raw[resultIndex] = result
			}
		}
		if options.memo != nil {
			for index, candidate := range pending {
				if !returned[index] {
					_ = options.memo.put(query, candidate, RerankResult{}, false)
				}
			}
		}
	}

	// A memo combines provider results from successive continuation depths.
	// Its explicit provider ordinal preserves the canonical order without
	// deriving a new order from scores that may not be comparable across calls.
	if options.memo != nil {
		sort.SliceStable(raw, func(i, j int) bool { return raw[i].NewRank < raw[j].NewRank })
	}
	if err := validateRerankResults(raw, candidates); err != nil {
		s.logPrintf("⚠️ Reranking failed (%s): %v", reranker.Name(), err)
		return failedrerankOutcome(candidates, options, reranker.Name(), err)
	}

	return completererankOutcome(candidates, raw, rankable, options)
}

func completererankOutcome(candidates []RerankCandidate, raw []RerankResult, rankable int, options rerankRunOptions) rerankOutcome {
	const (
		candidateUnused uint8 = iota
		candidateUsed
		candidateIneligible
	)
	var localState [128]uint8
	state := localState[:]
	if len(candidates) > len(localState) {
		state = make([]uint8, len(candidates))
	} else {
		state = state[:len(candidates)]
	}
	// Complete in place. Provider output is owner scratch, so reusing it avoids
	// a second result-sized allocation before appending the untouched tail.
	results := raw[:0]
	excludedIDs := make([]string, 0)
	providerFiltered := false
	var scoreMin, scoreMax float64
	for _, result := range raw {
		index := rerankResultCandidateIndex(result, candidates)
		if result.Filtered {
			providerFiltered = true
			state[index] = candidateIneligible
			excludedIDs = append(excludedIDs, candidates[index].ID)
			continue
		}
		candidate := candidates[index]
		result.ID = candidate.ID
		result.Content = candidate.Content
		result.OriginalRank = index + 1
		result.BiScore = candidate.Score
		if len(results) == 0 {
			scoreMin, scoreMax = result.FinalScore, result.FinalScore
		} else {
			scoreMin = math.Min(scoreMin, result.FinalScore)
			scoreMax = math.Max(scoreMax, result.FinalScore)
		}
		results = append(results, result)
		state[index] = candidateUsed
	}
	// Explicit filtering is a meaningful provider decision even when every row
	// is removed or the surviving scores are equal. Flat-score fallback applies
	// only when the provider made no eligibility decision at all.
	if !providerFiltered && rerankScoresFlat(len(results), rankable, func() float64 { return scoreMax - scoreMin }) {
		return originalrerankOutcome(candidates, options, SearchFallbackRerankScoresFlat)
	}

	for index, candidate := range candidates {
		if state[index] != candidateUnused {
			continue
		}
		results = append(results, passThroughRerankResult(candidate, index))
	}
	// MinScore applies to the completed within-TopK population, not only to
	// rows the provider returned, so omitted pass-through candidates cannot
	// bypass the same eligibility contract.
	results, excludedIDs = selectEligibleRerankResults(results, excludedIDs, options)
	return rerankOutcome{Results: results, Method: rerankMethodApplied, excludedIDs: excludedIDs}
}

// originalrerankOutcome keeps the candidates' order and scores. Without rerank
// output only explicit candidates are subject to the request minimum.
func originalrerankOutcome(candidates []RerankCandidate, options rerankRunOptions, reason SearchFallbackReason) rerankOutcome {
	if !options.minimumGovernsCandidates {
		options.minScore = 0
	}
	count := len(candidates)
	if options.minScore <= 0 && options.resultLimit > 0 && count > options.resultLimit {
		// Without a minimum no row past the limit can be selected.
		count = options.resultLimit
	}
	results := make([]RerankResult, count)
	for index := range results {
		results[index] = passThroughRerankResult(candidates[index], index)
	}
	results, excludedIDs := selectEligibleRerankResults(results, nil, options)
	return rerankOutcome{
		Results:           results,
		Method:            rerankMethodOriginal,
		FallbackTriggered: reason != SearchFallbackNone,
		FallbackReason:    reason,
		excludedIDs:       excludedIDs,
	}
}

func failedrerankOutcome(candidates []RerankCandidate, options rerankRunOptions, reranker string, cause error) rerankOutcome {
	outcome := originalrerankOutcome(candidates, options, SearchFallbackRerankFailed)
	outcome.failure = localizedError(localization.SearchRerankFailed(reranker), cause)
	return outcome
}

// selectEligibleRerankResults is the one request-minimum and result-limit
// step of every rerank outcome. A positive minimum is an inclusive floor on
// each row's FinalScore: the provider's score for a ranked row, and the
// candidate's own score for a row kept in its original position (an omitted
// row of rerank output, or an explicit candidate on a fallback). Rows below it
// are excluded before the limit applies; ranks are then numbered in the
// returned order.
func selectEligibleRerankResults(results []RerankResult, excludedIDs []string, options rerankRunOptions) ([]RerankResult, []string) {
	if options.minScore > 0 {
		eligible := results[:0]
		for _, result := range results {
			if result.FinalScore < options.minScore {
				excludedIDs = append(excludedIDs, result.ID)
				continue
			}
			eligible = append(eligible, result)
		}
		results = eligible
	}
	results = limitRerankResults(results, options.resultLimit)
	for index := range results {
		results[index].NewRank = index + 1
	}
	return results, excludedIDs
}

func passThroughRerankResult(candidate RerankCandidate, index int) RerankResult {
	return RerankResult{
		ID: candidate.ID, Content: candidate.Content,
		OriginalRank: index + 1, NewRank: index + 1,
		BiScore: candidate.Score, CrossScore: candidate.Score, FinalScore: candidate.Score,
	}
}

func limitRerankResults(results []RerankResult, limit int) []RerankResult {
	if limit > 0 && len(results) > limit {
		return results[:limit]
	}
	return results
}

// validateRerankResults is the single acceptance boundary for every reranker.
// Providers may return a unique subset, but every returned item must identify
// exactly one input candidate and the whole batch must be valid or none applies.
func validateRerankResults(results []RerankResult, candidates []RerankCandidate) error {
	if len(results) == 0 {
		return invalidRerankResults()
	}

	var localSeen [128]bool
	seen := localSeen[:]
	if len(candidates) > len(localSeen) {
		seen = make([]bool, len(candidates))
	} else {
		seen = seen[:len(candidates)]
	}

	for _, result := range results {
		index := rerankResultCandidateIndex(result, candidates)
		if index < 0 || seen[index] {
			return invalidRerankResults()
		}
		if err := validateRerankScore(result.CrossScore); err != nil {
			return err
		}
		if err := validateRerankScore(result.FinalScore); err != nil {
			return err
		}
		seen[index] = true
	}
	return nil
}

func rerankResultCandidateIndex(result RerankResult, candidates []RerankCandidate) int {
	index := result.OriginalRank - 1
	if result.OriginalRank == 0 {
		// Legacy/custom rerankers may omit OriginalRank. Resolve their ID only
		// when it identifies exactly one candidate; built-in adapters always
		// preserve the provider's raw index in OriginalRank.
		index = uniqueRerankCandidateIndex(candidates, result.ID)
	}
	if index < 0 || index >= len(candidates) || candidates[index].ID != result.ID {
		return -1
	}
	return index
}

func uniqueRerankCandidateIndex(candidates []RerankCandidate, id string) int {
	index := -1
	for i := range candidates {
		if candidates[i].ID != id {
			continue
		}
		if index >= 0 {
			return -1
		}
		index = i
	}
	return index
}

func invalidRerankResults() error {
	return localizedError(localization.SearchRerankResponseUnrecognized(), nil)
}

func validateRerankScore(score float64) error {
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return invalidRerankResults()
	}
	return nil
}
