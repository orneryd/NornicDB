package search

import (
	"context"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// stage2RerankCandidates builds the provider input of one bounded rerank
// window: results[start:end], in fused order. Candidate text is read only for
// a candidate the continuation memo hasn't ranked yet; an already-ranked
// candidate is rebuilt from its memo entry, and an unreadable or missing one
// is completed by the canonical outcome in fused order. It returns how many
// candidates carry new text for the provider and how many were rebuilt from
// the continuation memo instead.
func (s *Service) stage2RerankCandidates(ctx context.Context, query string, results []rrfResult, start, end int, opts *SearchOptions, seenOrphans map[string]bool) ([]RerankCandidate, int, int) {
	memo := rerankMemoFromContext(ctx)
	maxDocumentChars := effectiveRerankMaxChars(opts)
	candidates := make([]RerankCandidate, 0, end-start)
	newCandidates := 0
	memoized := 0
	for index := start; index < end; index++ {
		result := results[index]
		candidate := RerankCandidate{ID: result.ID, Score: result.RRFScore}
		if _, ranked := memo.get(query, result.ID); ranked {
			memoized++
			candidates = append(candidates, candidate)
			continue
		}
		node, err := s.readNodeImage(ctx, storage.NodeID(result.ID), metadataNodeImage)
		if err != nil {
			_ = s.handleOrphanedEmbedding(ctx, result.ID, err, seenOrphans)
			candidates = append(candidates, candidate)
			continue
		}
		if node == nil {
			candidates = append(candidates, candidate)
			continue
		}
		candidate.Content = s.rerankCandidateContent(ctx, node, result, query, maxDocumentChars)
		if candidate.Content != "" {
			newCandidates++
		}
		candidates = append(candidates, candidate)
	}
	return candidates, newCandidates, memoized
}

// stage2RerankResults maps a canonical rerank outcome back onto the fused list
// it was built from. The owner validated and rebound each OriginalRank to that
// exact list, so this needs neither an identity map nor a second lookup.
func stage2RerankResults(rerankInput []rrfResult, outcome rerankOutcome) []rrfResult {
	results := make([]rrfResult, 0, len(outcome.Results))
	for _, result := range outcome.Results {
		original := rerankInput[result.OriginalRank-1]
		results = append(results, rrfResult{
			ID:            result.ID,
			MatchID:       original.MatchID,
			RRFScore:      result.FinalScore,
			VectorRank:    original.VectorRank,
			BM25Rank:      original.BM25Rank,
			OriginalScore: original.OriginalScore,
		})
	}
	return results
}

// continuationRankedRow is one row of a continuation window's canonical
// outcome, paired with the fused row it was built from.
type continuationRankedRow struct {
	result RerankResult
	row    SearchResult
}

// applyContinuationRerank reranks one ranked continuation step's fused rows.
//
// The continuation producer calls this after the step's retrieval, so the
// candidate list is the step's fused order - the chunked owner's outer RRF of
// every chunk's list, or a one-chunk query's own fused list - and the memo key
// is the original query, never a chunk's text. TopK bounds one provider window,
// not the step: the memo records the batch its candidates were first admitted
// in, and each accepted batch's canonical runRerank outcome (validation, subset
// completion, filtering, fallback and retained decisions) is appended in
// admission order, so the page covers every row it retrieved. The provider's
// order holds inside a window.
//
// Accepted windows are stable across pulls: a row the memo already decided -
// ranked, filtered, omitted by a valid subset or in a failed batch - is rebuilt
// from that decision and never resubmitted, and a deeper pull admits the newly
// retrieved rows as new windows instead of re-forming an earlier one over a
// longer prefix. Its order and scores therefore cannot change, and a wholly
// omitted window completes in fused order rather than reading its empty
// retained result set as a failed provider response.
//
// Caller ceilings keep owning the stream: coverage is the depth this step
// actually retrieved (never the whole candidate population), a window reports
// its complete eligible population (resultLimit 0) and the page applies Limit.
// maxResults, rankedLimit and MaxCandidateLimit still decide admission and
// completion in continuationExecution.
func (s *Service) applyContinuationRerank(ctx context.Context, query string, response *SearchResponse, opts *SearchOptions, reranker Reranker) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if response == nil || !response.fusedOrder || len(response.Results) == 0 {
		// Only a fused selection is this stage's candidate list; a retrieval
		// fallback keeps the order it was selected in.
		return nil
	}
	memo := rerankMemoFromContext(ctx)
	windowSize := effectiveRerankTopK(opts)
	coverage := len(response.Results)
	if opts.Limit > 0 && coverage > opts.Limit {
		// A step admits at most Limit rows; deeper fused rows belong to a later
		// pull's windows, not to this step's provider work.
		coverage = opts.Limit
	}
	maxChars := effectiveRerankMaxChars(opts)
	seenOrphans := make(map[string]bool)
	positions := make(map[string]int, coverage)
	for index := 0; index < coverage; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := searchResultID(response.Results[index])
		if _, known := positions[id]; !known {
			positions[id] = index
		}
	}
	ranked := make([]continuationRankedRow, 0, coverage)
	excluded := make(map[string]struct{})
	applied := false
	reason := SearchFallbackNone
	newCandidates := 0
	windows := 0
	started := time.Now()
	collect := func(outcome rerankOutcome, members []int) {
		if outcome.Method == rerankMethodApplied {
			applied = true
		}
		if reason == SearchFallbackNone && outcome.FallbackReason != SearchFallbackNone {
			reason = outcome.FallbackReason
		}
		for _, id := range outcome.excludedIDs {
			excluded[id] = struct{}{}
		}
		for _, result := range outcome.Results {
			// OriginalRank is the row's place in this window's provider input.
			ranked = append(ranked, continuationRankedRow{result: result, row: response.Results[members[result.OriginalRank-1]]})
		}
	}

	// Rebuild every accepted window from the decisions the memo retained for it.
	admitted := make(map[string]struct{}, coverage)
	for _, ids := range memo.acceptedWindows(query) {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidates := make([]RerankCandidate, 0, len(ids))
		members := make([]int, 0, len(ids))
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			index, covered := positions[id]
			if !covered {
				// The row is no longer part of this step's coverage; its
				// retained decision stays recorded.
				continue
			}
			admitted[id] = struct{}{}
			candidates = append(candidates, RerankCandidate{ID: id, Score: response.Results[index].Score})
			members = append(members, index)
		}
		if len(candidates) == 0 {
			continue
		}
		windows++
		collect(s.runRerank(ctx, reranker, query, candidates, rerankRunOptions{
			providerLimit:      len(candidates),
			minScore:           opts.RerankMinScore,
			memo:               memo,
			continuationWindow: true,
			memoizedCandidates: len(candidates),
		}), members)
	}

	// Admit the newly retrieved rows, in fused order, as new windows.
	fresh := make([]int, 0, coverage)
	for index := 0; index < coverage; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, accepted := admitted[searchResultID(response.Results[index])]; accepted {
			continue
		}
		fresh = append(fresh, index)
	}
	for start := 0; start < len(fresh); start += windowSize {
		end := min(start+windowSize, len(fresh))
		ids := make([]string, 0, end-start)
		candidates := make([]RerankCandidate, 0, end-start)
		members := make([]int, 0, end-start)
		for _, index := range fresh[start:end] {
			if err := ctx.Err(); err != nil {
				return err
			}
			id := searchResultID(response.Results[index])
			candidate := RerankCandidate{ID: id, Score: response.Results[index].Score}
			if candidate.Content = s.continuationRerankContent(ctx, &response.Results[index], query, maxChars, seenOrphans); candidate.Content != "" {
				newCandidates++
			}
			ids = append(ids, id)
			candidates = append(candidates, candidate)
			members = append(members, index)
		}
		memo.acceptWindow(query, ids)
		windows++
		collect(s.runRerank(ctx, reranker, query, candidates, rerankRunOptions{
			providerLimit:      len(candidates),
			minScore:           opts.RerankMinScore,
			memo:               memo,
			continuationWindow: true,
		}), members)
	}

	rows := make([]SearchResult, len(ranked))
	for index, entry := range ranked {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := entry.row
		// The rerank outcome owns the delivered order and score; the row's
		// vector similarity and retrieval ranks stay its own.
		row.Score = entry.result.FinalScore
		row.RRFScore = entry.result.FinalScore
		rows[index] = row
		delete(excluded, searchResultID(row))
	}
	response.Results = rows
	response.Returned = len(rows)
	response.rerankApplied = applied
	response.rerankOrderOwned = opts.RerankEnabled
	if len(excluded) > 0 {
		// An excluded row that was not delivered stays ineligible for a later
		// phase of this stream.
		response.rerankExcluded = excluded
	}
	if applied && !strings.HasSuffix(response.SearchMethod, "+"+rerankMethodApplied) {
		response.SearchMethod += "+" + rerankMethodApplied
	}
	if reason != SearchFallbackNone && !isQueryEmbeddingFallback(response.FallbackReason) {
		// A query that wasn't wholly searched by meaning keeps that reason; a
		// ranking fallback explains the delivery this page actually made.
		response.FallbackTriggered = true
		response.FallbackReason = reason
	}
	if newCandidates > 0 && reranker != nil && reranker.Enabled() {
		s.logPrintf("🔄 Reranking %d new candidates in %d windows (query_len=%d reranker=%s)...",
			newCandidates, windows, len(query), reranker.Name())
	}
	if applied {
		s.logPrintf("✅ Reranking complete: %d results in %v (%s)", len(rows), time.Since(started), reranker.Name())
	}
	return ctx.Err()
}

// continuationRerankContent is the text a continuation window's reranker sees
// for one fused row. The row's node is the one its search already hydrated, so
// a delivered document isn't read and hydrated again just to build provider
// input; only a row without one (a projection that leaves properties out, or a
// caller provider that returns bare hits) is read from storage. A row whose
// node is gone or unreadable has no text, and the window keeps it in fused
// order.
func (s *Service) continuationRerankContent(ctx context.Context, result *SearchResult, query string, maxChars int, seenOrphans map[string]bool) string {
	id := result.NodeID
	if id == "" {
		id = storage.NodeID(searchResultID(*result))
	}
	node := result.sourceNode
	if node == nil || len(node.Properties) == 0 {
		read, err := s.readNodeImage(ctx, id, metadataNodeImage)
		if err != nil {
			_ = s.handleOrphanedEmbedding(ctx, string(id), err, seenOrphans)
		}
		if read == nil {
			return ""
		}
		node = read
	}
	return s.nodeRerankContent(ctx, result, node, id, query, maxChars)
}
