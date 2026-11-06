package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuantizedContinuationSelectionBoundsSingleMemberCandidates(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "scalar")
	population := newNOR406Population(t, 2000, 11)
	ctx, pipeline, release, err := population.service.vectorPipelineForQuery(t.Context())
	require.NoError(t, err)
	defer release()
	generator, ok := pipeline.candidateGen.(*HNSWCandidateGen)
	require.True(t, ok)
	query := []float32{1, 0}
	// Without service membership, a weak surfaced entry may have a hidden
	// strong sibling; the candidate generator must leave that proof to scoring.
	unbound, _, err := generator.searchCandidatesWithEntryEvidence(ctx, query, 4000, population.threshold, nil)
	require.NoError(t, err)
	require.Len(t, unbound, population.docs)
	indexed := withVectorMemberIndex(ctx, &population.service.vectorMembers)
	candidates, evidence, err := generator.searchCandidatesWithEntryEvidence(indexed, query, 4000, population.threshold, nil)
	require.NoError(t, err)
	require.True(t, evidence.exhausted, "selection must preserve whole-index coverage")
	require.Less(t, len(candidates), population.docs/2, "proven weak singleton candidates must not populate per-node selection")
	// A caller-owned member resolver can describe additional vectors; it must
	// disable the singleton shortcut even when service metadata is also present.
	custom := withVectorMembers(indexed, func([]string) map[string][]string { return nil })
	customCandidates, _, err := generator.searchCandidatesWithEntryEvidence(custom, query, 4000, population.threshold, nil)
	require.NoError(t, err)
	require.Len(t, customCandidates, population.docs)
	response, err := population.service.Search(context.Background(), "", query, &SearchOptions{Limit: population.docs, MinSimilarity: &population.threshold})
	require.NoError(t, err)
	require.Len(t, response.Results, population.eligible)
	for _, result := range response.Results {
		require.GreaterOrEqual(t, result.Similarity, population.threshold)
	}
	// A real node gaining a strong second chunk must keep its weak main
	// entry through the same generator boundary, not just through grouping.
	id := storage.NodeID("doc-001999")
	node, err := population.service.engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	updated := *node
	updated.ChunkEmbeddings = append(append([][]float32(nil), node.ChunkEmbeddings...), []float32{1, 0})
	require.NoError(t, population.service.engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{Before: node, After: &updated}))
	require.NoError(t, population.service.IndexNode(&updated))
	expandedCandidates, _, err := generator.searchCandidatesWithEntryEvidence(indexed, query, 4000, population.threshold, nil)
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(expandedCandidates, func(candidate Candidate) bool { return candidate.ID == nodeMainVectorKey(string(id)) }), "weak main row of a multi-vector node must be retained")
	expandedResponse, err := population.service.Search(t.Context(), "", query, &SearchOptions{Limit: population.docs, MinSimilarity: &population.threshold})
	require.NoError(t, err)
	require.Len(t, expandedResponse.Results, population.eligible+1)
	require.True(t, slices.ContainsFunc(expandedResponse.Results, func(result SearchResult) bool { return result.NodeID == id && result.Similarity > 0.9999 }))

}
