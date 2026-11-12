package search

import (
	"context"

	"github.com/orneryd/nornicdb/internal/queryprofile"
)

// Candidate-query rows count the prefix returned by a retrieval pass before
// metadata/decay postfilters. Hits count issued passes, including retries and
// completion. They are not ANN visits, vector comparisons or hydrated nodes.
const (
	profileKindNodeFulltextCandidates         = "NodeFulltextCandidates"
	profileKindNodeVectorCandidates           = "NodeVectorCandidates"
	profileKindRelationshipFulltextCandidates = "RelationshipFulltextCandidates"
	profileKindRelationshipVectorCandidates   = "RelationshipVectorCandidates"
)

func recordIndexCandidates(counter *queryprofile.ReadCounter, returned int, err error) {
	if counter == nil {
		return
	}
	if err != nil {
		returned = 0
	}
	counter.Add(int64(returned), 1)
}

// Only exact owned concrete implementations are certified. A custom type that
// embeds one, reuses its name or reports exhaustion remains an opaque callback.
// Ordinary requests return before classification and allocate no profile state.
func observeVectorProvider(ctx context.Context, provider any, category string) {
	if queryprofile.FromContext(ctx) == nil {
		return
	}
	switch provider.(type) {
	case *BruteForceCandidateGen, *FileStoreBruteForceCandidateGen, *GPUBruteForceCandidateGen,
		*HNSWCandidateGen, *KMeansCandidateGen, *GPUKMeansCandidateGen, *IVFHNSWCandidateGen, *IVFPQCandidateGen,
		*CPUExactScorer, *GPUExactScorer, *IdentityExactScorer:
		return
	}
	queryprofile.Callback(ctx, category, "")
}
