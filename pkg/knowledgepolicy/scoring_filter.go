package knowledgepolicy

// NodeScoringInput carries the primitive fields from a storage.Node needed for
// scoring without importing pkg/storage (avoiding an import cycle).
type NodeScoringInput struct {
	EntityID       string
	Labels         []string
	CreatedAtNanos int64
	VersionAtNanos int64
}

// ShouldSuppressNode determines whether a node should be hidden from query
// results based on its decay score. Returns the suppress decision and the
// full scoring resolution for diagnostics.
func ShouldSuppressNode(
	scorer *Scorer,
	input NodeScoringInput,
	accessMeta *AccessMetaEntry,
	nowNanos int64,
) (bool, ScoringResolution) {
	if scorer == nil || !scorer.decayEnabled {
		return false, NeutralResolution
	}

	res := scorer.ScoreNode(
		input.EntityID, input.Labels, accessMeta,
		input.CreatedAtNanos, input.VersionAtNanos, nowNanos,
	)
	return res.SuppressionEligible, res
}

// ShouldSuppressEdge determines whether an edge should be hidden from query
// results based on its decay score.
func ShouldSuppressEdge(
	scorer *Scorer,
	edgeType string,
	entityID string,
	accessMeta *AccessMetaEntry,
	createdAtNanos, nowNanos int64,
) (bool, ScoringResolution) {
	if scorer == nil || !scorer.decayEnabled {
		return false, NeutralResolution
	}

	res := scorer.ScoreEdge(
		entityID, edgeType, accessMeta,
		createdAtNanos, createdAtNanos, nowNanos,
	)
	return res.SuppressionEligible, res
}
