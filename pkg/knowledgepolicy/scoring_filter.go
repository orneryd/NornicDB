package knowledgepolicy

// NodeScoringInput carries the primitive fields from a storage.Node needed for
// scoring without importing pkg/storage (avoiding an import cycle).
type NodeScoringInput struct {
	EntityID           string
	Labels             []string
	CreatedAtNanos     int64
	VersionAtNanos     int64
	LegacyAccessCount  int64
	LegacyLastAccessed int64
}

// ShouldSuppressNode determines whether a node should be hidden from query
// results based on its decay score. Returns the suppress decision and the
// full scoring resolution for diagnostics.
//
// When accessMeta is nil and legacy fields are populated, a synthetic
// AccessMetaEntry is constructed from the legacy fields (Phase 4.1.1 fallback).
func ShouldSuppressNode(
	scorer *Scorer,
	input NodeScoringInput,
	accessMeta *AccessMetaEntry,
	nowNanos int64,
) (bool, ScoringResolution) {
	if scorer == nil || !scorer.decayEnabled {
		return false, NeutralResolution
	}

	if accessMeta == nil {
		accessMeta = SynthesizeLegacyAccessMeta(
			input.EntityID, input.LegacyAccessCount, input.LegacyLastAccessed,
		)
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

// SynthesizeLegacyAccessMeta constructs an AccessMetaEntry from legacy Node
// fields (AccessCount, LastAccessed) for pre-migration compatibility. When both
// fields are zero, returns nil (genuinely new node with no history).
// This function is removed in Phase 7 after migration completes.
func SynthesizeLegacyAccessMeta(entityID string, accessCount int64, lastAccessedNanos int64) *AccessMetaEntry {
	if accessCount == 0 && lastAccessedNanos == 0 {
		return nil
	}
	return &AccessMetaEntry{
		TargetID:    entityID,
		TargetScope: ScopeNode,
		Fixed: AccessMetaFixedFields{
			AccessCount:    accessCount,
			LastAccessedAt: lastAccessedNanos,
		},
	}
}
