package storage

// IsDecayEnabled reports whether this namespace's reads apply decay filtering.
func (n *NamespacedEngine) IsDecayEnabled() bool {
	policy, ok := ViewMetadataForEngine[interface{ IsDecayEnabled() bool }](n.GetInnerEngine())
	return ok && policy.IsDecayEnabled()
}

// HasManualSuppressions reports whether this namespace's storage may hide
// manually suppressed entities. A positive answer disables raw count shortcuts.
func (n *NamespacedEngine) HasManualSuppressions() bool {
	policy, ok := ViewMetadataForEngine[interface{ HasManualSuppressions() bool }](n.GetInnerEngine())
	return ok && policy.HasManualSuppressions()
}
