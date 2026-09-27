package storage

// NodeWriteChecked reports whether writing a node with these labels is
// checked by the schema: a constraint or property type on one of the labels,
// a constraint contract targeting one of them, or a relationship policy that
// names one of them as its source or target label (relabelling a node can
// break a policy on its relationships).
//
// The engine that stores the node checks all of these when it writes it. The
// AsyncEngine therefore writes a checked node through to that engine before
// acknowledging it, instead of caching it and checking only when it flushes;
// a write the check rejects is never acknowledged, so it can't fail the flush
// later (#700). A node no rule checks stays in the write cache.
func (sm *SchemaManager) NodeWriteChecked(labels []string) bool {
	if sm == nil || len(labels) == 0 {
		return false
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, label := range labels {
		for _, c := range sm.constraints {
			if c.Label == label || (c.Type == ConstraintPolicy && (c.SourceLabel == label || c.TargetLabel == label)) {
				return true
			}
		}
		for _, c := range sm.propertyTypeConstraints {
			if c.Label == label {
				return true
			}
		}
		for _, contract := range sm.constraintContracts {
			if contract.TargetEntityType == string(ConstraintEntityNode) && contract.TargetLabelOrType == label {
				return true
			}
		}
	}
	return false
}

// EdgeWriteChecked reports whether writing a relationship of this type is
// checked by the schema: a relationship constraint (UNIQUE, EXISTS, KEY,
// TEMPORAL, DOMAIN, CARDINALITY, POLICY), a property type or a constraint
// contract on the type. See NodeWriteChecked for how the AsyncEngine uses it.
func (sm *SchemaManager) EdgeWriteChecked(relType string) bool {
	if sm == nil || relType == "" {
		return false
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, c := range sm.constraints {
		if c.Label == relType {
			return true
		}
	}
	for _, c := range sm.propertyTypeConstraints {
		if c.Label == relType {
			return true
		}
	}
	for _, contract := range sm.constraintContracts {
		if contract.TargetEntityType == string(ConstraintEntityRelationship) && contract.TargetLabelOrType == relType {
			return true
		}
	}
	return false
}

// HasWriteRules reports whether the schema has any constraint, property type
// or constraint contract, i.e. whether any write can be checked at all.
func (sm *SchemaManager) HasWriteRules() bool {
	if sm == nil {
		return false
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.constraints) > 0 || len(sm.propertyTypeConstraints) > 0 || len(sm.constraintContracts) > 0
}
