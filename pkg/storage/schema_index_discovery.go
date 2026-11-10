package storage

import (
	"context"
)

// compositeKeyIndexLocked returns the equality index on exactly label's
// properties (in order), owned or standalone, or nil. The caller holds sm.mu.
func (sm *SchemaManager) compositeKeyIndexLocked(ctx context.Context, label string, properties []string) (*CompositeIndex, error) {
	var cancellation bulkReadCancellation
	for _, idx := range sm.compositeIndexes {
		if err := cancellation.check(ctx); err != nil {
			return nil, err
		}
		if idx == nil || idx.Label != label || len(idx.Properties) != len(properties) {
			continue
		}
		matches := true
		for i, property := range properties {
			if err := cancellation.check(ctx); err != nil {
				return nil, err
			}
			if idx.Properties[i] != property {
				matches = false
				break
			}
		}
		if matches {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return idx, nil
		}
	}
	return nil, ctx.Err()
}

// filledKeyIndex returns a filled equality index on exactly label's
// properties, or nil while there is none (validation then scans the label).
// Request-bearing MERGE and validation bound both schema admission and scan by
// ctx. A nil schema retains its existing empty-schema behavior without work.
func (sm *SchemaManager) filledKeyIndex(ctx context.Context, label string, properties []string) (*CompositeIndex, error) {
	if sm == nil {
		return nil, nil
	}
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer sm.mu.RUnlock()
	idx, err := sm.compositeKeyIndexLocked(ctx, label, properties)
	if err != nil {
		return nil, err
	}
	if idx != nil && idx.unfilled.Load() {
		idx = nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return idx, nil
}

// arity1PropertyIndexLocked returns the arity-1 composite index on label's
// property, or nil. The caller holds sm.mu. This is the single scan of
// sm.compositeIndexes for an equality index; ctx bounds it so a lookup that
// waited behind another goroutine's schema write leaves instead of scanning on
// its behalf, and a cancelled scan reports its error and no index.
func (sm *SchemaManager) arity1PropertyIndexLocked(ctx context.Context, label, property string) (*CompositeIndex, bool, error) {
	for _, idx := range sm.compositeIndexes {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if idx != nil && idx.Label == label && len(idx.Properties) == 1 && idx.Properties[0] == property {
			return idx, true, nil
		}
	}
	return nil, false, nil
}

// seekablePropertyIndexLocked returns the arity-1 index on label's property
// that seeks may use: one that exists and is filled (#875). The caller holds
// sm.mu, taken with ctx so the wait is cancellable. ctx/errors are carried
// through the arity-1 scan, and a cancelled lookup reports its error rather
// than an absent index.
func (sm *SchemaManager) seekablePropertyIndexLocked(ctx context.Context, label, property string) (*PropertyIndex, bool, error) {
	idx, exists, err := sm.arity1PropertyIndexLocked(ctx, label, property)
	if err != nil || !exists || idx == nil {
		return nil, false, err
	}
	if idx.unfilled.Load() {
		return nil, false, nil
	}
	return idx, true, nil
}

// MaintainsPropertyIndex reports whether writes must maintain an arity-1
// index on label's property, filled or not (#875): node writes and index
// rebuilds use it, seeks use GetPropertyIndex.
func (sm *SchemaManager) MaintainsPropertyIndex(label, property string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	// Write maintenance asks whether it must file keys, without a request of
	// its own: the scan has this call's lifetime and cannot cancel.
	_, exists, err := sm.arity1PropertyIndexLocked(context.Background(), label, property)
	return err == nil && exists
}

// ConstraintPropertyIndex returns the equality index constraint name owns
// (#875), if it has one.
func (sm *SchemaManager) ConstraintPropertyIndex(name string) (*PropertyIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if _, exists := sm.constraints[name]; !exists {
		return nil, false
	}
	for _, idx := range sm.compositeIndexes {
		if idx.OwningConstraint == name {
			return idx, true
		}
	}
	return nil, false
}

// GetPropertyIndex returns the filled property index the label and property
// combination can be seeked with. ctx is the request the lookup serves: it
// bounds the wait for the schema lock and the composite-index scan, and a
// cancelled lookup reports its error instead of an absent index.
func (sm *SchemaManager) GetPropertyIndex(ctx context.Context, label, property string) (*PropertyIndex, bool, error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, false, err
	}
	defer sm.mu.RUnlock()
	return sm.seekablePropertyIndexLocked(ctx, label, property)
}

// HasPropertyIndex reports whether a filled arity-1 index exists for the
// given label+property combination. Callers can use this to choose between
// index-backed per-row lookups and batch preloads.
func (sm *SchemaManager) HasPropertyIndex(ctx context.Context, label, property string) (bool, error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return false, err
	}
	defer sm.mu.RUnlock()
	_, exists, err := sm.seekablePropertyIndexLocked(ctx, label, property)
	return exists, err
}

// HasAnyPropertyIndexForLabel reports whether ANY equality index (arity-1 or
// composite) is declared against the given label. Used by storage-side index
// maintenance to short-circuit per-node work when no index touches the label.
func (sm *SchemaManager) HasAnyPropertyIndexForLabel(label string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.hasAnyCompositeIndexForLabelLocked(label)
}

// HasMultiPropertyIndexForLabel reports whether a composite index of two or
// more properties is declared against label: the only indexes a pattern read
// can use beyond its single-property ones.
func (sm *SchemaManager) HasMultiPropertyIndexForLabel(label string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, idx := range sm.compositeIndexes {
		if idx != nil && idx.Label == label && len(idx.Properties) > 1 {
			return true
		}
	}
	return false
}

// hasAnyCompositeIndexForLabelLocked reports whether any composite index is
// declared against label. The caller holds sm.mu.
func (sm *SchemaManager) hasAnyCompositeIndexForLabelLocked(label string) bool {
	for _, idx := range sm.compositeIndexes {
		if idx != nil && idx.Label == label {
			return true
		}
	}
	return false
}
