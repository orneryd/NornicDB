package storage

import "context"

// PropertyIndexValueCount returns how many nodes label's property index lists
// under value: the length of the value's committed posting list plus the
// pending writes that hold the value, without copying either. It is the
// estimate a query planner compares index seeks by (NOR-414); it is not a
// lookup: a pending write that supersedes a committed entry is counted twice,
// so the count is an upper bound on PropertyIndexLookup's result.
//
// indexed is false, with a zero count, when PropertyIndexLookup could not use
// the index for value: no seekable index on (label, property), or a value no
// index key holds.
func (sm *SchemaManager) PropertyIndexValueCount(ctx context.Context, label, property string, value interface{}) (count int, indexed bool, err error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return 0, false, err
	}
	idx, exists, lookupErr := sm.seekablePropertyIndexLocked(ctx, label, property)
	sm.mu.RUnlock()
	if lookupErr != nil || !exists {
		return 0, false, lookupErr
	}
	valueKey, ok, err := indexValueKey(ctx, value)
	if err != nil || !ok {
		return 0, false, err
	}
	view, source, err := sm.beginPendingRead(ctx)
	if err != nil {
		return 0, false, err
	}
	defer endPendingRead(source)
	pending := len(view.valueMatchSet(idx.Label, property, valueKey))
	if err := idx.mu.RLockContext(ctx); err != nil {
		return 0, false, err
	}
	stored := len(idx.values[valueKey])
	idx.mu.RUnlock()
	return stored + pending, true, nil
}

// CompositeIndexValueCount is PropertyIndexValueCount for a composite index
// read: the length of the committed posting list CompositeIndexLookup starts
// from for values (all properties when full, otherwise the leading ones),
// plus the most pending writes its lookup can add (compositeMatchBound), read
// from the same pending-write view without scanning it. indexed is false when
// the index cannot key values.
func (sm *SchemaManager) CompositeIndexValueCount(ctx context.Context, idx *CompositeIndex, values []interface{}, full bool) (count int, indexed bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if idx == nil || len(values) == 0 || len(values) > len(idx.Properties) || (full && len(values) != len(idx.Properties)) {
		return 0, false, nil
	}
	stored, indexed, err := compositeIndexStoredCount(ctx, idx, values)
	if err != nil || !indexed {
		return 0, false, err
	}
	view, source, err := sm.beginPendingRead(ctx)
	if err != nil {
		return 0, false, err
	}
	defer endPendingRead(source)
	pending, err := view.compositeMatchBound(ctx, idx.Label, idx.Properties, values)
	if err != nil {
		return 0, false, err
	}
	return stored + pending, true, nil
}

// compositeIndexStoredCount is the length of idx's committed posting list for
// values, or indexed false when idx cannot key them.
func compositeIndexStoredCount(ctx context.Context, idx *CompositeIndex, values []interface{}) (count int, indexed bool, err error) {
	if len(idx.Properties) == 1 {
		valueKey, ok, err := indexValueKey(ctx, values[0])
		if err != nil || !ok {
			return 0, false, err
		}
		if err := idx.mu.RLockContext(ctx); err != nil {
			return 0, false, err
		}
		count = len(idx.values[valueKey])
		idx.mu.RUnlock()
		return count, true, nil
	}
	canonical, ok, err := compositeLookupKeyValues(ctx, values)
	if err != nil || !ok {
		return 0, false, err
	}
	key, err := NewCompositeKey(ctx, canonical...)
	if err != nil {
		return 0, false, err
	}
	if err := idx.mu.RLockContext(ctx); err != nil {
		return 0, false, err
	}
	source := idx.prefixIndex
	if len(canonical) == len(idx.Properties) {
		source = idx.fullIndex
	}
	count = len(source[key.Hash])
	idx.mu.RUnlock()
	return count, true, nil
}
