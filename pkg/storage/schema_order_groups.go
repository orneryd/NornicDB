package storage

import (
	"context"
)

// VisitPropertyIndexGroups visits non-null property-index entries in key order.
// Each callback receives the complete group of IDs whose keys compare equally,
// including differently typed numeric keys. Returning false stops the visit.
// IDs within a group are ordered by node identity. The callback runs without
// schema/index locks so it can read nodes or perform index operations safely.
// The return value reports whether the index exists, even when it is empty.
// Unorderable memberships return ErrPropertyIndexValueOrderUnavailable before
// any callback; candidate planners can then order actual values themselves.
//
// Query planners use this to finish a primary-key tie before selecting a page
// by secondary ORDER BY keys. For example, a title index must visit every node
// with the boundary title before choosing the smallest timestamp/ID tuple.
func (sm *SchemaManager) VisitPropertyIndexGroups(ctx context.Context, label, property string, descending bool, visit func([]NodeID) bool) (bool, error) {
	return sm.VisitPropertyIndexGroupsInRange(ctx, label, property, descending, PropertyIndexBounds{}, visit)
}

// VisitPropertyIndexGroupsInRange is VisitPropertyIndexGroups starting and
// ending at bounds: groups whose keys fall outside them are skipped without
// being read, found by binary search where the keys allow it
// (sortedKeyRangeLocked). Bounds only skip groups that can't match; the
// caller still tests every visited node, and gets every group when the keys
// can't be searched. ORDER BY … LIMIT under a range or keyset predicate
// (WHERE n.t > $last …) starts at the bound instead of reading the whole
// index before it (#939).
func (sm *SchemaManager) VisitPropertyIndexGroupsInRange(ctx context.Context, label, property string, descending bool, bounds PropertyIndexBounds, visit func([]NodeID) bool) (bool, error) {
	snapshot, exists, err := sm.readPropertyIndexSnapshot(ctx, label, property, propertyIndexRead{descending: descending, limit: -1, bounds: bounds})
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	start := 0
	for _, end := range snapshot.groupEnds {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !visit(snapshot.ids[start:end]) {
			return true, ctx.Err()
		}
		start = end
	}
	return true, ctx.Err()
}
