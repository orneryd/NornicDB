package storage

import (
	"context"
	"github.com/orneryd/nornicdb/internal/ctxsort"
	"strings"
)

// VisitPropertyIndexGroups visits non-null property-index entries in key order.
// Each callback receives the complete group of IDs whose keys compare equally,
// including differently typed numeric keys. Returning false stops the visit.
// IDs within a group are ordered by node identity. The callback runs without
// schema/index locks so it can read nodes or perform index operations safely.
// The return value reports whether the index exists, even when it is empty.
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
	if err := sm.mu.RLockContext(ctx); err != nil {
		return false, err
	}
	idx, exists, lookupErr := sm.seekablePropertyIndexLocked(ctx, label, property)
	sm.mu.RUnlock()
	if lookupErr != nil {
		return false, lookupErr
	}
	if !exists {
		return false, nil
	}
	// Snapshot every group's membership before callbacks. No callback runs with
	// either lock held, and a canceled preparation exposes no partial snapshot.
	var ids []NodeID
	var groupEnds []int
	err := func() error {
		if err := idx.rLockWithFreshKeys(ctx); err != nil {
			return err
		}
		defer idx.mu.RUnlock()
		keys, _, err := idx.sortedKeyRangeLocked(ctx, bounds)
		if err != nil {
			return err
		}
		totalIDs := 0
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			totalIDs += len(idx.values[key])
		}
		ids = make([]NodeID, 0, totalIDs)
		groupEnds = make([]int, 0, len(keys))
		appendGroup := func(group []interface{}) error {
			start := len(ids)
			for _, key := range group {
				if err := ctx.Err(); err != nil {
					return err
				}
				for _, id := range idx.values[key] {
					if err := ctx.Err(); err != nil {
						return err
					}
					ids = append(ids, id)
				}
			}
			if len(ids) > start {
				groupEnds = append(groupEnds, len(ids))
			}
			return nil
		}
		if descending {
			for end := len(keys); end > 0; {
				if err := ctx.Err(); err != nil {
					return err
				}
				start := end - 1
				for start > 0 && compareSchemaIndexValues(keys[start-1], keys[end-1]) == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
					start--
				}
				if err := appendGroup(keys[start:end]); err != nil {
					return err
				}
				end = start
			}
		} else {
			for start := 0; start < len(keys); {
				if err := ctx.Err(); err != nil {
					return err
				}
				end := start + 1
				for end < len(keys) && compareSchemaIndexValues(keys[start], keys[end]) == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
					end++
				}
				if err := appendGroup(keys[start:end]); err != nil {
					return err
				}
				start = end
			}
		}
		return ctx.Err()
	}()
	if err != nil {
		return false, err
	}
	start := 0
	for _, end := range groupEnds {
		group := ids[start:end]
		if err := ctxsort.Sort(ctx, group, func(a, b NodeID) int { return strings.Compare(string(a), string(b)) }, false); err != nil {
			return false, err
		}
		if !visit(group) {
			return true, ctx.Err()
		}
		start = end
	}
	return true, ctx.Err()
}
