package storage

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/ctxsort"
)

// propertyIndexRead distinguishes the exact bound filtering of ordered ID APIs
// from group visitation's conservative binary-search bounds. limit<0 snapshots
// every group; a finite limit still captures the complete boundary tie.
type propertyIndexReadMode uint8

const (
	indexValueOrder propertyIndexReadMode = iota
	indexNonNilCandidates
)

type propertyIndexRead struct {
	mode        propertyIndexReadMode
	descending  bool
	limit       int
	bounds      PropertyIndexBounds
	exactBounds bool
}

type propertyIndexSnapshot struct {
	ids       []NodeID
	groupEnds []int
}

// readPropertyIndexSnapshot is the one ordered membership snapshot owner.
// The attached pending source remains stable through the index capture. All
// schema, pending and index leases end before owned-ID sorting and callbacks.
func (sm *SchemaManager) readPropertyIndexSnapshot(ctx context.Context, label, property string, request propertyIndexRead) (snapshot propertyIndexSnapshot, exists bool, err error) {
	err = func() error {
		if err := sm.mu.RLockContext(ctx); err != nil {
			return err
		}
		idx, found, err := sm.seekablePropertyIndexLocked(ctx, label, property)
		sm.mu.RUnlock()
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		view, source, err := sm.beginPendingRead(ctx)
		if err != nil {
			return err
		}
		defer endPendingRead(source)
		if request.mode == indexNonNilCandidates {
			if err := idx.mu.RLockContext(ctx); err != nil {
				return err
			}
		} else if err := idx.rLockWithFreshKeys(ctx); err != nil {
			return err
		}
		defer idx.mu.RUnlock()
		exists = true
		snapshot, err = idx.snapshotPropertyIndexLocked(ctx, view, property, request)
		return err
	}()
	if err != nil {
		return propertyIndexSnapshot{}, exists, err
	}
	start := 0
	for _, end := range snapshot.groupEnds {
		if err := ctxsort.Sort(ctx, snapshot.ids[start:end], func(a, b NodeID) int { return strings.Compare(string(a), string(b)) }, false); err != nil {
			return propertyIndexSnapshot{}, exists, err
		}
		start = end
	}
	if err := ctx.Err(); err != nil {
		return propertyIndexSnapshot{}, exists, err
	}
	if request.limit >= 0 && len(snapshot.ids) > request.limit {
		snapshot.ids = snapshot.ids[:request.limit]
		for i, end := range snapshot.groupEnds {
			if end >= request.limit {
				snapshot.groupEnds[i] = request.limit
				snapshot.groupEnds = snapshot.groupEnds[:i+1]
				break
			}
		}
	}
	return snapshot, exists, nil
}

// snapshotPropertyIndexLocked merges memberships using the same pending view
// as equality seeks. It returns privately owned IDs; no external callback runs
// with any read lease held. Caller owns an index reader and pending lease;
// value-order mode additionally requires a fresh sorted-key cache.
func (idx *PropertyIndex) snapshotPropertyIndexLocked(ctx context.Context, view pendingWriteView, property string, request propertyIndexRead) (propertyIndexSnapshot, error) {
	pending, err := view.propertyEntries(ctx, idx.Label, property)
	if err != nil {
		return propertyIndexSnapshot{}, err
	}
	var keys []interface{}
	if request.mode == indexNonNilCandidates {
		keys = make([]interface{}, 0, len(idx.values))
		for key, ids := range idx.values {
			if err := ctx.Err(); err != nil {
				return propertyIndexSnapshot{}, err
			}
			if key != nil && len(ids) > 0 {
				keys = append(keys, key)
			}
		}
		for key := range pending {
			if err := ctx.Err(); err != nil {
				return propertyIndexSnapshot{}, err
			}
			if key != nil && len(idx.values[key]) == 0 {
				keys = append(keys, key)
			}
		}
	} else {
		// Exact numeric/string bounds exclude noncomparable memberships. Other
		// ordered reads must decline before any callback, not sort key text.
		exactScalarBounds := request.exactBounds && request.bounds.bounded()
		if !exactScalarBounds {
			if idx.sortedKeyKinds.unordered {
				for key, ids := range idx.values {
					if err := ctx.Err(); err != nil {
						return propertyIndexSnapshot{}, err
					}
					if !unorderedIndexMembership(key) {
						continue
					}
					for _, id := range ids {
						if err := ctx.Err(); err != nil {
							return propertyIndexSnapshot{}, err
						}
						if view.keep(id) {
							return propertyIndexSnapshot{}, ErrPropertyIndexValueOrderUnavailable
						}
					}
				}
			}
			for key, ids := range pending {
				if err := ctx.Err(); err != nil {
					return propertyIndexSnapshot{}, err
				}
				if len(ids) > 0 && unorderedIndexMembership(key) {
					return propertyIndexSnapshot{}, ErrPropertyIndexValueOrderUnavailable
				}
			}
		}
		if len(pending) == 0 {
			keys, _, err = idx.sortedKeyRangeLocked(ctx, request.bounds)
			if err != nil {
				return propertyIndexSnapshot{}, err
			}
		} else {
			cached, kinds, err := idx.sortedKeysViewLocked(ctx)
			if err != nil {
				return propertyIndexSnapshot{}, err
			}
			// Reuse the committed-key capacity first; many pending values already
			// have a committed bucket and must not double this common allocation.
			keys = make([]interface{}, 0, len(cached))
			for _, key := range cached {
				if err := ctx.Err(); err != nil {
					return propertyIndexSnapshot{}, err
				}
				keys = append(keys, key)
			}
			for key := range pending {
				if err := ctx.Err(); err != nil {
					return propertyIndexSnapshot{}, err
				}
				if key == nil || unorderedIndexMembership(key) || len(idx.values[key]) > 0 {
					continue
				}
				keys = append(keys, key)
				isString, isNumber := indexValueKinds(key)
				kinds.strings = kinds.strings && isString
				kinds.numbers = kinds.numbers && isNumber
			}
			kinds.hasKeys = len(keys) > 0
			if err := ctxsort.Sort(ctx, keys, compareSchemaIndexValues, false); err != nil {
				return propertyIndexSnapshot{}, err
			}
			keys, _, err = propertyIndexKeyRange(ctx, keys, kinds, request.bounds)
			if err != nil {
				return propertyIndexSnapshot{}, err
			}
		}
	}
	capacity := 0
	for n := range keys {
		if err := ctx.Err(); err != nil {
			return propertyIndexSnapshot{}, err
		}
		i := n
		if request.descending {
			i = len(keys) - 1 - n
		}
		// A finite read needs the entire first tie before it can choose its
		// first IDs. Reserve that tie rather than growing from the limit.
		if request.limit >= 0 && n > 0 {
			first := 0
			if request.descending {
				first = len(keys) - 1
			}
			if compareSchemaIndexValues(keys[first], keys[i]) != 0 {
				break
			}
		}
		capacity += len(idx.values[keys[i]]) + len(pending[keys[i]])
	}
	groupCapacity := len(keys)
	if request.mode == indexNonNilCandidates {
		groupCapacity = 0
	}
	if request.limit >= 0 && len(keys) > 0 {
		capacity = max(capacity, request.limit)
		groupCapacity = min(groupCapacity, request.limit)
	}
	snapshot := propertyIndexSnapshot{ids: make([]NodeID, 0, capacity), groupEnds: make([]int, 0, groupCapacity)}
	appendGroup := func(group []interface{}) error {
		start := len(snapshot.ids)
		for _, key := range group {
			if err := ctx.Err(); err != nil {
				return err
			}
			if request.exactBounds && request.bounds.bounded() && !request.bounds.contains(key) {
				continue
			}
			if view.index == nil {
				var err error
				snapshot.ids, err = appendIndexNodeIDs(ctx, snapshot.ids, idx.values[key])
				if err != nil {
					return err
				}
			} else {
				for _, id := range idx.values[key] {
					if err := ctx.Err(); err != nil {
						return err
					}
					if view.keep(id) {
						snapshot.ids = append(snapshot.ids, id)
					}
				}
			}
			for _, id := range pending[key] {
				if err := ctx.Err(); err != nil {
					return err
				}
				snapshot.ids = append(snapshot.ids, id)
			}
		}
		if request.mode != indexNonNilCandidates && len(snapshot.ids) > start {
			snapshot.groupEnds = append(snapshot.groupEnds, len(snapshot.ids))
		}
		return nil
	}
	if request.mode == indexNonNilCandidates {
		if err := appendGroup(keys); err != nil {
			return propertyIndexSnapshot{}, err
		}
		if err := ctx.Err(); err != nil {
			return propertyIndexSnapshot{}, err
		}
		return snapshot, nil
	}
	for cursor := 0; cursor < len(keys); {
		if err := ctx.Err(); err != nil {
			return propertyIndexSnapshot{}, err
		}
		if request.descending {
			end := len(keys) - cursor
			start := end - 1
			for start > 0 && compareSchemaIndexValues(keys[start-1], keys[end-1]) == 0 {
				if err := ctx.Err(); err != nil {
					return propertyIndexSnapshot{}, err
				}
				start--
			}
			if err := appendGroup(keys[start:end]); err != nil {
				return propertyIndexSnapshot{}, err
			}
			cursor = len(keys) - start
		} else {
			end := cursor + 1
			for end < len(keys) && compareSchemaIndexValues(keys[cursor], keys[end]) == 0 {
				if err := ctx.Err(); err != nil {
					return propertyIndexSnapshot{}, err
				}
				end++
			}
			if err := appendGroup(keys[cursor:end]); err != nil {
				return propertyIndexSnapshot{}, err
			}
			cursor = end
		}
		if request.limit >= 0 && len(snapshot.ids) >= request.limit {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return propertyIndexSnapshot{}, err
	}
	return snapshot, nil
}
