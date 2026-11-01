package storage

import (
	"context"
	"errors"
)

// ErrPropertyIndexValueOrderUnavailable means an index cannot provide property
// value order for the requested population. No partial IDs or group callbacks
// are delivered. Canonical list keys and membership-only unkeyable values are
// not property values and must never be ordered by their encoded text.
var ErrPropertyIndexValueOrderUnavailable = errors.New("property index value order unavailable")

// PropertyIndexNonNilCandidates returns complete, unordered nonnull candidates,
// including accepted values without equality keys (such as byte arrays), merged
// with current namespace pending writes. found is false if no filled index is
// available; an available empty population is found=true with zero IDs.
//
// Query planners use this for IS NOT NULL, then evaluate residual predicates and
// ORDER BY on the actual node values. Callers needing index-provided value order
// use PropertyIndexAllNonNil, TopK or grouped visits; those return the explicit
// ErrPropertyIndexValueOrderUnavailable when the population cannot be ordered.
func (sm *SchemaManager) PropertyIndexNonNilCandidates(ctx context.Context, label, property string) (ids []NodeID, found bool, err error) {
	snapshot, found, err := sm.readPropertyIndexSnapshot(ctx, label, property, propertyIndexRead{mode: indexNonNilCandidates, limit: -1})
	return snapshot.ids, found, err
}
