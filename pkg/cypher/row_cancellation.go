package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)

// Large materialized operators observe cancellation between small batches of
// work, at the bulk owner's stride. Keep Context.Err (and the statement
// context's lock) off the per-row path.
const rowCancellationInterval = bulk.Stride

// materializedRowCheckpoint is the cancellation owner's check
// (bulk.Checkpoint): each materialized pass owns one, sized from the
// population it walks (bulk.RowsCheckpoint: a single row's producer observes
// it).
type materializedRowCheckpoint = bulk.Checkpoint

// A stopped pass records its statement's failure where the outcome reads it.
func init() { bulk.SetStopRecorder(recordExpressionFailure) }

// insideStopBoundary runs work inside the cancellation owner's boundary: a
// stop returns the statement's error (bulk.RecoverStop) and no value.
func insideStopBoundary[T any](work func() (T, error)) (value T, err error) {
	defer bulk.RecoverStop(&err)
	return work()
}

// valueWalk observes the statement across one recursive walk of a value (its
// text, key or canonical form): each value the walk reaches is one check, so
// a large value observes the statement however it nests, and sortWalkKeys
// sorts a map's keys through the bulk owner. A walk with no statement (nil)
// runs to completion. The first observation comes after a stride of values.
func valueWalk(ctx context.Context) *materializedRowCheckpoint {
	return bulk.WalkCheckpoint(ctx)
}

// sortWalkKeys sorts a map's keys for a walk; a canceled statement stops the
// walk (Check).
func sortWalkKeys(walk *materializedRowCheckpoint, keys []string) {
	if walk == nil {
		_ = bulk.Strings(context.Background(), keys)
		return
	}
	if err := sortStrings(walk.Context(), keys); err != nil {
		panic(bulk.Stopped{Err: err})
	}
}
