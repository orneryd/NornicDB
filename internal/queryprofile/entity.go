package queryprofile

import "context"

// EntityPopulation records the entities a bulk read actually returned: each
// non-nil entry of entities is one entity read and one row. It records them
// where the read handed them over, before any consumer narrows the population,
// and an empty population the consumer did receive records a known zero rather
// than no record at all.
//
// A statement without an observer returns before traversing entries or
// allocating, so an ordinary statement pays one observer lookup and nothing
// else.
func EntityPopulation[T any](ctx context.Context, kind, detail string, entities []*T) {
	counter := Access(ctx, kind, detail)
	if counter == nil {
		return
	}
	var entries int64
	for _, entity := range entities {
		if entity != nil {
			entries++
		}
	}
	counter.Add(entries, entries)
}

// EntityVisitor observes the entities a streaming read hands to its consumer,
// before that consumer filters any of them, and returns the callback the read
// streams through: visit itself when the statement has no observer, so an
// ordinary statement allocates no wrapper, and otherwise a wrapper that counts
// each non-nil entity as one entity read and one row before calling visit.
func EntityVisitor[T any](ctx context.Context, kind, detail string, visit func(*T) error) func(*T) error {
	counter := Access(ctx, kind, detail)
	if counter == nil {
		return visit
	}
	return func(entity *T) error {
		if entity != nil {
			counter.Add(1, 1)
		}
		return visit(entity)
	}
}

// EntityReadResult records the outcome of one actual point-read attempt. A
// successful non-nil image is one row; absence/failure is zero rows. The attempt
// is one logical request, independently of fields or payload decoded by storage.
// Invoke only after the reader ran; an unused capability is not an attempt.
func EntityReadResult[T any](counter *ReadCounter, entity *T, err error) {
	if counter == nil {
		return
	}
	var rows int64
	if err == nil && entity != nil {
		rows = 1
	}
	counter.Add(rows, 1)
}
