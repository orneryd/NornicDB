package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)

// Large materialized operators observe cancellation between small batches of
// work, at the bulk owner's stride. Keep Context.Err (and the statement
// context's lock) off the per-row path.
const rowCancellationInterval = bulk.Stride

func rowCancellationError(ctx context.Context, index int) error {
	if index&(rowCancellationInterval-1) == 0 {
		return ctx.Err()
	}
	return nil
}

// Each materialized pass owns a checkpoint sized from the population it walks:
// input rows, output rows, windowed rows, or groups (even when rows is nil).
// Single-row streaming callers already check cancellation in their producer.
type materializedRowCheckpoint struct {
	ctx   context.Context
	rows  int
	index int
}

// due stays small enough to inline: the slow context/error path runs only at
// a block boundary, without a function call on each projected row.
func (c *materializedRowCheckpoint) due() bool {
	index := c.index
	c.index++
	return c.rows != 1 && index&(rowCancellationInterval-1) == 0
}

func (c *materializedRowCheckpoint) canceled() bool {
	if err := c.ctx.Err(); err != nil {
		recordExpressionFailure(c.ctx, err)
		return true
	}
	return false
}

// valueWalk observes the statement across one recursive walk of a value (its
// text, key or canonical form): visit counts each value the walk reaches, so a
// large value observes the statement however it nests, and sortKeys sorts a
// map's keys through the bulk owner. A walk with no statement (nil) runs to
// completion. The first observation comes after a stride of values.
func valueWalk(ctx context.Context) *materializedRowCheckpoint {
	return &materializedRowCheckpoint{ctx: ctx, index: 1}
}

// visit counts one value of a walk: true means the statement was canceled
// (and its failure recorded), and the walk stops.
func (c *materializedRowCheckpoint) visit() bool {
	return c != nil && c.due() && c.canceled()
}

// sortKeys sorts a map's keys for a walk: true means the statement was
// canceled during the sort.
func (c *materializedRowCheckpoint) sortKeys(keys []string) bool {
	if c == nil {
		return bulk.Strings(context.Background(), keys) != nil
	}
	return sortStrings(c.ctx, keys) != nil
}

// A key may contain a large captured list even when its operator has one row.
// A zero population denotes that recursive traversal: count visited values.
type rowKeyCancellation struct{}

func recoverRowKeyCancellation() {
	if failure := recover(); failure != nil {
		if _, canceled := failure.(rowKeyCancellation); !canceled {
			panic(failure)
		}
	}
}
