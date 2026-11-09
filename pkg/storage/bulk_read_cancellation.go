package storage

import "context"

// bulkReadCancellationInterval is the number of visited records between two
// checks of a bulk read's caller context. Bulk reads (label scans, whole-graph
// scans, adjacency expansion and the namespace/projection copies built from
// them) visit their records in tight loops; reading the context once per
// interval lets a canceled request stop that loop promptly while keeping the
// per-record cost to one decrement and compare. Index-key preparation visits a
// list's elements or a map's entries in the same shape (schema_value_keys.go),
// so it amortizes its caller context here at the same interval.
const bulkReadCancellationInterval = 256

// bulkReadCancellation amortizes one caller context's cancellation checks
// across the records a bulk read visits. The zero value is ready to use and
// stays on the caller's stack: no context is retained beyond the loop that
// owns it and no allocation is added per record.
//
// A read returns the context's own error (context.Canceled or
// context.DeadlineExceeded) and no partial result, so a canceled request never
// publishes or caches what it managed to materialize.
//
// Every read that has a caller context threads that context here, including
// the private kernels (adjacency materialization, overlay capture, transaction
// pending merges, and writing a list or map value's canonical index-key text).
// A read whose own public API carries no caller context — a pre-existing point
// or first-row read, a write's own integrity/constraint read, or a maintenance
// walk — passes context.Background() at that boundary and says so where it
// reads. Those are the true context-free owners: a canceled statement read must
// never cancel an accepted write's checks, and there is no caller context to
// inherit at a point read. No helper stands between such a site and its
// documented lifetime, so the choice to have no caller context stays visible at
// the read.
type bulkReadCancellation struct {
	remaining int
}

// check reports the caller's cancellation on the first record and then once
// every bulkReadCancellationInterval records.
func (c *bulkReadCancellation) check(ctx context.Context) error {
	if c.remaining > 0 {
		c.remaining--
		return nil
	}
	c.remaining = bulkReadCancellationInterval - 1
	return ctx.Err()
}
