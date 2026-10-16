package cypher

import "context"

// memoryRetention is the one owner a growth site reserves through before it
// retains anything (#1343): rows it appends, slots it preallocates, values
// and accumulators it builds. The reservation comes first, then the
// allocation, so a statement over its limit stops before the allocation
// rather than after it. A failed reservation cancels the statement with the
// limit's error, and the site returns it at once.
//
// It is an account in the statement's memory scope (memoryAccount: local
// credit, the per-transaction limit kept exact) and a row estimator
// (rowEstimator: every row sized in full, shared lists once per column,
// collected lists from what collect() counted). With no limit configured it
// tracks nothing and estimates nothing: every method checks tracking before
// it sizes anything. close returns the unspent credit; call it on every exit
// (defer). What the site retained stays counted until the statement, or the
// nested execution it runs in, ends.
type memoryRetention struct {
	account memoryAccount
	rows    rowEstimator
}

// newMemoryRetention opens a retention owner in ctx's statement scope.
func newMemoryRetention(ctx context.Context) memoryRetention {
	scope := memoryScopeFromContext(ctx)
	return memoryRetention{account: memoryAccount{scope: scope}, rows: rowEstimator{known: scope.knownLists()}}
}

// tracking reports whether a limit is configured for the statement.
func (r *memoryRetention) tracking() bool {
	return r.account.scope != nil
}

// reserveBytes reserves n estimated bytes.
func (r *memoryRetention) reserveBytes(n int64) error {
	if r.account.scope == nil {
		return nil
	}
	return r.account.grow(n)
}

// reserveSlots reserves capacity for n slots of slotBytes each (a
// preallocated slice), before the slice is made.
func (r *memoryRetention) reserveSlots(n int, slotBytes int64) error {
	if r.account.scope == nil || n <= 0 {
		return nil
	}
	return r.account.grow(saturatingProduct(int64(n), slotBytes))
}

// reserveValue reserves a value the site keeps.
func (r *memoryRetention) reserveValue(value interface{}) error {
	if r.account.scope == nil {
		return nil
	}
	return r.account.growValue(value)
}

// reserveRow reserves a binding row the site keeps (or a copy of it).
func (r *memoryRetention) reserveRow(row pipelineRow) error {
	if r.account.scope == nil {
		return nil
	}
	return r.account.grow(r.rows.bytes(row))
}

// reserveMergedRow reserves the row merging outer's and inner's bindings,
// before it is built.
func (r *memoryRetention) reserveMergedRow(outer, inner pipelineRow) error {
	if r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes + mapHeaderBytes)
	for name, value := range outer {
		size += r.rows.entry(name, value)
	}
	for name, value := range inner {
		size += r.rows.entry(name, value)
	}
	return r.account.grow(size)
}

// reserveExtendedRow reserves the row extending outer with columns bound
// to values, before it is built.
func (r *memoryRetention) reserveExtendedRow(outer pipelineRow, columns []string, values []interface{}) error {
	if r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes + mapHeaderBytes)
	for name, value := range outer {
		size += r.rows.entry(name, value)
	}
	for index, column := range columns {
		if index < len(values) {
			size += r.rows.entry(column, values[index])
		}
	}
	return r.account.grow(size)
}

// reserveResultRow reserves a result row the site keeps.
func (r *memoryRetention) reserveResultRow(values []interface{}) error {
	if r.account.scope == nil {
		return nil
	}
	return r.account.grow(estimateResultRowBytes(values, r.rows.known))
}

// close returns the unspent credit.
func (r *memoryRetention) close() {
	r.account.close()
}

// maxReservation caps one reservation (1 PiB): larger than any limit, and
// far from overflowing the counters it is added to.
const maxReservation = int64(1) << 50

// saturatingProduct is a*b for non-negative a and b, capped at
// maxReservation: a reservation that large is one no limit admits.
func saturatingProduct(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > maxReservation/b {
		return maxReservation
	}
	return a * b
}
