package cypher

import (
	"context"
	"strings"
)

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
	return r != nil && r.account.scope != nil
}

// reserveBytes reserves n estimated bytes.
func (r *memoryRetention) reserveBytes(n int64) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(n)
}

// reserveSlots reserves capacity for n slots of slotBytes each (a
// preallocated slice), before the slice is made.
func (r *memoryRetention) reserveSlots(n int, slotBytes int64) error {
	if r == nil || r.account.scope == nil || n <= 0 {
		return nil
	}
	return r.account.grow(saturatingProduct(int64(n), slotBytes))
}

// reserveValue reserves a value the site keeps.
func (r *memoryRetention) reserveValue(value interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.growValue(value)
}

// reserveRow reserves a binding row the site keeps (or a copy of it).
func (r *memoryRetention) reserveRow(row pipelineRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(r.rows.bytes(row))
}

// reserveMergedRow reserves the row merging outer's and inner's bindings,
// before it is built. outer's values were counted when its row was built:
// the merged row adds only its slots for them (sharedBindingBytes).
func (r *memoryRetention) reserveMergedRow(outer, inner pipelineRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes+mapHeaderBytes) + sharedBindingBytes(outer)
	for name, value := range inner {
		size += r.rows.entry(name, value)
	}
	return r.account.grow(size)
}

// reserveExtendedRow reserves the row extending outer with columns bound
// to values, before it is built. Outer's values count as slots only, as in
// reserveMergedRow: a CALL subquery's outer row is shared by every row its
// body returns, not copied into each.
func (r *memoryRetention) reserveExtendedRow(outer pipelineRow, columns []string, values []interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes+mapHeaderBytes) + sharedBindingBytes(outer)
	for index, column := range columns {
		if index < len(values) {
			size += r.rows.entry(column, values[index])
		}
	}
	return r.account.grow(size)
}

// reserveResultRow reserves a result row the site keeps.
func (r *memoryRetention) reserveResultRow(values []interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(estimateResultRowBytes(values, r.rows.known))
}

// close returns the unspent credit.
func (r *memoryRetention) close() {
	if r != nil {
		r.account.close()
	}
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

// transientCheckBytes is the smallest value build an expression checks
// against the statement's limits: smaller builds cost less than the check.
const transientCheckBytes = 64 << 10

// expressionBudget is the statement's check on the values an expression
// builds (#1343): range() lists, comprehension results, reduce
// accumulators, an UNWIND's evaluated list while it streams. A build holds
// what it will take before it allocates (an accumulator, as it grows) and
// gives it back once built: the row or list that keeps the value counts it
// there. A build over a limit fails the statement before it allocates; an
// accumulator, within one step of its growth. The zero value checks
// nothing, and nothing is sized when no limit is configured.
type expressionBudget struct {
	scope *memoryScope
	held  int64
}

// expressionBudgetOf is the budget of the statement ctx runs.
func expressionBudgetOf(ctx context.Context) expressionBudget {
	return expressionBudget{scope: memoryScopeFromContext(ctx)}
}

// rowExpressionBudget is the budget of the statement a row carries
// (rowStatementContextKey; bound while a limit is configured, for an
// expression that builds values).
func rowExpressionBudget(values map[string]interface{}) expressionBudget {
	if !transactionMemoryActive.Load() {
		return expressionBudget{}
	}
	return expressionBudget{scope: memoryScopeFromContext(rowStatementContext(values))}
}

// active reports whether the budget checks anything.
func (b *expressionBudget) active() bool {
	return b != nil && b.scope != nil
}

// hold makes the budget hold size bytes: what the build takes now.
func (b *expressionBudget) hold(size int64) error {
	if !b.active() || (size < transientCheckBytes && b.held == 0) {
		return nil
	}
	switch {
	case size > b.held:
		if err := b.scope.grow(size - b.held); err != nil {
			return err
		}
		b.held = size
	case size < b.held:
		b.scope.give(b.held - size)
		b.held = size
	}
	return nil
}

// release gives back what the budget holds.
func (b *expressionBudget) release() {
	if b.active() && b.held > 0 {
		b.scope.give(b.held)
		b.held = 0
	}
}

// shallowValueBytes is a value's own size without what its elements hold:
// O(1), for a value that grows step by step (an accumulator), whose
// elements were counted where they were built.
func shallowValueBytes(value interface{}) int64 {
	switch v := value.(type) {
	case string:
		return valueSlotBytes + stringHeaderBytes + int64(len(v))
	case []interface{}:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*valueSlotBytes
	case []string:
		return valueSlotBytes + sliceHeaderBytes + int64(cap(v))*stringHeaderBytes
	case map[string]interface{}:
		return valueSlotBytes + mapHeaderBytes + int64(len(v))*(mapEntryBytes+stringHeaderBytes+valueSlotBytes)
	default:
		return valueSlotBytes
	}
}

// expressionBuildsValues reports whether an expression may build a large
// value: it calls range() or reduce(), or has a list comprehension
// ([x IN list | …] or [x IN list WHERE …]). A conservative read of the
// text: a false positive only binds the statement context for the budget.
func expressionBuildsValues(expression string) bool {
	lower := lowerASCII(expression)
	if strings.Contains(lower, "range(") || strings.Contains(lower, "reduce(") {
		return true
	}
	return strings.Contains(lower, "[") && strings.Contains(lower, " in ") &&
		(strings.Contains(lower, "|") || strings.Contains(lower, " where "))
}

// trackedRetention is a retention owner for ctx's statement while a limit
// is configured, nil otherwise: for a site whose closure would otherwise
// capture an owner it never uses (every method is nil-safe).
func trackedRetention(ctx context.Context) *memoryRetention {
	if !transactionMemoryActive.Load() {
		return nil
	}
	retention := newMemoryRetention(ctx)
	if !retention.tracking() {
		return nil
	}
	return &retention
}

// reserveTraversalRow reserves an OPTIONAL MATCH candidate row (its node,
// relationship and value bindings), before it is kept.
func (r *memoryRetention) reserveTraversalRow(row traversalOptRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes + 3*mapHeaderBytes)
	for name := range row.nodes {
		size += mapEntryBytes + stringHeaderBytes + int64(len(name)) + entityReferenceBytes
	}
	for name := range row.rels {
		size += mapEntryBytes + stringHeaderBytes + int64(len(name)) + entityReferenceBytes
	}
	for name, value := range row.values {
		size += r.rows.entry(name, value)
	}
	return r.account.grow(size)
}

// release gives back everything the owner reserved, for a working buffer
// the site drops when it is done (a sort buffer, a finalization index):
// what it retained is not kept past the site.
func (r *memoryRetention) release() {
	if r == nil || r.account.scope == nil {
		return
	}
	if taken := r.account.spent + r.account.credit; taken > 0 {
		r.account.scope.give(taken)
	}
	r.account.spent, r.account.credit = 0, 0
}

// sharedBindingBytes is what a new row takes for bindings it shares with an
// existing row: an entry, its key and a value slot each. The values belong
// to the row that already holds them and were counted when it was built.
func sharedBindingBytes(row pipelineRow) int64 {
	var size int64
	for name := range row {
		size += mapEntryBytes + stringHeaderBytes + int64(len(name)) + valueSlotBytes
	}
	return size
}
