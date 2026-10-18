package cypher

import (
	"context"
	"reflect"
	"strings"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The accountant's operator API (#1343). transaction_memory.go holds a
// statement's charge and transaction_memory_estimate.go sizes values; this
// file is how operators use them, and the only way: no other file sizes a
// value or charges a scope (TestMemoryChargeSitesUseTheAccountant). An
// operator charges what it keeps when it acquires it, and what it drops goes
// back:
//
//   - memoryRetention: the rows, values and working state one operator
//     keeps. owned declares rows it charged in full, carried values
//     included.
//   - rowGenerations: the clause pipeline's rows. A clause's output replaces
//     its input, whose charge goes back once the output is charged in full.
//   - memorySpan: what an operator took over a stretch of its work, given
//     back once the result that replaces it is charged.
//   - expressionBudget: the values an expression builds, held while they
//     are built.
//
// With no limit configured a statement has no scope: every type's zero value
// tracks nothing, and no method sizes a value before it checks that.

// memoryRetention is the one owner an operator reserves through before it
// retains anything: rows it appends, slots it preallocates, values and
// accumulators it builds. The reservation comes first, then the allocation,
// so a statement over its limit stops before the allocation rather than
// after it. A failed reservation cancels the statement with the limit's
// error, and the operator returns it at once.
//
// It is an account in the statement's memory scope (memoryAccount: local
// credit, the per-transaction limit kept exact) and a row estimator
// (rowEstimator: every row sized in full, shared lists once per column,
// collected lists from what collect() counted). close returns the unspent
// credit; call it on every exit (defer). What the operator retained stays
// counted until the rows that hold it are replaced (rowGenerations), its
// working state is released (release), or the statement ends.
type memoryRetention struct {
	account memoryAccount
	rows    *rowEstimator // made on the first row reservation
	// outer is the outer row the last extended or joined row counted in
	// full (outerBindingBytes).
	outer unsafe.Pointer
}

// newMemoryRetention opens a retention owner in ctx's statement scope.
func newMemoryRetention(ctx context.Context) memoryRetention {
	return retentionIn(memoryScopeFromContext(ctx))
}

// retentionIn opens a retention owner in scope (nil: tracks nothing).
func retentionIn(scope *memoryScope) memoryRetention {
	return memoryRetention{account: memoryAccount{scope: scope}}
}

// tracking reports whether a limit is configured for the statement.
func (r *memoryRetention) tracking() bool {
	return r != nil && r.account.scope != nil
}

func (r *memoryRetention) estimator() *rowEstimator {
	if r.rows == nil {
		r.rows = &rowEstimator{known: r.account.scope.knownLists()}
	}
	return r.rows
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

// reserveValue reserves a value the operator keeps.
func (r *memoryRetention) reserveValue(value interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.growValue(value)
}

// reserveCollected reserves a value collect() appends to its list: a list or
// map it already counted (collect($big), a value every row shares) counts
// as the slot that references it, any other in full.
func (r *memoryRetention) reserveCollected(value interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	switch value.(type) {
	case []interface{}, map[string]interface{}:
		// At any depth: collect({xs: $big}) counts $big once.
		rows := r.estimator()
		if rows.shared == nil {
			rows.shared = make(map[sizedValueKey]struct{})
		}
		return r.account.grow(estimateHeldBytes(value, rows.shared))
	case []string, []int64, []float64, []float32:
		return r.account.grow(r.estimator().columnBytes(memoryCollectColumn, value))
	}
	return r.account.growValue(value)
}

// memoryCollectColumn is the column collected values are counted in.
const memoryCollectColumn = "\x00collect"

// reserveRow reserves a binding row the operator keeps (or a copy of it).
func (r *memoryRetention) reserveRow(row pipelineRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(r.estimator().rowBytes(row))
}

// reserveDerivedRow reserves a row the operator built from source, a row
// that stays counted while this one is kept (an expansion's input row): a
// binding it carries from source unchanged counts as its slot, any other in
// full. An operator that runs once per input row would otherwise count the
// values every input row shares once per run.
func (r *memoryRetention) reserveDerivedRow(source, row pipelineRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	rows := r.estimator()
	size := int64(pipelineRowExtraBytes + mapHeaderBytes)
	for name, value := range row {
		if carried, ok := source[name]; ok && sameBindingValue(carried, value) {
			size += mapEntryBytes + stringHeaderBytes + int64(len(name)) + valueSlotBytes
			continue
		}
		size += rows.bindingBytes(name, value)
	}
	return r.account.grow(size)
}

// sameBindingValue reports whether b is the value a holds, not a copy: the
// same list or map, entity or string.
func sameBindingValue(a, b interface{}) bool {
	switch x := a.(type) {
	case []interface{}:
		y, ok := b.([]interface{})
		return ok && len(x) == len(y) && unsafe.SliceData(x) == unsafe.SliceData(y)
	case map[string]interface{}:
		y, ok := b.(map[string]interface{})
		return ok && len(x) == len(y) && reflect.ValueOf(x).UnsafePointer() == reflect.ValueOf(y).UnsafePointer()
	case string:
		y, ok := b.(string)
		return ok && len(x) == len(y) && unsafe.StringData(x) == unsafe.StringData(y)
	case *storage.Node:
		y, ok := b.(*storage.Node)
		return ok && x == y
	case *storage.Edge:
		y, ok := b.(*storage.Edge)
		return ok && x == y
	case nil, bool, int64, float64:
		return a == b
	default:
		return false
	}
}

// reserveStoredRow reserves a row of values the operator stores by itself
// (a percentile's inputs): each value in full.
func (r *memoryRetention) reserveStoredRow(row map[string]interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes + mapHeaderBytes)
	for name, value := range row {
		size += mapEntryBytes + stringHeaderBytes + int64(len(name)) + estimateValueBytes(value)
	}
	return r.account.grow(size)
}

// reserveGroup reserves a new aggregation group: its first row, its key and
// its states.
func (r *memoryRetention) reserveGroup(row pipelineRow, key []interface{}, states int) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(r.estimator().rowBytes(row) + r.keyBytes(key) + int64(states)*aggregateStateBytes)
}

// reserveKey reserves a key the operator indexes (a DISTINCT key): its
// slots, and its values as a row's are counted.
func (r *memoryRetention) reserveKey(key []interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(r.keyBytes(key) + mapEntryBytes)
}

// reserveSortKey reserves a sort key's value, evaluated for holder's row,
// while the sort keeps it: its slot was reserved with the key's slots, and a
// value holder binds (ORDER BY a projected list or string) counts in the
// row. Any other (range(i, i + 9999), a string it built, a property's value)
// counts in full.
func (r *memoryRetention) reserveSortKey(value interface{}, holder pipelineRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	switch value.(type) {
	case nil, bool, int64, float64, *storage.Node, *storage.Edge:
		return nil
	}
	for _, held := range holder {
		if sameBindingValue(held, value) {
			return nil
		}
	}
	return r.account.grow(r.account.scope.knownLists().valueBytes(value) - valueSlotBytes)
}

// keyBytes is the estimated size of a key copied from a row's values: a
// list or map the operator already counted (a value every group or row
// shares) counts as its slot.
func (r *memoryRetention) keyBytes(key []interface{}) int64 {
	rows := r.estimator()
	size := int64(valueSlotBytes + sliceHeaderBytes)
	for _, value := range key {
		size += rows.columnBytes(memoryKeyColumn, value)
	}
	return size
}

// memoryKeyColumn is the column keys' values are counted in.
const memoryKeyColumn = "\x00key"

// noteCollected records the list the operator's collect() built from the
// values it reserved: rows that keep the list size it from that count
// instead of walking it again.
func (r *memoryRetention) noteCollected(values []interface{}) {
	if r != nil {
		r.account.noteList(values)
	}
}

// reserveMergedRow reserves the row merging outer's and inner's bindings,
// before it is built: outer's values in full the first time a merged row is
// built on outer (outerBindingBytes), inner's as slots. inner is a row the
// operator charged when it built it.
func (r *memoryRetention) reserveMergedRow(outer, inner pipelineRow) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(int64(pipelineRowExtraBytes+mapHeaderBytes) + r.outerBindingBytes(outer) + sharedBindingBytes(inner))
}

// reserveExtendedRow reserves the row extending outer with columns bound
// to values, before it is built: outer's values in full the first time a row
// is built on outer (outerBindingBytes), then values in full, or as slots
// when values is nil (the operator charged them where it got them).
func (r *memoryRetention) reserveExtendedRow(outer pipelineRow, columns []string, values []interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	size := int64(pipelineRowExtraBytes+mapHeaderBytes) + r.outerBindingBytes(outer)
	for index, column := range columns {
		if values == nil {
			size += mapEntryBytes + stringHeaderBytes + int64(len(column)) + valueSlotBytes
		} else if index < len(values) {
			size += r.estimator().bindingBytes(column, values[index])
		}
	}
	return r.account.grow(size)
}

// outerBindingBytes is what a row built on outer takes for outer's
// bindings. The first row built on outer counts its values in full: the
// rows outer belongs to give their charge back once the rows built on them
// replace them (rowGenerations). Every further row built on the same outer
// row shares its values and counts their slots.
func (r *memoryRetention) outerBindingBytes(outer pipelineRow) int64 {
	identity := reflect.ValueOf(outer).UnsafePointer()
	if identity != nil && identity == r.outer {
		return sharedBindingBytes(outer)
	}
	r.outer = identity
	rows := r.estimator()
	var size int64
	for name, value := range outer {
		size += rows.bindingBytes(name, value)
	}
	return size
}

// reserveResultRow reserves a result row the operator keeps.
func (r *memoryRetention) reserveResultRow(values []interface{}) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(estimateResultRowBytes(values, r.account.scope.knownLists()))
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
	rows := r.estimator()
	for name, value := range row.values {
		size += rows.bindingBytes(name, value)
	}
	return r.account.grow(size)
}

// reserveCandidates reserves the n candidate nodes of a MATCH an operator
// holds while it matches them: an index seek's, a label scan's, or seed
// nodes from anywhere else. Rows built from them count the nodes they bind;
// the candidates count while the operator holds them, also when the rows it
// builds from them don't stay (RETURN count(*)).
// collectPipelineInitialNodeCandidates reserves what it returns into the
// owner its caller passes, which gives it back (release) once it drops them.
func (r *memoryRetention) reserveCandidates(n int) error {
	if r == nil || r.account.scope == nil || n <= 0 {
		return nil
	}
	return r.account.grow(sliceHeaderBytes + saturatingProduct(int64(n), entityReferenceBytes))
}

// The entities a statement reads from storage in bulk are held by their
// owner from the moment they are read (#1343): a scan's or a seek's nodes, a
// type's or a node's relationships, an index's node IDs, a batch get. Every
// such read is passed straight to one of the held methods below, of the
// owner that keeps the list, and counts there until the owner is released;
// a function that only returns what it read passes it on to its caller,
// which holds it (TestEntityReadsHaveAnOwner). An ID list and the nodes read
// from it count once, as entity references. A list that replaces the last
// one (a node's relationships, read per node) is held by an owner released
// before each read. A failed reservation returns the limit's error and no
// list.

// heldNodes holds nodes read from storage.
func (r *memoryRetention) heldNodes(nodes []*storage.Node, err error) ([]*storage.Node, error) {
	if err != nil || r == nil || r.account.scope == nil {
		return nodes, err
	}
	if err := r.reserveCandidates(len(nodes)); err != nil {
		return nil, err
	}
	return nodes, nil
}

// heldNodeSlice holds nodes read from storage by a read that can't fail.
func (r *memoryRetention) heldNodeSlice(nodes []*storage.Node) ([]*storage.Node, error) {
	return r.heldNodes(nodes, nil)
}

// heldEdges holds relationships read from storage.
func (r *memoryRetention) heldEdges(edges []*storage.Edge, err error) ([]*storage.Edge, error) {
	if err != nil || r == nil || r.account.scope == nil {
		return edges, err
	}
	if err := r.reserveCandidates(len(edges)); err != nil {
		return nil, err
	}
	return edges, nil
}

// heldEdgeHeaders holds relationships read from storage by a reader that
// reports whether it could answer.
func (r *memoryRetention) heldEdgeHeaders(edges []*storage.Edge, answered bool, err error) ([]*storage.Edge, bool, error) {
	edges, err = r.heldEdges(edges, err)
	return edges, answered && err == nil, err
}

// heldAdjacentEdges holds a node's outgoing and incoming relationships, read
// together.
func (r *memoryRetention) heldAdjacentEdges(outgoing, incoming []*storage.Edge, err error) ([]*storage.Edge, []*storage.Edge, error) {
	if err != nil || r == nil || r.account.scope == nil {
		return outgoing, incoming, err
	}
	if err := r.reserveCandidates(len(outgoing) + len(incoming)); err != nil {
		return nil, nil, err
	}
	return outgoing, incoming, nil
}

// holdAppended holds the entities a storage visitor appended to a list the
// owner keeps: length is the list's length now, and held how many of them
// the owner holds, which it updates. It reserves them in steps of
// entityHoldStep as the list grows, and the rest once done.
func (r *memoryRetention) holdAppended(held *int, length int, done bool) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	if n := length - *held; n > 0 && (done || n >= entityHoldStep) {
		*held = length
		return r.reserveSlots(n, entityReferenceBytes)
	}
	return nil
}

// entityHoldStep is how many entities a visitor appends between reservations.
const entityHoldStep = 1024

// heldNodeList is a node list a storage visitor appends to, held in owner as
// it grows (holdAppended).
type heldNodeList struct {
	owner *memoryRetention
	nodes []*storage.Node
	held  int
}

// add appends nodes to the list.
func (l *heldNodeList) add(nodes ...*storage.Node) error {
	l.nodes = append(l.nodes, nodes...)
	return l.owner.holdAppended(&l.held, len(l.nodes), false)
}

// done holds the nodes added since the last step.
func (l *heldNodeList) done() error {
	return l.owner.holdAppended(&l.held, len(l.nodes), true)
}

// heldNodeIDs holds node IDs read from storage.
func (r *memoryRetention) heldNodeIDs(ids []storage.NodeID, err error) ([]storage.NodeID, error) {
	if err != nil || r == nil || r.account.scope == nil {
		return ids, err
	}
	if err := r.reserveCandidates(len(ids)); err != nil {
		return nil, err
	}
	return ids, nil
}

// heldIndexIDs holds the node IDs an index lookup found.
func (r *memoryRetention) heldIndexIDs(ids []storage.NodeID) ([]storage.NodeID, error) {
	return r.heldNodeIDs(ids, nil)
}

// heldIndexRange holds the node IDs an index range found (ok: the index
// answered).
func (r *memoryRetention) heldIndexRange(ids []storage.NodeID, ok bool) ([]storage.NodeID, bool, error) {
	ids, err := r.heldNodeIDs(ids, nil)
	return ids, ok && err == nil, err
}

// heldNodeMap holds nodes a batch get read, by ID.
func (r *memoryRetention) heldNodeMap(nodes map[storage.NodeID]*storage.Node, err error) (map[storage.NodeID]*storage.Node, error) {
	if err != nil || r == nil || r.account.scope == nil {
		return nodes, err
	}
	if err := r.reserveCandidates(len(nodes)); err != nil {
		return nil, err
	}
	return nodes, nil
}

// failed reports whether the owner's statement failed over a memory limit:
// an operator that would otherwise fall back to another plan stops instead.
func (r *memoryRetention) failed() bool {
	return r != nil && r.account.scope != nil && r.account.scope.tracker.err() != nil
}

// reserveCacheKey reserves the entry of a cache the operator keeps a value
// under, keyed by key: its slot and the key's text.
func (r *memoryRetention) reserveCacheKey(key string) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(mapEntryBytes + stringHeaderBytes + int64(len(key)))
}

// reservePath reserves a matched path of nodes and relationships.
func (r *memoryRetention) reservePath(nodes, relationships int) error {
	if r == nil || r.account.scope == nil {
		return nil
	}
	return r.account.grow(estimatePathResultBytes(nodes, relationships))
}

// owned declares rows the operator charged in full, carried values
// included, and returns them: the clause pipeline gives back the charge of
// the rows they replace (rowGenerations). Rows an operator only partly
// charged (passed through, filled in by another) are not declared, and the
// rows they replace stay counted.
func (r *memoryRetention) owned(rows []pipelineRow) []pipelineRow {
	if r != nil && r.account.scope != nil {
		r.account.scope.ownRows(rows)
	}
	return rows
}

// keep hands on rows, what the operator keeps of the reserved rows it built
// (reserved of them): DISTINCT, WHERE, SKIP and LIMIT drop some, and rows
// kept only to order the others go once they're sorted. When it keeps them
// all they stay charged as they were reserved; otherwise what it reserved
// goes back and the rows it keeps are charged again, so the rows it drops
// count no more. The rows it keeps move to an array of their own: SKIP and
// LIMIT reslice, and WHERE filters in place, so the array they came in still
// holds the rows dropped, which would stay live past their charge. Either
// way they are declared owned. false: the charge went over a limit.
func (r *memoryRetention) keep(rows []pipelineRow, reserved int) ([]pipelineRow, bool) {
	if r == nil || r.account.scope == nil {
		return rows, true
	}
	if len(rows) != reserved {
		r.release()
		for _, row := range rows {
			if r.reserveRow(row) != nil {
				return nil, false
			}
		}
		rows = append(make([]pipelineRow, 0, len(rows)), rows...)
	}
	return r.owned(rows), true
}

// close returns the unspent credit.
func (r *memoryRetention) close() {
	if r != nil {
		r.account.close()
	}
}

// release gives back everything the owner reserved, for a working buffer
// the operator drops when it is done (a sort buffer, a distinct index, a
// search's frontiers): what it retained is not kept past the operator.
func (r *memoryRetention) release() {
	if r == nil || r.account.scope == nil {
		return
	}
	if taken := r.account.spent + r.account.credit; taken > 0 {
		r.account.scope.give(taken)
	}
	r.account.spent, r.account.credit = 0, 0
	// What it counted is no longer counted: a value it keeps again counts again.
	r.rows = nil
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

// rowGenerations keeps a clause pipeline's estimate at the rows it holds
// now: each clause's output replaces its input, which is garbage once the
// next clause has built its rows. Without it the estimate was the sum of
// every clause's output: 4.9 GiB counted while the statement held 0.3 GiB.
//
// The pipeline runs its clauses one after another, so what the statement
// took between two clauses is the clause's. When a clause hands on new rows
// that its operator charged in full, carried values included (owned,
// charge), what the clause took becomes their charge and the rows they
// replace give theirs back. Otherwise (rows passed through, filtered, built
// partly, or streamed for the next clause to build) what the clause took
// adds to the current rows' charge and goes back with it: the estimate
// never drops a value rows still hold.
type rowGenerations struct {
	scope *memoryScope
	base  int64 // the scope's use when the pipeline began; giving back never goes below it
	mark  int64 // the scope's use when the last clause began
	held  int64 // what the current rows hold
	data  unsafe.Pointer
	count int
}

func newRowGenerations(scope *memoryScope, rows []pipelineRow) rowGenerations {
	if scope == nil {
		return rowGenerations{}
	}
	used := scope.mark()
	return rowGenerations{scope: scope, base: used, mark: used, data: unsafe.Pointer(unsafe.SliceData(rows)), count: len(rows)}
}

// next is called before each clause, with the rows the clause will read.
func (g *rowGenerations) next(rows []pipelineRow) {
	if g.scope == nil {
		return
	}
	used := g.scope.mark()
	taken := max(used-g.mark, 0)
	data := unsafe.Pointer(unsafe.SliceData(rows))
	if rows != nil && (data != g.data || len(rows) != g.count) && g.scope.ownsRows(rows) {
		if give := min(g.held, used-g.base); give > 0 {
			g.scope.give(give)
			used -= give
		}
		g.held = taken
	} else {
		g.held += taken
	}
	if rows != nil {
		g.data, g.count = data, len(rows)
	}
	g.mark = used
}

// charge charges rows a clause built, every row in full with its carried
// values, and declares them owned. Rows their operator already charged in
// full (owned) are not charged again.
func (g *rowGenerations) charge(rows []pipelineRow) error {
	if g.scope == nil || g.scope.ownsRows(rows) {
		return nil
	}
	if err := g.scope.growRows(rows); err != nil {
		return err
	}
	g.scope.ownRows(rows)
	return nil
}

// memorySpan is what a scope took over a stretch of an operator's work
// (begin to end): working state, such as an aggregation's groups or an
// expansion's candidates, that the operator gives back once the result
// that replaces it is charged (giveBack).
type memorySpan struct {
	scope        *memoryScope
	start, taken int64
}

// beginMemorySpan starts a span in ctx's statement scope.
func beginMemorySpan(ctx context.Context) memorySpan {
	scope := memoryScopeFromContext(ctx)
	return memorySpan{scope: scope, start: scope.mark()}
}

// end records what the scope took since the span began.
func (p *memorySpan) end() {
	if p.scope != nil {
		p.taken = max(p.scope.mark()-p.start, 0)
	}
}

// giveBack gives back what the span took (end), never more than the scope
// took since the span began.
func (p *memorySpan) giveBack() {
	if p.scope == nil || p.taken <= 0 {
		return
	}
	if give := min(p.taken, p.scope.mark()-p.start); give > 0 {
		p.scope.give(give)
	}
	p.taken = 0
}

// transientCheckBytes is the smallest value build an expression checks
// against the statement's limits: smaller builds cost less than the check.
const transientCheckBytes = 64 << 10

// expressionBudget is the statement's check on the values an expression
// builds: range() lists, comprehension results, reduce accumulators, an
// UNWIND's evaluated list while it streams. A build holds what it will take
// before it allocates (an accumulator, as it grows) and gives it back once
// built: the row or list that keeps the value counts it there. A build over
// a limit fails the statement before it allocates; an accumulator, within
// one step of its growth. The zero value checks nothing, and nothing is
// sized when no limit is configured.
type expressionBudget struct {
	scope *memoryScope
	held  int64
	// built is what a list being built takes so far (beginList), checked
	// what it took when last checked (checkList), and seen the lists and
	// maps it counted (addElement).
	built, checked int64
	seen           map[sizedValueKey]struct{}
	// counted and elementBytes size a list accumulator by its new elements
	// (holdAccumulator).
	counted      int
	elementBytes int64
}

// expressionBudgetOf is the budget of the statement ctx runs.
func expressionBudgetOf(ctx context.Context) expressionBudget {
	return expressionBudget{scope: memoryScopeFromContext(ctx)}
}

// rowExpressionBudget is the budget of the statement a row carries
// (rowStatementContextKey; bound while a limit is configured, for an
// expression that builds values).
func rowExpressionBudget(values map[string]interface{}) expressionBudget {
	if !transactionMemoryTracking() {
		return expressionBudget{}
	}
	return expressionBudget{scope: memoryScopeFromContext(rowStatementContext(values))}
}

// active reports whether the budget checks anything.
func (b *expressionBudget) active() bool {
	return b != nil && b.scope != nil
}

// holdBytes makes the budget hold size bytes: what the build takes now.
func (b *expressionBudget) holdBytes(size int64) error {
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

// holdSlots holds a list of n slots of slotBytes each, before it is built.
func (b *expressionBudget) holdSlots(n, slotBytes int64) error {
	if !b.active() {
		return nil
	}
	return b.holdBytes(saturatingProduct(n, slotBytes) + sliceHeaderBytes)
}

// holdList holds an evaluated list while its elements stream: its slots,
// whose values count where they are kept (a row's list, a parameter).
func (b *expressionBudget) holdList(items []interface{}) error {
	if !b.active() {
		return nil
	}
	return b.holdBytes(shallowValueBytes(items))
}

// holdBuiltList holds a list the expression just built while its elements
// stream, with what its elements hold: its builders gave their reservation
// back, and nothing else holds it (UNWIND [x IN … | {xs: range(…)}]). A
// list or map its elements share counts once.
func (b *expressionBudget) holdBuiltList(items []interface{}) error {
	if !b.active() {
		return nil
	}
	return b.holdBytes(estimateHeldBytes(items, make(map[sizedValueKey]struct{})))
}

// beginList holds a list built from n items: its capacity, before it is
// made. addElement adds each value it gets; checkList holds what it built.
func (b *expressionBudget) beginList(n int) error {
	if !b.active() {
		return nil
	}
	b.built = sliceHeaderBytes + int64(n)*valueSlotBytes
	b.checked = b.built
	b.seen = nil
	return b.holdBytes(b.built)
}

// addElement adds the value the list being built got for item (beginList),
// with what it holds: the builder owns what the element built (a map's
// list, a list's maps), whose own builders gave back what they held. A list
// or map that is item, or that an earlier element holds, counts as its slot:
// the input holds item, and the builder counted the other already.
func (b *expressionBudget) addElement(item, value interface{}) {
	if !b.active() {
		return
	}
	if sameBindingValue(item, value) {
		b.built += shallowValueBytes(value)
		return
	}
	if b.seen == nil {
		b.seen = make(map[sizedValueKey]struct{})
	}
	if key, shared := sharedValueKey(item); shared {
		b.seen[key] = struct{}{}
	}
	b.built += estimateHeldBytes(value, b.seen)
}

// checkDue reports whether the list being built grew by transientCheckBytes
// since it was last checked (checkList): an element can hold a large value.
func (b *expressionBudget) checkDue() bool {
	return b.active() && b.built-b.checked >= transientCheckBytes
}

// checkList holds what the list being built took so far (beginList).
func (b *expressionBudget) checkList() error {
	if !b.active() {
		return nil
	}
	b.checked = b.built
	return b.holdBytes(b.built)
}

// holdAccumulator holds a reduce accumulator after a step. A list
// accumulator grows by its new elements (acc + [x]): those are sized as
// they come, so a step costs what it added, not the whole list. A map
// accumulator (apoc.map.setKey(m, k, v)) counts the lists and maps its
// entries hold once each, by identity, as they first appear: the step that
// built the map walked its entries already.
func (b *expressionBudget) holdAccumulator(accumulator interface{}) error {
	if !b.active() {
		return nil
	}
	size := shallowValueBytes(accumulator)
	switch v := accumulator.(type) {
	case []interface{}:
		if len(v) < b.counted {
			b.counted, b.elementBytes = 0, 0
		}
		for _, element := range v[b.counted:] {
			b.elementBytes += estimateValueBytes(element)
		}
		b.counted = len(v)
		size = valueSlotBytes + sliceHeaderBytes + int64(cap(v)-len(v))*valueSlotBytes + b.elementBytes
	case map[string]interface{}:
		if b.seen == nil {
			b.seen = make(map[sizedValueKey]struct{})
		}
		for _, element := range v {
			if key, shared := sharedValueKey(element); shared {
				if _, counted := b.seen[key]; !counted {
					// The entry's slot is in the map's shallow size.
					b.elementBytes += estimateHeldBytes(element, b.seen) - valueSlotBytes
				}
			}
		}
		size += b.elementBytes
	}
	return b.holdBytes(size)
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
	observeSizing()
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
// is configured, nil otherwise: for an operator whose closure would
// otherwise capture an owner it never uses (every method is nil-safe).
func trackedRetention(ctx context.Context) *memoryRetention {
	if !transactionMemoryTracking() {
		return nil
	}
	retention := newMemoryRetention(ctx)
	if !retention.tracking() {
		return nil
	}
	return &retention
}

// sharedBindingBytes is what a new row takes for bindings it shares with an
// existing row: an entry, its key and a value slot each. The values belong
// to the row that already holds them and were counted there.
func sharedBindingBytes(row pipelineRow) int64 {
	observeSizing()
	var size int64
	for name := range row {
		size += mapEntryBytes + stringHeaderBytes + int64(len(name)) + valueSlotBytes
	}
	return size
}
