package fabric

import (
	"context"
	"sync"
)

// ResultStream holds the tabular output of a fragment execution.
// It carries column names and rows in the same shape as cypher.ExecuteResult.
type ResultStream struct {
	// Columns is the ordered list of column names.
	Columns []string

	// Rows holds the result data. Each row has len(Columns) elements.
	Rows [][]interface{}
}

// RowIterator iterates result rows without requiring callers to index into a
// materialized [][]interface{} directly.
type RowIterator interface {
	Next() bool
	Row() []interface{}
	Err() error
	Close() error
}

// RowView exposes row values lazily by index.
type RowView interface {
	Len() int
	At(i int) interface{}
	Materialize() []interface{}
}

type sliceRowIterator struct {
	rows [][]interface{}
	idx  int
}

type sliceRowView struct {
	row []interface{}
}

func NewSliceRowView(row []interface{}) RowView {
	return &sliceRowView{row: row}
}

func (r *sliceRowView) Len() int {
	if r == nil {
		return 0
	}
	return len(r.row)
}

func (r *sliceRowView) At(i int) interface{} {
	if r == nil || i < 0 || i >= len(r.row) {
		return nil
	}
	return r.row[i]
}

func (r *sliceRowView) Materialize() []interface{} {
	if r == nil || len(r.row) == 0 {
		return nil
	}
	out := make([]interface{}, len(r.row))
	copy(out, r.row)
	return out
}

type joinedRowView struct {
	outer     []interface{}
	inner     []interface{}
	fromOuter []int
	fromInner []int
}

// NewJoinedRowView creates a lazy joined row view for precomputed column mappings.
func NewJoinedRowView(outer []interface{}, inner []interface{}, fromOuter []int, fromInner []int) RowView {
	return &joinedRowView{
		outer:     outer,
		inner:     inner,
		fromOuter: fromOuter,
		fromInner: fromInner,
	}
}

func (r *joinedRowView) Len() int {
	if r == nil {
		return 0
	}
	return len(r.fromOuter)
}

func (r *joinedRowView) At(i int) interface{} {
	if r == nil || i < 0 || i >= len(r.fromOuter) {
		return nil
	}
	if idx := r.fromInner[i]; idx >= 0 && idx < len(r.inner) {
		return r.inner[idx]
	}
	if idx := r.fromOuter[i]; idx >= 0 && idx < len(r.outer) {
		return r.outer[idx]
	}
	return nil
}

func (r *joinedRowView) Materialize() []interface{} {
	if r == nil {
		return nil
	}
	out := make([]interface{}, len(r.fromOuter))
	for i := 0; i < len(r.fromOuter); i++ {
		out[i] = r.At(i)
	}
	return out
}

type convertingRowIterator struct {
	base    RowIterator
	convert func([]interface{}) []interface{}
	cur     []interface{}
}

// NewConvertingRowIterator wraps an iterator and lazily converts each row on Row().
func NewConvertingRowIterator(base RowIterator, convert func([]interface{}) []interface{}) RowIterator {
	if base == nil {
		base = NewResultRowIterator(nil)
	}
	if convert == nil {
		convert = func(in []interface{}) []interface{} { return in }
	}
	return &convertingRowIterator{base: base, convert: convert}
}

func (it *convertingRowIterator) Next() bool {
	if it == nil {
		return false
	}
	if !it.base.Next() {
		it.cur = nil
		return false
	}
	it.cur = nil
	return true
}

func (it *convertingRowIterator) Row() []interface{} {
	if it == nil {
		return nil
	}
	if it.cur != nil {
		return it.cur
	}
	it.cur = it.convert(it.base.Row())
	return it.cur
}

func (it *convertingRowIterator) Err() error {
	if it == nil || it.base == nil {
		return nil
	}
	return it.base.Err()
}

func (it *convertingRowIterator) Close() error {
	if it == nil || it.base == nil {
		return nil
	}
	return it.base.Close()
}

type prefetchRowIterator struct {
	ctx    context.Context
	cancel context.CancelFunc
	base   RowIterator
	rows   chan []interface{}
	once   sync.Once
	errMu  sync.Mutex
	err    error
	cur    []interface{}
}

type concatRowIterator struct {
	its []RowIterator
	cur int
	row []interface{}
	err error
}

type distinctRowIterator struct {
	base RowIterator
	seen map[uint64]struct{}
	row  []interface{}
	err  error
}

// NewPrefetchRowIterator creates a bounded prefetch iterator with backpressure.
func NewPrefetchRowIterator(ctx context.Context, base RowIterator, buffer int) RowIterator {
	if base == nil {
		base = NewResultRowIterator(nil)
	}
	if buffer < 1 {
		buffer = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	it := &prefetchRowIterator{
		ctx:    runCtx,
		cancel: cancel,
		base:   base,
		rows:   make(chan []interface{}, buffer),
	}
	go it.produce()
	return it
}

func (it *prefetchRowIterator) produce() {
	defer close(it.rows)
	defer func() {
		if err := it.base.Close(); err != nil {
			it.setErr(err)
		}
	}()
	for it.base.Next() {
		row := it.base.Row()
		select {
		case <-it.ctx.Done():
			return
		case it.rows <- row:
		}
	}
	if err := it.base.Err(); err != nil {
		it.setErr(err)
	}
}

func (it *prefetchRowIterator) setErr(err error) {
	if err == nil {
		return
	}
	it.errMu.Lock()
	if it.err == nil {
		it.err = err
	}
	it.errMu.Unlock()
}

func (it *prefetchRowIterator) Next() bool {
	if it == nil {
		return false
	}
	select {
	case <-it.ctx.Done():
		return false
	case row, ok := <-it.rows:
		if !ok {
			it.cur = nil
			return false
		}
		it.cur = row
		return true
	}
}

func (it *prefetchRowIterator) Row() []interface{} {
	if it == nil {
		return nil
	}
	return it.cur
}

func (it *prefetchRowIterator) Err() error {
	if it == nil {
		return nil
	}
	it.errMu.Lock()
	defer it.errMu.Unlock()
	return it.err
}

func (it *prefetchRowIterator) Close() error {
	if it == nil {
		return nil
	}
	it.once.Do(func() { it.cancel() })
	return nil
}

// NewConcatRowIterator returns a row iterator that reads iterators in order.
func NewConcatRowIterator(iterators ...RowIterator) RowIterator {
	clean := make([]RowIterator, 0, len(iterators))
	for _, it := range iterators {
		if it != nil {
			clean = append(clean, it)
		}
	}
	return &concatRowIterator{its: clean}
}

func (it *concatRowIterator) Next() bool {
	if it == nil {
		return false
	}
	for it.cur < len(it.its) {
		cur := it.its[it.cur]
		if cur.Next() {
			it.row = cur.Row()
			return true
		}
		if err := cur.Err(); err != nil && it.err == nil {
			it.err = err
		}
		_ = cur.Close()
		it.cur++
	}
	it.row = nil
	return false
}

func (it *concatRowIterator) Row() []interface{} {
	if it == nil {
		return nil
	}
	return it.row
}

func (it *concatRowIterator) Err() error {
	if it == nil {
		return nil
	}
	if it.err != nil {
		return it.err
	}
	for i := it.cur; i < len(it.its); i++ {
		if err := it.its[i].Err(); err != nil {
			return err
		}
	}
	return nil
}

func (it *concatRowIterator) Close() error {
	if it == nil {
		return nil
	}
	var first error
	for i := it.cur; i < len(it.its); i++ {
		if err := it.its[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// NewDistinctRowIterator filters duplicate rows while streaming.
func NewDistinctRowIterator(base RowIterator) RowIterator {
	if base == nil {
		base = NewResultRowIterator(nil)
	}
	return &distinctRowIterator{
		base: base,
		seen: make(map[uint64]struct{}),
	}
}

func (it *distinctRowIterator) Next() bool {
	if it == nil {
		return false
	}
	for it.base.Next() {
		row := it.base.Row()
		key := hashRow(row)
		if _, exists := it.seen[key]; exists {
			continue
		}
		it.seen[key] = struct{}{}
		it.row = row
		return true
	}
	it.row = nil
	if err := it.base.Err(); err != nil && it.err == nil {
		it.err = err
	}
	return false
}

func (it *distinctRowIterator) Row() []interface{} {
	if it == nil {
		return nil
	}
	return it.row
}

func (it *distinctRowIterator) Err() error {
	if it == nil {
		return nil
	}
	if it.err != nil {
		return it.err
	}
	return it.base.Err()
}

func (it *distinctRowIterator) Close() error {
	if it == nil || it.base == nil {
		return nil
	}
	return it.base.Close()
}

// NewResultRowIterator wraps a ResultStream as a RowIterator.
func NewResultRowIterator(r *ResultStream) RowIterator {
	if r == nil || len(r.Rows) == 0 {
		return &sliceRowIterator{rows: nil, idx: -1}
	}
	return &sliceRowIterator{rows: r.Rows, idx: -1}
}

func (it *sliceRowIterator) Next() bool {
	if it == nil {
		return false
	}
	next := it.idx + 1
	if next >= len(it.rows) {
		return false
	}
	it.idx = next
	return true
}

func (it *sliceRowIterator) Row() []interface{} {
	if it == nil || it.idx < 0 || it.idx >= len(it.rows) {
		return nil
	}
	return it.rows[it.idx]
}

func (it *sliceRowIterator) Err() error { return nil }

func (it *sliceRowIterator) Close() error { return nil }

// Empty returns true if the result stream has no rows.
func (r *ResultStream) Empty() bool {
	return r == nil || len(r.Rows) == 0
}

// RowCount returns the number of rows.
func (r *ResultStream) RowCount() int {
	if r == nil {
		return 0
	}
	return len(r.Rows)
}

// Merge appends the rows from other into this result stream.
// Columns must match. If this stream has no columns yet, they are adopted from other.
func (r *ResultStream) Merge(other *ResultStream) {
	if other == nil || len(other.Rows) == 0 {
		return
	}
	if len(r.Columns) == 0 {
		r.Columns = other.Columns
	} else if !sameColumns(r.Columns, other.Columns) {
		return
	}
	r.Rows = append(r.Rows, other.Rows...)
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// UnionColumnOrder is the shared column-name contract for local and Fabric UNION.
func UnionColumnOrder(columns, branch []string) (order []int, sameNames bool) {
	if len(columns) != len(branch) {
		return nil, false
	}
	inOrder := true
	for index := range columns {
		if columns[index] != branch[index] {
			inOrder = false
			break
		}
	}
	if inOrder {
		return nil, true
	}
	positions := make(map[string]int, len(branch))
	for index, name := range branch {
		positions[name] = index
	}
	order = make([]int, len(columns))
	for index, name := range columns {
		position, found := positions[name]
		if !found {
			return nil, false
		}
		order[index] = position
	}
	return order, true
}
