package fabric

import (
	"context"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// ResultStream holds the tabular output of a fragment execution.
// It carries column names and rows in the same shape as cypher.ExecuteResult.
type ResultStream struct {
	// Columns is the ordered list of column names.
	Columns []string

	// Rows holds the result data. Each row has len(Columns) elements.
	Rows [][]interface{}

	// RowWrapper marks a result whose one column holds the APPLY pipeline's
	// row wrapper maps (executeApplyAsPipeline), not a query column. Only
	// the pipeline sets it, so no query column is ever read as the wrapper,
	// whatever its name.
	RowWrapper bool
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
	ctx         context.Context
	work        *lifecycle.WorkLifetime
	base        RowIterator
	rows        chan []interface{}
	nativeClose lifecycle.OnceCompletion // the base's one close: EOF, Close or cancellation
	stopSignal  func() bool
	cur         []interface{}
}

type concatRowIterator struct {
	its      []RowIterator
	cur      atomic.Int64
	closed   atomic.Bool
	row      []interface{}
	outcomes lifecycle.Outcomes
	closes   []lifecycle.OnceCompletion
}

type distinctRowIterator struct {
	base RowIterator
	seen map[uint64]struct{}
	row  []interface{}
	err  error

	// added, when set, owns row identity: it reports whether an equivalent
	// row was already added.
	added func([]interface{}) bool
}

// NewPrefetchRowIterator owns its producer before dispatch and preserves
// bounded backpressure. Close signals the base before joining real completion.
func NewPrefetchRowIterator(ctx context.Context, base RowIterator, buffer int) RowIterator {
	if base == nil {
		base = NewResultRowIterator(nil)
	}
	if buffer < 1 {
		buffer = 1
	}
	work := lifecycle.NewWorkLifetime(ctx)
	_, release, accepted := work.Acquire(work.Context())
	if !accepted {
		panic("fabric: new iterator lifetime refused its producer")
	}
	it := &prefetchRowIterator{ctx: work.Context(), work: work, base: base,
		rows: make(chan []interface{}, buffer)}
	// The producer obligation is enrolled before this stop signal is armed.
	// The callback only dispatches the same native stop owner; it never declares
	// native or producer completion, and can do no resource work after that owner.
	it.stopSignal = context.AfterFunc(it.ctx, func() { it.beginStop() })
	go func() { defer release(); it.produce() }()
	return it
}

// beginStop dispatches the base's one native close, shared by EOF, explicit
// Close and cancellation. The producer joins it before returning its ticket,
// so the close is always retained by the iterator's admitted producer.
func (it *prefetchRowIterator) beginStop() <-chan struct{} {
	return it.nativeClose.Go(func() error {
		err := it.base.Close()
		it.work.RecordError("iterator close", err)
		return err
	})
}
func (it *prefetchRowIterator) produce() {
	defer func() { <-it.beginStop(); it.stopSignal(); close(it.rows) }()
	if it.ctx.Err() != nil {
		return
	}
	for it.base.Next() {
		row := it.base.Row()
		select {
		case <-it.ctx.Done():
			return
		case it.rows <- row:
		}
	}
	it.work.RecordError("iterator producer", it.base.Err())
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
	if err := it.work.Err(); err != nil {
		return err
	}
	return it.ctx.Err()
}
func (it *prefetchRowIterator) Close() error {
	if it == nil {
		return nil
	}
	return it.work.Shutdown(context.Background(), nil)
}

// NewConcatRowIterator owns each child close once. Concurrent stop may interrupt
// a producer without holding the cursor/error publication lock across its code.
func NewConcatRowIterator(iterators ...RowIterator) RowIterator {
	clean := make([]RowIterator, 0, len(iterators))
	for _, it := range iterators {
		if it != nil {
			clean = append(clean, it)
		}
	}
	return &concatRowIterator{its: clean, closes: make([]lifecycle.OnceCompletion, len(clean))}
}
func (it *concatRowIterator) closeChild(index int) error {
	return it.closes[index].Do(func() error {
		err := it.its[index].Close()
		it.outcomes.Record("", err)
		return err
	})
}

func (it *concatRowIterator) Next() bool {
	if it == nil {
		return false
	}
	for !it.closed.Load() {
		index := int(it.cur.Load())
		if index >= len(it.its) {
			break
		}
		current := it.its[index]
		if current.Next() {
			if it.closed.Load() {
				return false
			}
			it.row = current.Row()
			return true
		}
		err := current.Err()
		it.outcomes.Record("", err)
		it.closeChild(index)
		it.cur.Store(int64(index + 1))
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
	err := it.outcomes.Err()
	// Foreign Err implementations run outside the owner's publication mutex.
	for i := int(it.cur.Load()); i < len(it.its); i++ {
		err = compose.Outcomes(err, it.its[i].Err())
	}
	return err
}
func (it *concatRowIterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed.Store(true)
	for i := int(it.cur.Load()); i < len(it.its); i++ {
		it.closeChild(i)
	}
	return it.Err()
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
		if it.added != nil {
			if it.added(row) {
				continue
			}
		} else {
			key := hashRow(row)
			if _, exists := it.seen[key]; exists {
				continue
			}
			it.seen[key] = struct{}{}
		}
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

// resultRows adapts a completed local or remote execution to the row protocol.
func resultRows(result *ResultStream, err error) ([]string, RowIterator, error) {
	if err != nil {
		return nil, nil, err
	}
	var columns []string
	if result != nil {
		columns = result.Columns
	}
	return columns, NewResultRowIterator(result), nil
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
