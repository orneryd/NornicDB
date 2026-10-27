package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultStream_Empty(t *testing.T) {
	var nilStream *ResultStream
	if !nilStream.Empty() {
		t.Error("nil stream should be empty")
	}

	emptyStream := &ResultStream{}
	if !emptyStream.Empty() {
		t.Error("empty stream should be empty")
	}

	nonEmpty := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}
	if nonEmpty.Empty() {
		t.Error("non-empty stream should not be empty")
	}
}

func TestResultStream_RowCount(t *testing.T) {
	var nilStream *ResultStream
	if nilStream.RowCount() != 0 {
		t.Errorf("nil stream should have 0 rows, got %d", nilStream.RowCount())
	}

	stream := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}, {2}, {3}},
	}
	if stream.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", stream.RowCount())
	}
}

func TestResultStream_Merge(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}, {2}},
	}
	b := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{3}, {4}},
	}

	a.Merge(b)
	if a.RowCount() != 4 {
		t.Errorf("expected 4 rows after merge, got %d", a.RowCount())
	}
}

func TestResultStream_MergeAdoptsColumns(t *testing.T) {
	a := &ResultStream{}
	b := &ResultStream{
		Columns: []string{"y"},
		Rows:    [][]interface{}{{99}},
	}

	a.Merge(b)
	if len(a.Columns) != 1 || a.Columns[0] != "y" {
		t.Errorf("expected columns [y], got %v", a.Columns)
	}
}

func TestResultStream_MergeNil(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}

	a.Merge(nil)
	if a.RowCount() != 1 {
		t.Errorf("expected 1 row after nil merge, got %d", a.RowCount())
	}
}

func TestResultStream_MergeEmptyRows(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}
	b := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{},
	}

	a.Merge(b)
	if a.RowCount() != 1 {
		t.Errorf("expected 1 row after empty merge, got %d", a.RowCount())
	}
}

func TestResultStream_MergeSkipsMismatchedColumns(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}
	b := &ResultStream{
		Columns: []string{"y"},
		Rows:    [][]interface{}{{2}},
	}

	a.Merge(b)
	if got := a.RowCount(); got != 1 {
		t.Fatalf("expected mismatched merge to be ignored, got %d rows", got)
	}
}

func TestResultRowIterator(t *testing.T) {
	stream := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}, {2}},
	}
	it := NewResultRowIterator(stream)
	defer func() { _ = it.Close() }()

	if !it.Next() {
		t.Fatal("expected first row")
	}
	if got := it.Row(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("unexpected first row: %v", got)
	}
	if !it.Next() {
		t.Fatal("expected second row")
	}
	if got := it.Row(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("unexpected second row: %v", got)
	}
	if it.Next() {
		t.Fatal("expected iterator end")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("unexpected iterator err: %v", err)
	}
}

func TestResultRowsRetainsExecutionOutcome(t *testing.T) {
	failure := errors.New("execution rejected")
	for _, tc := range []struct {
		name    string
		result  *ResultStream
		err     error
		columns []string
		rows    [][]interface{}
	}{
		{name: "failed result", result: &ResultStream{Columns: []string{"discarded"}, Rows: [][]interface{}{{1}}}, err: failure},
		{name: "nil result"},
		{name: "empty result", result: &ResultStream{Columns: []string{"value"}}, columns: []string{"value"}},
		{name: "rows", result: &ResultStream{Columns: []string{"value"}, Rows: [][]interface{}{{1}, {2}}}, columns: []string{"value"}, rows: [][]interface{}{{1}, {2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns, iterator, err := resultRows(tc.result, tc.err)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.columns, columns)
			if tc.err != nil {
				require.Nil(t, iterator)
				return
			}
			require.NotNil(t, iterator)
			var rows [][]interface{}
			for iterator.Next() {
				rows = append(rows, iterator.Row())
			}
			require.Equal(t, tc.rows, rows)
			require.NoError(t, iterator.Err())
			require.NoError(t, iterator.Close())
		})
	}
}

func TestJoinedRowView(t *testing.T) {
	outer := []interface{}{"s-1", "k-1"}
	inner := []interface{}{"en", "hello"}
	view := NewJoinedRowView(
		outer,
		inner,
		[]int{0, 1, -1, -1},
		[]int{-1, -1, 0, 1},
	)
	got := view.Materialize()
	if len(got) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(got))
	}
	if got[0] != "s-1" || got[1] != "k-1" || got[2] != "en" || got[3] != "hello" {
		t.Fatalf("unexpected joined row: %#v", got)
	}
}

func TestConvertingRowIterator(t *testing.T) {
	base := NewResultRowIterator(&ResultStream{
		Columns: []string{"v"},
		Rows:    [][]interface{}{{"a"}, {"b"}},
	})
	it := NewConvertingRowIterator(base, func(row []interface{}) []interface{} {
		out := make([]interface{}, len(row))
		copy(out, row)
		out[0] = "x-" + row[0].(string)
		return out
	})
	defer func() { _ = it.Close() }()

	if !it.Next() || it.Row()[0] != "x-a" {
		t.Fatalf("unexpected first converted row: %#v", it.Row())
	}
	if !it.Next() || it.Row()[0] != "x-b" {
		t.Fatalf("unexpected second converted row: %#v", it.Row())
	}
	if it.Next() {
		t.Fatal("expected iterator end")
	}
}

func TestPrefetchRowIterator(t *testing.T) {
	base := NewResultRowIterator(&ResultStream{
		Columns: []string{"i"},
		Rows:    [][]interface{}{{1}, {2}, {3}},
	})
	it := NewPrefetchRowIterator(context.Background(), base, 2)
	defer func() { _ = it.Close() }()

	sum := 0
	for it.Next() {
		row := it.Row()
		sum += row[0].(int)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("unexpected prefetch iterator error: %v", err)
	}
	if sum != 6 {
		t.Fatalf("unexpected row sum: %d", sum)
	}
}

func TestConcatAndDistinctRowIterator(t *testing.T) {
	a := NewResultRowIterator(&ResultStream{
		Columns: []string{"v"},
		Rows:    [][]interface{}{{"a"}, {"b"}},
	})
	b := NewResultRowIterator(&ResultStream{
		Columns: []string{"v"},
		Rows:    [][]interface{}{{"b"}, {"c"}},
	})

	it := NewDistinctRowIterator(NewConcatRowIterator(a, b))
	defer func() { _ = it.Close() }()
	var got []string
	for it.Next() {
		got = append(got, it.Row()[0].(string))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("unexpected iterator error: %v", err)
	}
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("unexpected concat/distinct rows: %#v", got)
	}
}
