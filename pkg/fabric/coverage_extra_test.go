package fabric

import (
	"context"
	"testing"
)

// --- SliceRowView ---

func TestSliceRowView_Len(t *testing.T) {
	v := NewSliceRowView([]interface{}{1, "two", 3.0})
	if v.Len() != 3 {
		t.Fatalf("expected Len 3, got %d", v.Len())
	}
}

func TestSliceRowView_LenEmpty(t *testing.T) {
	v := NewSliceRowView(nil)
	if v.Len() != 0 {
		t.Fatalf("expected Len 0 for nil row, got %d", v.Len())
	}
}

func TestSliceRowView_At(t *testing.T) {
	v := NewSliceRowView([]interface{}{"a", "b", "c"})
	if v.At(0) != "a" {
		t.Fatalf("expected 'a' at 0, got %v", v.At(0))
	}
	if v.At(2) != "c" {
		t.Fatalf("expected 'c' at 2, got %v", v.At(2))
	}
}

func TestSliceRowView_AtOutOfBounds(t *testing.T) {
	v := NewSliceRowView([]interface{}{"a"})
	if v.At(-1) != nil {
		t.Fatalf("expected nil for negative index, got %v", v.At(-1))
	}
	if v.At(5) != nil {
		t.Fatalf("expected nil for out-of-bounds index, got %v", v.At(5))
	}
}

func TestSliceRowView_Materialize(t *testing.T) {
	original := []interface{}{1, 2, 3}
	v := NewSliceRowView(original)
	mat := v.Materialize()
	if len(mat) != 3 || mat[0] != 1 || mat[1] != 2 || mat[2] != 3 {
		t.Fatalf("unexpected materialized row: %v", mat)
	}
	// Verify it's a copy, not a reference to the original
	mat[0] = 999
	if original[0] == 999 {
		t.Fatal("Materialize should return a copy, not a reference")
	}
}

func TestSliceRowView_MaterializeEmpty(t *testing.T) {
	v := NewSliceRowView(nil)
	if v.Materialize() != nil {
		t.Fatal("expected nil for empty row materialization")
	}
	v2 := NewSliceRowView([]interface{}{})
	if v2.Materialize() != nil {
		t.Fatal("expected nil for zero-length row materialization")
	}
}

// --- JoinedRowView.Len ---

func TestJoinedRowView_Len(t *testing.T) {
	v := NewJoinedRowView(
		[]interface{}{"a"},
		[]interface{}{"b"},
		[]int{0, -1},
		[]int{-1, 0},
	)
	if v.Len() != 2 {
		t.Fatalf("expected Len 2, got %d", v.Len())
	}
}

func TestJoinedRowView_LenNil(t *testing.T) {
	var v *joinedRowView
	if v.Len() != 0 {
		t.Fatalf("expected Len 0 for nil, got %d", v.Len())
	}
}

func TestJoinedRowView_AtOutOfBounds(t *testing.T) {
	v := NewJoinedRowView(
		[]interface{}{"a"},
		[]interface{}{"b"},
		[]int{0},
		[]int{-1},
	)
	if v.At(-1) != nil {
		t.Fatal("expected nil for negative index")
	}
	if v.At(99) != nil {
		t.Fatal("expected nil for large index")
	}
}

// --- Fragment marker methods ---

func TestFragmentInit_MarkerAndOutputColumns(t *testing.T) {
	f := &FragmentInit{Columns: []string{"x", "y"}}
	f.fragment() // marker method should not panic
	cols := f.OutputColumns()
	if len(cols) != 2 || cols[0] != "x" || cols[1] != "y" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestFragmentLeaf_MarkerAndOutputColumns(t *testing.T) {
	f := &FragmentLeaf{Columns: []string{"n"}}
	f.fragment()
	cols := f.OutputColumns()
	if len(cols) != 1 || cols[0] != "n" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestFragmentExec_MarkerAndOutputColumns(t *testing.T) {
	f := &FragmentExec{Columns: []string{"a", "b"}}
	f.fragment()
	cols := f.OutputColumns()
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
}

func TestFragmentApply_MarkerAndOutputColumns(t *testing.T) {
	f := &FragmentApply{Columns: []string{"merged"}}
	f.fragment()
	cols := f.OutputColumns()
	if len(cols) != 1 || cols[0] != "merged" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestFragmentUnion_MarkerAndOutputColumns(t *testing.T) {
	f := &FragmentUnion{Columns: []string{"u1", "u2"}}
	f.fragment()
	cols := f.OutputColumns()
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
}

// --- Location marker methods ---

func TestLocationLocal_MarkerAndDatabaseName(t *testing.T) {
	l := &LocationLocal{DBName: "neo4j"}
	l.location()
	if l.DatabaseName() != "neo4j" {
		t.Fatalf("expected 'neo4j', got %q", l.DatabaseName())
	}
}

func TestLocationRemote_MarkerAndDatabaseName(t *testing.T) {
	l := &LocationRemote{
		DBName:   "remote-db",
		URI:      "bolt://remote:7687",
		AuthMode: "oidc_forwarding",
	}
	l.location()
	if l.DatabaseName() != "remote-db" {
		t.Fatalf("expected 'remote-db', got %q", l.DatabaseName())
	}
}

// --- Tracing ---

func TestWithHotPathTrace_NilTraceReturnsOriginalContext(t *testing.T) {
	ctx := context.Background()
	got := WithHotPathTrace(ctx, nil)
	if got != ctx {
		t.Fatal("expected same context when trace is nil")
	}
}

func TestWithHotPathTrace_RoundTrip(t *testing.T) {
	ctx := context.Background()
	trace := &HotPathTrace{}
	trace.ApplyBatchedLookupRows.Store(true)

	ctx = WithHotPathTrace(ctx, trace)
	recovered := hotPathTraceFromContext(ctx)
	if recovered == nil {
		t.Fatal("expected non-nil trace from context")
	}
	if !recovered.ApplyBatchedLookupRows.Load() {
		t.Fatal("expected ApplyBatchedLookupRows to be true")
	}
}

func TestHotPathTraceFromContext_Nil(t *testing.T) {
	if hotPathTraceFromContext(nil) != nil {
		t.Fatal("expected nil for nil context")
	}
}

func TestHotPathTraceFromContext_NoValue(t *testing.T) {
	if hotPathTraceFromContext(context.Background()) != nil {
		t.Fatal("expected nil for context without trace")
	}
}

// --- prefetchRowIterator setErr ---

func TestPrefetchRowIterator_SetErrNilIgnored(t *testing.T) {
	base := NewResultRowIterator(&ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	})
	it := NewPrefetchRowIterator(context.Background(), base, 1)
	defer func() { _ = it.Close() }()

	// Drain
	for it.Next() {
	}
	if it.Err() != nil {
		t.Fatal("expected no error")
	}
}
