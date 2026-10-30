package fabric

import (
	"testing"
)

func TestFragmentInit_OutputColumns(t *testing.T) {
	f := &FragmentInit{Columns: []string{"a", "b"}}
	cols := f.OutputColumns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Errorf("expected [a b], got %v", cols)
	}
}

func TestFragmentInit_NilColumns(t *testing.T) {
	f := &FragmentInit{}
	cols := f.OutputColumns()
	if cols != nil {
		t.Errorf("expected nil, got %v", cols)
	}
}

func TestFragmentInit_ImportColumns(t *testing.T) {
	f := &FragmentInit{
		Columns:       []string{"x"},
		ImportColumns: []string{"imported_var"},
	}
	if len(f.ImportColumns) != 1 || f.ImportColumns[0] != "imported_var" {
		t.Errorf("expected [imported_var], got %v", f.ImportColumns)
	}
}

func TestFragmentLeaf_OutputColumns(t *testing.T) {
	f := &FragmentLeaf{
		Input:   &FragmentInit{},
		Clauses: "MATCH (n) RETURN n",
		Columns: []string{"n"},
	}
	cols := f.OutputColumns()
	if len(cols) != 1 || cols[0] != "n" {
		t.Errorf("expected [n], got %v", cols)
	}
}

func TestFragmentExec_OutputColumns(t *testing.T) {
	f := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n.id",
		GraphName: "shard1",
		Columns:   []string{"n.id"},
		IsWrite:   false,
	}
	cols := f.OutputColumns()
	if len(cols) != 1 || cols[0] != "n.id" {
		t.Errorf("expected [n.id], got %v", cols)
	}
}

func TestFragmentExec_IsWrite(t *testing.T) {
	f := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "CREATE (n:Test)",
		GraphName: "shard1",
		IsWrite:   true,
	}
	if !f.IsWrite {
		t.Error("expected IsWrite=true")
	}
}

func TestFragmentApply_OutputColumns(t *testing.T) {
	f := &FragmentApply{
		Input:   &FragmentInit{Columns: []string{"a"}},
		Inner:   &FragmentExec{Columns: []string{"b"}},
		Columns: []string{"a", "b"},
	}
	cols := f.OutputColumns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Errorf("expected [a b], got %v", cols)
	}
}

func TestFragmentUnion_OutputColumns(t *testing.T) {
	f := &FragmentUnion{
		Init:     &FragmentInit{},
		LHS:      &FragmentExec{Columns: []string{"x"}},
		RHS:      &FragmentExec{Columns: []string{"x"}},
		Distinct: true,
		Columns:  []string{"x"},
	}
	cols := f.OutputColumns()
	if len(cols) != 1 || cols[0] != "x" {
		t.Errorf("expected [x], got %v", cols)
	}
	if !f.Distinct {
		t.Error("expected Distinct=true")
	}
}

// TestFragmentInterface verifies all types implement Fragment.
func TestFragmentInterface(t *testing.T) {
	var _ Fragment = (*FragmentInit)(nil)
	var _ Fragment = (*FragmentLeaf)(nil)
	var _ Fragment = (*FragmentExec)(nil)
	var _ Fragment = (*FragmentApply)(nil)
	var _ Fragment = (*FragmentUnion)(nil)
}
