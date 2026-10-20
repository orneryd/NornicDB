package fabric

import (
	"reflect"
	"testing"
)

// The lowering input is the native descriptor itself. Comparing the complete
// value guards future syntax fields as well as today's optional/scope fields.
func TestCallLoweringPreservesNativeDescriptor(t *testing.T) {
	query := "WITH 1 AS x OPTIONAL CALL (x) { USE comp.one RETURN x AS y } CALL (*) { USE comp.two CREATE (:N) } CALL () { USE comp.one RETURN 2 AS z } RETURN x,y,z"
	structure := ReadQueryStructure(query)
	if structure.Err != nil {
		t.Fatal(structure.Err)
	}
	blocks, err := extractTopLevelCallBlocks(query)
	if err != nil {
		t.Fatal(err)
	}
	var want []QueryClause
	for _, clause := range structure.Clauses {
		if clause.Kind == "CALL" && clause.Nested != nil {
			want = append(want, clause)
		}
	}
	if !reflect.DeepEqual(want, blocks) {
		t.Fatal("lowering discarded native CALL metadata")
	}
}

type firstRowOnlyEquivalence struct{ seen bool }

func (index *firstRowOnlyEquivalence) Duplicate([]interface{}) bool {
	duplicate := index.seen
	index.seen = true
	return duplicate
}

func TestFabricDelegatesEveryDistinctConsumer(t *testing.T) {
	factory := func() RowEquivalence { return &firstRowOnlyEquivalence{} }
	exec := NewFabricExecutor(nil, nil, nil, factory)
	input := &ResultStream{Columns: []string{"x"}, Rows: [][]interface{}{{1}, {2}}}
	projected, ok := exec.executeApplyInMemoryProjection(input, "RETURN DISTINCT x AS x")
	if !ok || len(projected.Rows) != 1 {
		t.Fatal("projection DISTINCT bypassed supplied equivalence")
	}
	_, iterator, err := exec.mergeUnionRows(&FragmentUnion{Distinct: true}, input.Columns, NewResultRowIterator(input), input.Columns, NewResultRowIterator(input))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for iterator.Next() {
		count++
	}
	if err := iterator.Err(); err != nil {
		t.Fatal(err)
	}
	_ = iterator.Close()
	if count != 1 {
		t.Fatal("UNION DISTINCT bypassed supplied equivalence")
	}
	if len(deduplicateRows(input.Rows, factory())) != 1 {
		t.Fatal("materialized DISTINCT bypassed supplied equivalence")
	}
}
