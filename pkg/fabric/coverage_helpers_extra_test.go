package fabric

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fabricErrorIterator struct {
	rows     [][]interface{}
	idx      int
	err      error
	closeErr error
}

func (it *fabricErrorIterator) Next() bool {
	next := it.idx + 1
	if next >= len(it.rows) {
		return false
	}
	it.idx = next
	return true
}

func (it *fabricErrorIterator) Row() []interface{} {
	if it.idx < 0 || it.idx >= len(it.rows) {
		return nil
	}
	return it.rows[it.idx]
}

func (it *fabricErrorIterator) Err() error { return it.err }

func (it *fabricErrorIterator) Close() error { return it.closeErr }

func TestFragmentMarkerMethods(t *testing.T) {
	(&FragmentInit{}).fragment()
	(&FragmentLeaf{}).fragment()
	(&FragmentExec{}).fragment()
	(&FragmentApply{}).fragment()
	(&FragmentUnion{}).fragment()

	fragments := []Fragment{
		&FragmentInit{Columns: []string{"a"}},
		&FragmentLeaf{Columns: []string{"b"}},
		&FragmentExec{Columns: []string{"c"}},
		&FragmentApply{Columns: []string{"d"}},
		&FragmentUnion{Columns: []string{"e"}},
	}
	for _, fragment := range fragments {
		fragment.fragment()
		require.Len(t, fragment.OutputColumns(), 1)
	}
}

func TestLocationMarkerMethods(t *testing.T) {
	(&LocationLocal{}).location()
	(&LocationRemote{}).location()

	locations := []Location{
		&LocationLocal{DBName: "local"},
		&LocationRemote{DBName: "remote", URI: "bolt://example.invalid:7687", AuthMode: "oidc_forwarding"},
	}
	for _, location := range locations {
		location.location()
		require.NotEmpty(t, location.DatabaseName())
	}
}

func TestLocalFragmentExecutorExecuteRows(t *testing.T) {
	mock := &mockCypherExecutor{results: map[string]*ResultStream{
		"RETURN 1 AS n": {Columns: []string{"n"}, Rows: [][]interface{}{{1}, {2}}},
	}}
	local := newTestLocalExecutor(mock)

	columns, rows, err := local.ExecuteRows(context.Background(), &LocationLocal{DBName: "db1"}, "RETURN 1 AS n", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, columns)
	require.True(t, rows.Next())
	require.Equal(t, []interface{}{1}, rows.Row())
	require.True(t, rows.Next())
	require.Equal(t, []interface{}{2}, rows.Row())
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}

func TestRowViewAndIteratorNilBranches(t *testing.T) {
	var rowView *sliceRowView
	require.Equal(t, 0, rowView.Len())
	require.Nil(t, rowView.At(0))
	require.Nil(t, rowView.Materialize())

	view := NewSliceRowView([]interface{}{"a"})
	require.Nil(t, view.At(-1))
	require.Nil(t, view.At(2))
	require.Equal(t, []interface{}{"a"}, view.Materialize())

	var joined *joinedRowView
	require.Equal(t, 0, joined.Len())
	require.Nil(t, joined.At(0))
	require.Nil(t, joined.Materialize())

	iterator := NewConvertingRowIterator(nil, nil)
	require.False(t, iterator.Next())
	require.Nil(t, iterator.Row())
	require.NoError(t, iterator.Err())
	require.NoError(t, iterator.Close())
}

func TestIteratorErrorPropagationBranches(t *testing.T) {
	baseErr := errors.New("base iterator failed")
	closeErr := errors.New("close failed")
	prefetch := NewPrefetchRowIterator(context.Background(), &fabricErrorIterator{
		idx:      -1,
		rows:     [][]interface{}{{"x"}},
		err:      baseErr,
		closeErr: closeErr,
	}, 0)
	require.True(t, prefetch.Next())
	require.Equal(t, []interface{}{"x"}, prefetch.Row())
	require.False(t, prefetch.Next())
	require.ErrorIs(t, prefetch.Err(), baseErr)
	require.NoError(t, prefetch.Close())

	concat := NewConcatRowIterator(
		&fabricErrorIterator{idx: -1, rows: [][]interface{}{{"a"}}},
		&fabricErrorIterator{idx: -1, err: baseErr},
	)
	require.True(t, concat.Next())
	require.Equal(t, []interface{}{"a"}, concat.Row())
	require.False(t, concat.Next())
	require.ErrorIs(t, concat.Err(), baseErr)
	require.NoError(t, concat.Close())

	concatWithCloseErr := NewConcatRowIterator(&fabricErrorIterator{idx: -1, closeErr: closeErr})
	require.ErrorIs(t, concatWithCloseErr.Close(), closeErr)

	distinct := NewDistinctRowIterator(&fabricErrorIterator{idx: -1, err: baseErr})
	require.False(t, distinct.Next())
	require.Nil(t, distinct.Row())
	require.ErrorIs(t, distinct.Err(), baseErr)
	require.NoError(t, distinct.Close())
}

func TestFabricExecutorExecuteRowsIteratorPaths(t *testing.T) {
	mock := &mockCypherExecutor{results: map[string]*ResultStream{
		"RETURN 1 AS n": {Columns: []string{"n"}, Rows: [][]interface{}{{1}, {2}}},
		"RETURN 2 AS n": {Columns: []string{"n"}, Rows: [][]interface{}{{2}}},
	}}
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	cols, it, err := exec.executeRows(context.Background(), nil, &FragmentInit{Columns: []string{"seed"}}, nil, "")
	require.NoError(t, err)
	require.Equal(t, []string{"seed"}, cols)
	require.True(t, it.Next())
	require.Nil(t, it.Row())
	require.False(t, it.Next())
	require.NoError(t, it.Close())

	ctx := WithRecordBindings(context.Background(), map[string]interface{}{"outer": "value"})
	cols, it, err = exec.executeRows(ctx, nil, &FragmentExec{GraphName: "db1", Query: "RETURN 1 AS n"}, map[string]interface{}{"p": 1}, "")
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, cols)
	require.True(t, it.Next())
	require.Equal(t, []interface{}{1}, it.Row())
	require.True(t, it.Next())
	require.Equal(t, []interface{}{2}, it.Row())
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	cols, it, err = exec.executeRows(context.Background(), nil, &FragmentApply{
		Input:   &FragmentInit{},
		Inner:   &FragmentExec{GraphName: "db1", Query: "RETURN 2 AS n"},
		Columns: []string{"n"},
	}, nil, "")
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, cols)
	require.True(t, it.Next())
	require.Equal(t, []interface{}{2}, it.Row())
	require.False(t, it.Next())
	require.NoError(t, it.Close())

	_, _, err = exec.executeRows(context.Background(), nil, &FragmentExec{GraphName: "missing", Query: "RETURN 1"}, nil, "")
	require.ErrorContains(t, err, "cannot route query")
}

func TestFabricExecutorUnionRowsBranches(t *testing.T) {
	mock := &mockCypherExecutor{results: map[string]*ResultStream{
		"RETURN 'a' AS v": {Columns: []string{"v"}, Rows: [][]interface{}{{"a"}, {"dup"}}},
		"RETURN 'b' AS v": {Columns: []string{"v"}, Rows: [][]interface{}{{"dup"}, {"b"}}},
	}}
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	withAccessMode(t, func(query string, _ bool) bool { return query == "RETURN 'b' AS v" })
	union := &FragmentUnion{
		LHS:      &FragmentExec{GraphName: "db1", Query: "RETURN 'a' AS v"},
		RHS:      &FragmentExec{GraphName: "db1", Query: "RETURN 'b' AS v"},
		Distinct: true,
	}

	cols, it, err := exec.executeUnionRows(context.Background(), nil, union, nil, "")
	require.NoError(t, err)
	require.Equal(t, []string{"v"}, cols)
	var got []string
	for it.Next() {
		got = append(got, it.Row()[0].(string))
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Equal(t, []string{"a", "dup", "b"}, got)

	union.RHS = &FragmentExec{GraphName: "missing", Query: "RETURN 'b' AS v"}
	_, _, err = exec.executeUnionRows(context.Background(), nil, union, nil, "")
	require.ErrorContains(t, err, "union RHS failed")
}

func TestRemoteFragmentExecutorExecuteRowsErrorPath(t *testing.T) {
	remote := NewRemoteFragmentExecutor()
	defer func() { _ = remote.Close() }()

	_, _, err := remote.ExecuteRows(context.Background(), &LocationRemote{DBName: "db", URI: "ftp://example.invalid"}, "RETURN 1", nil, "")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "failed to connect to remote") || strings.Contains(err.Error(), "unsupported"), err.Error())
}

func TestFabricExecutorUnionMaterializers(t *testing.T) {
	mock := &mockCypherExecutor{results: map[string]*ResultStream{
		"RETURN 'a' AS v": {Columns: []string{"v"}, Rows: [][]interface{}{{"a"}}},
		"RETURN 'b' AS v": {Columns: []string{"v"}, Rows: [][]interface{}{{"b"}}},
	}}
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	withAccessMode(t, func(query string, _ bool) bool { return query == "RETURN 'a' AS v" })
	union := &FragmentUnion{
		LHS:     &FragmentExec{GraphName: "db1", Query: "RETURN 'a' AS v"},
		RHS:     &FragmentExec{GraphName: "db1", Query: "RETURN 'b' AS v"},
		Columns: []string{"v"},
	}

	seq, err := exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}, {"b"}}, seq.Rows)

	union.LHS = &FragmentExec{GraphName: "db1", Query: "RETURN 'a' AS v"}
	par, err := exec.executeUnionParallel(context.Background(), nil, union, nil, "")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}, {"b"}}, par.Rows)

	union.LHS = &FragmentExec{GraphName: "missing", Query: "RETURN 'a' AS v"}
	_, err = exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	require.ErrorContains(t, err, "union LHS failed")
	_, err = exec.executeUnionParallel(context.Background(), nil, union, nil, "")
	require.ErrorContains(t, err, "union LHS failed")
}

func TestFabricParserHelperExtraBranches(t *testing.T) {
	require.Equal(t, "", func() string { tok, _ := firstToken("   "); return tok }())
	tok, rest := firstToken("MATCH (n) RETURN n")
	require.Equal(t, "MATCH", tok)
	require.Equal(t, "(n) RETURN n", rest)

	require.Equal(t, -1, indexTopLevelKeyword("RETURN 'MATCH' AS word", "MATCH"))
	require.Equal(t, -1, indexTopLevelKeyword("RETURN `MATCH` AS word", "MATCH"))
	require.GreaterOrEqual(t, indexTopLevelKeyword("WITH x MATCH (n) RETURN n", "MATCH"), 0)

	items, ok := parseSimpleBatchedLookupReturnItems("n.name AS name, n.age AS age", "n")
	require.True(t, ok)
	require.Equal(t, []string{"name", "age"}, aliasesFromReturnItems(items))
	require.False(t, func() bool { _, ok := parseSimpleBatchedLookupReturnItems("m.name AS name", "n"); return ok }())
	require.False(t, func() bool { _, ok := parseSimpleBatchedLookupReturnItems("n.name AS bad-alias", "n"); return ok }())

	collectExpr, alias, ok := parseSimpleCollectReturnItem("collect(n) AS nodes")
	require.True(t, ok)
	require.Equal(t, "n", collectExpr)
	require.Equal(t, "nodes", alias)
	require.False(t, func() bool { _, _, ok := parseSimpleCollectReturnItem("count(n) AS c"); return ok }())
	require.False(t, func() bool { _, _, ok := parseSimpleCollectReturnItem("collect() AS empty"); return ok }())

	countItems, ok := parseSimpleBatchedCountReturnItems("count(*) AS total")
	require.True(t, ok)
	require.Equal(t, []string{"total"}, aliasesFromCountReturnItems(countItems))
	require.Equal(t, []interface{}{int64(0), int64(0)}, zeroCountValues(2))

	bindings := bindingsFromParentAndRow(map[string]interface{}{"parent": 1}, []string{"a", "", "c"}, []interface{}{2, 3})
	require.Equal(t, map[string]interface{}{"parent": 1, "a": 2}, bindings)
	require.Nil(t, bindingsFromParentAndRow(nil, nil, nil))
}
