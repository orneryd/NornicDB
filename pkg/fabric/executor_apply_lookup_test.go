package fabric

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFabricExecutor_Apply(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}, {"b"}},
			},
			"MATCH (m) WHERE m.ref = $id RETURN m.name AS name": {
				Columns: []string{"name"},
				Rows:    [][]interface{}{{"result"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n.id AS id",
		GraphName: "db1",
		Columns:   []string{"id"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id"}, ImportColumns: []string{"id"}},
		Query:     "MATCH (m) WHERE m.ref = $id RETURN m.name AS name",
		GraphName: "db1",
		Columns:   []string{"name"},
	}
	apply := &FragmentApply{
		Input:   outer,
		Inner:   inner,
		Columns: []string{"id", "name"},
	}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 outer rows x 1 inner row each = 2 combined rows
	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestFabricExecutor_ApplyBatchesCorrelatedCollectLookup(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.textKey128 AS textKey128"
	batchedInnerQuery := "MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, collect(tt) AS texts"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"textKey128"},
				Rows:    [][]interface{}{{"k1"}, {"k2"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "texts"},
				Rows:    [][]interface{}{{"k1", []interface{}{"hit-1", "hit-2"}}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     outerQuery,
		GraphName: "db1",
		Columns:   []string{"textKey128"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     "WITH textKey128 MATCH (tt:MongoDocument) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts",
		GraphName: "db1",
		Columns:   []string{"texts"},
	}
	apply := &FragmentApply{
		Input:   outer,
		Inner:   inner,
		Columns: []string{"textKey128", "texts"},
	}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched inner lookup, got %d calls", got)
	}
	if got := mock.calls["WITH textKey128 MATCH (tt:MongoDocument) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts"]; got != 0 {
		t.Fatalf("expected no per-row correlated inner calls, got %d", got)
	}
	// Row 1 matched.
	if v, ok := result.Rows[0][1].([]interface{}); !ok || len(v) != 2 {
		t.Fatalf("expected first row texts to contain 2 items, got %#v", result.Rows[0][1])
	}
	// Row 2 unmatched -> deterministic empty list.
	if v, ok := result.Rows[1][1].([]interface{}); !ok || len(v) != 0 {
		t.Fatalf("expected second row texts to be empty list, got %#v", result.Rows[1][1])
	}
}

func TestFabricExecutor_ApplyBatchesCorrelatedCollectLookup_WithUseAndExtraPredicate(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.textKey128 AS textKey128"
	batchedInnerQuery := "USE translations.txr MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys AND tt.language = 'es' RETURN tt.translationId AS __fabric_apply_key, collect(tt) AS texts"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"textKey128"},
				Rows:    [][]interface{}{{"k1"}, {"k2"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "texts"},
				Rows:    [][]interface{}{{"k2", []interface{}{"es-hit"}}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"textKey128"}}
	innerQuery := "WITH textKey128 USE translations.txr MATCH (tt:MongoDocument) WHERE tt.language = 'es' AND tt.translationId = textKey128 RETURN collect(tt) AS texts"
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     innerQuery,
		GraphName: "db1",
		Columns:   []string{"texts"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: []string{"textKey128", "texts"}}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched inner lookup, got %d calls", got)
	}
	if v, ok := result.Rows[0][1].([]interface{}); !ok || len(v) != 0 {
		t.Fatalf("expected first row empty list, got %#v", result.Rows[0][1])
	}
	if v, ok := result.Rows[1][1].([]interface{}); !ok || len(v) != 1 {
		t.Fatalf("expected second row one hit, got %#v", result.Rows[1][1])
	}
}

func TestFabricExecutor_ApplyBatchesCorrelatedCollectLookup_ReversedEquality(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.textKey128 AS textKey128"
	batchedInnerQuery := "MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, collect(tt) AS texts"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"textKey128"},
				Rows:    [][]interface{}{{"k1"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "texts"},
				Rows:    [][]interface{}{{"k1", []interface{}{"hit"}}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"textKey128"}}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     "WITH textKey128 MATCH (tt:MongoDocument) WHERE textKey128 = tt.translationId RETURN collect(tt) AS texts",
		GraphName: "db1",
		Columns:   []string{"texts"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: []string{"textKey128", "texts"}}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched inner lookup, got %d calls", got)
	}
	if v, ok := result.Rows[0][1].([]interface{}); !ok || len(v) != 1 {
		t.Fatalf("expected one hit, got %#v", result.Rows[0][1])
	}
}

func TestSplitTopLevelEquality_AndApplyLookupKeyString(t *testing.T) {
	lhs, rhs, ok := splitTopLevelEquality(`tt.translationId = textKey128`)
	if !ok || lhs != "tt.translationId" || rhs != "textKey128" {
		t.Fatalf("unexpected equality parse: ok=%v lhs=%q rhs=%q", ok, lhs, rhs)
	}
	// Must ignore non-equality operators.
	if _, _, ok := splitTopLevelEquality(`tt.translationId >= textKey128`); ok {
		t.Fatalf("expected non-equality operator to be ignored")
	}
	// Must ignore nested equals inside string literal.
	if _, _, ok := splitTopLevelEquality(`tt.expr = "a=b"`); !ok {
		t.Fatalf("expected top-level equality to be detected")
	}

	// Semantic normalization: quoted string forms compare equal across string/bytes inputs.
	if gotStr, gotBytes := applyLookupKeyString(`"abc"`), applyLookupKeyString([]byte(`"abc"`)); gotStr != gotBytes {
		t.Fatalf("expected normalized lookup keys to match for string/bytes, got %q vs %q", gotStr, gotBytes)
	}
	// Distinct values must remain distinct.
	if applyLookupKeyString(`"abc"`) == applyLookupKeyString(`"xyz"`) {
		t.Fatalf("expected different string lookup keys to remain distinct")
	}
	// Type domains should remain separate to avoid accidental cross-type joins.
	if applyLookupKeyString("42") == applyLookupKeyString(int64(42)) {
		t.Fatalf("expected string and int64 lookup keys to remain type-distinct")
	}
	// Numbers are keyed by Cypher equality, whatever their Go type.
	for _, equal := range [][2]interface{}{{int64(42), 42.0}, {42, int64(42)}, {int64(0), math.Copysign(0, -1)}, {uint64(7), int64(7)}, {float32(2.5), 2.5}} {
		if applyLookupKeyString(equal[0]) != applyLookupKeyString(equal[1]) {
			t.Fatalf("expected %T %v and %T %v to share a lookup key", equal[0], equal[0], equal[1], equal[1])
		}
	}
	for _, distinct := range [][2]interface{}{{int64(9007199254740993), 9007199254740992.0}, {2.5, int64(2)}, {math.NaN(), math.NaN()}} {
		if applyLookupKeyString(distinct[0]) == applyLookupKeyString(distinct[1]) {
			t.Fatalf("expected %v and %v to have distinct lookup keys", distinct[0], distinct[1])
		}
	}
}

func TestApplyLookupKeyString_Semantics(t *testing.T) {
	// Nil remains a stable sentinel.
	if got := applyLookupKeyString(nil); got == "" {
		t.Fatalf("expected non-empty sentinel key for nil")
	}
	// Strings and []byte with same text should produce same key.
	if a, b := applyLookupKeyString("world"), applyLookupKeyString([]byte("world")); a != b {
		t.Fatalf("expected stable equality for string/[]byte values, got %q vs %q", a, b)
	}
	// Booleans should remain stable and distinct.
	if applyLookupKeyString(true) == applyLookupKeyString(false) {
		t.Fatalf("expected true/false keys to be distinct")
	}
	// Different numerics should remain distinct.
	if applyLookupKeyString(int64(99)) == applyLookupKeyString(int64(100)) {
		t.Fatalf("expected numeric keys to remain distinct")
	}
	// A float equal to an integer shares its key, as Cypher's = holds.
	if applyLookupKeyString(float64(42)) != applyLookupKeyString(int64(42)) {
		t.Fatalf("expected float64/int64 lookup keys of equal numbers to match")
	}
}

func TestRewriteFragmentWithRuntimeImports_Apply(t *testing.T) {
	inner := &FragmentExec{
		Input: &FragmentInit{ImportColumns: []string{"a"}},
		Query: "RETURN a",
	}
	apply := &FragmentApply{
		Input: &FragmentInit{},
		Inner: inner,
	}
	got := rewriteFragmentWithRuntimeImports(apply, []string{"b"})
	result, ok := got.(*FragmentApply)
	if !ok {
		t.Fatalf("expected *FragmentApply, got %T", got)
	}
	exec, ok := result.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected inner *FragmentExec, got %T", result.Inner)
	}
	if !strings.Contains(exec.Query, "$") {
		t.Fatalf("expected rewritten query with params, got %q", exec.Query)
	}
}

func TestAliasesFromReturnItems(t *testing.T) {
	items := []fabricReturnItem{
		{prop: "name", alias: "name"},
		{prop: "age", alias: "age"},
	}
	facts, ok := readApplyLookupProjection(applyLookupShapeRows, "n.name AS name, n.age AS age", "n")
	require.True(t, ok)
	require.Len(t, facts.returnItems, len(items))
	for i, expected := range items {
		require.Equal(t, expected.prop, facts.returnItems[i].prop)
		require.Equal(t, expected.alias, facts.returnItems[i].alias)
	}
	aliases := facts.aliases
	if len(aliases) != 2 || aliases[0] != "name" || aliases[1] != "age" {
		t.Fatalf("expected [name age], got %v", aliases)
	}
}

func TestFabricExecutor_ApplyBatchedLookupRows(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.id AS id"
	batchedInnerQuery := "MATCH (m:Person) WHERE m.id IN $__fabric_apply_keys RETURN m.id AS __fabric_apply_key, m.name AS name, m.age AS age"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"id1"}, {"id2"}, {"id3"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "name", "age"},
				Rows: [][]interface{}{
					{"id1", "Alice", 30},
					{"id2", "Bob", 25},
				},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"id"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id"}, ImportColumns: []string{"id"}},
		Query:     "WITH id MATCH (m:Person) WHERE m.id = id RETURN m.name AS name, m.age AS age",
		GraphName: "db1",
		Columns:   []string{"name", "age"},
	}
	apply := &FragmentApply{
		Input: outer, Inner: inner, Columns: []string{"id", "name", "age"},
	}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// id1 and id2 matched, id3 has no matches -> 2 rows.
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched lookup, got %d", got)
	}
}

func TestExecuteApply_EmptyInnerResultNoCollect(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}},
			},
			"MATCH (m) WHERE m.ref = $id RETURN m.name AS name": {
				Columns: []string{"name"},
				Rows:    [][]interface{}{}, // Empty inner result.
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id",
		GraphName: "db1", Columns: []string{"id"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id"}, ImportColumns: []string{"id"}},
		Query:     "MATCH (m) WHERE m.ref = $id RETURN m.name AS name",
		GraphName: "db1", Columns: []string{"name"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: []string{"id", "name"}}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Empty inner means the outer row is dropped (no collect).
	if result.RowCount() != 0 {
		t.Fatalf("expected 0 rows for empty inner without collect, got %d", result.RowCount())
	}
}

func TestExecuteApply_EmptyInnerWithCollectSynthesizes(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}},
			},
			"MATCH (m) WHERE m.ref = $id RETURN collect(m.name) AS names": {
				Columns: []string{"names"},
				Rows:    [][]interface{}{}, // Empty — synthesized with [].
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id",
		GraphName: "db1", Columns: []string{"id"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id"}, ImportColumns: []string{"id"}},
		Query:     "MATCH (m) WHERE m.ref = $id RETURN collect(m.name) AS names",
		GraphName: "db1", Columns: []string{"names"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: []string{"id", "names"}}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row with synthesized empty collect, got %d", result.RowCount())
	}
}

func TestExecuteApply_EmptyInnerResultDropsRow(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}},
			},
			"RETURN 1 AS one": {
				Columns: []string{"one"},
				Rows:    [][]interface{}{}, // Empty result — row dropped.
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id",
		GraphName: "db1", Columns: []string{"id"},
	}
	inner := &FragmentExec{
		Input: &FragmentInit{}, Query: "RETURN 1 AS one",
		GraphName: "db1", Columns: []string{"one"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Empty inner result → outer row is dropped.
	if result.RowCount() != 0 {
		t.Fatalf("expected 0 rows, got %d", result.RowCount())
	}
}

func TestExecuteApply_InputError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{err: fmt.Errorf("input error")}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id",
		GraphName: "db1",
	}
	inner := &FragmentExec{
		Input: &FragmentInit{}, Query: "RETURN 1",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	_, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err == nil {
		t.Fatal("expected error for apply input failure")
	}
	if !strings.Contains(err.Error(), "apply input failed") {
		t.Fatalf("expected 'apply input failed' in error, got: %v", err)
	}
}

func TestExtractApplyCorrelationWhere_NoCorrelation(t *testing.T) {
	_, _, _, ok := cyphertext.ExtractCorrelationWhere("a = b", "textKey128", splitTopLevelAnd, splitTopLevelEquality, parseFabricVarProp, cyphertext.IsIdentifier)
	if ok {
		t.Fatal("expected no correlation when import col not found")
	}
}

// TestTryExecuteApplyBatchedLookupRows_AllowsImportNotNullGuard covers the
// row-lookup admission owner: an import IS NOT NULL guard is sanitized away and
// the lookup stays batched.
func TestTryExecuteApplyBatchedLookup_AdmissionGate(t *testing.T) {
	cases := []struct {
		name              string
		collect, nilInner bool
		columns           []string
		rows              [][]interface{}
		query             string
	}{
		{collect: true, name: "collect/nil input", nilInner: true, columns: nil, rows: nil, query: ""},
		{collect: true, name: "collect/empty aggregate rows", columns: []string{"k"}, rows: [][]interface{}{}, query: ""},
		{collect: true, name: "collect/non matching query", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "MATCH (n) RETURN n"},
		{collect: true, name: "collect/write query", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k CREATE (n) RETURN collect(n) AS nodes"},
		{collect: true, name: "collect/import col not in outer columns", columns: []string{"otherCol"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k RETURN collect(n) AS nodes"},
		{collect: true, name: "collect/standalone import col in other where", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k AND k > 0 RETURN collect(n) AS nodes"},
		{collect: true, name: "collect/no where clause", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) RETURN collect(n) AS nodes"},
		{name: "rows/nil input", nilInner: true, columns: nil, rows: nil, query: ""},
		{name: "rows/non matching query", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "MATCH (n) RETURN n"},
		{name: "rows/query contains collect", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k RETURN collect(n) AS nodes"},
		{name: "rows/write query", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k SET n.x = 1 RETURN n.x AS x"},
		{name: "rows/import col not in outer columns", columns: []string{"other"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k RETURN n.name AS name"},
		{name: "rows/empty return clause", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k RETURN "},
		{name: "rows/no correlation", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.x = 'literal' RETURN n.y AS y"},
		{name: "rows/non simple return items", columns: []string{"k"}, rows: [][]interface{}{{"v"}}, query: "WITH k MATCH (n) WHERE n.id = k RETURN count(n) AS cnt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewFabricExecutor(NewCatalog(), nil, nil)
			var input *ResultStream
			if tc.columns != nil {
				input = &ResultStream{Columns: tc.columns, Rows: tc.rows}
			}
			var inner *FragmentExec
			if !tc.nilInner {
				inner = &FragmentExec{Query: tc.query}
			}
			var result *ResultStream
			var handled bool
			var err error
			if tc.collect {
				result, handled, err = exec.tryExecuteApplyBatchedLookup(context.Background(), nil, tc.columns, nil, inner, nil, "", input)
			} else {
				result, handled, err = exec.tryExecuteApplyBatchedLookupRows(context.Background(), nil, input, inner, nil, "")
			}
			if handled {
				t.Fatalf("expected not handled, got result %#v", result)
			}
			if result != nil {
				t.Fatalf("expected nil result for unhandled case, got %#v", result)
			}
			if err != nil {
				t.Fatalf("expected no error for unhandled case, got %v", err)
			}
		})
	}
}

func TestTryExecuteApplyBatchedLookup_AllNilKeys(t *testing.T) {
	for _, tc := range []struct {
		name        string
		collect     bool
		columns     []string
		query       string
		wantColumns string
		wantRows    int
	}{
		{"collect", true, []string{"textKey128"}, "WITH textKey128 MATCH (tt) WHERE tt.id = textKey128 RETURN collect(tt) AS texts", "textKey128,texts", 2},
		{"rows", false, []string{"id"}, "WITH id MATCH (n) WHERE n.id = id RETURN n.name AS name", "id,name", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			mock := &mockCypherExecutor{results: map[string]*ResultStream{}, calls: map[string]int{}}
			exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
			input := &ResultStream{Columns: tc.columns, Rows: [][]interface{}{{nil}, {nil}}}
			inner := &FragmentExec{Query: tc.query, GraphName: "db1"}
			var result *ResultStream
			var handled bool
			var err error
			if tc.collect {
				result, handled, err = exec.tryExecuteApplyBatchedLookup(context.Background(), nil, input.Columns, nil, inner, nil, "", input)
			} else {
				result, handled, err = exec.tryExecuteApplyBatchedLookupRows(context.Background(), nil, input, inner, nil, "")
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !handled {
				t.Fatal("expected handled for all-nil keys")
			}
			if len(result.Rows) != tc.wantRows {
				t.Fatalf("expected %d rows, got %d", tc.wantRows, len(result.Rows))
			}
			if got := strings.Join(result.Columns, ","); got != tc.wantColumns {
				t.Fatalf("expected columns %q, got %q", tc.wantColumns, got)
			}
			for i, row := range result.Rows {
				if row[0] != nil {
					t.Fatalf("row %d: expected nil outer key preserved, got %#v", i, row[0])
				}
				texts, ok := row[1].([]interface{})
				if !ok || len(texts) != 0 {
					t.Fatalf("row %d: expected empty collect list, got %#v", i, row[1])
				}
			}
			if len(mock.calls) != 0 {
				t.Fatalf("expected no inner query execution for all-nil keys, got calls %v", mock.calls)
			}
		})
	}
}

func TestTryExecuteApplyBatchedLookupRows_AllowsImportNotNullGuard(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	batchedQuery := "MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, tt.language AS language, tt.translatedText AS translatedText"
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			batchedQuery: {
				Columns: []string{"__fabric_apply_key", "language", "translatedText"},
				Rows: [][]interface{}{
					{"s1", "es", "uno"},
					{"s2", "fr", "deux"},
				},
			},
		},
		calls: map[string]int{},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	input := &ResultStream{
		Columns: []string{"sourceId"},
		Rows:    [][]interface{}{{"s1"}, {"s2"}},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"sourceId"}, ImportColumns: []string{"sourceId"}},
		Query:     "WITH sourceId MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId AND sourceId IS NOT NULL RETURN tt.language AS language, tt.translatedText AS translatedText",
		GraphName: "db1",
		Columns:   []string{"language", "translatedText"},
	}
	res, handled, err := exec.tryExecuteApplyBatchedLookupRows(context.Background(), nil, input, inner, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handled {
		t.Fatal("expected handled for import not-null guard")
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if mock.calls[batchedQuery] != 1 {
		t.Fatalf("expected batched query execution; calls=%v", mock.calls)
	}
}

func TestTryExecuteApplyBatchedCountLookup_TraversalPattern(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	batchedQuery := "MATCH (tt:MongoDocument)-[:IN_LANG]->(l:Lang) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, count(*) AS c"
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			batchedQuery: {
				Columns: []string{"__fabric_apply_key", "c"},
				Rows: [][]interface{}{
					{"s1", int64(2)},
					{"s2", int64(1)},
				},
			},
		},
		calls: map[string]int{},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	input := &ResultStream{
		Columns: []string{"sourceId"},
		Rows:    [][]interface{}{{"s1"}, {"s2"}, {"s3"}},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"sourceId"}, ImportColumns: []string{"sourceId"}},
		Query:     "WITH sourceId MATCH (tt:MongoDocument)-[:IN_LANG]->(l:Lang) WHERE tt.translationId = sourceId RETURN count(*) AS c",
		GraphName: "db1",
		Columns:   []string{"c"},
	}

	res, handled, err := exec.tryExecuteApplyBatchedCountLookup(context.Background(), nil, input, inner, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handled {
		t.Fatal("expected handled traversal count lookup")
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	if got := res.Rows[0][1]; got != int64(2) {
		t.Fatalf("expected s1 count=2, got %#v", got)
	}
	if got := res.Rows[1][1]; got != int64(1) {
		t.Fatalf("expected s2 count=1, got %#v", got)
	}
	if got := res.Rows[2][1]; got != int64(0) {
		t.Fatalf("expected s3 count=0, got %#v", got)
	}
	if mock.calls[batchedQuery] < 1 {
		t.Fatalf("expected batched traversal count query execution; calls=%v", mock.calls)
	}
}

func TestTryExecuteApplyBatchedCountLookup_AllowsImportNotNullGuard(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	batchedQuery := "MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, count(*) AS c"
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			batchedQuery: {
				Columns: []string{"__fabric_apply_key", "c"},
				Rows: [][]interface{}{
					{"s1", int64(3)},
				},
			},
		},
		calls: map[string]int{},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	input := &ResultStream{
		Columns: []string{"sourceId"},
		Rows:    [][]interface{}{{"s1"}},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"sourceId"}, ImportColumns: []string{"sourceId"}},
		Query:     "WITH sourceId MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId AND sourceId IS NOT NULL RETURN count(*) AS c",
		GraphName: "db1",
		Columns:   []string{"c"},
	}

	res, handled, err := exec.tryExecuteApplyBatchedCountLookup(context.Background(), nil, input, inner, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handled {
		t.Fatal("expected handled for import not-null guard")
	}
	if len(res.Rows) != 1 || res.Rows[0][1] != int64(3) {
		t.Fatalf("unexpected result rows: %#v", res.Rows)
	}
	if mock.calls[batchedQuery] < 1 {
		t.Fatalf("expected batched query execution; calls=%v", mock.calls)
	}
}

func TestTryExecuteApplyBatchedCountLookup_WriteQueryNotHandled(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	input := &ResultStream{Columns: []string{"sourceId"}, Rows: [][]interface{}{{"s1"}}}
	inner := &FragmentExec{
		Query: "WITH sourceId MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId CREATE (x:Audit) RETURN count(*) AS c",
	}
	_, handled, _ := exec.tryExecuteApplyBatchedCountLookup(context.Background(), nil, input, inner, nil, "")
	if handled {
		t.Fatal("expected not handled for write query")
	}
}

func TestExecuteApply_InnerError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	callCount := 0
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}},
			},
		},
	}
	localExec := NewLocalFragmentExecutor(&callCountCypherExecutor{
		mock:      mock,
		callCount: &callCount,
		failAt:    2, // Fail on second call (inner execution).
	}, func(name string) (storage.Engine, error) {
		return &mockEngine{}, nil
	})
	exec := NewFabricExecutor(catalog, localExec, nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id",
		GraphName: "db1", Columns: []string{"id"},
	}
	inner := &FragmentExec{
		Input: &FragmentInit{}, Query: "RETURN 1 AS one",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	_, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err == nil || !strings.Contains(err.Error(), "apply inner failed") {
		t.Fatalf("expected apply inner error, got: %v", err)
	}
}

func TestExecuteApply_BatchedCollectLookupError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	// Outer query succeeds, inner batched query fails.
	callCount := 0
	outerQuery := "MATCH (n) RETURN n.id AS textKey128"
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			outerQuery: {Columns: []string{"textKey128"}, Rows: [][]interface{}{{"k1"}}},
		},
	}
	localExec := NewLocalFragmentExecutor(&callCountCypherExecutor{
		mock: mock, callCount: &callCount, failAt: 2,
	}, func(name string) (storage.Engine, error) { return &mockEngine{}, nil })
	exec := NewFabricExecutor(catalog, localExec, nil)

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"textKey128"}}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     "WITH textKey128 MATCH (tt) WHERE tt.id = textKey128 RETURN collect(tt) AS texts",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	_, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err == nil {
		t.Fatal("expected error from batched collect lookup")
	}
	if !strings.Contains(err.Error(), "apply inner failed") {
		t.Fatalf("expected 'apply inner failed' in error, got: %v", err)
	}
}

func TestExecuteApply_BatchedRowLookupError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	callCount := 0
	outerQuery := "MATCH (n) RETURN n.id AS id"
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			outerQuery: {Columns: []string{"id"}, Rows: [][]interface{}{{"k1"}}},
		},
	}
	localExec := NewLocalFragmentExecutor(&callCountCypherExecutor{
		mock: mock, callCount: &callCount, failAt: 2,
	}, func(name string) (storage.Engine, error) { return &mockEngine{}, nil })
	exec := NewFabricExecutor(catalog, localExec, nil)

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"id"}}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id"}, ImportColumns: []string{"id"}},
		Query:     "WITH id MATCH (n) WHERE n.id = id RETURN n.name AS name, n.age AS age",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	_, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err == nil {
		t.Fatal("expected error from batched row lookup")
	}
	if !strings.Contains(err.Error(), "apply inner failed") {
		t.Fatalf("expected 'apply inner failed' in error, got: %v", err)
	}
}

func TestExecuteApply_InnerNonExec(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}},
			},
			"RETURN 1 AS x": {
				Columns: []string{"x"},
				Rows:    [][]interface{}{{1}},
			},
			"RETURN 2 AS x": {
				Columns: []string{"x"},
				Rows:    [][]interface{}{{2}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	// Inner is a union (not FragmentExec) — skips all optimization paths.
	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id",
		GraphName: "db1", Columns: []string{"id"},
	}
	inner := &FragmentUnion{
		Init:    &FragmentInit{},
		LHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1"},
		RHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 2 AS x", GraphName: "db1"},
		Columns: []string{"x"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1 outer row × 2 inner rows = 2 combined rows.
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestParseApplyCorrelatedLookupSubquery_WithModifiersAndOptionalWhere(t *testing.T) {
	spec, ok := parseApplyCorrelatedLookupSubquery(t.Context(),
		"WITH sourceId USE translations.txr MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId RETURN tt.language AS language, tt.translatedText AS translatedText ORDER BY tt.language DESC LIMIT 1",
	)
	if !ok {
		t.Fatal("expected parse to succeed")
	}
	if spec.importCol != "sourceId" {
		t.Fatalf("unexpected import column: %q", spec.importCol)
	}
	if spec.useClause != "USE translations.txr" {
		t.Fatalf("unexpected use clause: %q", spec.useClause)
	}
	if spec.matchPart != "(tt:MongoDocument)" {
		t.Fatalf("unexpected match part: %q", spec.matchPart)
	}
	if spec.wherePart != "tt.translationId = sourceId" {
		t.Fatalf("unexpected where part: %q", spec.wherePart)
	}
	if spec.returnProjection != "tt.language AS language, tt.translatedText AS translatedText" {
		t.Fatalf("unexpected return projection: %q", spec.returnProjection)
	}
	if spec.returnModifiers != "ORDER BY tt.language DESC LIMIT 1" {
		t.Fatalf("unexpected return modifiers: %q", spec.returnModifiers)
	}

	spec, ok = parseApplyCorrelatedLookupSubquery(t.Context(),
		"WITH sourceId USE translations.txr MATCH (tt:MongoDocument) RETURN tt.language AS language",
	)
	if !ok {
		t.Fatal("expected parse without WHERE to succeed")
	}
	if spec.wherePart != "" {
		t.Fatalf("expected empty where part, got %q", spec.wherePart)
	}
}

func TestFabricExecutor_ApplyBatchedLookupRows_ModifiersPerOuterKey(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.id AS sourceId"
	batchedInnerQuery := "USE translations.txr MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys AND tt.language IS NOT NULL AND tt.translatedText IS NOT NULL RETURN tt.translationId AS __fabric_apply_key, tt.language AS language, tt.translatedText AS translatedText"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"sourceId"},
				Rows:    [][]interface{}{{"id1"}, {"id2"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "language", "translatedText"},
				Rows: [][]interface{}{
					{"id1", "en", "hello"},
					{"id1", "es", "hola"},
					{"id2", "de", "hallo"},
					{"id2", "fr", "bonjour"},
				},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"sourceId"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"sourceId"}, ImportColumns: []string{"sourceId"}},
		Query:     "WITH sourceId USE translations.txr MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId AND tt.language IS NOT NULL AND tt.translatedText IS NOT NULL RETURN tt.language AS language, tt.translatedText AS translatedText ORDER BY tt.language DESC LIMIT 1",
		GraphName: "db1",
		Columns:   []string{"language", "translatedText"},
	}
	apply := &FragmentApply{
		Input: outer, Inner: inner, Columns: []string{"sourceId", "language", "translatedText"},
	}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched lookup, got %d", got)
	}
	if result.Rows[0][0] != "id1" || result.Rows[0][1] != "es" {
		t.Fatalf("unexpected first row: %v", result.Rows[0])
	}
	if result.Rows[1][0] != "id2" || result.Rows[1][1] != "fr" {
		t.Fatalf("unexpected second row: %v", result.Rows[1])
	}
}

func TestBatchedReturnItemReaders(t *testing.T) {
	type parserSelector int
	const (
		parseProperties parserSelector = iota
		parseCounts
	)

	tests := []struct {
		name        string
		parser      parserSelector
		text        string
		matchVar    string
		wantOK      bool
		wantLen     int
		wantAliases []string // nil means aliases are not asserted
		wantProps   []string // nil means props are not asserted; element checked only when non-empty
	}{
		{
			name:        "lookup success",
			parser:      parseProperties,
			text:        "n.name AS name, n.age AS age",
			matchVar:    "n",
			wantOK:      true,
			wantLen:     2,
			wantAliases: []string{"name", "age"},
			wantProps:   []string{"name", "age"},
		},
		{
			name:     "lookup wrong match variable",
			parser:   parseProperties,
			text:     "m.name AS name",
			matchVar: "n",
			wantOK:   false,
		},
		{
			name:     "lookup empty return",
			parser:   parseProperties,
			text:     "",
			matchVar: "n",
			wantOK:   false,
		},
		{
			name:     "lookup non-simple alias",
			parser:   parseProperties,
			text:     "n.name AS n.alias",
			matchVar: "n",
			wantOK:   false,
		},
		{
			name:     "lookup expression without dot",
			parser:   parseProperties,
			text:     "justName",
			matchVar: "n",
			wantOK:   false,
		},
		{
			name:        "count success",
			parser:      parseCounts,
			text:        "count(*) AS c, count(tt) AS n, count(tt.id) AS ids",
			wantOK:      true,
			wantLen:     3,
			wantAliases: []string{"c", "n", "ids"},
		},
		{
			name:   "count non-count aggregate",
			parser: parseCounts,
			text:   "sum(tt.x) AS s",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var aliases, props []string
			var ok bool
			switch tt.parser {
			case parseProperties:
				facts, parsed := readApplyLookupProjection(applyLookupShapeRows, tt.text, tt.matchVar)
				ok = parsed
				for _, item := range facts.returnItems {
					aliases = append(aliases, item.alias)
					props = append(props, item.prop)
				}
				require.Equal(t, aliases, facts.aliases)
			case parseCounts:
				facts, parsed := readApplyLookupProjection(applyLookupShapeCount, tt.text, "")
				ok = parsed
				for _, item := range facts.countItems {
					aliases = append(aliases, item.alias)
				}
				require.Equal(t, aliases, facts.aliases)
			}

			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if len(aliases) != tt.wantLen {
				t.Fatalf("len(aliases) = %d, want %d", len(aliases), tt.wantLen)
			}
			for index, want := range tt.wantAliases {
				if aliases[index] != want {
					t.Fatalf("items[%d].alias = %q, want %q", index, aliases[index], want)
				}
			}

			if tt.wantProps != nil {
				for i, want := range tt.wantProps {
					if props[i] != want {
						t.Fatalf("items[%d].prop = %q, want %q", i, props[i], want)
					}
				}
			}
		})
	}
}

func TestExtractApplyCorrelationWhere_Success(t *testing.T) {
	for _, tc := range []struct {
		name, where, otherWhere, failed, matchFailed, otherFailed string
	}{
		{"TestExtractApplyCorrelationWhere_RHSVarProp", "textKey128 = tt.translationId", "", "expected correlation extraction to succeed", "expected tt.translationId, got %s.%s", "expected no other where, got %q"},
		{"TestExtractApplyCorrelationWhere_WithExtraTerms", "tt.translationId = textKey128 AND tt.language = 'es'", "tt.language = 'es'", "expected extraction to succeed", "unexpected match: %s.%s", "unexpected otherWhere: %q"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matchVar, matchProp, otherWhere, ok := cyphertext.ExtractCorrelationWhere(tc.where, "textKey128", splitTopLevelAnd, splitTopLevelEquality, parseFabricVarProp, cyphertext.IsIdentifier)
			if !ok {
				t.Fatal(tc.failed)
			}
			if matchVar != "tt" || matchProp != "translationId" {
				t.Fatalf(tc.matchFailed, matchVar, matchProp)
			}
			if otherWhere != tc.otherWhere {
				t.Fatalf(tc.otherFailed, otherWhere)
			}
		})
	}
}
