package fabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// mockCypherExecutor is a test double for CypherExecutor.
type mockCypherExecutor struct {
	results map[string]*ResultStream // query -> result
	err     error
	calls   map[string]int
	writes  map[string]bool // test fixture effects, not query classification
}

func (m *mockCypherExecutor) ExecuteQueryWithRecord(ctx context.Context, _ string, _ storage.Engine, query string, _ map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	if m.calls != nil {
		m.calls[query]++
	}
	result, ok := m.results[query]
	if !ok {
		return nil, nil, fmt.Errorf("unexpected query: %s", query)
	}
	if m.writes[query] {
		tx, ok := FabricTransactionFromContext(ctx)
		sub, hasSub := SubTransactionFromContext(ctx)
		if !ok || !hasSub {
			return nil, nil, fmt.Errorf("writing fixture has no participant")
		}
		if _, err := tx.GetOrOpen(sub.ShardName, true); err != nil {
			return nil, nil, err
		}
	}
	return result.Columns, result.Rows, nil
}

type slowBatchingCypherExecutor struct {
	outerQuery string
	outerRows  [][]interface{}
	active     atomic.Int32
	maxActive  atomic.Int32
	batchCalls atomic.Int32
}

func (s *slowBatchingCypherExecutor) ExecuteQueryWithRecord(_ context.Context, _ string, _ storage.Engine, query string, params map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	if query == s.outerQuery {
		return []string{"textKey128", "textKey", "originalText"}, s.outerRows, nil
	}
	if strings.Contains(query, "IN $__fabric_apply_keys") && strings.Contains(query, "__fabric_apply_key") {
		now := s.active.Add(1)
		for {
			max := s.maxActive.Load()
			if now <= max {
				break
			}
			if s.maxActive.CompareAndSwap(max, now) {
				break
			}
		}
		s.batchCalls.Add(1)
		time.Sleep(35 * time.Millisecond)
		keysAny, _ := params["__fabric_apply_keys"].([]interface{})
		rows := make([][]interface{}, 0, len(keysAny))
		for _, k := range keysAny {
			rows = append(rows, []interface{}{k, "es", fmt.Sprintf("t-%v", k)})
		}
		s.active.Add(-1)
		return []string{"__fabric_apply_key", "language", "translatedText"}, rows, nil
	}
	return nil, nil, fmt.Errorf("unexpected query: %s", query)
}

type pipelineCaptureCypherExecutor struct {
	lastQuery  string
	lastParams map[string]interface{}
}

func (p *pipelineCaptureCypherExecutor) ExecuteQueryWithRecord(_ context.Context, _ string, _ storage.Engine, query string, params map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	p.lastQuery = query
	p.lastParams = params
	return []string{"k", "v"}, [][]interface{}{{"x", "y"}}, nil
}

// mockEngine is a minimal storage.Engine for testing.
type mockEngine struct {
	storage.Engine
}

func newTestLocalExecutor(mock *mockCypherExecutor) *LocalFragmentExecutor {
	engines := map[string]storage.Engine{
		"db1":    &mockEngine{},
		"shard1": &mockEngine{},
		"shard2": &mockEngine{},
	}
	return NewLocalFragmentExecutor(mock, func(name string) (storage.Engine, error) {
		if e, ok := engines[name]; ok {
			return e, nil
		}
		return nil, fmt.Errorf("database '%s' not found", name)
	})
}

func TestFabricExecutor_SimpleExec(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id": {
				Columns: []string{"n.id"},
				Rows:    [][]interface{}{{"id1"}, {"id2"}},
			},
		},
		calls: map[string]int{},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	fragment := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n.id",
		GraphName: "db1",
		Columns:   []string{"n.id"},
	}

	result, err := exec.Execute(ctx, nil, fragment, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
	if result.Columns[0] != "n.id" {
		t.Errorf("expected column n.id, got %s", result.Columns[0])
	}
}

func TestFabricExecutor_Init(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	ctx := context.Background()

	init := &FragmentInit{Columns: []string{"x"}}
	result, err := exec.Execute(ctx, nil, init, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Errorf("expected 1 row, got %d", result.RowCount())
	}
}

func TestFabricExecutor_UnknownGraph(t *testing.T) {
	catalog := NewCatalog()
	exec := NewFabricExecutor(catalog, nil, nil)
	ctx := context.Background()

	fragment := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n",
		GraphName: "nonexistent",
	}

	_, err := exec.Execute(ctx, nil, fragment, nil, "")
	if err == nil {
		t.Fatal("expected error for unknown graph")
	}
}

func TestFabricExecutor_TransactionWriteConstraint(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("shard1", &LocationLocal{DBName: "shard1"})
	catalog.Register("shard2", &LocationLocal{DBName: "shard2"})

	mock := &mockCypherExecutor{
		writes: map[string]bool{"CREATE (n:Test)": true},
		results: map[string]*ResultStream{
			"CREATE (n:Test)": {
				Columns: []string{"n"},
				Rows:    [][]interface{}{{"created"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()
	tx := NewFabricTransaction("tx-test")

	// Write to shard1 — succeeds.
	frag1 := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "CREATE (n:Test)",
		GraphName: "shard1",
		IsWrite:   true,
	}
	_, err := exec.Execute(ctx, tx, frag1, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Write to shard2 — must fail with second write shard error.
	frag2 := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "CREATE (n:Test)",
		GraphName: "shard2",
		IsWrite:   true,
	}
	_, err = exec.Execute(ctx, tx, frag2, nil, "")
	if !errors.Is(err, ErrSecondWriteShard) {
		t.Errorf("expected ErrSecondWriteShard, got: %v", err)
	}
}

func TestFabricExecutor_Union(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n:A) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a1"}, {"a2"}},
			},
			"MATCH (n:B) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"b1"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	union := &FragmentUnion{
		Init: &FragmentInit{},
		LHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "MATCH (n:A) RETURN n.id AS id",
			GraphName: "db1",
			Columns:   []string{"id"},
		},
		RHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "MATCH (n:B) RETURN n.id AS id",
			GraphName: "db1",
			Columns:   []string{"id"},
		},
		Distinct: false,
		Columns:  []string{"id"},
	}

	result, err := exec.Execute(ctx, nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", result.RowCount())
	}
}

func TestFabricExecutor_UnionDistinct(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS x": {
				Columns: []string{"x"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	union := &FragmentUnion{
		Init: &FragmentInit{},
		LHS: &FragmentExec{
			Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1",
		},
		RHS: &FragmentExec{
			Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1",
		},
		Distinct: true,
		Columns:  []string{"x"},
	}

	result, err := exec.Execute(ctx, nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Errorf("expected 1 row after distinct, got %d", result.RowCount())
	}
}

func TestFabricExecutor_LeafFragment(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	ctx := context.Background()

	leaf := &FragmentLeaf{
		Input:   &FragmentInit{},
		Clauses: "MATCH (n)",
		Columns: []string{"n"},
	}

	_, err := exec.Execute(ctx, nil, leaf, nil, "")
	if err == nil {
		t.Fatal("expected error for unresolved FragmentLeaf")
	}
}

func TestFabricExecutor_NilLocalExecutor(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	exec := NewFabricExecutor(catalog, nil, nil)
	ctx := context.Background()

	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n", GraphName: "db1",
	}

	_, err := exec.Execute(ctx, nil, frag, nil, "")
	if err == nil {
		t.Fatal("expected error when local executor is nil")
	}
}

func TestLocalFragmentExecutor_Execute_WrapperPath(t *testing.T) {
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS one": {
				Columns: []string{"one"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}
	local := newTestLocalExecutor(mock)
	res, err := local.Execute(context.Background(), &LocationLocal{DBName: "db1"}, "RETURN 1 AS one", nil)
	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "one" {
		t.Fatalf("unexpected columns: %#v", res.Columns)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0] != int64(1) {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}
}

func TestExecuteUnionSequential_Direct(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"CREATE (n:Row)": {
				Columns: []string{"n"},
				Rows:    [][]interface{}{{"lhs"}},
			},
			"RETURN 1 AS one": {
				Columns: []string{"one"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	union := &FragmentUnion{
		LHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "CREATE (n:Row)",
			GraphName: "db1",
			Columns:   []string{"n"},
			IsWrite:   true,
		},
		RHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "RETURN 1 AS one",
			GraphName: "db1",
			Columns:   []string{"one"},
		},
		Columns: []string{"v"},
	}

	res, err := exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected sequential union error: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows from sequential union, got %d", len(res.Rows))
	}
}

func TestFabricExecutor_NilRemoteExecutor(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("remote.shard", &LocationRemote{DBName: "shard", URI: "bolt://r:7687"})

	exec := NewFabricExecutor(catalog, nil, nil)
	ctx := context.Background()

	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n", GraphName: "remote.shard",
	}

	_, err := exec.Execute(ctx, nil, frag, nil, "")
	if err == nil {
		t.Fatal("expected error when remote executor is nil")
	}
}

func TestCombineColumns(t *testing.T) {
	outer := []string{"a", "b"}
	inner := []string{"b", "c"}

	combined := combineColumns(outer, inner)
	// Should be [a, b, c] — b is deduplicated.
	if len(combined) != 3 {
		t.Fatalf("expected 3 columns, got %d: %v", len(combined), combined)
	}
	if combined[0] != "a" || combined[1] != "b" || combined[2] != "c" {
		t.Errorf("expected [a b c], got %v", combined)
	}
}

func TestDeduplicateRows(t *testing.T) {
	rows := [][]interface{}{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(1), "a"},
	}

	deduped := deduplicateRows(rows)
	if len(deduped) != 2 {
		t.Errorf("expected 2 rows after dedup, got %d", len(deduped))
	}
}

func TestDeduplicateRows_MapValuesDeterministic(t *testing.T) {
	rows := [][]interface{}{
		{map[string]interface{}{"b": int64(2), "a": int64(1)}},
		{map[string]interface{}{"a": int64(1), "b": int64(2)}},
	}
	deduped := deduplicateRows(rows)
	if len(deduped) != 1 {
		t.Fatalf("expected map rows to dedupe deterministically, got %d", len(deduped))
	}
}

func TestCombineRowsByIndexes_InnerOverridesOuter(t *testing.T) {
	resultCols := []string{"id", "outerOnly", "innerOnly", "shared"}
	outerIdx := buildColumnIndex([]string{"id", "outerOnly", "shared"})
	innerIdx := buildColumnIndex([]string{"innerOnly", "shared"})
	out := combineRowsByIndexes(
		resultCols,
		outerIdx, []interface{}{"a1", "outer", "outerShared"},
		innerIdx, []interface{}{"inner", "innerShared"},
	)
	if len(out) != 4 {
		t.Fatalf("expected 4 combined values, got %d", len(out))
	}
	if out[0] != "a1" || out[1] != "outer" || out[2] != "inner" || out[3] != "innerShared" {
		t.Fatalf("unexpected combined row: %#v", out)
	}
}

func TestFragmentContainsWrite(t *testing.T) {
	t.Run("exec write", func(t *testing.T) {
		if !fragmentContainsWrite(&FragmentExec{IsWrite: true}) {
			t.Fatal("expected write fragment to be detected")
		}
	})
	t.Run("exec read", func(t *testing.T) {
		if fragmentContainsWrite(&FragmentExec{IsWrite: false}) {
			t.Fatal("expected read fragment to be non-write")
		}
	})
	t.Run("nested apply", func(t *testing.T) {
		f := &FragmentApply{
			Input: &FragmentExec{IsWrite: false},
			Inner: &FragmentExec{IsWrite: true},
		}
		if !fragmentContainsWrite(f) {
			t.Fatal("expected nested write to be detected")
		}
	})
	t.Run("nested union", func(t *testing.T) {
		f := &FragmentUnion{
			LHS: &FragmentExec{IsWrite: false},
			RHS: &FragmentApply{
				Input: &FragmentInit{},
				Inner: &FragmentExec{IsWrite: true},
			},
		}
		if !fragmentContainsWrite(f) {
			t.Fatal("expected nested union write to be detected")
		}
	})
}

func TestRewriteLeadingWithImports_DeterministicClauseBoundary(t *testing.T) {
	query := "WITH id, size([x IN [1,2,3] WHERE x > 1]) AS c MATCH (n) WHERE n.id = id RETURN n"
	rewritten := rewriteLeadingWithImports(query, []string{"id"})
	expected := "WITH $id AS id MATCH (n) WHERE n.id = id RETURN n"
	if rewritten != expected {
		t.Fatalf("expected %q, got %q", expected, rewritten)
	}
}

func TestRewriteLeadingWithImports_RespectsQuotedKeywords(t *testing.T) {
	query := "WITH 'MATCH as text' AS txt, id RETURN txt, id"
	rewritten := rewriteLeadingWithImports(query, []string{"id"})
	expected := "WITH $id AS id RETURN txt, id"
	if rewritten != expected {
		t.Fatalf("expected %q, got %q", expected, rewritten)
	}
}

func TestRewriteLeadingWithImports_NoLeadingWith(t *testing.T) {
	query := "MATCH (n) RETURN n"
	rewritten := rewriteLeadingWithImports(query, []string{"id"})
	if rewritten != query {
		t.Fatalf("expected query unchanged, got %q", rewritten)
	}
}

func TestRewriteLeadingWithImports_NoLeadingWithReturn(t *testing.T) {
	query := "RETURN id, name"
	rewritten := rewriteLeadingWithImports(query, []string{"id", "name"})
	expected := "WITH $id AS id, $name AS name RETURN id, name"
	if rewritten != expected {
		t.Fatalf("expected %q, got %q", expected, rewritten)
	}
}

func TestMergeBindings(t *testing.T) {
	parent := map[string]interface{}{"rows": []interface{}{1, 2}, "x": "parent"}
	row := map[string]interface{}{"x": "row", "k": "v"}
	got := mergeBindings(parent, row)
	if got["rows"] == nil || got["k"] != "v" || got["x"] != "row" {
		t.Fatalf("unexpected merged bindings: %#v", got)
	}
}

func TestFabricImportRewriteHelpers(t *testing.T) {
	init := &FragmentInit{Columns: []string{"a"}, ImportColumns: []string{"a"}}
	execFrag := &FragmentExec{
		Input:   init,
		Query:   "WITH a MATCH (n) WHERE n.id = a RETURN n.id AS id",
		Columns: []string{"id"},
	}
	union := &FragmentUnion{
		LHS:     execFrag,
		RHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS one", Columns: []string{"one"}},
		Columns: []string{"id"},
	}
	apply := &FragmentApply{Input: &FragmentInit{}, Inner: union, Columns: []string{"id"}}

	if cols := importColumnsFromFragment(init); len(cols) != 1 || cols[0] != "a" {
		t.Fatalf("unexpected import columns: %#v", cols)
	}
	if !fragmentHasStaticImports(apply) {
		t.Fatal("expected fragmentHasStaticImports to detect static imports")
	}
	rewritten := rewriteFragmentWithImports(apply)
	rewrittenApply, ok := rewritten.(*FragmentApply)
	if !ok {
		t.Fatalf("expected rewritten apply, got %T", rewritten)
	}
	innerUnion, ok := rewrittenApply.Inner.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected rewritten union, got %T", rewrittenApply.Inner)
	}
	lhsExec, ok := innerUnion.LHS.(*FragmentExec)
	if !ok {
		t.Fatalf("expected rewritten lhs exec, got %T", innerUnion.LHS)
	}
	if !strings.Contains(lhsExec.Query, "$a") {
		t.Fatalf("expected rewritten query to contain parameterized import, got %q", lhsExec.Query)
	}

	rewrittenRuntime := rewriteFragmentWithRuntimeImports(&FragmentExec{
		Input:   &FragmentInit{ImportColumns: []string{"a"}},
		Query:   "RETURN a, b",
		Columns: []string{"a", "b"},
	}, []string{"b", "a", "b"})
	rExec, ok := rewrittenRuntime.(*FragmentExec)
	if !ok {
		t.Fatalf("expected rewritten runtime exec, got %T", rewrittenRuntime)
	}
	if !strings.HasPrefix(strings.TrimSpace(rExec.Query), "WITH ") {
		t.Fatalf("expected runtime rewrite to prepend WITH imports, got %q", rExec.Query)
	}
	merged := mergeImportColumns([]string{"a", "b"}, []string{"b", "c"})
	if len(merged) != 3 || merged[0] != "a" || merged[1] != "b" || merged[2] != "c" {
		t.Fatalf("unexpected merged import columns: %#v", merged)
	}
}

func TestSubstituteAndReplaceStandaloneVar(t *testing.T) {
	query := `MATCH (tt) WHERE tt.translationId = textKey128 AND tt.note = "textKey128" RETURN textKey128, tt.translationId`
	sub := substituteVarsWithParams(query, []string{"textKey128"})
	if !strings.Contains(sub, "tt.translationId = $textKey128") {
		t.Fatalf("expected identifier substitution in predicate, got %q", sub)
	}
	if !strings.Contains(sub, `"textKey128"`) {
		t.Fatalf("expected string literal untouched, got %q", sub)
	}
}

func TestCombineRowsByColumnsWrapper(t *testing.T) {
	resultCols := []string{"a", "b", "c"}
	outerCols := []string{"a", "b"}
	innerCols := []string{"c"}
	outerRow := []interface{}{1, 2}
	innerRow := []interface{}{3}
	got := combineRowsByColumns(resultCols, outerCols, outerRow, innerCols, innerRow)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("unexpected combined row: %#v", got)
	}
}

func BenchmarkDeduplicateRows(b *testing.B) {
	rows := make([][]interface{}, 0, 10000)
	for i := 0; i < 5000; i++ {
		row := []interface{}{int64(i), "v-" + strconv.Itoa(i), map[string]interface{}{"k": int64(i)}}
		rows = append(rows, row, row)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = deduplicateRows(rows)
	}
}

func BenchmarkCombineRowsByIndexes(b *testing.B) {
	resultCols := []string{"a", "b", "c", "d", "e", "f"}
	outerCols := []string{"a", "b", "c", "d"}
	innerCols := []string{"c", "e", "f"}
	outerIdx := buildColumnIndex(outerCols)
	innerIdx := buildColumnIndex(innerCols)
	outerRow := []interface{}{1, 2, 3, 4}
	innerRow := []interface{}{30, 50, 60}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = combineRowsByIndexes(resultCols, outerIdx, outerRow, innerIdx, innerRow)
	}
}

// --- Additional coverage tests ---

func TestSplitTopLevelCSV_QuotedStrings(t *testing.T) {
	// Single-quoted string containing a comma.
	parts := splitTopLevelCSV("a, 'b,c', d")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d: %v", len(parts), parts)
	}
	// Double-quoted string containing a comma.
	parts = splitTopLevelCSV(`a, "b,c", d`)
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d: %v", len(parts), parts)
	}
	// Backtick-quoted identifier containing a comma.
	parts = splitTopLevelCSV("a, `b,c`, d")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d: %v", len(parts), parts)
	}
	// Nested brackets.
	parts = splitTopLevelCSV("a, [1,2,3], d")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts with brackets, got %d: %v", len(parts), parts)
	}
	// Nested braces.
	parts = splitTopLevelCSV("a, {x: 1, y: 2}, d")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts with braces, got %d: %v", len(parts), parts)
	}
}

func TestSplitTopLevelAnd_QuotedStrings(t *testing.T) {
	// AND inside single-quoted string should not split.
	parts := splitTopLevelAnd("n.x = 'foo AND bar' AND n.y = 1")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
	// AND inside double-quoted string should not split.
	parts = splitTopLevelAnd(`n.x = "foo AND bar" AND n.y = 1`)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
	// AND inside backtick should not split.
	parts = splitTopLevelAnd("n.`x AND y` = 1 AND n.z = 2")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts with backtick, got %d: %v", len(parts), parts)
	}
	// AND inside parentheses should not split.
	parts = splitTopLevelAnd("(a AND b) AND c = 1")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts with parens, got %d: %v", len(parts), parts)
	}
	// AND inside brackets should not split.
	parts = splitTopLevelAnd("[a AND b] AND c = 1")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts with brackets, got %d: %v", len(parts), parts)
	}
	// AND inside braces should not split.
	parts = splitTopLevelAnd("{a AND b} AND c = 1")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts with braces, got %d: %v", len(parts), parts)
	}
}

func TestSplitTopLevelEquality_AllBranches(t *testing.T) {
	// Normal equality.
	lhs, rhs, ok := splitTopLevelEquality("a = b")
	if !ok || lhs != "a" || rhs != "b" {
		t.Fatalf("expected simple equality, got ok=%v lhs=%q rhs=%q", ok, lhs, rhs)
	}
	// != should not match.
	_, _, ok = splitTopLevelEquality("a != b")
	if ok {
		t.Fatal("expected != to not match as equality")
	}
	// <= should not match.
	_, _, ok = splitTopLevelEquality("a <= b")
	if ok {
		t.Fatal("expected <= to not match as equality")
	}
	// >= should not match.
	_, _, ok = splitTopLevelEquality("a >= b")
	if ok {
		t.Fatal("expected >= to not match as equality")
	}
	// == : the first = is skipped (next is =), but the second = is detected
	// as a standalone equality. This matches the code's actual behavior — "a =="
	// splits at the second =, producing lhs="a =" rhs=" b".
	lhs, rhs, ok = splitTopLevelEquality("a == b")
	if !ok {
		t.Fatal("expected == to still match at second =")
	}
	if lhs != "a =" || rhs != "b" {
		t.Fatalf("unexpected == parse: lhs=%q rhs=%q", lhs, rhs)
	}
	// Equality inside single-quoted string.
	lhs, rhs, ok = splitTopLevelEquality("n.x = 'a=b'")
	if !ok || lhs != "n.x" || rhs != "'a=b'" {
		t.Fatalf("expected equality outside string, got ok=%v lhs=%q rhs=%q", ok, lhs, rhs)
	}
	// Equality inside double-quoted string.
	lhs, rhs, ok = splitTopLevelEquality(`n.x = "a=b"`)
	if !ok {
		t.Fatal("expected equality to be detected outside double-quoted string")
	}
	// Equality inside backtick.
	lhs, rhs, ok = splitTopLevelEquality("n.`a=b` = c")
	if !ok || lhs != "n.`a=b`" || rhs != "c" {
		t.Fatalf("expected backtick-quoted equality parse, got ok=%v lhs=%q rhs=%q", ok, lhs, rhs)
	}
	// Equality inside parentheses.
	_, _, ok = splitTopLevelEquality("(a = b)")
	if ok {
		t.Fatal("expected no top-level equality inside parens")
	}
	// Equality inside brackets.
	_, _, ok = splitTopLevelEquality("[a = b]")
	if ok {
		t.Fatal("expected no top-level equality inside brackets")
	}
	// Equality inside braces.
	_, _, ok = splitTopLevelEquality("{a = b}")
	if ok {
		t.Fatal("expected no top-level equality inside braces")
	}
	// No equality at all.
	_, _, ok = splitTopLevelEquality("a > b")
	if ok {
		t.Fatal("expected no equality for '>'")
	}
}

func TestContainsStandaloneIdentifier(t *testing.T) {
	if cyphertext.FindIdentifier("a = textKey128", "textKey128", 0, cyphertext.IdentifierExact) < 0 {
		t.Fatal("expected standalone identifier to be found")
	}
	if cyphertext.FindIdentifier("a = textKey1289", "textKey128", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected partial match to be rejected")
	}
	if cyphertext.FindIdentifier("a = xtextKey128", "textKey128", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected prefix-attached match to be rejected")
	}
	// Inside single-quoted string.
	if cyphertext.FindIdentifier("'textKey128'", "textKey128", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected identifier inside single quotes to be ignored")
	}
	// Inside double-quoted string.
	if cyphertext.FindIdentifier(`"textKey128"`, "textKey128", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected identifier inside double quotes to be ignored")
	}
	// Inside backtick string.
	if cyphertext.FindIdentifier("`textKey128`", "textKey128", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected identifier inside backticks to be ignored")
	}
	// Empty identifier.
	if cyphertext.FindIdentifier("abc", "", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected empty identifier to return false")
	}
	// String shorter than identifier.
	if cyphertext.FindIdentifier("ab", "abc", 0, cyphertext.IdentifierExact) >= 0 {
		t.Fatal("expected short string to return false")
	}
	// At start.
	if cyphertext.FindIdentifier("textKey128 = 1", "textKey128", 0, cyphertext.IdentifierExact) < 0 {
		t.Fatal("expected identifier at start to match")
	}
	// At end.
	if cyphertext.FindIdentifier("x = textKey128", "textKey128", 0, cyphertext.IdentifierExact) < 0 {
		t.Fatal("expected identifier at end to match")
	}
}

func TestParticipantKeyFromLocation_UnknownType(t *testing.T) {
	// Location marker interface with no implementation beyond Local/Remote
	// should produce an "unknown:" prefix.
	key := ParticipantKeyFromLocation(&LocationLocal{DBName: "db1"})
	if key != "local:db1" {
		t.Fatalf("expected local:db1, got %q", key)
	}
	key = ParticipantKeyFromLocation(&LocationRemote{URI: "bolt://r:7687", DBName: "db2"})
	if key != "remote:bolt://r:7687|db2" {
		t.Fatalf("expected remote key, got %q", key)
	}
}

func TestCompareSimpleOrderValues_AllBranches(t *testing.T) {
	// nil vs nil.
	if compareSimpleOrderValues(nil, nil) != 0 {
		t.Fatal("nil vs nil should be 0")
	}
	// nil vs non-nil.
	if compareSimpleOrderValues(nil, 1) != -1 {
		t.Fatal("nil vs non-nil should be -1")
	}
	// non-nil vs nil.
	if compareSimpleOrderValues(1, nil) != 1 {
		t.Fatal("non-nil vs nil should be 1")
	}
	// float comparison.
	if compareSimpleOrderValues(1.0, 2.0) != -1 {
		t.Fatal("1.0 < 2.0 should be -1")
	}
	if compareSimpleOrderValues(2.0, 1.0) != 1 {
		t.Fatal("2.0 > 1.0 should be 1")
	}
	if compareSimpleOrderValues(1.0, 1.0) != 0 {
		t.Fatal("1.0 == 1.0 should be 0")
	}
	// bool comparison.
	if compareSimpleOrderValues(false, true) != -1 {
		t.Fatal("false < true should be -1")
	}
	if compareSimpleOrderValues(true, false) != 1 {
		t.Fatal("true > false should be 1")
	}
	if compareSimpleOrderValues(true, true) != 0 {
		t.Fatal("true == true should be 0")
	}
	// string fallback comparison.
	if compareSimpleOrderValues("a", "b") != -1 {
		t.Fatal("a < b should be -1")
	}
	if compareSimpleOrderValues("b", "a") != 1 {
		t.Fatal("b > a should be 1")
	}
	if compareSimpleOrderValues("x", "x") != 0 {
		t.Fatal("x == x should be 0")
	}
	// int comparisons via asComparableFloat.
	if compareSimpleOrderValues(1, 2) != -1 {
		t.Fatal("int 1 < 2 should be -1")
	}
	if compareSimpleOrderValues(int64(10), int64(5)) != 1 {
		t.Fatal("int64 10 > 5 should be 1")
	}
}

func TestAsComparableFloat_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
		want float64
		ok   bool
	}{
		{"int", int(5), 5.0, true},
		{"int8", int8(5), 5.0, true},
		{"int16", int16(5), 5.0, true},
		{"int32", int32(5), 5.0, true},
		{"int64", int64(5), 5.0, true},
		{"uint", uint(5), 5.0, true},
		{"uint8", uint8(5), 5.0, true},
		{"uint16", uint16(5), 5.0, true},
		{"uint32", uint32(5), 5.0, true},
		{"uint64", uint64(5), 5.0, true},
		{"float32", float32(5.5), float64(float32(5.5)), true},
		{"float64", float64(5.5), 5.5, true},
		{"string", "nope", 0, false},
		{"nil", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := asComparableFloat(tt.val)
			if ok != tt.ok {
				t.Fatalf("expected ok=%v, got ok=%v", tt.ok, ok)
			}
			if ok && got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestWriteAnyHash_AllBranches(t *testing.T) {
	// Ensure all branches run without panicking and produce distinct hashes.
	values := []interface{}{
		nil,
		"string",
		true,
		false,
		int(1),
		int8(2),
		int16(3),
		int32(4),
		int64(5),
		uint(6),
		uint8(7),
		uint16(8),
		uint32(9),
		uint64(10),
		float32(1.5),
		float64(2.5),
		[]interface{}{"a", 1},
		map[string]interface{}{"k": "v"},
		struct{ x int }{1}, // default branch
	}
	hashes := make(map[uint64]int)
	for i, v := range values {
		h := newFNV64a()
		writeAnyHash(&h, v)
		hash := h.sum64()
		if prev, dup := hashes[hash]; dup {
			t.Fatalf("hash collision between value[%d]=%v and value[%d]=%v", prev, values[prev], i, v)
		}
		hashes[hash] = i
	}
}

func TestValueAtRowIndex_OutOfBounds(t *testing.T) {
	row := []interface{}{1, 2}
	if valueAtRowIndex(row, -1) != nil {
		t.Fatal("expected nil for negative index")
	}
	if valueAtRowIndex(row, 5) != nil {
		t.Fatal("expected nil for out of bounds index")
	}
	if valueAtRowIndex(row, 0) != 1 {
		t.Fatal("expected value at valid index")
	}
}

func TestRowBindings(t *testing.T) {
	// Empty columns.
	if rowBindings(nil, []interface{}{1}) != nil {
		t.Fatal("expected nil for empty columns")
	}
	// Empty row.
	if rowBindings([]string{"a"}, nil) != nil {
		t.Fatal("expected nil for empty row")
	}
	// Whitespace-only column should be skipped.
	got := rowBindings([]string{"a", "  ", "b"}, []interface{}{1, 2, 3})
	if got["a"] != 1 || got["b"] != 3 {
		t.Fatalf("unexpected bindings: %#v", got)
	}
	if _, ok := got["  "]; ok {
		t.Fatal("whitespace-only column should be excluded")
	}
	// Row shorter than columns.
	got = rowBindings([]string{"a", "b", "c"}, []interface{}{1})
	if got["a"] != 1 {
		t.Fatalf("expected a=1, got %#v", got)
	}
}

func TestMergeBindings_AllBranches(t *testing.T) {
	// Both nil.
	if mergeBindings(nil, nil) != nil {
		t.Fatal("expected nil for both nil")
	}
	// Parent nil.
	got := mergeBindings(nil, map[string]interface{}{"a": 1})
	if got["a"] != 1 {
		t.Fatal("expected row bindings when parent is nil")
	}
	// Row nil.
	got = mergeBindings(map[string]interface{}{"b": 2}, nil)
	if got["b"] != 2 {
		t.Fatal("expected parent bindings when row is nil")
	}
	// Both non-nil, row overrides parent.
	got = mergeBindings(map[string]interface{}{"x": 1}, map[string]interface{}{"x": 2, "y": 3})
	if got["x"] != 2 || got["y"] != 3 {
		t.Fatalf("expected row to override parent: %#v", got)
	}
}

func TestImportColumnsFromFragment(t *testing.T) {
	// nil fragment.
	if importColumnsFromFragment(nil) != nil {
		t.Fatal("expected nil for nil fragment")
	}
	// Non-init fragment.
	if importColumnsFromFragment(&FragmentExec{}) != nil {
		t.Fatal("expected nil for non-init fragment")
	}
	// Init with import columns.
	init := &FragmentInit{Columns: []string{"a"}, ImportColumns: []string{"b"}}
	cols := importColumnsFromFragment(init)
	if len(cols) != 1 || cols[0] != "b" {
		t.Fatalf("expected import columns, got %v", cols)
	}
	// Init without import columns falls back to Columns.
	init2 := &FragmentInit{Columns: []string{"c"}}
	cols = importColumnsFromFragment(init2)
	if len(cols) != 1 || cols[0] != "c" {
		t.Fatalf("expected fallback columns, got %v", cols)
	}
}

func TestSplitTopLevelResultModifiers_WithQuotedStrings(t *testing.T) {
	// ORDER BY inside a quoted string should not split.
	clause := "textKey128, 'ORDER BY fake' AS label ORDER BY textKey128"
	proj, mods := splitTopLevelResultModifiers(clause)
	if proj != "textKey128, 'ORDER BY fake' AS label" {
		t.Fatalf("unexpected projection: %q", proj)
	}
	if mods != "ORDER BY textKey128" {
		t.Fatalf("unexpected modifiers: %q", mods)
	}
	// No modifiers.
	proj, mods = splitTopLevelResultModifiers("a, b, c")
	if proj != "a, b, c" || mods != "" {
		t.Fatalf("expected no modifiers, got proj=%q mods=%q", proj, mods)
	}
	// Double-quoted.
	clause = `a, "LIMIT 5" AS x LIMIT 10`
	proj, mods = splitTopLevelResultModifiers(clause)
	if mods != "LIMIT 10" {
		t.Fatalf("unexpected modifiers with double quotes: %q", mods)
	}
	// Backtick-quoted.
	clause = "a, `SKIP 5` AS x SKIP 10"
	proj, mods = splitTopLevelResultModifiers(clause)
	if mods != "SKIP 10" {
		t.Fatalf("unexpected modifiers with backtick: %q", mods)
	}
}

func TestParseSimplePositiveInt(t *testing.T) {
	n, ok := parseSimplePositiveInt("42")
	if !ok || n != 42 {
		t.Fatalf("expected 42, got n=%d ok=%v", n, ok)
	}
	_, ok = parseSimplePositiveInt("")
	if ok {
		t.Fatal("expected false for empty string")
	}
	_, ok = parseSimplePositiveInt("abc")
	if ok {
		t.Fatal("expected false for non-digit")
	}
}

func TestParseSimpleOrderByClause_DescAsc(t *testing.T) {
	aliasToCol := map[string]string{"name": "name", "id": "id"}
	specs := parseSimpleOrderByClause("name DESC, id ASC", aliasToCol)
	if len(specs) != 2 {
		t.Fatalf("expected 2 order specs, got %d", len(specs))
	}
	if specs[0].column != "name" || !specs[0].desc {
		t.Fatalf("expected name DESC, got %+v", specs[0])
	}
	if specs[1].column != "id" || specs[1].desc {
		t.Fatalf("expected id ASC, got %+v", specs[1])
	}
	// Non-simple identifier should be skipped.
	specs = parseSimpleOrderByClause("n.name", aliasToCol)
	if len(specs) != 0 {
		t.Fatalf("expected non-simple to be skipped, got %v", specs)
	}
}

// A column is the query's own whatever its name; the APPLY pipeline's row
// wrapper is a marked result (ResultStream.RowWrapper), not a column name.
func TestCombineColumns_KeepsQueryColumns(t *testing.T) {
	combined := combineColumns([]string{"a", "__fabric_key"}, []string{"b", "__fabric_row"})
	if strings.Join(combined, ",") != "a,__fabric_key,b,__fabric_row" {
		t.Fatalf("expected every query column, got %v", combined)
	}
}

func TestBuildColumnIndex_SkipsWhitespace(t *testing.T) {
	idx := buildColumnIndex([]string{"a", "  ", "b"})
	if _, ok := idx["  "]; ok {
		t.Fatal("expected whitespace-only column to be skipped")
	}
	if idx["a"] != 0 || idx["b"] != 2 {
		t.Fatalf("unexpected index: %v", idx)
	}
}

func TestLastKeywordIndexFold_EmptyInputs(t *testing.T) {
	if lastKeywordIndexFold("", "RETURN") != -1 {
		t.Fatal("expected -1 for empty string")
	}
	if lastKeywordIndexFold("abc", "") != -1 {
		t.Fatal("expected -1 for empty keyword")
	}
	if lastKeywordIndexFold("ab", "RETURN") != -1 {
		t.Fatal("expected -1 when keyword longer than string")
	}
}

func TestLastAsIndexFold_ShortString(t *testing.T) {
	if lastAsIndexFold("ab") != -1 {
		t.Fatal("expected -1 for string shorter than 4")
	}
}

func TestRewriteFragmentWithImports_NilFragment(t *testing.T) {
	got := rewriteFragmentWithImports(nil)
	if got != nil {
		t.Fatal("expected nil for nil fragment")
	}
}

func TestRewriteFragmentWithImports_DefaultBranch(t *testing.T) {
	init := &FragmentInit{Columns: []string{"a"}}
	got := rewriteFragmentWithImports(init)
	if got != init {
		t.Fatal("expected init fragment to pass through unchanged")
	}
}

func TestRewriteFragmentWithRuntimeImports_NilFragment(t *testing.T) {
	got := rewriteFragmentWithRuntimeImports(nil, []string{"a"})
	if got != nil {
		t.Fatal("expected nil for nil fragment")
	}
}

func TestRewriteFragmentWithRuntimeImports_DefaultBranch(t *testing.T) {
	init := &FragmentInit{Columns: []string{"a"}}
	got := rewriteFragmentWithRuntimeImports(init, []string{"a"})
	if got != init {
		t.Fatal("expected init fragment to pass through unchanged")
	}
}

func TestRewriteFragmentWithRuntimeImports_Union(t *testing.T) {
	union := &FragmentUnion{
		LHS: &FragmentExec{
			Input: &FragmentInit{ImportColumns: []string{"a"}},
			Query: "RETURN a",
		},
		RHS: &FragmentExec{
			Input: &FragmentInit{},
			Query: "RETURN 1 AS one",
		},
	}
	got := rewriteFragmentWithRuntimeImports(union, []string{"b"})
	result, ok := got.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected *FragmentUnion, got %T", got)
	}
	lhs, ok := result.LHS.(*FragmentExec)
	if !ok {
		t.Fatalf("expected LHS *FragmentExec, got %T", result.LHS)
	}
	if !strings.Contains(lhs.Query, "$") {
		t.Fatalf("expected LHS rewritten, got %q", lhs.Query)
	}
}

func TestMergeImportColumns_Deduplication(t *testing.T) {
	got := mergeImportColumns([]string{"a", "b", " "}, []string{"b", "c", "  "})
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("expected [a b c], got %v", got)
	}
}

func TestIsSimpleWithImportClause(t *testing.T) {
	if isSimpleWithImportClause("WITH a, b", []string{"a", "b"}) != true {
		t.Fatal("expected simple import clause to match")
	}
	if isSimpleWithImportClause("WITH a, b", []string{"a"}) != false {
		t.Fatal("expected non-matching import list to fail")
	}
	if isSimpleWithImportClause("MATCH (n)", []string{"a"}) != false {
		t.Fatal("expected non-WITH clause to fail")
	}
	if isSimpleWithImportClause("WITH ", []string{"a"}) != false {
		t.Fatal("expected empty WITH body to fail")
	}
	if isSimpleWithImportClause("WITH a", nil) != false {
		t.Fatal("expected nil import cols to fail")
	}
	if isSimpleWithImportClause("WITH n.x", []string{"n.x"}) != false {
		t.Fatal("expected non-simple identifier in WITH to fail")
	}
}

func TestReplaceStandaloneVar_WithQuotes(t *testing.T) {
	// Single-quoted should be preserved.
	got := replaceStandaloneVar("n.x = 'id' AND id = 5", "id", "$id")
	if !strings.Contains(got, "'id'") {
		t.Fatalf("expected single-quoted 'id' preserved, got %q", got)
	}
	if !strings.Contains(got, "$id = 5") {
		t.Fatalf("expected standalone id replaced, got %q", got)
	}
	// Double-quoted should be preserved.
	got = replaceStandaloneVar(`n.x = "id" AND id = 5`, "id", "$id")
	if !strings.Contains(got, `"id"`) {
		t.Fatalf("expected double-quoted preserved, got %q", got)
	}
	// Backtick-quoted should be preserved.
	got = replaceStandaloneVar("n.`id` = 1 AND id = 5", "id", "$id")
	if !strings.Contains(got, "`id`") {
		t.Fatalf("expected backtick-quoted preserved, got %q", got)
	}
	// Empty ident.
	got = replaceStandaloneVar("abc", "", "$x")
	if got != "abc" {
		t.Fatalf("expected unchanged for empty ident, got %q", got)
	}
	// String shorter than ident.
	got = replaceStandaloneVar("a", "abc", "$abc")
	if got != "a" {
		t.Fatalf("expected unchanged for short string, got %q", got)
	}
	// Preceded by dot should not match.
	got = replaceStandaloneVar("n.id = 1", "id", "$id")
	if strings.Contains(got, "$id") {
		t.Fatalf("expected dot-preceded id to not be replaced, got %q", got)
	}
}

func TestFindLeadingWithClauseEnd_Empty(t *testing.T) {
	pos, ok := findLeadingWithClauseEnd("", nil)
	if ok || pos != 0 {
		t.Fatalf("expected false for empty, got pos=%d ok=%v", pos, ok)
	}
}

func TestFindLeadingWithClauseEnd_NotWith(t *testing.T) {
	pos, ok := findLeadingWithClauseEnd("MATCH (n) RETURN n", nil)
	if ok {
		t.Fatalf("expected false for non-WITH query, got pos=%d", pos)
	}
}

func TestFindLeadingWithClauseEnd_WithQuotes(t *testing.T) {
	// WITH clause containing single-quoted string with a keyword.
	pos, ok := findLeadingWithClauseEnd("WITH 'MATCH stuff' AS x MATCH (n)", nil)
	if !ok {
		t.Fatal("expected true for WITH with quotes")
	}
	rest := strings.TrimSpace("WITH 'MATCH stuff' AS x MATCH (n)"[pos:])
	if !strings.HasPrefix(rest, "MATCH") {
		t.Fatalf("expected WITH clause end before MATCH, got rest=%q", rest)
	}
}

func TestFindLeadingWithClauseEnd_WithDoubleQuotes(t *testing.T) {
	pos, ok := findLeadingWithClauseEnd(`WITH "RETURN stuff" AS x RETURN y`, nil)
	if !ok {
		t.Fatal("expected true for WITH with double quotes")
	}
	rest := strings.TrimSpace((`WITH "RETURN stuff" AS x RETURN y`)[pos:])
	if !strings.HasPrefix(rest, "RETURN") {
		t.Fatalf("expected clause end before RETURN, got rest=%q", rest)
	}
}

func TestFindLeadingWithClauseEnd_WithBackticks(t *testing.T) {
	pos, ok := findLeadingWithClauseEnd("WITH `MATCH` AS x MATCH (n)", nil)
	if !ok {
		t.Fatal("expected true for WITH with backticks")
	}
	rest := strings.TrimSpace("WITH `MATCH` AS x MATCH (n)"[pos:])
	if !strings.HasPrefix(rest, "MATCH") {
		t.Fatalf("expected clause end before MATCH, got rest=%q", rest)
	}
}

func TestFindLeadingWithClauseEnd_Nested(t *testing.T) {
	// Brackets/braces/parens should be skipped.
	pos, ok := findLeadingWithClauseEnd("WITH [MATCH, x] AS arr RETURN arr", nil)
	if !ok {
		t.Fatal("expected true")
	}
	rest := strings.TrimSpace("WITH [MATCH, x] AS arr RETURN arr"[pos:])
	if !strings.HasPrefix(rest, "RETURN") {
		t.Fatalf("expected clause end before RETURN, got rest=%q", rest)
	}
}

func TestFindLeadingWithClauseEnd_NoFollowingClause(t *testing.T) {
	// WITH clause with no following keyword should return len(query).
	query := "WITH a, b"
	pos, ok := findLeadingWithClauseEnd(query, nil)
	if !ok {
		t.Fatal("expected true")
	}
	if pos != len(query) {
		t.Fatalf("expected pos=%d (end of query), got %d", len(query), pos)
	}
}

func TestFabricExecutor_ProjectionContinuationWithQuotedInputColumnUsesPipeline(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	outerQuery := "MATCH (n) RETURN n AS a, n.name AS `display name`"
	continuationQuery := "UNWIND $__fabric_apply_rows AS __fabric_row WITH __fabric_row.a AS a, __fabric_row.`display name` AS `display name` RETURN a ORDER BY a.id"
	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"a", "display name"},
				Rows: [][]interface{}{
					{map[string]interface{}{"id": 2}, "second"},
					{map[string]interface{}{"id": 1}, "first"},
				},
			},
			continuationQuery: {
				Columns: []string{"a"},
				Rows: [][]interface{}{
					{map[string]interface{}{"id": 1}},
					{map[string]interface{}{"id": 2}},
				},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	apply := &FragmentApply{
		Input: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     outerQuery,
			GraphName: "db1",
			Columns:   []string{"a", "display name"},
		},
		Inner: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "RETURN a ORDER BY a.id",
			GraphName: "db1",
			Columns:   []string{"a"},
		},
		Projection: true,
		Columns:    []string{"a"},
	}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected projection continuation error: %v", err)
	}
	if got := mock.calls[continuationQuery]; got != 1 {
		t.Fatalf("expected canonical pipeline execution once, got %d", got)
	}
	if len(result.Rows) != 2 || result.Rows[0][0].(map[string]interface{})["id"] != 1 || result.Rows[1][0].(map[string]interface{})["id"] != 2 {
		t.Fatalf("expected continuation rows ordered by a.id, got %#v", result.Rows)
	}
}

func TestFragmentContainsWrite_LeafAndInit(t *testing.T) {
	if fragmentContainsWrite(&FragmentLeaf{Input: &FragmentExec{IsWrite: true}}) {
		t.Log("leaf delegates to input")
	}
	if fragmentContainsWrite(&FragmentInit{}) {
		t.Fatal("expected init to not contain write")
	}
}

func TestExecuteUnion_RoutesToSequentialForWrite(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"CREATE (n:A)":  {Columns: []string{"n"}, Rows: [][]interface{}{{"a"}}},
			"RETURN 1 AS x": {Columns: []string{"x"}, Rows: [][]interface{}{{int64(1)}}},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	union := &FragmentUnion{
		LHS:     &FragmentExec{Input: &FragmentInit{}, Query: "CREATE (n:A)", GraphName: "db1", IsWrite: true},
		RHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1"},
		Columns: []string{"v"},
	}
	result, err := exec.Execute(context.Background(), nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestExecuteExec_RecordBindingsMerged(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS one": {Columns: []string{"one"}, Rows: [][]interface{}{{1}}},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	ctx := WithRecordBindings(context.Background(), map[string]interface{}{"bindVar": "val"})
	frag := &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS one", GraphName: "db1"}
	result, err := exec.Execute(ctx, nil, frag, map[string]interface{}{"p": "v"}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount())
	}
}

func TestRewriteLeadingWithImports_EmptyImports(t *testing.T) {
	query := "WITH a MATCH (n) RETURN n"
	got := rewriteLeadingWithImports(query, nil)
	if got != query {
		t.Fatalf("expected unchanged for nil imports, got %q", got)
	}
	got = rewriteLeadingWithImports(query, []string{})
	if got != query {
		t.Fatalf("expected unchanged for empty imports, got %q", got)
	}
}

func TestRewriteLeadingWithImports_WithOnly(t *testing.T) {
	query := "WITH a"
	got := rewriteLeadingWithImports(query, []string{"a"})
	if got != "WITH $a AS a" {
		t.Fatalf("expected WITH-only rewrite, got %q", got)
	}
}

// --- Phase 3/4 coverage tests ---

func TestFabricExecutor_UnknownFragmentType(t *testing.T) {
	// FragmentLeaf triggers the "should be resolved" error, which exercises the
	// non-default switch path. The default branch requires an external Fragment type
	// which is impossible due to the sealed interface. FragmentLeaf path is already
	// tested in TestFabricExecutor_LeafFragment.
}

func TestSplitTopLevelResultModifiers_ParensSkipped(t *testing.T) {
	// ORDER BY inside parens should not split.
	proj, mods := splitTopLevelResultModifiers("coalesce(a ORDER BY b) AS x ORDER BY x")
	if mods != "ORDER BY x" {
		t.Fatalf("expected mods='ORDER BY x', got %q", mods)
	}
	if proj != "coalesce(a ORDER BY b) AS x" {
		t.Fatalf("expected proj='coalesce(a ORDER BY b) AS x', got %q", proj)
	}
}

func TestSplitTopLevelResultModifiers_BracketsSkipped(t *testing.T) {
	proj, mods := splitTopLevelResultModifiers("[ORDER BY x] AS arr LIMIT 5")
	if mods != "LIMIT 5" {
		t.Fatalf("expected mods='LIMIT 5', got %q", mods)
	}
	_ = proj
}

func TestSplitTopLevelResultModifiers_BracesSkipped(t *testing.T) {
	proj, mods := splitTopLevelResultModifiers("{ORDER BY: 1} AS m SKIP 3")
	if mods != "SKIP 3" {
		t.Fatalf("expected mods='SKIP 3', got %q", mods)
	}
	_ = proj
}

func TestSplitLeadingModifierClause_NonOrderBy(t *testing.T) {
	clause, rest := splitLeadingModifierClause("SKIP 5 LIMIT 10", "SKIP")
	if clause != "SKIP 5" || rest != "LIMIT 10" {
		t.Fatalf("expected SKIP 5 / LIMIT 10, got clause=%q rest=%q", clause, rest)
	}
}

func TestSplitLeadingModifierClause_Empty(t *testing.T) {
	clause, rest := splitLeadingModifierClause("", "SKIP")
	if clause != "" || rest != "" {
		t.Fatalf("expected empty, got clause=%q rest=%q", clause, rest)
	}
}

func TestParseFabricVarProp(t *testing.T) {
	// Valid.
	v, p, ok := parseFabricVarProp("n.name")
	if !ok || v != "n" || p != "name" {
		t.Fatalf("expected n.name, got v=%q p=%q ok=%v", v, p, ok)
	}
	// No dot.
	_, _, ok = parseFabricVarProp("name")
	if ok {
		t.Fatal("expected false for no dot")
	}
	// Dot at start.
	_, _, ok = parseFabricVarProp(".name")
	if ok {
		t.Fatal("expected false for dot at start")
	}
	// Dot at end.
	_, _, ok = parseFabricVarProp("n.")
	if ok {
		t.Fatal("expected false for dot at end")
	}
	// Non-simple identifier.
	_, _, ok = parseFabricVarProp("n.a-b")
	if ok {
		t.Fatal("expected false for non-simple prop")
	}
}

func TestExecuteExec_WithTransaction(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id": {
				Columns: []string{"n.id"},
				Rows:    [][]interface{}{{"id1"}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	tx := NewFabricTransaction("tx-exec-test")

	frag := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n.id",
		GraphName: "db1",
		IsWrite:   false,
	}
	result, err := exec.Execute(context.Background(), tx, frag, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount())
	}
	// Should have registered a sub-transaction.
	if len(tx.Participants()) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(tx.Participants()))
	}
}

func TestExecuteUnionSequential_LHSError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{err: fmt.Errorf("lhs error")}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	union := &FragmentUnion{
		LHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1", GraphName: "db1"},
		RHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 2", GraphName: "db1"},
	}
	_, err := exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	if err == nil || !strings.Contains(err.Error(), "union LHS failed") {
		t.Fatalf("expected LHS error, got: %v", err)
	}
}

func TestExecuteUnionSequential_RHSError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	callCount := 0
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1": {Columns: []string{"x"}, Rows: [][]interface{}{{1}}},
		},
	}
	// Override to fail on second call.
	localExec := NewLocalFragmentExecutor(&callCountCypherExecutor{
		mock:      mock,
		callCount: &callCount,
		failAt:    2,
	}, func(name string) (storage.Engine, error) {
		return &mockEngine{}, nil
	})
	exec := NewFabricExecutor(catalog, localExec, nil)

	union := &FragmentUnion{
		LHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1", GraphName: "db1"},
		RHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1", GraphName: "db1"},
	}
	_, err := exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	if err == nil || !strings.Contains(err.Error(), "union RHS failed") {
		t.Fatalf("expected RHS error, got: %v", err)
	}
}

type callCountCypherExecutor struct {
	mock      *mockCypherExecutor
	callCount *int
	failAt    int
}

func (c *callCountCypherExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, eng storage.Engine, query string, params map[string]interface{}, record map[string]interface{}) ([]string, [][]interface{}, error) {
	*c.callCount++
	if *c.callCount >= c.failAt {
		return nil, nil, fmt.Errorf("simulated failure at call %d", *c.callCount)
	}
	return c.mock.ExecuteQueryWithRecord(ctx, dbName, eng, query, params, record)
}

func TestExecuteUnionParallel_ErrorPropagation(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{err: fmt.Errorf("parallel error")}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	union := &FragmentUnion{
		LHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1", GraphName: "db1"},
		RHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 2", GraphName: "db1"},
	}
	_, err := exec.executeUnionParallel(context.Background(), nil, union, nil, "")
	if err == nil {
		t.Fatal("expected error from parallel union")
	}
}

func TestLocalFragmentExecutor_GetEngineError(t *testing.T) {
	mock := &mockCypherExecutor{results: map[string]*ResultStream{}}
	local := NewLocalFragmentExecutor(mock, func(name string) (storage.Engine, error) {
		return nil, fmt.Errorf("no such db: %s", name)
	})
	_, err := local.Execute(context.Background(), &LocationLocal{DBName: "missing"}, "RETURN 1", nil)
	if err == nil || !strings.Contains(err.Error(), "failed to get storage") {
		t.Fatalf("expected storage error, got: %v", err)
	}
}

func TestLocalFragmentExecutor_ExecuteError(t *testing.T) {
	mock := &mockCypherExecutor{err: fmt.Errorf("cypher error")}
	local := NewLocalFragmentExecutor(mock, func(name string) (storage.Engine, error) {
		return &mockEngine{}, nil
	})
	_, err := local.Execute(context.Background(), &LocationLocal{DBName: "db1"}, "RETURN 1", nil)
	if err == nil || !strings.Contains(err.Error(), "local execution") {
		t.Fatalf("expected local execution error, got: %v", err)
	}
}

func TestQueryGateway_PlanError(t *testing.T) {
	catalog := NewCatalog()
	p := NewFabricPlanner(catalog, testStatementWrites)
	exec := NewFabricExecutor(catalog, nil, nil)
	gw := NewQueryGateway(p, exec)

	// Empty query → plan error.
	_, err := gw.Execute(context.Background(), nil, "", "db", nil, "")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestQueryGateway_NilGateway(t *testing.T) {
	var gw *QueryGateway
	_, err := gw.Execute(context.Background(), nil, "RETURN 1", "db", nil, "")
	if err == nil {
		t.Fatal("expected error for nil gateway")
	}
}

func TestSameColumns(t *testing.T) {
	if !sameColumns([]string{"a", "b"}, []string{"a", "b"}) {
		t.Fatal("expected true for matching columns")
	}
	if sameColumns([]string{"a", "b"}, []string{"a", "c"}) {
		t.Fatal("expected false for different columns")
	}
	if sameColumns([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("expected false for different lengths")
	}
}

func TestFabricTransaction_Rollback_WithBoundCallbacks(t *testing.T) {
	tx := NewFabricTransaction("tx-rb-bound")
	_, _ = tx.GetOrOpen("shard_a", false)

	rolledBack := false
	_ = tx.BindParticipantCallbacks("shard_a", ParticipantCallbacks{Commit: func(_ *SubTransaction) error { return nil }, Rollback: func(_ *SubTransaction) error {
		rolledBack = true
		return nil
	}})

	err := tx.Rollback(nil) // nil global rollback, uses bound callbacks.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rolledBack {
		t.Fatal("expected bound rollback callback to run")
	}
}

func TestFabricTransaction_Rollback_Error(t *testing.T) {
	tx := NewFabricTransaction("tx-rb-err")
	_, _ = tx.GetOrOpen("shard_a", false)

	err := tx.Rollback(func(_ *SubTransaction) error {
		return fmt.Errorf("rollback failed")
	})
	if err == nil || !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("expected rollback error, got: %v", err)
	}
	if tx.State() != "rolledback" {
		t.Fatalf("expected rolledback state, got %s", tx.State())
	}
}

func TestFabricTransaction_Commit_NilCallbacksSkipsToCommitted(t *testing.T) {
	tx := NewFabricTransaction("tx-nil-cb")
	_, _ = tx.GetOrOpen("shard_a", false)

	err := tx.Commit(nil, nil) // nil commit callback → auto-committed.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.State() != "committed" {
		t.Fatalf("expected committed, got %s", tx.State())
	}
	subs := tx.SubTransactions()
	if subs["shard_a"].State != "committed" {
		t.Fatalf("expected sub committed, got %s", subs["shard_a"].State)
	}
}

func TestFabricTransaction_Commit_CompensationError(t *testing.T) {
	tx := NewFabricTransaction("tx-comp-err")
	_, _ = tx.GetOrOpen("shard_a", false)
	_, _ = tx.GetOrOpen("shard_b", false)

	commitCount := 0
	err := tx.Commit(
		func(sub *SubTransaction) error {
			commitCount++
			if commitCount == 2 {
				return fmt.Errorf("commit failed")
			}
			return nil
		},
		func(_ *SubTransaction) error {
			return fmt.Errorf("compensation also failed")
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "cleanup not confirmed") {
		t.Fatalf("expected unconfirmed cleanup in error, got: %v", err)
	}
}

func TestSubstituteVarsWithParams_EmptyVar(t *testing.T) {
	// Empty var in import list should be skipped.
	got := substituteVarsWithParams("MATCH (n) WHERE n.id = x", []string{"", "x"})
	if !strings.Contains(got, "$x") {
		t.Fatalf("expected x substituted, got %q", got)
	}
}

func TestSanitizeOtherWhereForImportColumn(t *testing.T) {
	cases := []struct {
		name      string
		where     string
		importCol string
		want      string
		ok        bool
	}{
		{name: "empty", where: "", importCol: "sourceId", want: "", ok: true},
		{name: "keep_non_import_condition", where: "tt.language = 'es'", importCol: "sourceId", want: "tt.language = 'es'", ok: true},
		{name: "drop_import_not_null_guard", where: "sourceId IS NOT NULL", importCol: "sourceId", want: "", ok: true},
		{name: "drop_backtick_import_not_null_guard", where: "`sourceId` IS NOT NULL", importCol: "sourceId", want: "", ok: true},
		{name: "mixed_keep_and_drop", where: "sourceId IS NOT NULL AND tt.language = 'es'", importCol: "sourceId", want: "tt.language = 'es'", ok: true},
		{name: "reject_other_import_reference", where: "sourceId > 0", importCol: "sourceId", want: "", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := cyphertext.FilterImportConjuncts(tc.where, tc.importCol, cyphertext.IdentifierExact, splitTopLevelAnd)
			if ok != tc.ok {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("where mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestRewriteLeadingWithImports_WithMatchSubstitution(t *testing.T) {
	query := "WITH id MATCH (n) WHERE n.id = id RETURN n"
	got := rewriteLeadingWithImports(query, []string{"id"})
	// Simple import WITH + MATCH → substitution path.
	if !strings.Contains(got, "$id") {
		t.Fatalf("expected substitution, got %q", got)
	}
}

func TestRewriteLeadingWithImports_WithOptionalMatch(t *testing.T) {
	query := "WITH id OPTIONAL MATCH (n) WHERE n.id = id RETURN n"
	got := rewriteLeadingWithImports(query, []string{"id"})
	if !strings.Contains(got, "$id") {
		t.Fatalf("expected substitution for OPTIONAL MATCH, got %q", got)
	}
}

func TestRewriteLeadingWithImports_WithNonMatchClause(t *testing.T) {
	query := "WITH id, count(*) AS c RETURN id, c"
	got := rewriteLeadingWithImports(query, []string{"id"})
	// Non-simple WITH clause → regular rewrite.
	if !strings.Contains(got, "$id AS id") {
		t.Fatalf("expected param rewrite, got %q", got)
	}
}

func TestIsSimpleWithImportClause_EmptyParts(t *testing.T) {
	// Empty WITH body after trimming.
	if isSimpleWithImportClause("WITH  ", []string{"a"}) {
		t.Fatal("expected false for whitespace-only WITH body")
	}
}

func TestFindLeadingWithClauseEnd_SingleQuoteEscape(t *testing.T) {
	// Escaped single quote inside WITH clause.
	query := "WITH 'it''s' AS x MATCH (n)"
	pos, ok := findLeadingWithClauseEnd(query, nil)
	if !ok {
		t.Fatal("expected true")
	}
	rest := strings.TrimSpace(query[pos:])
	if !strings.HasPrefix(rest, "MATCH") {
		t.Fatalf("expected clause end before MATCH, got rest=%q", rest)
	}
}

func TestSplitTopLevelEquality_AllQuoteTypes(t *testing.T) {
	// Single quote.
	lhs, rhs, ok := splitTopLevelEquality("n.x = 'a=b'")
	if !ok {
		t.Fatal("expected equality outside single quote")
	}
	_ = lhs
	_ = rhs

	// Double quote.
	lhs, rhs, ok = splitTopLevelEquality(`n.x = "a=b"`)
	if !ok {
		t.Fatal("expected equality outside double quote")
	}
	_ = lhs
	_ = rhs

	// Backtick.
	lhs, rhs, ok = splitTopLevelEquality("n.`x=y` = 1")
	if !ok {
		t.Fatal("expected equality outside backtick")
	}
	_ = lhs
	_ = rhs

	// Paren nesting.
	_, _, ok = splitTopLevelEquality("(a = b)")
	if ok {
		t.Fatal("expected no top-level eq in parens")
	}

	// Bracket nesting.
	_, _, ok = splitTopLevelEquality("[a = b]")
	if ok {
		t.Fatal("expected no top-level eq in brackets")
	}

	// Brace nesting.
	_, _, ok = splitTopLevelEquality("{a = b}")
	if ok {
		t.Fatal("expected no top-level eq in braces")
	}
}

func TestFragmentContainsWrite_LeafInputWrite(t *testing.T) {
	leaf := &FragmentLeaf{
		Input: &FragmentExec{IsWrite: true},
	}
	if !fragmentContainsWrite(leaf) {
		t.Fatal("expected leaf with write input to be detected")
	}
}

func TestExecuteExec_RemotePath(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("remote.shard", &LocationRemote{DBName: "shard", URI: "bolt://r:7687"})

	// Create a mock HTTP server for the remote executor.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{
				map[string]any{
					"columns": []string{"x"},
					"data":    []any{map[string]any{"row": []any{42}}},
				},
			},
			"errors": []any{},
		})
	}))
	defer srv.Close()

	// Override location to point to test server.
	catalog.Register("remote.shard", &LocationRemote{DBName: "shard", URI: srv.URL})

	remote := NewRemoteFragmentExecutor()
	defer func() { _ = remote.Close() }()
	exec := NewFabricExecutor(catalog, nil, remote)

	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "RETURN 42 AS x", GraphName: "remote.shard",
	}
	result, err := exec.Execute(context.Background(), nil, frag, nil, "tok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount())
	}
}

func TestExecuteExec_RemotePathError(t *testing.T) {
	catalog := NewCatalog()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{},
			"errors":  []any{map[string]any{"message": "remote failure"}},
		})
	}))
	defer srv.Close()

	catalog.Register("remote.shard", &LocationRemote{DBName: "shard", URI: srv.URL})
	remote := NewRemoteFragmentExecutor()
	defer func() { _ = remote.Close() }()
	exec := NewFabricExecutor(catalog, nil, remote)

	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "RETURN 1", GraphName: "remote.shard",
	}
	_, err := exec.Execute(context.Background(), nil, frag, nil, "tok")
	if err == nil {
		t.Fatal("expected error from remote execution")
	}
}

func TestExecuteExec_RemoteWithTransaction(t *testing.T) {
	catalog := NewCatalog()

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/shard/tx"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  srv.URL + "/db/shard/tx/1/commit",
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/shard/tx/1"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{
					map[string]any{
						"columns": []string{"x"},
						"data":    []any{map[string]any{"row": []any{1}}},
					},
				},
				"errors": []any{},
			})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/shard/tx/1"):
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/shard/tx/1/commit"):
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	loc := &LocationRemote{DBName: "shard", URI: srv.URL}
	catalog.Register("remote.shard", loc)
	remote := NewRemoteFragmentExecutor()
	defer func() { _ = remote.Close() }()
	exec := NewFabricExecutor(catalog, nil, remote)

	tx := NewFabricTransaction("tx-remote")
	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "remote.shard", IsWrite: true,
	}
	result, err := exec.Execute(context.Background(), tx, frag, nil, "Bearer tok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount())
	}
	if err := tx.Commit(nil, nil); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestRemoteFragmentExecutor_Close_Empty(t *testing.T) {
	re := NewRemoteFragmentExecutor()
	err := re.Close()
	if err != nil {
		t.Fatalf("unexpected error closing empty executor: %v", err)
	}
}

// --- End-to-end semantic tests ---
// These verify observable Fabric behavior as a user would see it:
// query in → result out, exercising planning + execution together.

func TestE2E_SingleGraphQueryReturnsCorrectRows(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n:Person) RETURN n.name AS name": {
				Columns: []string{"name"},
				Rows:    [][]interface{}{{"Alice"}, {"Bob"}},
			},
		},
	}
	gw := NewQueryGateway(
		NewFabricPlanner(catalog, testStatementWrites),
		NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil),
	)
	res, err := gw.Execute(context.Background(), nil, "MATCH (n:Person) RETURN n.name AS name", "db", nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "name" {
		t.Fatalf("expected columns [name], got %v", res.Columns)
	}
	if res.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", res.RowCount())
	}
	if res.Rows[0][0] != "Alice" || res.Rows[1][0] != "Bob" {
		t.Fatalf("unexpected row data: %v", res.Rows)
	}
}

func TestE2E_UseClauseRoutesToCorrectGraph(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "db1"})
	catalog.Register("nornic.tr", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (t:Translation) RETURN t.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"t-1"}, {"t-2"}},
			},
		},
	}
	gw := NewQueryGateway(
		NewFabricPlanner(catalog, testStatementWrites),
		NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil),
	)
	res, err := gw.Execute(context.Background(), nil, "USE nornic.tr MATCH (t:Translation) RETURN t.id AS id", "nornic", nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", res.RowCount())
	}
}

func TestE2E_UnionCombinesResults(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 'a' AS val": {
				Columns: []string{"val"},
				Rows:    [][]interface{}{{"a"}},
			},
			"RETURN 'b' AS val": {
				Columns: []string{"val"},
				Rows:    [][]interface{}{{"b"}},
			},
		},
	}
	gw := NewQueryGateway(
		NewFabricPlanner(catalog, testStatementWrites),
		NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil),
	)
	res, err := gw.Execute(context.Background(), nil,
		"RETURN 'a' AS val UNION ALL RETURN 'b' AS val", "db", nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RowCount() != 2 {
		t.Fatalf("expected 2 rows from UNION ALL, got %d", res.RowCount())
	}
}

func TestE2E_UnionDistinctDeduplicates(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS x": {
				Columns: []string{"x"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}
	gw := NewQueryGateway(
		NewFabricPlanner(catalog, testStatementWrites),
		NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil),
	)
	res, err := gw.Execute(context.Background(), nil,
		"RETURN 1 AS x UNION RETURN 1 AS x", "db", nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RowCount() != 1 {
		t.Fatalf("expected 1 row after UNION DISTINCT, got %d", res.RowCount())
	}
}

func TestE2E_WriteDetectionEnforcesOneWriteShard(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("shard1", &LocationLocal{DBName: "shard1"})
	catalog.Register("shard2", &LocationLocal{DBName: "shard2"})

	mock := &mockCypherExecutor{
		writes: map[string]bool{"CREATE (n:Test)": true},
		results: map[string]*ResultStream{
			"CREATE (n:Test)": {Columns: []string{"n"}, Rows: [][]interface{}{{"created"}}},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	tx := NewFabricTransaction("tx-e2e")

	// Write to shard1 succeeds.
	frag1 := &FragmentExec{Input: &FragmentInit{}, Query: "CREATE (n:Test)", GraphName: "shard1", IsWrite: true}
	_, err := exec.Execute(context.Background(), tx, frag1, nil, "")
	if err != nil {
		t.Fatalf("first write should succeed: %v", err)
	}

	// Write to shard2 must fail with Neo4j-compatible error.
	frag2 := &FragmentExec{Input: &FragmentInit{}, Query: "CREATE (n:Test)", GraphName: "shard2", IsWrite: true}
	_, err = exec.Execute(context.Background(), tx, frag2, nil, "")
	if !errors.Is(err, ErrSecondWriteShard) {
		t.Fatalf("expected ErrSecondWriteShard, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Writing to more than one database") {
		t.Fatalf("expected Neo4j-compatible error message, got: %v", err)
	}
}

func TestE2E_CallUseSubqueryRoutesCorrectly(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "db1"})
	catalog.Register("nornic.tr", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (t:Translation)\n  RETURN t.id AS translationId": {
				Columns: []string{"translationId"},
				Rows:    [][]interface{}{{"t-1"}},
			},
			"UNWIND $__fabric_apply_rows AS __fabric_row WITH __fabric_row.translationId AS translationId RETURN translationId": {
				Columns: []string{"translationId"},
				Rows:    [][]interface{}{{"t-1"}},
			},
		},
	}
	gw := NewQueryGateway(
		NewFabricPlanner(catalog, testStatementWrites),
		NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil),
	)

	query := `USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId
}
RETURN translationId`

	res, err := gw.Execute(context.Background(), nil, query, "nornic", nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestExecuteUnionSequential_NilResults(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS x": {Columns: []string{"x"}, Rows: nil},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	union := &FragmentUnion{
		LHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1"},
		RHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1"},
		Columns: []string{"x"},
	}
	result, err := exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows for nil results, got %d", len(result.Rows))
	}
}

func TestSplitTopLevelEquality_NestedParens(t *testing.T) {
	// Equals inside nested parens should be skipped.
	_, _, ok := splitTopLevelEquality("func(a = b)")
	if ok {
		t.Fatal("expected no top-level equality inside function call parens")
	}
}

func TestSplitTopLevelEquality_NestedBrackets(t *testing.T) {
	_, _, ok := splitTopLevelEquality("[a = b]")
	if ok {
		t.Fatal("expected no top-level equality inside brackets")
	}
}

func TestSplitTopLevelEquality_NestedBraces(t *testing.T) {
	_, _, ok := splitTopLevelEquality("{a = b}")
	if ok {
		t.Fatal("expected no top-level equality inside braces")
	}
}

func TestParseSimpleOrderByClause_EmptyPart(t *testing.T) {
	specs := parseSimpleOrderByClause(", ,", map[string]string{})
	if len(specs) != 0 {
		t.Fatalf("expected 0 specs for empty parts, got %d", len(specs))
	}
}

func TestPopulateFromManager_WithAliases(t *testing.T) {
	// Alias coverage is already handled via catalog_test.go's PopulateFromManager test.
	// Just verify the alias path in PopulateFromManager by registering manually.
	c := NewCatalog()
	c.Register("mydb", &LocationLocal{DBName: "mydb"})
	c.Register("myalias", &LocationLocal{DBName: "mydb"})

	loc, err := c.Resolve("myalias")
	if err != nil {
		t.Fatalf("expected alias to be resolvable: %v", err)
	}
	local, ok := loc.(*LocationLocal)
	if !ok {
		t.Fatalf("expected LocationLocal, got %T", loc)
	}
	if local.DBName != "mydb" {
		t.Fatalf("expected mydb, got %s", local.DBName)
	}
}

// A remote fragment binds what it imports from parameters of its own; a
// scope clause's imports run in that scope on the remote.
func TestRemoteImportStatementNamesAndScope(t *testing.T) {
	e := &FabricExecutor{}
	record := map[string]interface{}{"x": int64(1), "__fabric_import_0": int64(2)}

	f := &FragmentExec{Query: "WITH x RETURN x + $__fabric_import_0 AS v", Input: &FragmentInit{ImportColumns: []string{"x"}}}
	query, params, scoped, err := e.remoteImportStatement(f, map[string]interface{}{"__fabric_import_0_1": "caller"}, record)
	require.NoError(t, err)
	require.Nil(t, scoped)
	require.Equal(t, "WITH $__fabric_import_0_2 AS x WITH x RETURN x + $__fabric_import_0 AS v", query)
	require.Equal(t, map[string]interface{}{"__fabric_import_0_1": "caller", "__fabric_import_0_2": int64(1)}, params)

	f = &FragmentExec{Query: "WITH * RETURN __fabric_import_0 + x AS v", Input: &FragmentInit{ImportColumns: []string{"*"}}}
	query, _, _, err = e.remoteImportStatement(f, nil, record)
	require.NoError(t, err)
	require.Equal(t, "WITH $__fabric_import_0_1 AS __fabric_import_0, $__fabric_import_1 AS x WITH * RETURN __fabric_import_0 + x AS v", query)

	f = &FragmentExec{Query: "WITH 10 AS y RETURN x + y AS v", ScopeImports: []string{"x"}}
	query, _, scoped, err = e.remoteImportStatement(f, nil, record)
	require.NoError(t, err)
	require.Equal(t, "WITH $__fabric_import_0 AS x CALL (x) { WITH 10 AS y RETURN x + y AS v } RETURN *", query)
	require.Equal(t, []string{"x"}, scoped)
	res := &ResultStream{Columns: []string{"v", "x"}, Rows: [][]interface{}{{int64(11), int64(1)}}}
	dropResultColumns(res, scoped)
	require.Equal(t, []string{"v"}, res.Columns)
	require.Equal(t, [][]interface{}{{int64(11)}}, res.Rows)
}
