package cypher

// Lane L6 (closeout, partial root causes): test-side diagnostics only.
//
//   TestL6YieldBuiltInExplain        G-procedure-yield-validation, built-in db.labels() EXPLAIN
//   TestL6HeaderOnlyVersusExecute    G-result-columns, P-N152-63
//   TestL6StaticTypeLossOwners       G-static-clause-types, NOR-79 matrix / NOR-82 / P-N152-147
//   TestL6RelationshipSeedExtraTerms G-relationship-id-seed, E38.8
//   TestL6LabelScanPageShapes        G-label-scan-projected, E38.16 page shapes

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// zzL6YieldAgainstDeclaredReturns is the missing step, test-side: compare
// each YIELD item with the registry's declared outputs (procedureDeclaredReturns,
// which also reads a built-in's signature) before anything runs.
func zzL6YieldAgainstDeclaredReturns(ctx context.Context, cypher string) (string, bool, []string) {
	yield := parseYieldClause(ctx, cypher)
	name, err := extractProcedureName(ctx, cypher)
	if err != nil || yield == nil || yield.yieldAll {
		return "", true, nil
	}
	proc, found := globalProcedureRegistry.Get(name)
	if !found {
		return "", true, nil
	}
	declared := columnNames(procedureDeclaredReturns(proc.Spec))
	set := map[string]bool{}
	for _, d := range declared {
		set[d] = true
	}
	for _, item := range yield.items {
		if !set[item.name] {
			return item.name, false, declared
		}
	}
	return "", true, declared
}

func zzL6Names(n interface {
	Visit(func([]string), func())
}) string {
	out := "<unknown>"
	n.Visit(func(known []string) { out = fmt.Sprintf("%q", known) }, func() {})
	return out
}

func TestL6YieldBuiltInExplain(t *testing.T) {
	ctx := context.Background()
	ensureBuiltInProceduresRegistered()
	exec := newCallScopeExecutor(t)
	for _, tc := range []struct{ name, call string }{
		{"trigger_undeclared", "CALL db.labels() YIELD missing"},
		{"control_declared", "CALL db.labels() YIELD label"},
		{"control_no_yield", "CALL db.labels()"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, found := globalProcedureRegistry.Get("db.labels")
			declared := []string(nil)
			if found {
				declared = columnNames(procedureDeclaredReturns(proc.Spec))
			}
			projected := zzL6Names(projectedProcedureNames(ctx, tc.call))
			headerCols, headerErr := exec.StatementResultColumnsContext(ctx, tc.call, AcceptedSource)
			undeclared, ok, _ := zzL6YieldAgainstDeclaredReturns(ctx, tc.call)
			res, explainErr := exec.Execute(ctx, "EXPLAIN "+tc.call, nil)
			explainCols := []string(nil)
			if res != nil {
				explainCols = res.Columns
			}
			_, plainErr := exec.Execute(ctx, tc.call, nil)
			t.Logf("L6_YIELD_BUILTIN case=%s call=%q registry_found=%v declared=%q projectedProcedureNames(result_columns_scope.go:175-181)=%s StatementResultColumnsContext=%q err=%v | EXPLAIN columns=%q err=%v | plain err=%v | corrected_declaration_check ok=%v undeclared=%q",
				tc.name, tc.call, found, declared, projected, headerCols, headerErr, explainCols, explainErr, plainErr, ok, undeclared)
			if tc.name == "trigger_undeclared" {
				if ok {
					t.Fatalf("premise: corrected check accepted an undeclared YIELD item")
				}
				if explainErr == nil {
					t.Errorf("CAUSE: EXPLAIN accepted undeclared YIELD %q: header compiler projected %s from the YIELD items without consulting declared %q", undeclared, projected, declared)
				}
			} else if !ok {
				t.Fatalf("control rejected by corrected check: %q", undeclared)
			}
		})
	}
}

func TestL6HeaderOnlyVersusExecute(t *testing.T) {
	ctx := context.Background()
	for _, q := range []string{
		"WITH 'value' AS `a\\u0062` RETURN `a\\u0062`",
		"RETURN {`a\\u0062`: 1} AS m, 1 AS `a\\u0062`",
		"WITH 'value' AS `ab` RETURN `ab`",
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "l6hdr"))
		headerRaw, headerRawErr := exec.StatementResultColumns(q, RawSource)
		headerCtx, headerCtxErr := exec.StatementResultColumnsContext(ctx, q, RawSource)
		result, execErr := exec.Execute(ctx, q, nil)
		var execCols []string
		if result != nil {
			execCols = result.Columns
		}
		_, source := cyphertext.WithSource(ctx, q)
		restored := "<no source rewrite>"
		if rewrite := sourceQueryRewrite(source); rewrite != nil && headerRawErr == nil {
			r, _ := (resultRestoration{source: rewrite}).restore(ctx, &ExecuteResult{Columns: append([]string(nil), headerRaw...)}, nil, nil)
			if r != nil {
				restored = fmt.Sprintf("%q", r.Columns)
			}
		}
		// Bypass: execute the decoded text, which carries no source edit, so no restoration runs.
		decoded, decErr := exec.Execute(ctx, source.Text, nil)
		var decodedCols []string
		if decoded != nil {
			decodedCols = decoded.Columns
		}
		t.Logf("L6_HEADER query=%q edits=%d | StatementResultColumns(Raw)=%q err=%v | StatementResultColumnsContext(Raw)=%q err=%v | Execute=%q err=%v | restore(headerOnly)=%s | Execute(decoded text %q)=%q err=%v",
			q, len(source.Edits), headerRaw, headerRawErr, headerCtx, headerCtxErr, execCols, execErr, restored, source.Text, decodedCols, decErr)
		if fmt.Sprintf("%q", headerRaw) != fmt.Sprintf("%q", execCols) {
			t.Errorf("CAUSE: header-only API %q differs from Execute %q; restoring the header-only columns through the source rewrite gives %s", headerRaw, execCols, restored)
		}
	}
}

func TestL6StaticTypeLossOwners(t *testing.T) {
	ctx := context.Background()
	params := map[string]interface{}{"i": int64(1)}
	// NOR-79 matrix: the parameter check sees only a bare $name argument.
	for _, stmt := range []string{
		"UNWIND [] AS r MATCH (n) RETURN toUpper($i) AS a",
		"UNWIND [] AS r MATCH (n) WITH n, $i AS v RETURN toUpper(v) AS a",
		"UNWIND [] AS r MATCH (n) WITH n, $i AS v RETURN toUpper($i) AS a",
	} {
		err := validateStaticFunctionParameters(ctx, stmt, params, false)
		t.Logf("L6_STATIC NOR-79 validateStaticFunctionParameters(static_function_argument_types.go:615) stmt=%q -> %v", stmt, err)
	}
	noParams := staticTypeScope{}
	withParams := staticTypeScope{params: params}
	t.Logf("L6_STATIC NOR-79 operandOf($i) scope-without-params=%q scope-with-params=%q staticExpressionType-with-params=%q",
		noParams.operandOf(ctx, "$i").typeName(), withParams.operandOf(ctx, "$i").typeName(), withParams.staticExpressionType(ctx, "$i"))
	aliasAsBuilt := staticTypeScope{kinds: matchSemanticScope{"n": matchBindingNode}, values: map[string]string{"v": noParams.operandOf(ctx, "$i").typeName()}, complete: true}
	aliasCorrected := staticTypeScope{kinds: matchSemanticScope{"n": matchBindingNode}, values: map[string]string{"v": withParams.staticExpressionType(ctx, "$i")}, complete: true}
	errBuilt := validateStaticFunctionVariablesTyped(ctx, "toUpper(v)", func() staticTypeScope { return aliasAsBuilt }, staticTypeScope.staticExpressionType)
	errCorrected := validateStaticFunctionVariablesTyped(ctx, "toUpper(v)", func() staticTypeScope { return aliasCorrected }, staticTypeScope.staticExpressionType)
	t.Logf("L6_STATIC NOR-79 alias v typed as built (params not in scope, match_semantic_validation.go:526) values[v]=%q -> toUpper(v) err=%v | corrected (param type carried) values[v]=%q -> err=%v",
		aliasAsBuilt.values["v"], errBuilt, aliasCorrected.values["v"], errCorrected)
	if errBuilt == nil && errCorrected != nil {
		t.Errorf("CAUSE NOR-79 matrix: alias of $i is untyped in the semantic scope (no params); carrying the parameter type refuses toUpper(v): %v", errCorrected)
	}

	// NOR-82: where the uncertain addition loses its type.
	scope := staticTypeScope{kinds: matchSemanticScope{"n": matchBindingNode}, complete: true}
	for _, expr := range []string{"n.k", "date()", "[date()]", "n.k + date()", "n.k + [date()]", "head(n.k + date())", "head(n.k + [date()])"} {
		op := scope.operandOf(ctx, expr)
		t.Logf("L6_STATIC NOR-82 operandOf(%q) typeName=%q operand=%+v", expr, op.typeName(), op)
	}
	left, right, list := scope.operandOf(ctx, "n.k"), scope.operandOf(ctx, "date()"), scope.operandOf(ctx, "[date()]")
	sumDate, errDate := checkOperator('+', left, right, false)
	sumList, errList := checkOperator('+', left, list, false)
	t.Logf("L6_STATIC NOR-82 checkOperator('+', n.k, date()) -> typeName=%q operand=%+v err=%v additionResultTypes=%q", sumDate.typeName(), sumDate, errDate, additionResultTypes(left, right))
	t.Logf("L6_STATIC NOR-82 checkOperator('+', n.k, [date()]) -> typeName=%q operand=%+v err=%v additionResultTypes=%q", sumList.typeName(), sumList, errList, additionResultTypes(left, list))
	for _, expr := range []string{"toUpper(head(n.k + date()))", "toUpper(head(n.k + [date()]))"} {
		err := validateStaticFunctionVariablesTyped(ctx, expr, func() staticTypeScope { return scope }, staticTypeScope.staticExpressionType)
		t.Logf("L6_STATIC NOR-82 validateStaticFunctionVariablesTyped(%q) -> %v", expr, err)
	}

	// P-N152-147 (COUNT in a procedure filter, NOR-80's other position) and NOR-80 DELETE.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "l6static"))
	for _, stmt := range []string{
		"UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { MATCH (m) } > 0 RETURN label",
		"UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { CALL { MATCH (m) RETURN m } RETURN m } > 0 RETURN label",
		"UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN n END",
		"UNWIND [] AS s MATCH (n) DELETE CASE WHEN true THEN n END",
		"UNWIND [] AS s MATCH (n) DELETE n",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		t.Logf("L6_STATIC EXEC %q -> err=%v", stmt, err)
	}
}

func TestL6RelationshipSeedExtraTerms(t *testing.T) {
	t.Setenv("NORNICDB_QUERY_CACHE_ENABLED", "false")
	ctx := context.Background()
	base, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "seedprobe"))
	n := 2000
	if v, err := strconv.Atoi(os.Getenv("ZZL6_SEED_ROWS")); err == nil && v > 0 {
		n = v
	}
	if _, err := exec.Execute(ctx, "CREATE (h:Hub {k: 'hub'}) WITH h UNWIND range(0, $n - 1) AS i CREATE (h)-[:R {w: i}]->(:Leaf {i: i})", map[string]any{"n": int64(n)}); err != nil {
		t.Fatal(err)
	}
	ids, err := exec.Execute(ctx, "MATCH (a:Hub)-[r:R]->(b:Leaf) RETURN elementId(r), elementId(a), elementId(b), r.w", nil)
	if err != nil || len(ids.Rows) != n {
		t.Fatalf("ids: err=%v rows=%d", err, len(ids.Rows))
	}
	rows := make([]any, 0, n)
	for _, row := range ids.Rows {
		rows = append(rows, map[string]any{"r": row[0], "a": row[1], "b": row[2], "w": row[3]})
	}
	repeat := func(term string, k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = term
		}
		return strings.Join(parts, " AND ")
	}
	variants := []struct{ name, where string }{
		{"pin", "elementId(r) = x.r"},
		{"pin+endpoints", "elementId(r) = x.r AND elementId(a) = x.a AND elementId(b) = x.b"},
		{"pin+31prop", "elementId(r) = x.r AND " + repeat("r.w >= 0", 31)},
		{"pin+31id", "elementId(r) = x.r AND " + repeat("elementId(a) = x.a", 31)},
	}
	first := ids.Rows[0]
	for _, v := range variants {
		// Seed recognition with the row's values as literals (the per-row form).
		literal := strings.NewReplacer("x.r", "'"+fmt.Sprint(first[0])+"'", "x.a", "'"+fmt.Sprint(first[1])+"'", "x.b", "'"+fmt.Sprint(first[2])+"'").Replace(v.where)
		edge, none, seeded := exec.relationshipIDSeedEdge(ctx, "(a:Hub)-[r:R]->(b:Leaf)", literal)
		edgeID := ""
		if edge != nil {
			edgeID = string(edge.ID)
		}
		terms := len(splitTopLevelAndConjuncts(ctx, v.where))
		q := "UNWIND $rows AS x MATCH (a:Hub)-[r:R]->(b:Leaf) WHERE " + v.where + " RETURN count(*) AS c"
		p := map[string]any{"rows": rows}
		if _, err := exec.Execute(ctx, q, p); err != nil {
			t.Fatalf("%s warm: %v", v.name, err)
		}
		times := []float64{}
		var allocs []uint64
		for i := 0; i < 5; i++ {
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			start := time.Now()
			if _, err := exec.Execute(ctx, q, p); err != nil {
				t.Fatal(err)
			}
			times = append(times, time.Since(start).Seconds()*1000)
			runtime.ReadMemStats(&m1)
			allocs = append(allocs, m1.TotalAlloc-m0.TotalAlloc)
		}
		var res *ExecuteResult
		rcaPprofTop(t, "seed-"+v.name, func() { res, _ = exec.Execute(ctx, q, p) })
		count := any(nil)
		if res != nil && len(res.Rows) > 0 {
			count = res.Rows[0][0]
		}
		t.Logf("L6_SEED variant=%s terms=%d seed_ok=%v seed_none=%v seed_edge=%q ms=%v allocs=%v count=%v per_row_ms_median=%.4f",
			v.name, terms, seeded, none, edgeID, times, allocs, count, zzL6Median(times)/float64(n))
	}
}

func zzL6Median(values []float64) float64 {
	c := append([]float64(nil), values...)
	for i := range c {
		for j := i + 1; j < len(c); j++ {
			if c[j] < c[i] {
				c[i], c[j] = c[j], c[i]
			}
		}
	}
	return c[len(c)/2]
}

func TestL6LabelScanPageShapes(t *testing.T) {
	ctx := context.Background()
	base, err := storage.NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	must := func(q string, p map[string]any) {
		if _, err := exec.Execute(ctx, q, p); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, label := range []string{"PDGraphNode", "PDDocument", "PDPoint", "PDContext"} {
		must("CREATE INDEX l6_"+label+" FOR (n:"+label+") ON (n.id)", nil)
	}
	seed := []any{}
	for i := 0; i < 100; i++ {
		seed = append(seed, map[string]any{"id": fmt.Sprintf("d%04d", i), "ctx": fmt.Sprintf("c%04d", i)})
	}
	must(`UNWIND $rows AS r CREATE (d:PDGraphNode:PDDocument {id:r.id,document_id:r.id}) CREATE (c:PDGraphNode:PDContext {id:r.ctx,updated_at:1})`, map[string]any{"rows": seed})
	kinds := map[string]any{"kinds": []any{"mentions", "in_context"}, "after": nil, "limit": int64(100)}
	shapes := []struct{ name, q string }{
		{"relation-page", `MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL AND ($after IS NULL OR n.id > $after) RETURN DISTINCT n.id AS id ORDER BY id LIMIT $limit`},
		{"relation-count", `MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL RETURN count(DISTINCT n.id) AS count`},
		{"projection-page", `MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext AND ($after IS NULL OR n.id > $after) RETURN DISTINCT n.id AS id ORDER BY id LIMIT $limit`},
		{"projection-count", `MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext RETURN count(DISTINCT n.id) AS count`},
	}
	spec := os.Getenv("ZZL6_SHAPE_SIZES")
	if spec == "" {
		spec = "10000,40000"
	}
	have := 0
	for _, s := range strings.Split(spec, ",") {
		n, _ := strconv.Atoi(strings.TrimSpace(s))
		for b := have; b < n; b += 500 {
			batch := []any{}
			for i := b; i < b+500 && i < n; i++ {
				batch = append(batch, map[string]any{"id": fmt.Sprintf("p%07d", i), "doc": fmt.Sprintf("d%04d", i%100), "ctx": fmt.Sprintf("c%04d", i%100), "body": strings.Repeat("research body ", 80)})
			}
			must(`UNWIND $rows AS r CREATE (n:PDGraphNode:PDPoint {id:r.id,document_id:r.doc,search_text:r.body,assessed_projection:'old',updated_at:1})`, map[string]any{"rows": batch})
			must(`UNWIND $rows AS r MATCH (p:PDPoint {id:r.id}) MATCH (d:PDDocument {id:r.doc}) MATCH (c:PDContext {id:r.ctx}) CREATE (d)-[:PD_RELATION {id:r.id+'d',kind:'mentions',updated_at:1,eff_usable:true}]->(p) CREATE (p)-[:PD_RELATION {id:r.id+'c',kind:'in_context',updated_at:1,eff_usable:true}]->(c)`, map[string]any{"rows": batch})
		}
		have = n
		for _, shape := range shapes {
			var res *ExecuteResult
			var err error
			start := time.Now()
			rcaPprofTop(t, fmt.Sprintf("l6-%s-n%d", shape.name, n), func() { res, err = exec.Execute(ctx, shape.q, kinds) })
			elapsed := time.Since(start).Seconds()
			rowsN, first := 0, any(nil)
			if res != nil {
				rowsN = len(res.Rows)
				if rowsN > 0 && len(res.Rows[0]) > 0 {
					first = res.Rows[0][0]
				}
			}
			t.Logf("L6_SHAPE shape=%s points=%d seconds=%.6f rows=%d first=%v err=%v", shape.name, n, elapsed, rowsN, first, err)
			if prof, perr := exec.Execute(ctx, "PROFILE "+shape.q, kinds); perr == nil && prof != nil {
				t.Logf("L6_PLAN shape=%s points=%d metadata=%v", shape.name, n, prof.Metadata)
			} else {
				t.Logf("L6_PLAN shape=%s points=%d err=%v", shape.name, n, perr)
			}
		}
	}
}
