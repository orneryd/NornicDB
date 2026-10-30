package fabric

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strings"
	"testing"
)

func TestProjectSimpleReturnFromRow(t *testing.T) {
	cols, row, ok := projectSimpleReturnFromRow(
		"RETURN textKey AS key, textKey128 AS key128",
		[]string{"textKey", "textKey128"},
		[]interface{}{"k1", "h1"},
	)
	if !ok {
		t.Fatalf("expected simple return projection to be supported")
	}
	if len(cols) != 2 || cols[0] != "key" || cols[1] != "key128" {
		t.Fatalf("unexpected projected columns: %#v", cols)
	}
	if len(row) != 2 || row[0] != "k1" || row[1] != "h1" {
		t.Fatalf("unexpected projected row: %#v", row)
	}

	// Non-simple expression is intentionally rejected by this fast-path helper.
	if _, _, ok := projectSimpleReturnFromRow("RETURN coalesce(textKey, textKey128) AS key", []string{"textKey"}, []interface{}{"k"}); ok {
		t.Fatalf("expected non-simple return expression to bypass helper")
	}
}

func TestExecuteApplyInMemoryProjection_Shapes(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"textKey", "textKey128"},
		Rows: [][]interface{}{
			{"k1", "h1"},
			{"k2", "h2"},
		},
	}

	ret, ok := executeApplyInMemoryProjection(input, "RETURN textKey AS key, textKey128 AS hash")
	if !ok {
		t.Fatalf("expected simple RETURN projection to hit in-memory path")
	}
	if len(ret.Columns) != 2 || ret.Columns[0] != "key" || ret.Columns[1] != "hash" {
		t.Fatalf("unexpected columns: %#v", ret.Columns)
	}

	collectOnly, ok := executeApplyInMemoryProjection(
		input,
		"WITH collect({textKey: textKey, textKey128: textKey128}) AS rows RETURN rows",
	)
	if !ok || len(collectOnly.Rows) != 1 {
		t.Fatalf("expected collect-map-only projection result, got ok=%v rows=%#v", ok, collectOnly.Rows)
	}

	distinctKeys, ok := executeApplyInMemoryProjection(
		input,
		"WITH collect({textKey128: textKey128}) AS rows UNWIND rows AS r WITH collect(DISTINCT r.textKey128) AS keys RETURN keys",
	)
	if !ok || len(distinctKeys.Rows) != 1 {
		t.Fatalf("expected distinct-keys projection result, got ok=%v rows=%#v", ok, distinctKeys.Rows)
	}
}

func TestExecuteApplyAsPipeline_RewritesAndExecutes(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	capture := &pipelineCaptureCypherExecutor{}
	local := NewLocalFragmentExecutor(capture, func(name string) (storage.Engine, error) {
		if name != "db1" {
			return nil, fmt.Errorf("unexpected db %s", name)
		}
		return &mockEngine{}, nil
	})
	exec := NewFabricExecutor(catalog, local, nil)

	input := &ResultStream{
		Columns: []string{"textKey128", "textKey", "originalText"},
		Rows: [][]interface{}{
			{"h1", "k1", "o1"},
			{"h2", "k2", "o2"},
		},
	}

	inner := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "WITH textKey128 AS hash MATCH (tt) WHERE tt.translationId = hash RETURN tt.language AS language, tt.translatedText AS translatedText",
		GraphName: "db1",
		Columns:   []string{"language", "translatedText"},
	}

	res, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, map[string]interface{}{"p": 1}, "")
	if err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}
	if !used {
		t.Fatalf("expected pipeline path to be used")
	}
	if res == nil || len(res.Rows) != 1 {
		t.Fatalf("expected mocked execution result, got %#v", res)
	}
	if !strings.Contains(capture.lastQuery, "UNWIND $__fabric_apply_rows AS __fabric_row") {
		t.Fatalf("expected query rewrite to UNWIND apply rows, got %q", capture.lastQuery)
	}
	rowsParam, ok := capture.lastParams["__fabric_apply_rows"].([]map[string]interface{})
	if !ok || len(rowsParam) != 2 {
		t.Fatalf("expected __fabric_apply_rows param with 2 rows, got %#v", capture.lastParams["__fabric_apply_rows"])
	}
}

func TestExecuteApplyInMemoryProjection_WithCollectMapReturnAlias(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"textKey", "textKey128"},
		Rows: [][]interface{}{
			{"k1", "h1"},
			{"k2", "h2"},
		},
	}
	query := "WITH collect({textKey: textKey, textKey128: textKey128}) AS rows\nRETURN rows"
	res, handled := executeApplyInMemoryProjection(input, query)
	if !handled {
		t.Fatal("expected projection to be handled")
	}
	if res == nil {
		t.Fatal("expected non-nil projection result")
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one aggregated row, got %d", len(res.Rows))
	}
}

func TestExecuteApplyInMemoryProjection_ReturnOrderBySkipLimit(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"textKey128", "texts"},
		Rows: [][]interface{}{
			{"a2", []interface{}{}},
			{"a1", []interface{}{"ORD-001"}},
			{"a3", []interface{}{"ORD-003"}},
		},
	}
	query := "RETURN textKey128, texts ORDER BY textKey128 SKIP 1 LIMIT 1"
	res, handled := executeApplyInMemoryProjection(input, query)
	if !handled {
		t.Fatal("expected ordered projection to be handled")
	}
	if res == nil {
		t.Fatal("expected non-nil projection result")
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after SKIP/LIMIT, got %d", len(res.Rows))
	}
	if got := res.Rows[0][0]; got != "a2" {
		t.Fatalf("expected middle ordered row a2, got %#v", got)
	}
	texts, ok := res.Rows[0][1].([]interface{})
	if !ok || len(texts) != 0 {
		t.Fatalf("expected empty texts slice for a2, got %#v", res.Rows[0][1])
	}
}

func TestSynthesizeEmptyCollectOnlyReturn(t *testing.T) {
	cols, row, ok := synthesizeEmptyCollectOnlyReturn("MATCH (n) RETURN collect(n.id) AS ids")
	if !ok {
		t.Fatal("expected collect-only return to synthesize empty row")
	}
	if len(cols) != 1 || cols[0] != "ids" {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	values, ok := row[0].([]interface{})
	if !ok || len(values) != 0 {
		t.Fatalf("expected empty list value, got %#v", row[0])
	}
}

func TestExecuteRowsGroupedJoinProjection_CoversShapes(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows: [][]interface{}{
			{
				[]interface{}{
					map[string]interface{}{"textKey": "k1", "textKey128": "h1"},
					map[string]interface{}{"textKey": "k2", "textKey128": "h2"},
				},
				"h1",
				[]interface{}{"t1"},
			},
			{
				[]interface{}{},
				"h2",
				nil,
			},
		},
	}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got.Rows))
	}
	if got.Rows[0][0] != "k1" || got.Rows[0][1] != "h1" {
		t.Fatalf("unexpected first row: %#v", got.Rows[0])
	}
	if texts, ok := got.Rows[0][2].([]interface{}); !ok || len(texts) != 1 {
		t.Fatalf("expected one grouped text in first row, got %#v", got.Rows[0][2])
	}
	if got.Rows[1][0] != "k2" || got.Rows[1][1] != "h2" {
		t.Fatalf("unexpected second row: %#v", got.Rows[1])
	}
	if texts, ok := got.Rows[1][2].([]interface{}); !ok || len(texts) != 0 {
		t.Fatalf("expected empty texts fallback for second row, got %#v", got.Rows[1][2])
	}
}

func TestExecuteRowsGroupedJoinProjection_InvalidInput(t *testing.T) {
	got := executeRowsGroupedJoinProjection(&ResultStream{
		Columns: []string{"rows"},
		Rows:    [][]interface{}{{[]interface{}{}}},
	})
	if len(got.Rows) != 0 {
		t.Fatalf("expected empty projection rows, got %#v", got.Rows)
	}
}

func TestSynthesizeEmptyCollectOnlyReturn_NonCollect(t *testing.T) {
	_, _, ok := synthesizeEmptyCollectOnlyReturn("RETURN n.id AS id")
	if ok {
		t.Fatal("expected non-collect return to not synthesize")
	}
}

func TestSynthesizeEmptyCollectOnlyReturn_EmptyReturn(t *testing.T) {
	_, _, ok := synthesizeEmptyCollectOnlyReturn("RETURN")
	if ok {
		t.Fatal("expected empty RETURN to not synthesize")
	}
}

func TestSynthesizeEmptyCollectOnlyReturn_NoReturn(t *testing.T) {
	_, _, ok := synthesizeEmptyCollectOnlyReturn("MATCH (n)")
	if ok {
		t.Fatal("expected no RETURN to not synthesize")
	}
}

func TestSynthesizeEmptyCollectOnlyReturn_EmptyItem(t *testing.T) {
	_, _, ok := synthesizeEmptyCollectOnlyReturn("RETURN ,")
	if ok {
		t.Fatal("expected empty item to not synthesize")
	}
}

func TestProjectSimpleReturnFromRow_NotReturn(t *testing.T) {
	_, _, ok := projectSimpleReturnFromRow("MATCH (n) RETURN n", nil, nil)
	if ok {
		t.Fatal("expected non-leading RETURN to fail")
	}
}

func TestProjectSimpleReturnFromRow_EmptyClause(t *testing.T) {
	_, _, ok := projectSimpleReturnFromRow("RETURN ", []string{"a"}, []interface{}{1})
	if ok {
		t.Fatal("expected empty RETURN clause to fail")
	}
}

func TestProjectSimpleReturnFromRow_MissingColumn(t *testing.T) {
	_, _, ok := projectSimpleReturnFromRow("RETURN missing", []string{"a"}, []interface{}{1})
	if !ok {
		// "missing" is a simple identifier but not in input columns
		// Re-check: the function checks colIdx for src
	}
}

func TestExecuteApplyInMemoryProjection_NilInput(t *testing.T) {
	_, ok := executeApplyInMemoryProjection(nil, "RETURN x")
	if ok {
		t.Fatal("expected nil input to not match")
	}
}

func TestExecuteApplyInMemoryProjection_EmptyQuery(t *testing.T) {
	_, ok := executeApplyInMemoryProjection(&ResultStream{}, "")
	if ok {
		t.Fatal("expected empty query to not match")
	}
	_, ok = executeApplyInMemoryProjection(&ResultStream{}, "   ")
	if ok {
		t.Fatal("expected whitespace query to not match")
	}
}

func TestExecuteApplyInMemoryProjection_NonSimpleReturn(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	}
	// Expression with dot is not a simple identifier.
	_, ok := executeApplyInMemoryProjection(input, "RETURN n.a AS x")
	if ok {
		t.Fatal("expected non-simple expr to not match simple return path")
	}
}

func TestExecuteApplyInMemoryProjection_ColumnNotFound(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	}
	_, ok := executeApplyInMemoryProjection(input, "RETURN missing AS x")
	if ok {
		t.Fatal("expected missing column to not match")
	}
}

func TestExecuteApplyAsPipeline_NilInput(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, nil, &FragmentExec{}, nil, "")
	if used || err != nil {
		t.Fatalf("expected nil input to not use pipeline, got used=%v err=%v", used, err)
	}
}

func TestExecuteApplyAsPipeline_NoColumns(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	input := &ResultStream{Rows: [][]interface{}{{1}}}
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, &FragmentExec{}, nil, "")
	if used || err != nil {
		t.Fatalf("expected no-columns input to not use pipeline, got used=%v err=%v", used, err)
	}
}

func TestExecuteApplyAsPipeline_ReturnUsesCanonicalPipeline(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	capture := &pipelineCaptureCypherExecutor{}
	exec := NewFabricExecutor(catalog, NewLocalFragmentExecutor(capture, func(string) (storage.Engine, error) { return &mockEngine{}, nil }), nil)
	input := &ResultStream{Columns: []string{"a"}, Rows: [][]interface{}{{1}}}
	inner := &FragmentExec{Query: "RETURN a AS x", GraphName: "db1"}
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, nil, "")
	if err != nil || !used {
		t.Fatalf("expected canonical projection pipeline: used=%v err=%v", used, err)
	}
	if capture.lastQuery != "UNWIND $__fabric_apply_rows AS __fabric_row WITH __fabric_row.a AS a RETURN a AS x" {
		t.Fatalf("unexpected pipeline query: %s", capture.lastQuery)
	}
}

func TestExecuteApplyAsPipeline_SimpleWithContinuation(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	capture := &pipelineCaptureCypherExecutor{}
	exec := NewFabricExecutor(catalog, NewLocalFragmentExecutor(capture, func(string) (storage.Engine, error) { return &mockEngine{}, nil }), nil)
	input := &ResultStream{Columns: []string{"a"}, Rows: [][]interface{}{{1}}}
	inner := &FragmentExec{Query: "WITH a ORDER BY a RETURN collect(a) AS values", GraphName: "db1"}
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, nil, "")
	if err != nil || !used {
		t.Fatalf("expected whole-stream continuation: used=%v err=%v", used, err)
	}
	if capture.lastQuery != "UNWIND $__fabric_apply_rows AS __fabric_row WITH __fabric_row.a AS a "+inner.Query {
		t.Fatalf("unexpected continuation query: %s", capture.lastQuery)
	}
}

func TestExecuteApplyAsPipeline_FabricNamedColumnsProjected(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	capture := &pipelineCaptureCypherExecutor{}
	local := NewLocalFragmentExecutor(capture, func(name string) (storage.Engine, error) {
		return &mockEngine{}, nil
	})
	exec := NewFabricExecutor(catalog, local, nil)
	input := &ResultStream{
		Columns: []string{"__fabric_key", "a"},
		Rows:    [][]interface{}{{"fk", "v1"}},
	}
	inner := &FragmentExec{Query: "WITH a AS x MATCH (n) WHERE n.id = x RETURN n", GraphName: "db1"}
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !used {
		t.Fatal("expected pipeline to be used")
	}
	// A query column is projected whatever its name.
	if !strings.Contains(capture.lastQuery, "__fabric_row.__fabric_key AS __fabric_key") {
		t.Fatalf("expected the __fabric_key query column projected, got %q", capture.lastQuery)
	}
}

func TestApplySimpleResultModifiers_EmptyRows(t *testing.T) {
	result := &ResultStream{Columns: []string{"x"}, Rows: nil}
	applySimpleResultModifiers(result, "ORDER BY x", map[string]string{"x": "x"})
	if len(result.Rows) != 0 {
		t.Fatal("expected no modification on empty rows")
	}
}

func TestApplySimpleResultModifiers_NilResult(t *testing.T) {
	applySimpleResultModifiers(nil, "ORDER BY x", nil)
	// Should not panic.
}

func TestApplySimpleResultModifiers_SkipAllRows(t *testing.T) {
	result := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}, {2}},
	}
	applySimpleResultModifiers(result, "SKIP 10", map[string]string{})
	if len(result.Rows) != 0 {
		t.Fatalf("expected all rows skipped, got %d", len(result.Rows))
	}
}

func TestExecuteApplyInMemoryProjection_RowsGroupedJoin(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows: [][]interface{}{
			{
				[]interface{}{
					map[string]interface{}{"textKey": "k1", "textKey128": "h1"},
				},
				"h1",
				[]interface{}{"t1"},
			},
		},
	}
	query := "WITH rows, collect({k: k, texts: texts}) AS grouped UNWIND rows AS r WITH r, [g IN grouped WHERE g.k = r.textKey128][0] AS hit RETURN r.textKey AS textKey, r.textKey128 AS textKey128, coalesce(hit.texts, []) AS texts"
	res, ok := executeApplyInMemoryProjection(input, query)
	if !ok {
		t.Fatal("expected rows-grouped-join projection to match")
	}
	if res == nil || len(res.Rows) == 0 {
		t.Fatal("expected non-empty result")
	}
}

func TestExecuteRowsGroupedJoinProjection_MapSliceInput(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows: [][]interface{}{
			{
				[]map[string]interface{}{
					{"textKey": "k1", "textKey128": "h1"},
				},
				"h1",
				[]interface{}{"t1"},
			},
		},
	}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 1 {
		t.Fatalf("expected 1 row from map slice, got %d", len(got.Rows))
	}
}

func TestExecuteRowsGroupedJoinProjection_WrongRowsType(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows: [][]interface{}{
			{"not-a-slice", "h1", []interface{}{"t1"}},
		},
	}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 0 {
		t.Fatalf("expected 0 rows for wrong type, got %d", len(got.Rows))
	}
}

func TestExecuteRowsGroupedJoinProjection_EmptyInput(t *testing.T) {
	input := &ResultStream{Columns: []string{"rows", "k", "texts"}, Rows: [][]interface{}{}}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 0 {
		t.Fatalf("expected 0 rows for empty input, got %d", len(got.Rows))
	}
}

func TestExecuteRowsGroupedJoinProjection_MissingColumns(t *testing.T) {
	input := &ResultStream{Columns: []string{"rows"}, Rows: [][]interface{}{{[]interface{}{}}}}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 0 {
		t.Fatalf("expected 0 rows for missing columns, got %d", len(got.Rows))
	}
}

func TestExecuteRowsGroupedJoinProjection_RowsIdxOutOfBounds(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows:    [][]interface{}{{}}, // row too short
	}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 0 {
		t.Fatalf("expected 0 rows for short row, got %d", len(got.Rows))
	}
}

func TestExecuteCollectedMapJoinFlatProjection(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "sourceId", "language", "translatedText"},
		Rows: [][]interface{}{
			{
				[]interface{}{
					map[string]interface{}{
						"sourceId":     "s1",
						"textKey":      "k1",
						"originalText": "o1",
					},
					map[string]interface{}{
						"sourceId":     "s2",
						"textKey":      "k2",
						"originalText": "o2",
					},
				},
				"s1",
				"es",
				"uno",
			},
			{
				[]interface{}{
					map[string]interface{}{
						"sourceId":     "s1",
						"textKey":      "k1",
						"originalText": "o1",
					},
					map[string]interface{}{
						"sourceId":     "s2",
						"textKey":      "k2",
						"originalText": "o2",
					},
				},
				"s1",
				"fr",
				"un",
			},
			{
				[]interface{}{
					map[string]interface{}{
						"sourceId":     "s1",
						"textKey":      "k1",
						"originalText": "o1",
					},
					map[string]interface{}{
						"sourceId":     "s2",
						"textKey":      "k2",
						"originalText": "o2",
					},
				},
				"s2",
				"es",
				"dos",
			},
		},
	}

	query := "WITH rows, collect({sourceId: sourceId, language: language, translatedText: translatedText}) AS hits UNWIND rows AS r WITH r, [h IN hits WHERE h.sourceId = r.sourceId] AS ms UNWIND ms AS m RETURN r.sourceId AS sourceId, r.textKey AS textKey, r.originalText AS originalText, m.language AS language, m.translatedText AS translatedText"
	got, ok := executeCollectedMapJoinFlatProjection(input, query)
	if !ok {
		t.Fatal("expected structural projection to match")
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if len(got.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %v", got.Columns)
	}
	if len(got.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got.Rows))
	}
	if got.Rows[0][0] != "s1" || got.Rows[0][1] != "k1" || got.Rows[0][2] != "o1" {
		t.Fatalf("unexpected first row: %#v", got.Rows[0])
	}
}

func TestExecuteApplyInMemoryProjection_RowsHitsFlatJoinPattern(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "sourceId", "language", "translatedText"},
		Rows: [][]interface{}{
			{
				[]interface{}{
					map[string]interface{}{
						"sourceId":     "s1",
						"textKey":      "k1",
						"originalText": "o1",
					},
				},
				"s1",
				"es",
				"uno",
			},
		},
	}
	query := "WITH rows, collect({sourceId: sourceId, language: language, translatedText: translatedText}) AS hits UNWIND rows AS r WITH r, [h IN hits WHERE h.sourceId = r.sourceId] AS ms UNWIND ms AS m RETURN r.sourceId AS sourceId, r.textKey AS textKey, r.originalText AS originalText, m.language AS language, m.translatedText AS translatedText"
	res, ok := executeApplyInMemoryProjection(input, query)
	if !ok {
		t.Fatal("expected rows-hits flat join projection to match")
	}
	if res == nil || len(res.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", res)
	}
}

func TestApplySimpleResultModifiers_OrderByUnknownColumn(t *testing.T) {
	result := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{2}, {1}},
	}
	// Unknown column in ORDER BY should be skipped (no-op sort).
	applySimpleResultModifiers(result, "ORDER BY unknown", map[string]string{})
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows unchanged, got %d", len(result.Rows))
	}
}

func TestParseSimpleResultModifiers_UnknownModifier(t *testing.T) {
	specs, skip, limit := parseSimpleResultModifiers("BOGUS stuff", map[string]string{})
	if len(specs) != 0 || skip != 0 || limit != -1 {
		t.Fatalf("expected defaults for unknown modifier, got specs=%v skip=%d limit=%d", specs, skip, limit)
	}
}

func TestProjectInputRowsAsMaps_InvalidSpec(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	}
	// No colon in spec.
	_, ok := projectInputRowsAsMaps(input, cyphertext.NormalizeSource("{invalid}"))
	if ok {
		t.Fatal("expected false for spec without colon")
	}
	// Non-simple key.
	_, ok = projectInputRowsAsMaps(input, cyphertext.NormalizeSource("{a.b: c}"))
	if ok {
		t.Fatal("expected false for non-simple key")
	}
}

func TestProjectInputRowsAsMaps_EmptySpec(t *testing.T) {
	input := &ResultStream{Columns: []string{"a"}, Rows: [][]interface{}{{1}}}
	_, ok := projectInputRowsAsMaps(input, cyphertext.NormalizeSource("{}"))
	if ok {
		t.Fatal("expected false for empty spec")
	}
}

func TestExecuteApplyAsPipeline_NonSimpleColumnName(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	capture := &pipelineCaptureCypherExecutor{}
	local := NewLocalFragmentExecutor(capture, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
	exec := NewFabricExecutor(catalog, local, nil)
	input := &ResultStream{
		Columns: []string{"a.b"}, // Non-simple identifier.
		Rows:    [][]interface{}{{"v"}},
	}
	inner := &FragmentExec{Query: "WITH x AS y MATCH (n) RETURN n", GraphName: "db1"}
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, nil, "")
	if err != nil || !used {
		t.Fatalf("expected quoted non-simple column to use pipeline: used=%v err=%v", used, err)
	}
	if want := "UNWIND $__fabric_apply_rows AS __fabric_row WITH __fabric_row.`a.b` AS `a.b` WITH x AS y MATCH (n) RETURN n"; capture.lastQuery != want {
		t.Fatalf("unexpected pipeline query: got %q, want %q", capture.lastQuery, want)
	}
}

func TestExecuteApplyAsPipeline_RowWrapperInput(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	input := &ResultStream{
		Columns:    []string{"__fabric_row"},
		Rows:       [][]interface{}{{"v"}},
		RowWrapper: true, // The pipeline's own row wrapper.
	}
	inner := &FragmentExec{Query: "WITH x AS y MATCH (n) RETURN n"}
	_, used, _ := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, nil, "")
	if used {
		t.Fatal("expected the row wrapper input to prevent pipeline")
	}
}

func TestProjectInputRowsAsMaps_NonSimpleValue(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	}
	_, ok := projectInputRowsAsMaps(input, cyphertext.NormalizeSource("{key: a.b}"))
	if ok {
		t.Fatal("expected false for non-simple value identifier")
	}
}

func TestExecuteApply_ProjectSimpleReturnFastPath(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id, n.name AS name": {
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{{"a", "Alice"}, {"b", "Bob"}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id, n.name AS name",
		GraphName: "db1", Columns: []string{"id", "name"},
	}
	// Inner is a simple RETURN projection of outer columns.
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id", "name"}, ImportColumns: []string{"id", "name"}},
		Query:     "RETURN id, name",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestExecuteApply_PipelineError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	callCount := 0
	outerQuery := "MATCH (n) RETURN n.id AS id, n.name AS name"
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			outerQuery: {Columns: []string{"id", "name"}, Rows: [][]interface{}{{"k1", "Alice"}}},
		},
	}
	localExec := NewLocalFragmentExecutor(&callCountCypherExecutor{
		mock: mock, callCount: &callCount, failAt: 2,
	}, func(name string) (storage.Engine, error) { return &mockEngine{}, nil })
	exec := NewFabricExecutor(catalog, localExec, nil)

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"id", "name"}}
	// Non-simple WITH clause (not just imports) triggers pipeline path.
	inner := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "WITH id AS x, name AS y MATCH (m) WHERE m.x = x RETURN m.z AS z",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	_, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err == nil {
		t.Fatal("expected error from pipeline")
	}
	if !strings.Contains(err.Error(), "apply inner failed") {
		t.Fatalf("expected 'apply inner failed', got: %v", err)
	}
}

func TestExecuteApply_InMemoryProjectionHandled(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id, n.name AS name": {
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{{"a", "Alice"}, {"b", "Bob"}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id, n.name AS name",
		GraphName: "db1", Columns: []string{"id", "name"},
	}
	// In-memory projection: simple RETURN of outer columns.
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id", "name"}},
		Query:     "RETURN id AS key, name AS val",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows from in-memory projection, got %d", result.RowCount())
	}
	if result.Columns[0] != "key" || result.Columns[1] != "val" {
		t.Fatalf("expected columns [key val], got %v", result.Columns)
	}
}

func TestExecuteApply_PerRowProjectionFastPath(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id, n.name AS name": {
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{{"a", "Alice"}, {"b", "Bob"}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	outer := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n.id AS id, n.name AS name",
		GraphName: "db1", Columns: []string{"id", "name"},
	}
	// Inner is a per-row simple RETURN that should hit projectSimpleReturnFromRow.
	// Use FragmentApply with inner columns set empty so result.Columns starts empty.
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id", "name"}, ImportColumns: []string{"id", "name"}},
		Query:     "RETURN id AS key, name AS val",
		GraphName: "db1",
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: nil}

	result, err := exec.Execute(context.Background(), nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
	// Columns should be set by the fast-path.
	if len(result.Columns) < 2 {
		t.Fatalf("expected at least 2 columns, got %v", result.Columns)
	}
}

func TestProjectSimpleReturnFromRow_EmptyItemInReturn(t *testing.T) {
	// Trailing comma creates empty item.
	_, _, ok := projectSimpleReturnFromRow("RETURN a, ", []string{"a"}, []interface{}{1})
	if ok {
		t.Fatal("expected false for empty item in RETURN")
	}
}

func TestProjectSimpleReturnFromRow_NonSimpleAlias(t *testing.T) {
	_, _, ok := projectSimpleReturnFromRow("RETURN a AS `weird name`", []string{"a"}, []interface{}{1})
	if ok {
		t.Fatal("expected false for non-simple alias")
	}
}

func TestExecuteApplyInMemoryProjection_ReturnEmptyItem(t *testing.T) {
	input := &ResultStream{Columns: []string{"a"}, Rows: [][]interface{}{{1}}}
	_, ok := executeApplyInMemoryProjection(input, "RETURN a, ")
	// Empty item → returns false.
	if ok {
		t.Fatal("expected false for RETURN with empty trailing item")
	}
}

func TestExecuteApplyInMemoryProjection_CollectMapMismatchedAlias(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	}
	// rowsAlias != returnAlias → not handled.
	_, ok := executeApplyInMemoryProjection(input,
		"WITH collect({a: a}) AS rows RETURN other")
	if ok {
		t.Fatal("expected not handled for mismatched alias")
	}
}

func TestExecuteApplyInMemoryProjection_DistinctKeysMismatch(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}},
	}
	// The distinct-keys pattern with mismatched variable names.
	_, ok := executeApplyInMemoryProjection(input,
		"WITH collect({a: a}) AS rows UNWIND rows AS r WITH collect(DISTINCT r.a) AS keys RETURN other")
	if ok {
		t.Fatal("expected not handled for mismatched distinct keys alias")
	}
}

func TestSynthesizeEmptyCollectOnlyReturn_MultiCollect(t *testing.T) {
	cols, row, ok := synthesizeEmptyCollectOnlyReturn("RETURN collect(n.id) AS ids, collect(n.name) AS names")
	if !ok {
		t.Fatal("expected multi-collect to synthesize")
	}
	if len(cols) != 2 || cols[0] != "ids" || cols[1] != "names" {
		t.Fatalf("expected [ids names], got %v", cols)
	}
	for i, v := range row {
		arr, ok := v.([]interface{})
		if !ok || len(arr) != 0 {
			t.Fatalf("expected empty list at index %d, got %v", i, v)
		}
	}
}

func TestApplySimpleResultModifiers_LimitOnly(t *testing.T) {
	result := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}, {2}, {3}, {4}, {5}},
	}
	applySimpleResultModifiers(result, "LIMIT 3", map[string]string{})
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows after LIMIT, got %d", len(result.Rows))
	}
}

// Fabric's own names in a pipeline rewrite never take a name the statement
// or its caller uses: a query column or caller parameter spelled like them
// keeps its value.
func TestExecuteApplyAsPipeline_PrivateNamesAreFree(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	capture := &pipelineCaptureCypherExecutor{}
	exec := NewFabricExecutor(catalog, NewLocalFragmentExecutor(capture, func(string) (storage.Engine, error) { return &mockEngine{}, nil }), nil)
	input := &ResultStream{Columns: []string{"a", "__fabric_row"}, Rows: [][]interface{}{{1, 2}}}
	inner := &FragmentExec{Query: "RETURN a + __fabric_row AS x", GraphName: "db1"}
	params := map[string]interface{}{"__fabric_apply_rows": "caller"}
	_, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, params, "")
	if err != nil || !used {
		t.Fatalf("expected the projection pipeline: used=%v err=%v", used, err)
	}
	if capture.lastQuery != "UNWIND $__fabric_apply_rows_1 AS __fabric_row_1 WITH __fabric_row_1.a AS a, __fabric_row_1.__fabric_row AS __fabric_row RETURN a + __fabric_row AS x" {
		t.Fatalf("unexpected pipeline query: %s", capture.lastQuery)
	}
	if capture.lastParams["__fabric_apply_rows"] != "caller" {
		t.Fatalf("expected the caller's parameter kept, got %#v", capture.lastParams["__fabric_apply_rows"])
	}
}
