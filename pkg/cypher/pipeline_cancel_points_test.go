package cypher

import (
	"context"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pipelineCancelPointContext discovers the shared probe's actual polling
// sites. A replay cancels its real context at one selected site.
// This makes cancellation inside later copy/merge loops deterministic without
// adding production hooks or relying on a scheduler winning a narrow race.
// The large, uninstrumented deadline probes separately exercise running work.
type pipelineCancelPointContext struct {
	context.Context
	mu          sync.Mutex
	sites       map[string]bool
	target      string
	hit         bool
	cancel      context.CancelFunc
	cancelledAt time.Time
}

func (ctx *pipelineCancelPointContext) Err() error {
	var callers [16]uintptr
	frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
	inProbe := false
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, "(*pipelineCancellation).poll") {
			inProbe = true
		} else if inProbe && !strings.HasSuffix(frame.Function, "(*pipelineCancellation).check") && !strings.HasSuffix(frame.Function, "(*pipelineCancellation).Continue") {
			site := frame.Function + ":" + strconv.Itoa(frame.Line)
			ctx.mu.Lock()
			if ctx.sites != nil {
				ctx.sites[site] = true
			}
			stop := site == ctx.target && !ctx.hit
			if stop {
				ctx.hit = true
				ctx.cancelledAt = time.Now()
			}
			ctx.mu.Unlock()
			if stop && ctx.cancel != nil {
				ctx.cancel()
			}
			break
		}
		if !more {
			break
		}
	}
	return ctx.Context.Err()
}

func TestPipelineCancellationAtEveryReachableStagePoll(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "pipeline_cancel_points")
	executor := NewStorageExecutor(engine)
	executor.cache = nil
	const size = 257 // crosses the amortized interval in every row-copy phase
	rows := make([]pipelineRow, size)
	values := make([][]interface{}, size)
	nodes := make([]*storage.Node, size)
	list := make([]interface{}, size)
	fields := make(map[string]interface{}, size)
	for index := range rows {
		x := int64(size - index)
		nodes[index] = &storage.Node{ID: storage.NodeID(strconv.Itoa(index)), Labels: []string{"CancelPoint"}, Properties: map[string]interface{}{"x": x}}
		rows[index] = pipelineRow{"x": x, "n": nodes[index], "items": []interface{}{x}}
		values[index] = []interface{}{x, nodes[index]}
		list[index] = map[string]interface{}{"x": x}
		fields[strconv.Itoa(index)] = x
		_, err := engine.CreateNode(nodes[index])
		require.NoError(t, err)
	}
	root := &storage.Node{ID: "cancel-point-root", Labels: []string{"Left"}, Properties: map[string]interface{}{"x": int64(1)}}
	_, err := engine.CreateNode(root)
	require.NoError(t, err)
	nodes[0].Labels = append(nodes[0].Labels, "RightOnly")
	require.NoError(t, engine.UpdateNode(nodes[0]))
	edges := make([]*storage.Edge, size)
	boundRows := make([]pipelineRow, size)
	for index := range rows {
		edges[index] = &storage.Edge{ID: storage.EdgeID("cancel-edge-" + strconv.Itoa(index)), Type: "R", StartNode: root.ID, EndNode: nodes[index].ID}
		require.NoError(t, engine.CreateEdge(edges[index]))
		boundRows[index] = pipelineRow{"a": root, "x": int64(index)}
	}
	cloneRows := func() []pipelineRow { return append([]pipelineRow(nil), rows...) }
	result := func() *ExecuteResult {
		return &ExecuteResult{Columns: []string{"x", "n"}, Rows: append([][]interface{}(nil), values...)}
	}
	finish := func(ctx context.Context, ok bool) error {
		if !ok {
			return getExpressionFailure(ctx)
		}
		return nil
	}
	type stage struct {
		name string
		run  func(context.Context) error
	}
	_, indexErr := executor.Execute(context.Background(), "CREATE INDEX cancel_point_x FOR (n:CancelPoint) ON (n.x)", nil)
	require.NoError(t, indexErr)
	require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{Name: "test.pipelineCancellationRows", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "x", Type: "INTEGER"}}}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"x"}, Rows: values}, nil
	}))
	t.Cleanup(ClearUserProcedures)
	membershipGeneration := 0
	stages := []stage{
		{"indexed_single_parameter", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			items := make([]int, size)
			for i := range items {
				items[i] = i + 1
			}
			_, _, err := executor.tryCollectNodesFromPropertyIndexInAnyOfWithCancellation(nodePatternInfo{variable: "n", labels: []string{"CancelPoint"}}, "n.x IN $xs", map[string]interface{}{"xs": items}, nil, &probe)
			return err
		}},
		{"indexed_collector", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			items := make([]interface{}, size)
			for i := range items {
				items[i] = int64(i + 1)
			}
			_, _, err := executor.collectIndexedPropertyValues(nodePatternInfo{labels: []string{"CancelPoint"}}, nil, engine.GetSchema(), []indexedPropertyValues{{[]string{"CancelPoint"}, "x", items}}, &probe)
			return err
		}},

		{"compiled_size", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			plan := planRowPredicate("size(text)+0>0")
			require.NotNil(t, plan)
			_ = executor.evaluateRowPredicatePlanWithCancellation(ctx, plan, map[string]interface{}{"text": strings.Repeat("é", 4097)}, &probe)
			return getExpressionFailure(ctx)
		}},
		{"compiled_list_add", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			plan := planRowPredicate("items+items=items")
			require.NotNil(t, plan)
			_ = executor.evaluateRowPredicatePlanWithCancellation(ctx, plan, map[string]interface{}{"items": list}, &probe)
			return getExpressionFailure(ctx)
		}},
		{"write_stretch", func(ctx context.Context) error {
			clauses, ok := canExecuteAsPipeline("CREATE (:WritePoint {x:x})")
			require.True(t, ok)
			_, _, err := executor.runPipelineClauseRows(ctx, rows, map[string]struct{}{"x": {}}, clauses, clauses, nil)
			return err
		}},
		{"indexed_projection", func(ctx context.Context) error {
			items := make([]interface{}, size)
			for i := range items {
				items[i] = int64(i + 1)
			}
			params := map[string]interface{}{"xs": items}
			clauses, _ := canExecuteAsPipeline("MATCH (n:CancelPoint) WHERE n.x IN $xs OR n.x IN $xs RETURN n.x+1")
			_, _, err := executor.tryExecutePipelineSimpleNodeReadPlan(withQueryParams(ctx, params), clauses, params)
			return err
		}},
		{"node_source", func(ctx context.Context) error {
			source, _, err := executor.pipelineNodeMatchSourceWithHint(ctx, pipelineRowsSource([]pipelineRow{{}}), "MATCH (n:CancelPoint)", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
			if err != nil {
				return err
			}
			_, ok := materializePipelineSource(ctx, source)
			return finish(ctx, ok)
		}},
		{"node_source_stream", func(ctx context.Context) error {
			source, _, err := executor.pipelineNodeMatchSourceWithHint(ctx, pipelineRowsSource([]pipelineRow{{}}), "MATCH (n:CancelPoint)", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, streamScan: true})
			if err != nil {
				return err
			}
			_, ok := materializePipelineSource(ctx, source)
			return finish(ctx, ok)
		}},
		{"typed_conversion_validator", func(ctx context.Context) error {
			return executor.validateRowConversionArguments("[x IN items | toInteger(x)]", pipelineRow{"items": make([]int, 257), rowStatementContextKey: withPipelineCancellation(ctx)})
		}},
		{"fallback_map_value", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = cypherEquivalenceKeyWithCancellation(struct{ Map map[int]string }{map[int]string{1: strings.Repeat("é", 4097)}}, &probe)
			return probe.err
		}},
		{"wildcard_wide", func(ctx context.Context) error {
			_ = pipelineWildcardColumns(ctx, []pipelineRow{pipelineRow(fields)})
			return getExpressionFailure(ctx)
		}},
		{"validation_wide", func(ctx context.Context) error {
			return executor.validatePipelineProjectionValues(ctx, []pipelineRow{pipelineRow(fields)}, "RETURN 1", "RETURN")
		}},
		{"predicate_and", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			plan := planRowPredicate(strings.Repeat("x>=0 AND ", 256) + "x>=0")
			_ = executor.evaluateRowPredicatePlanWithCancellation(ctx, plan, map[string]interface{}{"x": int64(1)}, &probe)
			return probe.err
		}},
		{"predicate_or", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			plan := planRowPredicate(strings.Repeat("x<0 OR ", 256) + "x<0")
			_ = executor.evaluateRowPredicatePlanWithCancellation(ctx, plan, map[string]interface{}{"x": int64(1)}, &probe)
			return probe.err
		}},
		{"membership_prepare", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			plan := planRowPredicate(strings.Repeat("x>=0 AND ", 256) + "x>=0")
			memberships := compiledRowMemberships{}
			memberships.prepareWithCancellation(&plan.root, compiledRowScope{}, &probe)
			return probe.err
		}},
		{"procedure_rows", func(ctx context.Context) error {
			_, _, _, err := executor.pipelineApplyProcedureCall(ctx, []pipelineRow{{}}, "CALL test.pipelineCancellationRows() YIELD x")
			return err
		}},
		{"dynamic_merge", func(ctx context.Context) error {
			input := make([]pipelineRow, size)
			for index := range input {
				input[index] = pipelineRow{"l": "CancelPoint"}
			}
			_, _, err := executor.pipelineApplyRowDynamicMerge(ctx, input, "MERGE (n:$(l) {x:1})", "(n:$(l) {x:1})")
			return err
		}},
		{"pattern_projection", func(ctx context.Context) error {
			_, ok := executor.evaluateRowExpressionWithContext(ctx, "[(a)-[:R]->(b) | b.x]", pipelineRow{"a": root})
			return finish(ctx, ok)
		}},
		{"minimum", func(ctx context.Context) error {
			input := []pipelineRow{{"x": list}, {"x": list}}
			_, ok := executor.evaluatePipelineAggregateWithContext(ctx, input, "min", "x", false)
			return finish(ctx, ok)
		}},
		{"binding_membership", func(ctx context.Context) error {
			items := make([]interface{}, size)
			for i := range items {
				items[i] = int64(i)
			}
			name := "numbers_" + strconv.Itoa(membershipGeneration)
			membershipGeneration++
			predicate := executor.newBindingFilterPredicate(ctx, "n.x IN $"+name, map[string]interface{}{name: items})
			_ = predicate.matches(binding{"n": nodes[0]}, nil)
			_ = predicate.matchesRelationships(binding{"n": nodes[0]}, nil, nil)
			return getExpressionFailure(ctx)
		}},
		{"distinct_rows", func(ctx context.Context) error {
			_ = deduplicatePipelineRows(ctx, rows, []string{"x"})
			return getExpressionFailure(ctx)
		}},
		{"distinct_result", func(ctx context.Context) error {
			_ = deduplicatePipelineResultRows(ctx, values)
			return getExpressionFailure(ctx)
		}},
		{"materialize", func(ctx context.Context) error {
			_, ok := materializePipelineSource(ctx, pipelineRowsSource(rows))
			return finish(ctx, ok)
		}},
		{"wildcard_columns", func(ctx context.Context) error {
			_ = pipelineWildcardColumns(ctx, rows)
			return getExpressionFailure(ctx)
		}},
		{"sort", func(ctx context.Context) error {
			return finish(ctx, executor.orderPipelineRowsWithScopes(ctx, cloneRows(), rows, []orderByTerm{{column: "x+1"}}))
		}},
		{"filter", func(ctx context.Context) error {
			_ = executor.filterPipelineRows(ctx, rows, "x > 0")
			return getExpressionFailure(ctx)
		}},
		{"window_filter", func(ctx context.Context) error {
			_ = executor.filterPipelineWindow(ctx, rows, rows, 0, -1, "x > 0")
			return getExpressionFailure(ctx)
		}},
		{"node_projection", func(ctx context.Context) error {
			_ = projectPipelineSimpleNodeRead(ctx, nodes, []returnItem{{expr: "n.x"}}, "n")
			return getExpressionFailure(ctx)
		}},
		{"call_order_window", func(ctx context.Context) error {
			_, err := executor.applyCallResultOrderWindow(ctx, result(), []orderByTerm{{column: "x"}}, 0, -1)
			return err
		}},
		{"call_rows", func(ctx context.Context) error {
			_ = callPipelineRowsFromResult(ctx, result())
			return getExpressionFailure(ctx)
		}},
		{"call_columns", func(ctx context.Context) error {
			_ = callPipelineResultFromRows(ctx, rows, nil, nil, nil)
			return getExpressionFailure(ctx)
		}},
		{"call_join", func(ctx context.Context) error {
			_ = appendCallReturnRows(ctx, nil, rows[0], []string{"x", "n"}, values)
			return getExpressionFailure(ctx)
		}},
		{"optional_call", func(ctx context.Context) error {
			ok := true
			_, err := pipelineApplyOptionally(ctx, rows, func(input []pipelineRow) ([]pipelineRow, []string, bool, error) { return input, nil, true, nil }, &ok)
			return err
		}},
		{"projection_validation", func(ctx context.Context) error {
			return executor.validatePipelineProjectionValues(ctx, rows, "RETURN size(items), range(1,x), toString(x), items[0]", "RETURN")
		}},
		{"pattern_validation", func(ctx context.Context) error { return validateBoundPatternNodes(ctx, rows, "(n)") }},
		{"percentile", func(ctx context.Context) error {
			_, ok := executor.evaluatePipelinePercentile(ctx, rows, "percentilecont", "x, 0.5", true)
			return finish(ctx, ok)
		}},
		{"aggregate", func(ctx context.Context) error {
			_, ok := executor.evaluatePipelineAggregateWithContext(ctx, rows, "collect", "x", true)
			return finish(ctx, ok)
		}},
		{"unwind", func(ctx context.Context) error {
			_, ok := executor.pipelineApplyUnwind(ctx, rows, "UNWIND range(1,257) AS y")
			return finish(ctx, ok)
		}},
		{"unwind_typed", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = coerceToUnwindItemsWithCancellation(make([]int64, size), &probe)
			return probe.err
		}},
		{"range", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_, _ = evaluateCypherRangeWithCancellation([]interface{}{int64(1), int64(size)}, &probe)
			return probe.err
		}},
		{"nested_key", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = cypherEquivalenceKeyWithCancellation(map[string]interface{}{"items": list}, &probe)
			return probe.err
		}},
		{"wide_map_key", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = cypherEquivalenceKeyWithCancellation(fields, &probe)
			return probe.err
		}},
		{"map_order", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = compareValuesForSortWithCancellation(fields, fields, &probe)
			return probe.err
		}},
		{"path_order", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			path := PathResult{Nodes: nodes}
			_ = compareValuesForSortWithCancellation(path, path, &probe)
			return probe.err
		}},
	}
	for _, clause := range []string{"WITH x AS y", "WITH * ORDER BY x", "WITH x AS y ORDER BY y", "WITH x, collect(n) AS ns ORDER BY x", "WITH DISTINCT x AS y", "WITH DISTINCT *", "RETURN x", "RETURN *", "RETURN DISTINCT *", "RETURN DISTINCT x ORDER BY x", "RETURN DISTINCT x,collect(n) AS ns", "RETURN x, collect(n) AS ns ORDER BY x"} {
		clause := clause
		stages = append(stages, stage{clause, func(ctx context.Context) error {
			if strings.HasPrefix(clause, "WITH") {
				_, ok := executor.pipelineApplyWith(ctx, cloneRows(), clause)
				return finish(ctx, ok)
			}
			_, ok := executor.pipelineApplyReturn(ctx, cloneRows(), clause)
			return finish(ctx, ok)
		}})
	}
	for _, clause := range []string{"RETURN x", "RETURN DISTINCT x", "RETURN *", "RETURN DISTINCT *"} {
		clause := clause
		stages = append(stages, stage{"source_" + clause, func(ctx context.Context) error {
			_, ok := executor.pipelineApplyReturnPlan(ctx, nil, returnProjectionPlanFor(clause), pipelineRowsSource(rows), false)
			return finish(ctx, ok)
		}})
	}
	for _, all := range []bool{false, true} {
		all := all
		stages = append(stages, stage{"union_all_" + strconv.FormatBool(all), func(ctx context.Context) error {
			query := "RETURN x,n UNION RETURN n,x"
			if all {
				query = "RETURN x,n UNION ALL RETURN n,x"
			}
			_, err := executor.executeUnionBranches(ctx, query, all, func(branch string) (*ExecuteResult, error) {
				r := result()
				if strings.HasPrefix(branch, "RETURN n") {
					r.Columns = []string{"n", "x"}
				}
				return r, nil
			})
			return err
		}})
	}
	for _, query := range []string{
		"UNWIND range(1,257) AS x CALL (x) { RETURN x+1 AS y } RETURN y",
		"UNWIND range(1,257) AS x CALL () { UNWIND range(1,3) AS y RETURN y } RETURN x,y",
		"CYPHER 25 UNWIND range(1,257) AS x LET y=x+1 FILTER y>0 RETURN y",
	} {
		query := query
		stages = append(stages, stage{query, func(ctx context.Context) error { _, err := executor.Execute(ctx, query, nil); return err }})
	}
	for _, expression := range []string{
		"range(1,257)", "[x IN items | x.x+1]", "[x IN items | [y IN [1,2,3] | x.x+y]]",
		"reduce(s=0,x IN items | s+x.x)", "all(x IN items WHERE x.x>=0)", "reverse(items)", "reverse(text)",
		"lpad('',257,'🙂')", "rpad('é',257,'🙂')", "apoc.text.join(words,',')", "-1 IN numbers", "items=items", "items<=items",
		"items+items", "text+text", "CASE items WHEN items THEN 1 ELSE 0 END", "fields{.*}",
		"[" + strings.Repeat("numbers,", 256) + "numbers]", "{" + strings.Join(func() []string {
			parts := make([]string, size)
			for i := range parts {
				parts[i] = "x" + strconv.Itoa(i) + ":numbers"
			}
			return parts
		}(), ",") + "}",
		"CASE items " + strings.Repeat("WHEN [] THEN 1 ", 257) + "ELSE 0 END", "CASE " + strings.Repeat("WHEN false THEN 1 ", 257) + "ELSE 0 END",
	} {
		expression := expression
		name := expression
		if len(name) > 120 {
			name = "wide_" + strconv.Itoa(len(stages))
		}
		stages = append(stages, stage{"expression_" + name, func(ctx context.Context) error {
			words := make([]interface{}, size)
			numbers := make([]interface{}, size)
			for index := range words {
				words[index] = "text"
				numbers[index] = int64(index)
			}
			_, ok := executor.evaluateRowExpressionWithContext(ctx, expression, pipelineRow{"items": list, "text": strings.Repeat("é", 4097), "words": words, "numbers": numbers, "fields": fields})
			return finish(ctx, ok)
		}})
	}
	stages = append(stages, []stage{
		{"conversion_validator", func(ctx context.Context) error {
			scope := pipelineRow{"items": list, rowStatementContextKey: withPipelineCancellation(ctx)}
			return executor.validateRowConversionArguments("[x IN items | toInteger(x.x)]", scope)
		}},
		{"graph_validator", func(ctx context.Context) error {
			scope := pipelineRow{"items": nodes, rowStatementContextKey: withPipelineCancellation(ctx)}
			return executor.validateRowGraphFunctionArguments("[x IN items | labels(x)]", scope)
		}},
		{"relationship_copy", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			items := make([]interface{}, size)
			for index := range items {
				items[index] = edges[index]
			}
			_, _ = pipelineRelationshipListWithCancellation(items, &probe)
			return probe.err
		}},
		{"relationship_unique", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = relationshipListReusesEdgeWithCancellation(edges, &probe)
			return probe.err
		}},
		{"relationship_trace", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			chain := make([]*storage.Edge, size)
			for index := range chain {
				chain[index] = &storage.Edge{StartNode: storage.NodeID(strconv.Itoa(index)), EndNode: storage.NodeID(strconv.Itoa(index + 1))}
			}
			_ = traceRelationshipListWithCancellation(chain, "outgoing", &probe)
			return probe.err
		}},
		{"bound_relationship_match", func(ctx context.Context) error {
			chain := make([]interface{}, size-1)
			for index := range chain {
				chain[index] = &storage.Edge{ID: storage.EdgeID("chain-" + strconv.Itoa(index)), StartNode: nodes[index].ID, EndNode: nodes[index+1].ID}
			}
			input := make([]pipelineRow, size)
			for index := range input {
				input[index] = pipelineRow{"r": chain}
			}
			_, _, err := executor.pipelineApplyBoundRelationshipListMatch(ctx, input, "MATCH (a)-[r*]->(b)")
			return err
		}},
		{"optional_match", func(ctx context.Context) error {
			_, err := executor.pipelineApplyOptionalMatch(ctx, boundRows, "OPTIONAL MATCH (a)-[:R]->(b:RightOnly)")
			return err
		}},
		{"chained_match", func(ctx context.Context) error {
			_, ok := executor.pipelineApplyChainedMatch(ctx, boundRows, "MATCH (a)-[:R]->(b:RightOnly)")
			return finish(ctx, ok)
		}},
		{"initial_traversal", func(ctx context.Context) error {
			_, _, err := executor.pipelineApplyInitialTraversalMatch(ctx, []pipelineRow{{}}, "MATCH (a:Left)-[:R]->(b)", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
			return err
		}},
		{"initial_node", func(ctx context.Context) error {
			_, _, err := executor.pipelineApplyInitialNodeMatch(ctx, []pipelineRow{{}}, "MATCH (n:CancelPoint)", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
			return err
		}},
		{"node_join", func(ctx context.Context) error {
			_, _, err := executor.pipelineApplyNodeJoinProduct(ctx, rows, []string{"(a:Left)", "(b:CancelPoint)"}, "a.x=b.x")
			return err
		}},
		{"anonymous_product", func(ctx context.Context) error {
			_, _, err := executor.pipelineApplyMatchProduct(ctx, rows, []string{"(:Left)", "(:RightOnly)"}, "true")
			return err
		}},
		{"path_product", func(ctx context.Context) error {
			_, _, err := executor.pipelineApplyMatchProduct(ctx, []pipelineRow{{}}, []string{"p=(a:Left)-[:R]->(b:CancelPoint)", "q=(c:Left)-[:R]->(d:RightOnly)"}, "true")
			return err
		}},
		{"partitioned_aggregate", func(ctx context.Context) error {
			patterns := []struct {
				variable string
				nodes    []*storage.Node
			}{{"n", nodes}}
			_, _, err := executor.tryCartesianAggregatePartitions(ctx, patterns, returnProjectionPlanFor("RETURN n.x, sum(n.x)"), 2)
			return err
		}},
		{"membership_index_build", func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			cache := bindingParamMembershipCache{}
			_, _, _ = cache.getValidatedWithCancellation(list, &probe)
			return probe.err
		}},
		{"membership_index_validate", func(ctx context.Context) error {
			items := make([]interface{}, size)
			for index := range items {
				items[index] = int64(index)
			}
			cache := bindingParamMembershipCache{}
			_, _, _ = cache.getValidated(items)
			probe := pipelineCancellation{ctx: ctx}
			_, _, _ = cache.getValidatedWithCancellation(items, &probe)
			return probe.err
		}},
	}...)
	for _, expression := range []string{"head(words)", "last(words)", "reverse(items)", "reverse(text)", "all(x IN items WHERE x>=0)", "[x IN words]", "[x IN items | x+1]", "reduce(s=0,x IN items | s+x)"} {
		expression := expression
		stages = append(stages, stage{"graph_expression_" + expression, func(ctx context.Context) error {
			items := make([]interface{}, size)
			words := make([]string, size)
			for index := range items {
				items[index] = int64(index)
				words[index] = "word"
			}
			ctx = withValueBindings(ctx, map[string]interface{}{"items": items, "words": words, "text": strings.Repeat("é", 4097)})
			_ = executor.evaluateExpressionWithContextFull(ctx, expression, nil, nil, nil, nil, nil, 0)
			return getExpressionFailure(ctx)
		}})
	}
	// Reflective fallbacks include paths and extension values. Exercise every
	// container phase, including quoting one large string and sorted map keys.
	wideFields := make([]reflect.StructField, size)
	for index := range wideFields {
		wideFields[index] = reflect.StructField{Name: "Field" + strconv.Itoa(index), Type: reflect.TypeOf("")}
	}
	wideValue := reflect.New(reflect.StructOf(wideFields)).Elem().Interface()
	for _, value := range []interface{}{struct{ Text string }{strings.Repeat("é", 1<<20)}, struct{ Fields map[int]interface{} }{func() map[int]interface{} {
		m := map[int]interface{}{}
		for i := 0; i < size; i++ {
			m[i] = i
		}
		return m
	}()}, wideValue} {
		value := value
		stages = append(stages, stage{"fallback_" + strconv.Itoa(len(stages)), func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = cypherEquivalenceKeyWithCancellation(value, &probe)
			return probe.err
		}})
	}
	for _, value := range []interface{}{strings.Repeat("x", 1<<20), []byte(strings.Repeat("x", 1<<20)), PathResult{Nodes: nodes}, map[string]interface{}{"prefix": strings.Repeat("x", 1<<20), "tail": list}, map[string]interface{}{strings.Repeat("x", 1<<20) + "a": int64(1), strings.Repeat("x", 1<<20) + "b": int64(2)}} {
		value := value
		stages = append(stages, stage{"large_leaf_" + strconv.Itoa(len(stages)), func(ctx context.Context) error {
			probe := pipelineCancellation{ctx: ctx}
			_ = cypherEquivalenceKeyWithCancellation(value, &probe)
			return probe.err
		}})
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			runPipelineCancelPointStage(t, stage.run)
		})
	}
}

func runPipelineCancelPointStage(t *testing.T, run func(context.Context) error) {
	t.Helper()
	healthy, healthyCancel := context.WithCancel(context.Background())
	defer healthyCancel()
	collector := &pipelineCancelPointContext{Context: healthy, sites: map[string]bool{}}
	require.NoError(t, run(withExpressionFailureSlot(collector)), "healthy stage")
	sites := make([]string, 0, len(collector.sites))
	for site := range collector.sites {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	require.NotEmpty(t, sites, "stage must reach the shared cancellation owner")
	for _, site := range sites {
		t.Run(site, func(t *testing.T) {
			// Randomized map iteration changes the sorting comparison count
			// and therefore which check lands on a sample. Retry healthy
			// traversals to reach the selected sampled site, without sleeps.
			for attempt := 0; attempt < 32; attempt++ {
				base, expire := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer expire()
				base, cancel := context.WithCancel(base)
				defer cancel()
				probe := &pipelineCancelPointContext{Context: base, target: site, cancel: cancel}
				err := run(withExpressionFailureSlot(probe))
				if !probe.hit {
					cancel()
					continue
				}
				require.ErrorIs(t, err, context.Canceled)
				require.Less(t, time.Since(probe.cancelledAt), 250*time.Millisecond, "stage continued after cancellation")
				return
			}
			t.Fatal("selected stage poll must execute")
		})
	}
}
