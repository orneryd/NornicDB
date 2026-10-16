package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var quotedVariableWorkQueries = []string{
	"", "RETURN 1", "RETURN 'x`y', \"`z`\"", "RETURN `unfinished",
	"WITH 1 AS `a b` RETURN `a b`", "WITH 1 AS `a``b` RETURN `a``b`",
	"WITH 1 AS `match` RETURN `match`", "WITH 1 AS `ñandú` RETURN `ñandú`",
	"WITH 1 AS `x` RETURN x + 1, `x` + 1", "WITH 1 AS ` a ` RETURN ` a `",
	"MATCH (`n n`:P)-[`r r`:R]->(`m m`) RETURN `r r`, `m m`",
	"MATCH `p p` = ()-->() RETURN `p p`", "RETURN [`p p` = ()-->() | `p p`]",
	"WITH {`a b`: 1} AS `a b` RETURN `a b`.`a b`", "RETURN $`a b`, $`p`, $`a b`",
	"WITH 5 AS `a b` RETURN m {.k, `a b`}", "RETURN {`map key`: 1}",
	"RETURN `db`.labels(), `function`(1)", "CALL db.labels() YIELD `label` RETURN `label`",
	"CALL db.labels() YIELD `label` AS `l l` RETURN `l l`", "USE `a b` RETURN 1",
	"MATCH (n:`a b`|`c d`&!`e f`) RETURN n", "MATCH ()-[:`a b`]->() RETURN 1",
	"RETURN [`e e` IN [1,2] | `e e`], all(`x x` IN [1] WHERE `x x` > 0)",
	"RETURN reduce(`s s` = 0, `e e` IN [1,2] | `s s` + `e e`)",
	"WITH 1 AS `a b` RETURN *", "EXPLAIN WITH 1 AS `a b` RETURN `a b`",
}

func TestQuotedVariableCheckedParity(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, query := range quotedVariableWorkQueries {
		t.Run(query, func(t *testing.T) {
			want, legacy := canonicalizeQuotedVariables(query)
			p := &pipelineCancellation{ctx: context.Background()}
			got, names := canonicalizeQuotedVariablesWithCancellation(query, p)
			require.NoError(t, p.err)
			require.Equal(t, want, got)
			require.Equal(t, scanQuotedTokens(query), scanQuotedTokensWithCancellation(query, p))
			if legacy == nil {
				require.Nil(t, names)
				return
			}
			require.NotNil(t, names)
			for _, identifier := range legacy.internalOrder {
				require.Equal(t, legacy.restoreText(identifier), names.restoreTextWithCancellation(identifier, p))
				require.Equal(t, legacy.restoreColumn(identifier), names.restoreColumnWithCancellation(identifier, p))
			}
			for start := 0; start <= len(got); start++ {
				want, wok := legacy.originalSpan(start, len(got))
				got, gok := names.originalSpanWithCancellation(start, len(got), p)
				require.Equal(t, wok, gok)
				require.Equal(t, want, got)
			}
			params := map[string]interface{}{"a b": int64(7), "p": "value"}
			bound := map[string]interface{}{}
			require.True(t, names.bindParameterValuesWithCancellation(func(key string) (interface{}, bool) { v, ok := params[key]; return v, ok }, func(key string, v interface{}) bool { bound[key] = v; return true }, p))
			for key, value := range legacy.parameterValues(params) {
				if _, original := params[key]; !original {
					require.Equal(t, value, bound[key])
					require.Equal(t, legacy.parameterName(key), names.parameterNameWithCancellation(key, p))
				}
			}
			if len(legacy.internalOrder) == 0 {
				return
			}
			id := legacy.internalOrder[0]
			result := &ExecuteResult{Columns: []string{id}, Rows: [][]interface{}{{int64(1)}}}
			old, oldErr := legacy.restore(result, errors.New("unknown "+id), exec.parseReturnItems)
			restored, restoredErr := names.restoreWithCancellation(result, errors.New("unknown "+id), exec.parseReturnItems, p)
			require.Equal(t, old, restored)
			require.EqualError(t, restoredErr, oldErr.Error())
			require.NoError(t, p.err)
		})
	}
}

func TestQuotedVariableCheckedLargeInputs(t *testing.T) {
	long := strings.Repeat("é x`", 1<<17)
	for name, run := range map[string]func(*pipelineCancellation){
		"query without quote": func(p *pipelineCancellation) {
			canonicalizeQuotedVariablesWithCancellation(strings.Repeat("x", 1<<20), p)
		},
		"quoted identifier": func(p *pipelineCancellation) {
			scanQuotedTokensWithCancellation("WITH 1 AS `"+strings.Repeat("x", 1<<20)+"`", p)
		},
		"escaped identifier": func(p *pipelineCancellation) {
			symbolicNameValueWithCancellation("`"+strings.Repeat("``", 1<<19)+"`", p)
		},
		"name hash": func(p *pipelineCancellation) { internalVariableIdentifierWithCancellation("RETURN 1", long, nil, p) },
		"source collision search": func(p *pipelineCancellation) {
			internalVariableIdentifierWithCancellation(strings.Repeat("x", 1<<20), "a b", nil, p)
		},
		"plain name unicode": func(p *pipelineCancellation) { isPlainVariableNameWithCancellation(strings.Repeat("é", 1<<19), p) },
		"backward gaps":      func(p *pipelineCancellation) { previousWordWithCancellation(strings.Repeat(" ", 1<<20), 1<<20, p) },
		"forward gaps":       func(p *pipelineCancellation) { nextSignificantWithCancellation(strings.Repeat(" ", 1<<20), 0, p) },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
			p := &pipelineCancellation{ctx: ctx, remaining: 8}
			run(p)
			require.ErrorIs(t, p.err, context.Canceled)
			require.Equal(t, 1, ctx.calls)
		})
	}
	for _, name := range []string{"", "a b", "a`b", "ñandú", long} {
		p := &pipelineCancellation{ctx: context.Background()}
		query := "RETURN nornicq123v"
		want := internalVariableIdentifier(query, name, nil)
		require.Equal(t, want, internalVariableIdentifierWithCancellation(query, name, nil, p))
		used := map[string]struct{}{want: {}}
		require.Equal(t, internalVariableIdentifier(query+want, name, used), internalVariableIdentifierWithCancellation(query+want, name, used, p))
		require.NoError(t, p.err)
	}
}

func TestQuotedVariableCheckedReplay(t *testing.T) {
	for _, query := range quotedVariableWorkQueries {
		replayPrepassWork(t, func(p *pipelineCancellation) { canonicalizeQuotedVariablesWithCancellation(query, p) })
	}
	_, names := canonicalizeQuotedVariablesWithCancellation("WITH 1 AS `a b`, 2 AS `c d` RETURN `a b` + `c d`", &pipelineCancellation{ctx: context.Background()})
	id := names.internalOrder[0]
	plan := &ExecutionPlan{Mode: ModeExplain, Query: names.canonical, Root: &PlanOperator{Description: id, Identifiers: []string{id}, Arguments: map[string]interface{}{"key": id}, Children: []*PlanOperator{{Description: id}, nil}}}
	value := map[string]interface{}{id: []interface{}{id, plan, (*ExecutionPlan)(nil), (*PlanOperator)(nil), [][]interface{}{{id}}}}
	for _, run := range []func(*pipelineCancellation){
		func(p *pipelineCancellation) { names.restoreValueWithCancellation(value, p) },
		func(p *pipelineCancellation) { names.restoreStatementTextWithCancellation(names.canonical, p) },
		func(p *pipelineCancellation) { names.restoreColumnWithCancellation(id+" + "+id, p) },
		func(p *pipelineCancellation) { names.originalTextWithCancellation(0, len(names.canonical), p) },
		func(p *pipelineCancellation) { formatPlanWithCancellation(plan, p) },
	} {
		replayPrepassWork(t, run)
	}
}

func TestQuotedVariableCheckedRestoreFailureAndPlans(t *testing.T) {
	exec, _ := newTestExecutor(t)
	_, names := canonicalizeQuotedVariablesWithCancellation("EXPLAIN WITH 1 AS `a b` RETURN `a b`", &pipelineCancellation{ctx: context.Background()})
	id := names.internalOrder[0]
	for _, mode := range []ExecutionMode{ModeExplain, ModeProfile} {
		plan := &ExecutionPlan{Mode: mode, Query: names.canonical, Root: &PlanOperator{Description: id, Identifiers: []string{id}}}
		p := &pipelineCancellation{ctx: context.Background()}
		result := &ExecuteResult{Columns: []string{id}, Rows: [][]interface{}{{id}}, Metadata: map[string]interface{}{"plan": plan, "planString": formatPlan(plan)}}
		want, _ := names.restore(result, nil, exec.parseReturnItems)
		got, err := names.restoreWithCancellation(result, nil, exec.parseReturnItems, p)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, formatPlan(plan), formatPlanWithCancellation(plan, p))
		runPrivacyReaderSamples(t, func(p *pipelineCancellation) interface{} {
			value, _ := names.restoreWithCancellation(result, nil, exec.parseReturnItems, p)
			return value
		})
	}
	for _, originalErr := range []error{nil, context.DeadlineExceeded} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p := &pipelineCancellation{ctx: ctx, remaining: pipelineCancellationInterval}
		_, err := names.restoreWithCancellation(nil, originalErr, func(string) []returnItem { t.Fatal("must not parse"); return nil }, p)
		if originalErr != nil {
			require.ErrorIs(t, err, originalErr)
		} else {
			require.ErrorIs(t, err, context.Canceled)
		}
	}
	var absent *quotedVariableNames
	require.True(t, absent.bindParameterValuesWithCancellation(nil, nil, nil))
	require.Equal(t, "x", absent.parameterNameWithCancellation("x", nil))
}

func TestQuotedVariableCheckedBindingsAndStorage(t *testing.T) {
	p := &pipelineCancellation{ctx: context.Background()}
	_, names := canonicalizeQuotedVariablesWithCancellation("RETURN $`a b`, $`c d`", p)
	require.NotNil(t, names)
	require.Nil(t, names.internalIndex.bounded.probe)
	require.Nil(t, names.parameterIndex.bounded.probe)
	require.Len(t, names.parameterValues(map[string]interface{}{"a b": int64(7)}), 2)
	require.True(t, names.bindParameterValuesWithCancellation(func(string) (interface{}, bool) { return nil, false }, func(string, interface{}) bool { t.Fatal("missing values must not bind"); return false }, p))
	require.False(t, names.bindParameterValuesWithCancellation(func(string) (interface{}, bool) { return 1, true }, func(string, interface{}) bool { return false }, p))
	for _, stopInBind := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		p := &pipelineCancellation{ctx: ctx}
		calls := 0
		require.False(t, names.bindParameterValuesWithCancellation(func(string) (interface{}, bool) {
			if !stopInBind {
				cancel()
			}
			return 1, true
		}, func(string, interface{}) bool { calls++; cancel(); return true }, p))
		require.Equal(t, map[bool]int{false: 0, true: 1}[stopInBind], calls)
		require.ErrorIs(t, p.err, context.Canceled)
	}
	legacy := &quotedVariableNames{internal: map[string]quotedVariableName{"internal": {name: "a b", written: "`a b`"}}, internalOrder: []string{"internal"}, parameters: map[string]string{"internal": "a b"}}
	require.Equal(t, "a b", legacy.parameterNameWithCancellation("internal", p))
	require.Equal(t, "`a b`", legacy.restoreTextWithCancellation("internal", p))
	require.Equal(t, "missing", legacy.parameterNameWithCancellation("missing", p))
	index := newQuotedVariableIndex[int](p)
	require.True(t, index.set("first", 1, p))
	require.True(t, index.set("first", 2, p))
	value, exists := index.get("first", p)
	require.True(t, exists)
	require.Equal(t, 2, value)
	require.Nil(t, index.bounded.probe)
	ctx := withQuotedVariableNames(context.Background(), names)
	require.Same(t, names, quotedVariableNamesForWithCancellation(ctx, names.canonical, p))
	require.Nil(t, quotedVariableNamesForWithCancellation(ctx, "other", p))
	require.Nil(t, quotedVariableNamesForWithCancellation(nil, "other", p))
}

func TestQuotedVariableCheckedClassifierBoundaries(t *testing.T) {
	for _, query := range quotedVariableWorkQueries {
		p := &pipelineCancellation{ctx: context.Background()}
		for at := 0; at <= len(query); at++ {
			want, wi := previousSignificant(query, at)
			got, gi := previousSignificantWithCancellation(query, at, p)
			require.Equal(t, want, got)
			require.Equal(t, wi, gi)
			want, wi = nextSignificant(query, at)
			got, gi = nextSignificantWithCancellation(query, at, p)
			require.Equal(t, want, got)
			require.Equal(t, wi, gi)
			require.Equal(t, previousWord(query, at), previousWordWithCancellation(query, at, p))
			require.Equal(t, nextWordAt(query, at), nextWordAtWithCancellation(query, at, p))
			require.Equal(t, mapProjectionBrace(query, at), mapProjectionBraceWithCancellation(query, at, p))
			require.Equal(t, inYieldList(query, at), inYieldListWithCancellation(query, at, p))
		}
		require.NoError(t, p.err)
	}
	for _, text := range []string{"DISTINCT *", "DISTINCT * WHERE x", "DISTINCT * ,x", "DISTINCT * + 1", "DISTINCT {.x}", "DISTINCT .x", "DISTINCT AS x", "DISTINCT IS ::INTEGER", "DISTINCT IS NULL", "DISTINCT (x)", "distinctName", "DISTINCT"} {
		want, wc := cutDistinct(text)
		p := &pipelineCancellation{ctx: context.Background()}
		got, gc := cutDistinctWithCancellation(text, p)
		require.Equal(t, want, got)
		require.Equal(t, wc, gc)
		replayPrepassWork(t, func(p *pipelineCancellation) { cutDistinctWithCancellation(text, p) })
	}
}

func TestQuotedVariableCheckedColumnSort(t *testing.T) {
	for _, rows := range [][][]interface{}{{{1, 2}, {3, 4}}, {{1}}, nil} {
		original := &ExecuteResult{Columns: []string{"z", "a"}, Rows: rows}
		p := &pipelineCancellation{ctx: context.Background()}
		got := *original
		sortColumnsByNameWithCancellation(&got, p)
		want := *original
		sortColumnsByName(&want)
		require.Equal(t, want, got)
		replayPrepassWork(t, func(p *pipelineCancellation) { copy := *original; sortColumnsByNameWithCancellation(&copy, p) })
	}
}

func TestQuotedVariableCheckedWrapperContracts(t *testing.T) {
	for _, text := range []string{"", "x", "`a b`", "`a``b`", "a—b", "€a", "ñandú"} {
		p := &pipelineCancellation{ctx: context.Background()}
		want, wok := isOneSymbolicName(text)
		got, gok := isOneSymbolicNameWithCancellation(text, p)
		require.Equal(t, want, got)
		require.Equal(t, wok, gok)
		require.Equal(t, isPlainVariableName(text), isPlainVariableNameWithCancellation(text, p))
		require.Equal(t, isUnquotedName(text), isUnquotedNameWithCancellation(text, p))
		replayPrepassWork(t, func(p *pipelineCancellation) { isOneSymbolicNameWithCancellation(text, p) })
		replayPrepassWork(t, func(p *pipelineCancellation) { isPlainVariableNameWithCancellation(text, p) })
	}
	for _, query := range quotedVariableWorkQueries {
		for _, token := range scanQuotedTokens(query) {
			want, got := token, token
			classifyQuotedToken(query, &want, '{')
			classifyQuotedTokenWithCancellation(query, &got, '{', &pipelineCancellation{ctx: context.Background()})
			require.Equal(t, want, got)
		}
		for at := 0; at < len(query); at++ {
			p := &pipelineCancellation{ctx: context.Background()}
			switch query[at] {
			case '(':
				require.Equal(t, callParenthesis(query, at), callParenthesisWithCancellation(query, at, p))
			case '.':
				require.Equal(t, dottedNameCall(query, at), dottedNameCallWithCancellation(query, at, p))
			case ':':
				require.Equal(t, isLabelColon(query, at, '{'), isLabelColonWithCancellation(query, at, '{', p))
			case '|', '&', '!':
				require.Equal(t, inLabelExpression(query, at, '{'), inLabelExpressionWithCancellation(query, at, '{', p))
			}
			require.Equal(t, declaredAt(query, 0, at), declaredAtWithCancellation(query, 0, at, p))
		}
		require.Equal(t, lastKeywordBefore(query, len(query), "RETURN"), lastKeywordBeforeWithCancellation(query, len(query), "RETURN", &pipelineCancellation{ctx: context.Background()}))
		want, we, wok := finalReturnBody(query)
		got, ge, gok := finalReturnBodyWithCancellation(query, &pipelineCancellation{ctx: context.Background()})
		require.Equal(t, want, got)
		require.Equal(t, we, ge)
		require.Equal(t, wok, gok)
		replayPrepassWork(t, func(p *pipelineCancellation) { finalReturnBodyWithCancellation(query, p) })
	}
	for _, query := range []string{":", "{key:1}", "{`a b`:1}", "{n:A|B&C|D}", "n:|A", "n:!A", "n:`A B`|C"} {
		for at := 0; at < len(query); at++ {
			if query[at] == ':' {
				replayPrepassWork(t, func(p *pipelineCancellation) { isLabelColonWithCancellation(query, at, '{', p) })
			}
			if query[at] == '|' || query[at] == '&' || query[at] == '!' {
				replayPrepassWork(t, func(p *pipelineCancellation) { inLabelExpressionWithCancellation(query, at, '{', p) })
			}
		}
	}
	_, names := canonicalizeQuotedVariables("WITH 1 AS `x` RETURN `x`")
	require.True(t, names.touches("x"))
	require.NotNil(t, names.restoreValue([]interface{}{"x"}))
	require.Equal(t, names.original, names.restoreStatementText(names.canonical))
	replayPrepassWork(t, func(p *pipelineCancellation) { names.touchesWithCancellation("x", p) })
	for _, mode := range []ExecutionMode{ModeExplain, ModeProfile} {
		plan := &ExecutionPlan{Mode: mode, Root: &PlanOperator{OperatorType: "Projection", Description: "quoted variables", Children: []*PlanOperator{{OperatorType: "Argument"}}}}
		replayPrepassWork(t, func(p *pipelineCancellation) { formatPlanWithCancellation(plan, p) })
		var out strings.Builder
		formatOperator(&out, plan.Root, 0, mode == ModeProfile)
		require.Contains(t, out.String(), "Projection")
	}
	var absent *quotedVariableNames
	result, err := absent.restoreWithCancellation(nil, nil, nil, &pipelineCancellation{ctx: context.Background()})
	require.Nil(t, result)
	require.NoError(t, err)
}

func TestQuotedVariableCheckedRestorePublication(t *testing.T) {
	exec, _ := newTestExecutor(t)
	// Replay the checked metadata owner: native-map iteration must not vary
	// the number of reachable polls between discovery and cancellation.
	_, names := canonicalizeQuotedVariablesWithCancellation("WITH 1 AS `a b`, 2 AS `c d` RETURN `a b`, `c d`", &pipelineCancellation{ctx: context.Background()})
	result := &ExecuteResult{Columns: append([]string(nil), names.internalOrder...), Metadata: map[string]interface{}{"description": strings.Repeat(names.internalOrder[0], 100)}, Rows: [][]interface{}{{1, 2}}}
	healthy := &pipelineCountingContext{Context: context.Background()}
	_, err := names.restoreWithCancellation(result, nil, exec.parseReturnItems, &pipelineCancellation{ctx: healthy})
	require.NoError(t, err)
	for stop := 1; stop <= healthy.calls; stop++ {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: stop}
		got, err := names.restoreWithCancellation(result, nil, exec.parseReturnItems, &pipelineCancellation{ctx: ctx})
		require.Same(t, result, got, "partly restored results must not be published")
		require.ErrorIs(t, err, context.Canceled)
	}
	for _, stopAt := range []int{1, 2} {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		parse := func(text string) []returnItem {
			calls++
			if calls == stopAt {
				cancel()
			}
			return exec.parseReturnItems(text)
		}
		got, err := names.restoreWithCancellation(result, nil, parse, &pipelineCancellation{ctx: ctx})
		require.Same(t, result, got)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, stopAt, calls)
	}
	want, _ := names.restore(result, nil, exec.parseReturnItems)
	copy := *result
	names.restoreProjection(&copy, exec.parseReturnItems)
	require.Equal(t, want.Columns, copy.Columns)
	_, params := canonicalizeQuotedVariables("RETURN $`a b`")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, params.bindParameterValuesWithCancellation(nil, nil, &pipelineCancellation{ctx: ctx}))
	require.Nil(t, quotedVariableNamesForWithCancellation(context.Background(), "x", &pipelineCancellation{ctx: ctx}))
}
