package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectionPreparationBudgetBoundaries(t *testing.T) {
	executor, _ := newTestExecutor(t)
	for _, body := range []string{
		"", "*", "*, x AS `alias name`", "DISTINCT x AS value ORDER BY value SKIP 1 LIMIT 2",
		"`" + strings.Repeat("a", 129) + "`", "CASE WHEN true THEN 'AS' ELSE 'x' END AS value",
		"collect(DISTINCT '" + strings.Repeat("x", 129) + "') AS values",
		"sum(x)+count(x) AS value", "COUNT { RETURN count(*) } AS value", "apoc.coll.sum([1,2]) AS value",
		"'" + strings.Repeat("\\", 130) + "' AS value", "collect" + strings.Repeat(" ", 129) + "(x) AS values",
		strings.Repeat("x,", 129) + "x", "x AS `a``b`", "DISTINCT distinct AS value", "count(DISTINCT) AS value",
	} {
		t.Run(body[:min(len(body), 50)], func(t *testing.T) {
			want := executor.parseReturnItems(body)
			runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
				actual := executor.parseReturnItemsWithCancellation(body, probe)
				if probe.err == nil {
					require.Equal(t, want, actual)
				}
			})
			clause := "RETURN " + body
			plan := parseReturnProjectionPlan(clause)
			runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
				actual := parseReturnProjectionPlanWithCancellation(clause, probe)
				if probe.err == nil {
					require.Equal(t, plan, actual)
				}
			})
		})
	}
}

func TestProjectionAggregateReaderBudgetBoundaries(t *testing.T) {
	for _, expr := range []string{
		"", "x", "sum(x)", "SUM (x) + 1", "collect(" + strings.Repeat("x", 129) + ")", "count(DISTINCT x)",
		"COUNT(DISTINCT)", "sum(x)+count(x)", "sum(x", "count" + strings.Repeat(" ", 129) + "(x)",
		"'" + strings.Repeat("x", 129) + "' + sum(x)", "'" + strings.Repeat("\\", 129) + "' + sum(x)",
		"COUNT { RETURN count(*) }", "COLLECT { RETURN sum(x) }", "EXISTS { RETURN count(*) }",
		"count" + strings.Repeat("x", 129) + "{x}", "apoc.coll.sum([1,2])", "gds.graph.project(x,y,{})",
	} {
		want := findAggregateSpans(expr)
		name, inner, distinct, ok := parsePipelineAggregate(expr)
		runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
			actual := findAggregateSpansWithCancellation(expr, probe)
			if probe.err == nil {
				require.Equal(t, want, actual)
			}
		})
		runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
			n, i, d, o := parsePipelineAggregateWithCancellation(expr, probe)
			if probe.err == nil {
				require.Equal(t, name, n)
				require.Equal(t, inner, i)
				require.Equal(t, distinct, d)
				require.Equal(t, ok, o)
			}
		})
	}
}

func TestProjectionPlanCacheCancellationPublication(t *testing.T) {
	clause := "RETURN '" + strings.Repeat("x", 1<<16) + "' AS value"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe := &pipelineCancellation{ctx: ctx}
	returnProjectionPlans.Lock()
	returnProjectionPlans.plans.DeleteWithCancellation(clause, nil)
	returnProjectionPlans.Unlock()
	plan := returnProjectionPlanForWithCancellation(clause, probe)
	require.False(t, plan.valid)
	require.ErrorIs(t, probe.err, context.Canceled)
	returnProjectionPlans.RLock()
	_, found := returnProjectionPlans.plans.Get(clause)
	returnProjectionPlans.RUnlock()
	require.False(t, found, "cancellation cannot publish a partial cached projection")
}

func TestQuotedParameterCallerBridgeCancellation(t *testing.T) {
	_, names := canonicalizeQuotedVariables("RETURN $`a b` AS `quoted name`")
	require.NotNil(t, names)
	params := map[string]interface{}{"a b": int64(1), strings.Repeat("key", 129): int64(2)}
	expected := names.parameterValues(params)
	runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
		actual := bindQuotedQueryParametersWithCancellation(names, params, p)
		if p.err == nil {
			require.Equal(t, expected, actual)
		}
	})
	require.Len(t, params, 2, "quoted aliases cannot mutate caller parameters")
	require.Equal(t, expected, bindQuotedQueryParametersWithCancellation(names, params, nil))
}
