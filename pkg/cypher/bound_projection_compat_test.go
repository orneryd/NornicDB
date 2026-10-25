package cypher

import (
	"context"
	"strings"
)

// Author: #1640 lane (Codex). Existing fragment-oriented test probes keep their
// syntax adapters here. Production aggregation admission receives the retained
// plan directly from the canonical constructor; these adapters own no rules.

func validateReturnOrderBySemanticScope(ctx context.Context, clause string) error {
	return validateReturnOrderBySemanticScopeIn(ctx, clause, nil)
}

// Retained imports are available throughout a subquery, including a DISTINCT
// or aggregating horizon's ORDER BY. They remain outside its projection key.
func validateReturnOrderBySemanticScopeIn(ctx context.Context, clause string, retained typeScope) error {
	body := strings.TrimSpace(clause[len("RETURN"):])
	orderIndex := topLevelKeywordIndex(body, "ORDER BY")
	if orderIndex < 0 {
		return nil
	}
	projectionBody := strings.TrimSpace(body[:orderIndex])
	orderBody := strings.TrimSpace(body[keywordEndAt(body, orderIndex, "ORDER BY"):])
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(orderBody, keyword); index >= 0 {
			orderBody = strings.TrimSpace(orderBody[:index])
		}
	}
	plan := returnProjectionPlanFor(ctx, "RETURN "+projectionBody)
	return validateReturnOrderPlan(ctx, plan, "ORDER BY "+orderBody, retained)
}

func validateReturnAggregationSemantics(ctx context.Context, body string) error {
	return validateReturnAggregationSemanticsIn(ctx, body, nil)
}

// validateReturnAggregationSemanticsIn is validateReturnAggregationSemantics
// with outer's variables constants: in an EXISTS / COUNT / COLLECT body the
// enclosing query's variables aren't grouping keys.
func validateReturnAggregationSemanticsIn(ctx context.Context, body string, outer typeScope) error {
	end := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	body, _ = cutDistinct(body)
	plan := returnProjectionPlanFor(ctx, "RETURN "+body)
	return validateProjectionAggregatePlan(plan, outer)
}

// pipelineReturnSourceColumns is a legacy semantic/source-spelling adapter of
// execution's projection plan, not another RETURN parser. The semantic walk
// still uses nil for bare star and a star marker for mixed projections; the
// execution header path expands wildcards through the complete statement owner.
func pipelineReturnSourceColumns(ctx context.Context, clause string) []string {
	plan := returnProjectionPlanFor(ctx, clause)
	if !plan.valid {
		return nil
	}
	if plan.star {
		if len(plan.starItems) == 0 {
			return nil
		}
		return append([]string{"*"}, plan.withStarExpanded(nil).columns...)
	}
	return append([]string{}, plan.columns...)
}

func returnsStar(ctx context.Context, clause string) bool {
	node, found := canonicalQueryClause(ctx, clause)
	return found && projectionPlanFromSyntax(node).star
}
