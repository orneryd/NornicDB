// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// cypherVersionKey carries the statement's Cypher language version ("5" or
// "25") to the subquery bodies it evaluates.
type cypherVersionKey struct{}

// withCypherVersion records the language version of the statement that
// starts here. A nested statement (a subquery body run on its own, with no
// CYPHER prefix) keeps its parent's version. Cypher 5, the default, isn't
// recorded: a nested statement under a Cypher 5 one reads 5 either way, and
// the common statement costs no allocation.
func withCypherVersion(ctx context.Context, query string) context.Context {
	if _, ok := ctx.Value(cypherVersionKey{}).(string); ok {
		return ctx
	}
	switch version := cypherGrammarVersion(query); version {
	case "5":
		return ctx
	case "25":
		return context.WithValue(ctx, cypherVersionKey{}, "25")
	default:
		return context.WithValue(ctx, cypherVersionKey{}, version)
	}
}

// cypherVersionFromContext is the statement's language version, "5" by
// default.
func cypherVersionFromContext(ctx context.Context) string {
	if version, ok := ctx.Value(cypherVersionKey{}).(string); ok {
		return version
	}
	return "5"
}

// correlatedGrouping carries physical Cypher 5 grouping keys for actual WITH
// occurrences. It never inserts projected items into the retained source.
type correlatedGroupingKey struct{}
type correlatedGrouping struct {
	origin  cypherantlr.SyntaxOrigin
	clauses map[int][]string
}

func correlatedGroupingKeys(ctx context.Context, clause pipelineClause) []string {
	policy, _ := ctx.Value(correlatedGroupingKey{}).(*correlatedGrouping)
	if policy == nil || policy.origin != clause.syntax.Origin() {
		return nil
	}
	return policy.clauses[clause.syntax.ID()]
}

// cypher5ImportGrouping applies the existing imported-aggregation policy to
// retained projection occurrences. Grouping keys are physical inputs, not
// invented WITH syntax. Cypher 25 keeps its own aggregate-row contract.
func cypher5ImportGrouping(ctx context.Context, body queryBody, imports map[string]interface{}) (context.Context, error) {
	policy := &correlatedGrouping{origin: body.syntax.Origin(), clauses: map[int][]string{}}
	if cypherVersionFromContext(ctx) == "25" || len(imports) == 0 {
		return context.WithValue(ctx, correlatedGroupingKey{}, policy), nil
	}
	for _, branch := range body.branches {
		for index, clause := range branch.clauses {
			if clause.kind != pipelineClauseWith && clause.kind != pipelineClauseReturn {
				continue
			}
			plan := projectionPlanOfNode(clause.syntax)
			if !plan.hasAggregate {
				continue
			}
			projected := make(map[string]bool, len(plan.projections))
			for _, item := range plan.projections {
				projected[item.alias] = true
				if !item.isAggr {
					continue
				}
				for _, name := range expressionVariables(removeAggregateCalls(item.expr)) {
					if _, imported := imports[name]; imported {
						return ctx, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreGroupByImplicitGroupingKey(name))
					}
				}
			}
			if clause.kind != pipelineClauseWith {
				continue
			}
			var after strings.Builder
			for _, next := range branch.clauses[index+1:] {
				after.WriteString(next.syntax.Code())
				after.WriteByte(' ')
			}
			var keys []string
			for _, name := range expressionVariables(after.String()) {
				if _, imported := imports[name]; imported && !projected[name] {
					keys = append(keys, name)
				}
			}
			if len(keys) > 0 {
				policy.clauses[clause.syntax.ID()] = keys
			}
		}
	}
	return context.WithValue(ctx, correlatedGroupingKey{}, policy), nil
}
