package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	graphPatternPredicate = "PATTERN"
	graphPatternComprehension = "COMPREHENSION"
)

// graphExpressionPlan owns the original graph form and the physical strategies
// for its value. Syntax is immutable and contains no executor, row or parameter
// values. The static body scope comes exclusively from semanticScopeWalk's
// bodyScopeOf; every strategy receives the same resolved runtime frame.
type graphExpressionPlan struct {
	form, body, projection string
	variables []string // named pattern positions, for the plain-predicate form rule
	hop *TraversalMatch
}

var graphExpressionPlans = newBoundedCache[string, *graphExpressionPlan](1024, boundedCacheStringShardIndex)

func planGraphExpression(form, body, projection string) *graphExpressionPlan {
	body = strings.TrimSpace(body)
	key := form + "\x00" + body + "\x00" + projection
	if plan, cached := graphExpressionPlans.get(key); cached { return plan }
	plan := &graphExpressionPlan{form: form, body: body, projection: projection}
	if form == graphPatternPredicate {
		plan.variables = append(extractNodeVariables(body), extractRelationshipVariables(body)...)
	}
	// Compile the fixed-hop syntax once. Maps and label/type expressions are
	// normalised with the statement's parameters before any strategy is selected.
	if (form == graphPatternPredicate || form == "EXISTS" || form == "COUNT") &&
		!strings.ContainsAny(body, "{$&!%") && !patternHasInlineWhere(body) && isRelationshipPatternOperand(body) {
		var parser StorageExecutor
		if hop := parser.parseTraversalPattern(context.Background(), body); hop != nil && !hop.IsChained && !hop.Relationship.VariableLength && hop.Relationship.MinHops == 1 && hop.Relationship.MaxHops == 1 {
			plan.hop = hop
		}
	}
	graphExpressionPlans.put(key, plan)
	return plan
}

// scopeBody is a complete query for the static owner's existing body contract.
// A comprehension's projection participates in scope analysis just like RETURN;
// it is never evaluated merely to discover dependencies.
func (plan *graphExpressionPlan) scopeBody() string {
	body := plan.body
	if plan.form == graphPatternPredicate { body = "MATCH " + body }
	// The static owner receives the original comprehension form, never a MATCH
	// translation: its named path is local even if the enclosing row binds it.
	if plan.form == graphPatternComprehension { return "[" + body + " | " + plan.projection + "]" }
	return rowSubqueryQuery("COUNT", body)
}

func (plan *graphExpressionPlan) evaluate(e *StorageExecutor, ctx context.Context, scope compiledRowScope) (interface{}, bool, error) {
	frame := e.resolveGraphExpressionFrame(ctx, scope)
	if plan.form == graphPatternPredicate {
		if err := e.validatePatternPredicateVariables(plan.variables, func(name string) bool { _, present := frame.lookup(name); return present }); err != nil { return nil, true, err }
	}
	frame, err := plan.resolveFrame(e, ctx, frame)
	if err != nil { return nil, true, err }
	ctx = frame.enter(withoutLocalSubqueryScope(ctx))
	values := frame.expressionValues(ctx)
	if plan.form == graphPatternComprehension {
		return plan.comprehensionValue(e, ctx, values), true, getExpressionFailure(ctx)
	}
	kind := plan.form
	if kind == graphPatternPredicate { kind = "EXISTS" }
	if hop := plan.hop; hop != nil && chainVariablesBindEntities(hop, values) {
		if seed, _ := values[hop.StartNode.variable].(*storage.Node); seed != nil {
			if count, expanded := e.boundExpandCount(ctx, hop, seed, "", values, kind == "EXISTS"); expanded { return rowCountValue(kind, count), true, getExpressionFailure(ctx) }
		}
	}
	body, err := plan.normalizedBody(ctx)
	if err != nil { return nil, true, err }
	if kind == "EXISTS" || kind == "COUNT" {
		if count, ok := e.boundDegreeCount(ctx, body, values); ok { return rowCountValue(kind, count), true, getExpressionFailure(ctx) }
		if count, ok := e.boundChainCount(ctx, body, values, kind == "EXISTS"); ok { return rowCountValue(kind, count), true, getExpressionFailure(ctx) }
		if count, ok := e.boundNodeLookupCount(ctx, body, values, kind == "EXISTS"); ok { return rowCountValue(kind, count), true, getExpressionFailure(ctx) }
		if nodes, rels, ok := boundPatternCountBindings(body, values); ok {
			if rows, matched := e.boundPatternRows(ctx, body, nodes, rels, values); matched { return rowCountValue(kind, int64(len(rows))), true, getExpressionFailure(ctx) }
		}
	}
	if cypherVersionFromContext(ctx) != "25" {
		body, err = cypher5ImportGrouping(body, frame.imports())
		if err != nil { return nil, true, err }
	}
	result, err := e.runCorrelatedSubquery(ctx, rowSubqueryQueryFor(ctx, kind, body), values)
	if err != nil { return nil, true, err }
	if result == nil { return nil, false, nil }
	if kind == "EXISTS" || kind == "COUNT" { return rowCountValue(kind, int64(len(result.Rows))), true, nil }
	if len(result.Columns) != 1 {
		return nil, true, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCollectSubquery", localization.CypherSubqueriesCollectSingleColumn(len(result.Columns)))
	}
	collected := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows { if len(row) > 0 { collected = append(collected, row[0]) } }
	return collected, true, nil
}

type normalizedGraphBody struct { body string; err error }

func (plan *graphExpressionPlan) normalizedBody(ctx context.Context) (string, error) {
	parsed := clauseAnalysisFor(ctx, clauseAnalysisGraphExpressionBody, plan.form+"\x00"+plan.body, func(string) normalizedGraphBody {
		body := plan.body
		if plan.form == graphPatternPredicate { body = "MATCH " + body }
		body, _, err := desugarLabelExpressions(body, getParamsFromContext(ctx), cypherVersionFromContext(ctx) == "25")
		return normalizedGraphBody{body, err}
	})
	return parsed.body, parsed.err
}
