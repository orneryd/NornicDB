package cypher

import "strings"

type propertyExpressionPlan struct {
	expressions []pipelinePropertyExpression
	err         error
}

var propertyExpressionPlans = newBoundedCache[string, propertyExpressionPlan](512, boundedCacheStringShardIndex)

// parsePropertyExpressions returns a read-only syntax plan. Row values, errors
// from evaluation and property maps are never cached with the expression text.
func parsePropertyExpressions(text string) ([]pipelinePropertyExpression, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, nil
	}
	if !strings.HasPrefix(text, "{") {
		text = "{" + text + "}"
	}
	if plan, cached := propertyExpressionPlans.get(text); cached {
		return plan.expressions, plan.err
	}
	var expressions []pipelinePropertyExpression
	err := forEachMapEntry(text, func(key, expr string) {
		expressions = append(expressions, pipelinePropertyExpression{key: key, expr: expr})
	})
	propertyExpressionPlans.put(text, propertyExpressionPlan{expressions: expressions, err: err})
	return expressions, err
}
