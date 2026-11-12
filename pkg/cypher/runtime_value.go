package cypher

import (
	"context"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// runtimeProcedureResult admits registered/plugin rows before YIELD or a query
// tail consumes them. A handler can retain its result, so copy its row envelope;
// FunctionResult copies only rows and nested containers needing normalization.
func runtimeProcedureResult(result *ExecuteResult) *ExecuteResult {
	if result == nil {
		return nil
	}
	normalized := *result
	normalized.Rows = make([][]interface{}, len(result.Rows))
	for index, row := range result.Rows {
		normalized.Rows[index] = cypherfn.FunctionResult(row).([]interface{})
	}
	return &normalized
}

// runtimeRowResult preserves all three evaluator outcomes. In particular an
// unresolved expression's incidental value is never a successfully read map.
func runtimeRowResult(value interface{}, resolved bool, err error) (interface{}, bool, error) {
	if err != nil || !resolved {
		return nil, false, err
	}
	return cypherfn.RuntimeValue(value), true, nil
}

// checkedPropertyExpression is the consumer boundary for property materializing
// adapters. Errors and unhandled expressions cannot be stored as NULL or text.
func (e *StorageExecutor) checkedPropertyExpression(ctx context.Context, expr string, row pipelineRow) (interface{}, error) {
	ctx = withExpressionFailureSlot(ctx)
	value, resolved := e.evaluateRowExpressionWithContext(ctx, expr, row)
	return checkedPropertyResult(ctx, expr, value, resolved, nil)
}

// checkedPropertyResult is the only result-to-value boundary for materialization.
// A resolved NULL is a value; an unresolved result never supplies one.
func checkedPropertyResult(ctx context.Context, expr string, value interface{}, resolved bool, err error) (interface{}, error) {
	if recorded := getExpressionFailure(ctx); recorded != nil {
		err = recorded
	}
	if err == nil && !resolved {
		err = unresolvedExpressionError(expr)
	}
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, err
	}
	return cypherfn.RuntimeValue(value), nil
}

func unresolvedExpressionError(expr string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "could not evaluate expression: "+expr)
}

// materializePropertyMap evaluates syntactic entries against their complete row.
// No partial map escapes on error. Only CREATE's successful NULL fields are omitted.
// orderedValue retains HTTP traversal metadata for a map that is an expression
// value. Private pattern/property bags never escape as that map value.
func (e *StorageExecutor) materializePropertyMap(ctx context.Context, text string, row pipelineRow, retainNull, orderedValue bool) (map[string]interface{}, error) {
	expressions, err := parsePropertyExpressions(text)
	if err != nil {
		return nil, err
	}
	starts, err := propertyExpressionValueStarts(row, text)
	if err != nil {
		return nil, err
	}
	return e.materializePropertyExpressions(ctx, expressions, row, retainNull, orderedValue, starts)
}

type propertyExpressionPlan struct {
	expressions []pipelinePropertyExpression
	err         error
}

var propertyExpressionPlans = newBoundedCache[string, propertyExpressionPlan](512, boundedCacheStringShardIndex)

// parsePropertyExpressions returns a read-only syntax plan. Row values, errors
// from evaluation and property maps are never cached with the expression text.
func parsePropertyExpressions(text string) ([]pipelinePropertyExpression, error) {
	text = propertyExpressionText(text)
	if text == "" {
		return nil, nil
	}
	if plan, cached := propertyExpressionPlans.get(text); cached {
		return plan.expressions, plan.err
	}
	var expressions []pipelinePropertyExpression
	err := forEachMapEntry(text, mapSourceText, func(key, expr string) {
		expressions = append(expressions, pipelinePropertyExpression{key: key, expr: expr})
	}, nil)
	propertyExpressionPlans.put(text, propertyExpressionPlan{expressions: expressions, err: err})
	return expressions, err
}

// propertyExpressionText is the one normalization of a map literal text for the
// entry reader: trimmed, and enclosed in braces when the caller wrote the entry
// list alone.
func propertyExpressionText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" || strings.HasPrefix(text, "{") {
		return text
	}
	return "{" + text + "}"
}

// propertyExpressionValueStarts returns where each entry's value starts in text,
// in entry order, for a row a statement is profiling. It is the PROFILE sidecar:
// the entry positions a map literal's scalar subqueries report, read from the
// same map scanner that produced the entries and never cached with them. An
// ordinary row binds no expression origin, so it reads no positions and
// allocates nothing.
//
// Each position is an offset in text because a reader only measures entries when
// its text begins at the byte the row's origin names: the map literal the row
// evaluator hands this reader, trimmed and braced. Text the normalizer would
// change has no such offset and reports none.
func propertyExpressionValueStarts(row pipelineRow, text string) ([]int, error) {
	if text == "" || row == nil || !rowExpressionOrigin(row).had {
		return nil, nil
	}
	if propertyExpressionText(text) != text {
		return nil, nil
	}
	var starts []int
	if err := forEachMapEntry(text, mapSourceText, func(string, string) {}, &starts); err != nil {
		return nil, err
	}
	return starts, nil
}

// materializePropertyExpressions adapts text expressions to the same checked
// materializer used by compiled maps. Only syntax plans, never values, are cached.
func (e *StorageExecutor) materializePropertyExpressions(ctx context.Context, expressions []pipelinePropertyExpression, row pipelineRow, retainNull, orderedValue bool, starts []int) (map[string]interface{}, error) {
	ctx = withExpressionFailureSlot(ctx)
	origin := rowExpressionOriginBinding{}
	if starts != nil {
		origin = rowExpressionOrigin(row)
	}
	return materializePropertyEntries(ctx, expressions, retainNull, orderedValue, func(index int) (interface{}, bool, error) {
		saved, bound := rowExpressionOriginBinding{}, false
		if starts != nil {
			saved, bound = bindRowExpressionOrigin(row, origin.previous+starts[index])
		}
		value, resolved := e.evaluateRowExpressionWithContext(ctx, expressions[index].expr, row)
		if bound {
			restoreRowExpressionOrigin(row, saved)
		}
		return value, resolved, nil
	})
}

// materializePropertyEntries is the map value boundary for both execution
// forms: every entry is resolved and error-free before any complete map escapes.
func materializePropertyEntries(ctx context.Context, expressions []pipelinePropertyExpression, retainNull, orderedValue bool, evaluate func(int) (interface{}, bool, error)) (map[string]interface{}, error) {
	props := make(map[string]interface{}, len(expressions))
	var keys []string
	if orderedValue {
		keys = make([]string, 0, len(expressions))
	}
	for index, expression := range expressions {
		value, resolved, err := evaluate(index)
		value, err = checkedPropertyResult(ctx, expression.expr, value, resolved, err)
		if err != nil {
			return nil, err
		}
		if value == nil && !retainNull {
			continue
		}
		if orderedValue {
			if _, exists := props[expression.key]; !exists {
				keys = append(keys, expression.key)
			}
		}
		// Expression maps carry runtime values, including exact unsigned
		// numbers and typed lists. Storage coercion belongs to property bags;
		// applying it here corrupts values before a numeric consumer reads them.
		if orderedValue {
			value = normalizeExpressionValue(value)
		} else {
			value = normalizePropValue(value)
		}
		props[expression.key] = value
	}
	if orderedValue {
		if failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure); ok {
			failure.recordMapKeyOrder(props, keys)
		}
	}
	return props, nil
}

// rowMapContext preserves a row map's existing ordering/error metadata when
// its compiled or text execution crosses into the shared materializer.
func rowMapContext(ctx context.Context, values pipelineRow) context.Context {
	if failure, ok := values["\x00mapKeyOrders"].(*expressionFailure); ok && ctx.Value(expressionFailureKey{}) != failure {
		return context.WithValue(ctx, expressionFailureKey{}, failure)
	}
	return ctx
}
