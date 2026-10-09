package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) filterNodes(ctx context.Context, nodes []*storage.Node, variable, whereClause string) []*storage.Node {
	return parallelFilterNodes(nodes, e.compileNodeWhereFilter(ctx, variable, whereClause))
}

func (e *StorageExecutor) getCompiledSimpleWhere(ctx context.Context, variable, whereClause string) (func(*storage.Node) bool, bool) {
	plan := planRowPredicate(whereClause)
	if plan == nil || !plan.complete {
		return nil, false
	}
	parameters := getParamsFromContext(ctx)
	values := make(map[string]interface{}, len(e.fabricRecordBindings)+len(valueBindingsFromContext(ctx)))
	for name, value := range e.fabricRecordBindings {
		values[name] = value
	}
	for name, value := range valueBindingsFromContext(ctx) {
		values[name] = value
	}
	return func(node *storage.Node) bool {
		return e.evaluateRowPredicatePartScope(ctx, &plan.root, compiledRowScope{values: values, nodes: binding{variable: node}, parameters: parameters}, nil)
	}, true
}

func (e *StorageExecutor) compileNodeWhereFilter(ctx context.Context, variable, whereClause string) func(*storage.Node) bool {
	if compiled, ok := e.getCompiledSimpleWhere(ctx, variable, whereClause); ok {
		return compiled
	}
	return func(node *storage.Node) bool {
		return e.evaluateWhere(ctx, node, variable, whereClause)
	}
}

// evaluateWhere reports whether a single-node WHERE clause holds, i.e. is
// known true under Cypher's three-valued logic (null and false both drop the
// row).
func (e *StorageExecutor) evaluateWhere(ctx context.Context, node *storage.Node, variable, whereClause string) bool {
	if strings.TrimSpace(whereClause) == "" {
		return true
	}
	values := pipelineNodeRow(ctx, variable, node)
	for name, value := range e.fabricRecordBindings {
		if name != variable {
			values[name] = value
		}
	}
	for name, value := range valueBindingsFromContext(ctx) {
		if name != variable {
			values[name] = value
		}
	}
	return e.evaluateRowPredicate(ctx, whereClause, values)
}

func hasPrefixFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func hasSuffixFold(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return strings.EqualFold(s[len(s)-len(suffix):], suffix)
}

func containsFold(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	if len(s) < len(sub) {
		return false
	}
	max := len(s) - len(sub)
	for i := 0; i <= max; i++ {
		if strings.EqualFold(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}

// normalizeNodeIDValue converts canonical element ID strings to raw internal IDs.
// Examples:
// - "4:nornicdb:abc-123" -> "abc-123"
// - "abc-123" -> "abc-123"
func normalizeNodeIDValue(v interface{}) interface{} {
	s, ok := v.(string)
	if !ok {
		return v
	}
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, ":", 3)
	if len(parts) == 3 && parts[0] == "4" {
		return parts[2]
	}
	return s
}

// evaluateWhereAsBoolean evaluates a WHERE expression (e.g. size(n.content) > 10000, exists(n.prop))
// using the expression evaluator and returns a boolean. Used when evaluateWhere does not handle
// the condition as id(), elementId(), or variable.property.
func (e *StorageExecutor) evaluateWhereAsBoolean(ctx context.Context, whereClause, variable string, node *storage.Node) bool {
	nodes := map[string]*storage.Node{variable: node}
	result := e.evaluateExpressionWithContext(ctx, whereClause, nodes, nil)
	return predicateValueIsTrue(ctx, result, whereClause)
}

// parseValue extracts the actual value from a Cypher literal
func (e *StorageExecutor) parseValue(ctx context.Context, s string) interface{} {
	s = strings.TrimSpace(s)

	if v, ok := resolveParamPathRef(ctx, s); ok {
		return normalizePropValue(v)
	}

	// Handle arrays: [0.1, 0.2, 0.3]
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return e.parseArrayValue(ctx, s)
	}
	// Handle map literals: {key: value}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		return e.parseProperties(ctx, s)
	}

	// Handle quoted strings with escape sequence support
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		if decoded, ok := decodeCypherQuotedString(s); ok {
			return decoded
		}
	}

	// Handle booleans
	upper := upperASCII(s)
	if upper == "TRUE" {
		return true
	}
	if upper == "FALSE" {
		return false
	}
	if upper == "NULL" {
		return nil
	}

	// Handle numbers - preserve int64 for integers, use float64 only for decimals
	// The comparison functions use toFloat64() which handles both types
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i // Keep as int64 for Neo4j compatibility
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	if e.hasArithmeticOperator(s) {
		if evaluated, ok := e.evaluateScalarPropertyExpression(ctx, s); ok {
			return normalizePropValue(evaluated)
		}
	}

	// Fabric correlated bindings: resolve bare identifier values from outer record context.
	if len(e.fabricRecordBindings) > 0 {
		isIdent := true
		for i, ch := range s {
			if i == 0 {
				if !isIdentStartRune(ch) {
					isIdent = false
					break
				}
			} else {
				if !isIdentRune(ch) {
					isIdent = false
					break
				}
			}
		}
		if isIdent {
			if v, ok := e.fabricRecordBindings[s]; ok {
				return v
			}
		}
	}

	return s
}

func cloneStringAnyMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (e *StorageExecutor) resolveReturnItem(ctx context.Context, item returnItem, variable string, node *storage.Node) interface{} {
	row := e.mergeBindingRow(ctx, map[string]*storage.Node{variable: node}, nil)
	projected, err := e.projectMergeReturn(ctx, []pipelineRow{row}, "RETURN "+item.expr)
	if err != nil || len(projected.Rows) == 0 || len(projected.Rows[0]) == 0 {
		return nil
	}
	return projected.Rows[0][0]
}
