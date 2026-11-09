package cypher

import (
	"context"
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

// compileRelationshipWhereFilter compiles a retrieval's where for relationship
// candidates: the candidate bound as relationship, its endpoints as start and
// end (null when not visible) and node as null, with the statement's
// parameters and the calling row's variables, as evaluateWhere binds them for
// a node candidate.
func (e *StorageExecutor) compileRelationshipWhereFilter(ctx context.Context, whereClause string) func(*storage.Edge, *storage.Node, *storage.Node) bool {
	return func(edge *storage.Edge, start, end *storage.Node) bool {
		values := pipelineNodeRow(ctx, "node", nil)
		for name, value := range e.fabricRecordBindings {
			values[name] = value
		}
		for name, value := range valueBindingsFromContext(ctx) {
			values[name] = value
		}
		values["node"] = nil
		values["relationship"] = edge
		values["start"], values["end"] = nil, nil
		if start != nil {
			values["start"] = start
		}
		if end != nil {
			values["end"] = end
		}
		return e.evaluateRowPredicate(ctx, whereClause, values)
	}
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

// evaluateWhereAsBoolean evaluates a WHERE expression (e.g. size(n.content) > 10000, exists(n.prop))
// using the expression evaluator and returns a boolean. Used when evaluateWhere does not handle
// the condition as id(), elementId(), or variable.property.
func (e *StorageExecutor) evaluateWhereAsBoolean(ctx context.Context, whereClause, variable string, node *storage.Node) bool {
	nodes := map[string]*storage.Node{variable: node}
	result := e.evaluateExpressionWithContext(ctx, whereClause, nodes, nil)
	return predicateValueIsTrue(ctx, result, whereClause)
}

// parseValue extracts the actual value from a Cypher literal
func (e *StorageExecutor) parseValue(ctx context.Context, s string) (interface{}, error) {
	return e.checkedPropertyExpression(ctx, s, e.fabricRecordBindings)
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
