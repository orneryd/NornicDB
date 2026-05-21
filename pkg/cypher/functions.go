// Cypher function implementations for NornicDB.
//
// This file holds the public expression-evaluation entrypoints. The heavy
// implementation is split across `functions_eval_part*.go`.
package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// PluginFunctionLookup is a callback to look up functions from loaded plugins.
// Set by pkg/nornicdb during database initialization.
// Returns the function handler and true if found, nil and false otherwise.
var PluginFunctionLookup func(name string) (handler interface{}, found bool)

func isFunctionCall(expr, funcName string) bool {
	return isFunctionCallWS(expr, funcName)
}

// evaluateExpression evaluates an expression for a single node context.
func (e *StorageExecutor) evaluateExpression(ctx context.Context, expr string, varName string, node *storage.Node) interface{} {
	return e.evaluateExpressionWithContext(ctx, expr, map[string]*storage.Node{varName: node}, nil)
}

// evaluateExpressionWithPathContext evaluates an expression with full path context.
func (e *StorageExecutor) evaluateExpressionWithPathContext(ctx context.Context, expr string, pathCtx PathContext) interface{} {
	return e.evaluateExpressionWithContextFull(ctx, expr, pathCtx.nodes, pathCtx.rels, pathCtx.paths, pathCtx.allPathEdges, pathCtx.allPathNodes, pathCtx.pathLength)
}

func (e *StorageExecutor) evaluateExpressionWithContext(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContextFull(ctx, expr, nodes, rels, nil, nil, nil, 0)
}

func (e *StorageExecutor) evaluateExpressionWithContextFull(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	// Direct $param resolution preserves declared types end-to-end.
	// substituteParams's type-preserving short-circuit leaves "$name" as
	// a literal here for composite values; without this branch the
	// generic expression evaluator returns the unresolved string. With
	// it, []string / []float64 / map[string]any params keep their
	// declared shape through reduce(), list comprehensions, SET, and
	// every other expression context.
	if v, ok := resolveDirectParamRef(ctx, expr); ok {
		return v
	}
	return e.evaluateExpressionWithContextFullFunctions(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}
