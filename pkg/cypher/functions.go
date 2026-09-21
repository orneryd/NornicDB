// Cypher function implementations for NornicDB.
//
// This file holds the public expression-evaluation entrypoints. The heavy
// implementation is split across `functions_eval_part*.go`.
package cypher

import (
	"context"
	"strconv"
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
	if pattern, projection, ok := splitPatternComprehension(expr); ok {
		return e.evaluatePatternComprehension(ctx, pattern, projection, nodes, rels)
	}
	if subquery, ok := standaloneCountSubquery(expr); ok {
		return int64(len(e.evaluateBoundPatternRows(ctx, subquery, nodes, rels)))
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
	// Resolve dotted/bracketed parameter map paths like $d.uuid and
	// $d['uuid'] as typed values.
	if v, ok := resolveParamPathRef(ctx, expr); ok {
		return normalizePropValue(v)
	}
	if baseExpr, property, ok := splitPostfixPropertyAccess(expr); ok {
		base := e.evaluateExpressionWithContextFull(ctx, baseExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		switch value := base.(type) {
		case map[string]interface{}:
			return value[property]
		case *storage.Node:
			if value != nil {
				return value.Properties[property]
			}
			return nil
		case *storage.Edge:
			if value != nil {
				return value.Properties[property]
			}
			return nil
		}
	}
	if v, ok := e.evaluateExpressionFastLeaf(expr, nodes, rels, paths); ok {
		return v
	}
	if hasTopLevelExpressionOperator(expr) {
		return e.evaluateExpressionWithContextFullOperators(ctx, expr, "", nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}
	return e.evaluateExpressionWithContextFullFunctions(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func splitPostfixPropertyAccess(expr string) (string, string, bool) {
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	inSingle, inDouble := false, false
	lastDot := -1
	for index := 0; index < len(expr); index++ {
		ch := expr[index]
		if ch == '\\' && (inSingle || inDouble) {
			index++
			continue
		}
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				parenDepth++
			}
		case ')':
			if !inSingle && !inDouble && parenDepth > 0 {
				parenDepth--
			}
		case '[':
			if !inSingle && !inDouble {
				bracketDepth++
			}
		case ']':
			if !inSingle && !inDouble && bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			if !inSingle && !inDouble {
				braceDepth++
			}
		case '}':
			if !inSingle && !inDouble && braceDepth > 0 {
				braceDepth--
			}
		case '.':
			if !inSingle && !inDouble && parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
				lastDot = index
			}
		}
	}
	if lastDot <= 0 || lastDot == len(expr)-1 {
		return "", "", false
	}
	property := strings.TrimSpace(expr[lastDot+1:])
	if !isValidIdentifier(property) {
		return "", "", false
	}
	return strings.TrimSpace(expr[:lastDot]), property, true
}

func isSimpleIdentifierOrProperty(expr string) bool {
	if expr == "" {
		return false
	}
	expectIdentStart := true
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch {
		case c == '.':
			if expectIdentStart {
				return false
			}
			expectIdentStart = true
		case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
			expectIdentStart = false
		case c >= '0' && c <= '9':
			if expectIdentStart {
				return false
			}
		default:
			return false
		}
	}
	return !expectIdentStart
}

func (e *StorageExecutor) evaluateExpressionFastLeaf(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult) (interface{}, bool) {
	if isWholeCypherQuotedString(expr) {
		if decoded, ok := decodeCypherQuotedString(expr); ok {
			return decoded, true
		}
	}

	if equalFoldASCII(expr, "null") {
		return nil, false
	}
	if equalFoldASCII(expr, "true") {
		return true, true
	}
	if equalFoldASCII(expr, "false") {
		return false, true
	}

	if exprCanBeNumber(expr) {
		if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
			return num, true
		}
		if num, err := strconv.ParseFloat(expr, 64); err == nil {
			return num, true
		}
	}

	if !isSimpleIdentifierOrProperty(expr) {
		return nil, false
	}

	if dotIdx := strings.IndexByte(expr, '.'); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]

		if node, ok := nodes[varName]; ok {
			if node == nil {
				return nil, true
			}
			if propName == "has_embedding" {
				if node.EmbedMeta != nil {
					if val, ok := node.EmbedMeta["has_embedding"]; ok {
						return val, true
					}
				}
				return len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0, true
			}
			if val, ok := node.Properties[propName]; ok {
				return val, true
			}
			return nil, true
		}
		if rel, ok := rels[varName]; ok {
			if rel == nil {
				return nil, true
			}
			if val, ok := rel.Properties[propName]; ok {
				return val, true
			}
			return nil, true
		}
		if val, ok := e.fabricRecordBindings[varName]; ok {
			switch v := val.(type) {
			case map[string]interface{}:
				if propVal, exists := v[propName]; exists {
					return propVal, true
				}
				return nil, true
			case *storage.Node:
				if propVal, exists := v.Properties[propName]; exists {
					return propVal, true
				}
				return nil, true
			}
		}
		return nil, false
	}

	if node, ok := nodes[expr]; ok {
		if node == nil {
			return nil, true
		}
		if len(node.Properties) == 1 {
			if val, hasValue := node.Properties["value"]; hasValue {
				return val, true
			}
		}
		return node, true
	}
	if rel, ok := rels[expr]; ok {
		if rel == nil {
			return nil, true
		}
		return rel, true
	}
	if val, ok := e.fabricRecordBindings[expr]; ok {
		return val, true
	}
	if paths != nil {
		if pathResult, ok := paths[expr]; ok && pathResult != nil {
			return map[string]interface{}{
				"_pathResult": pathResult,
				"length":      pathResult.Length,
				"nodes":       pathResult.Nodes,
				"rels":        pathResult.Relationships,
			}, true
		}
	}

	return nil, true
}

func exprCanBeNumber(expr string) bool {
	if expr == "" {
		return false
	}
	first := expr[0]
	if first >= '0' && first <= '9' {
		return true
	}
	return first == '-' && len(expr) > 1 && expr[1] >= '0' && expr[1] <= '9'
}
