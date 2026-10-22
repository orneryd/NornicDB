package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullPropsLiterals(admission admittedRelationshipStatement,
	ctx context.Context,
	expr string,
	lowerExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	paths map[string]*PathResult,
	allPathEdges []*storage.Edge,
	allPathNodes []*storage.Node,
	pathLength int,
) interface{} {
	// Property Access: n.property
	// ========================================
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]

		if node, ok := nodes[varName]; ok {
			if node == nil {
				return nil
			}
			// has_embedding is stored in EmbedMeta by the managed embedding system
			if propName == "has_embedding" {
				return nodeHasEmbeddingValue(node)
			}
			if val, ok := e.queryEntityProperties(node)[propName]; ok {
				return val
			}
			return nil
		}
		if rel, ok := rels[varName]; ok {
			if rel == nil {
				return nil
			}
			if val, ok := e.queryEntityProperties(rel)[propName]; ok {
				return val
			}
			return nil
		}
		if val, ok := e.boundValue(ctx, varName); ok {
			if property, supported := rowPropertyValue(e.queryVisibleValue(val), propName); supported {
				return property
			}
		}
	}

	// ========================================
	// Variable Reference - return whole node/rel/path
	// ========================================
	if node, ok := nodes[expr]; ok {
		if node == nil {
			return nil
		}
		return node
	}
	if rel, ok := rels[expr]; ok {
		if rel == nil {
			return nil
		}
		// Return *storage.Edge so Bolt encodes it as a proper Relationship (0x52);
		// returning a map caused drivers to receive a generic map and display null for r.
		return rel
	}
	if val, ok := e.boundValue(ctx, expr); ok {
		return val
	}
	// Check if this is a path variable - return the PathResult as a map
	// This allows path functions like length(path), relationships(path) to work after WITH
	if paths != nil {
		if pathResult, ok := paths[expr]; ok && pathResult != nil {
			return pathContextEntryValue(pathResult)
		}
	}

	// ========================================
	// Literals
	// ========================================

	// null
	if lowerExpr == "null" {
		return nil
	}

	// Boolean
	if lowerExpr == "true" {
		return true
	}
	if lowerExpr == "false" {
		return false
	}

	// String literal (single or double quotes)
	if isWholeCypherQuotedString(expr) {
		if decoded, ok := decodeCypherQuotedString(expr); ok {
			return decoded
		}
	}

	// Number literal
	if num, ok := parseIntFast(expr); ok {
		return num
	}
	if num, ok := parseFloatFast(expr); ok {
		return num
	}

	// Only a complete list literal enters the checked list-value boundary.
	// Subscripts and expressions over lists stay with the row evaluator.
	if isListLiteral(expr) {
		value, err := e.checkedPropertyExpression(admission, ctx, expr, e.entityRowWithPaths(ctx, nodes, rels, paths))
		recordExpressionFailure(ctx, err)
		return value
	}

	// Map literal {key: value}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		value, err := e.parseProperties(admission, ctx, expr)
		recordExpressionFailure(ctx, err)
		return value
	}

	recordExpressionFailure(ctx, unresolvedExpressionError(expr))
	return nil
}
