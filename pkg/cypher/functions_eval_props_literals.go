package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullPropsLiterals(
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
				if node.EmbedMeta != nil {
					if val, ok := node.EmbedMeta["has_embedding"]; ok {
						return val
					}
				}
				return len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0
			}
			if val, ok := node.Properties[propName]; ok {
				return val
			}
			return nil
		}
		if rel, ok := rels[varName]; ok {
			if rel == nil {
				return nil
			}
			if val, ok := rel.Properties[propName]; ok {
				return val
			}
			return nil
		}
		if val, ok := e.boundValue(ctx, varName); ok {
			switch v := val.(type) {
			case nil:
				return nil
			case map[string]interface{}:
				if pv, exists := v[propName]; exists {
					return pv
				}
				return nil
			case *storage.Node:
				if pv, exists := v.Properties[propName]; exists {
					return pv
				}
				return nil
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
			// A variable-length relationship variable is represented in the path
			// context by a relationship-only PathResult. Its Cypher value is the
			// ordered list of relationships, not a named path value.
			if len(pathResult.Nodes) == 0 {
				values := make([]interface{}, len(pathResult.Relationships))
				for index, relationship := range pathResult.Relationships {
					values[index] = relationship
				}
				return values
			}
			// Return path as a serializable structure that can be used later
			return map[string]interface{}{
				"_pathResult": pathResult, // Keep the PathResult for later use
				"length":      pathResult.Length,
				"nodes":       pathResult.Nodes,
				"rels":        pathResult.Relationships,
			}
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

	// List literal [a, b, c]: every element is an expression evaluated in the
	// same context (as the row and path evaluators do), so [a.v, b.v] yields
	// the property values rather than their text.
	if inner, isList := stripEnclosingRowDelimiter(expr, '[', ']'); isList {
		inner = strings.TrimSpace(inner)
		if inner == "" {
			return []interface{}{}
		}
		elements := e.splitArrayElements(inner)
		result := make([]interface{}, len(elements))
		for i, element := range elements {
			result[i] = e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(element), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
		return result
	}
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		return e.parseArrayValue(ctx, expr)
	}

	// Map literal {key: value}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return e.parseProperties(ctx, expr)
	}

	// Check if this looks like a variable reference (identifier pattern)
	// If it's a valid identifier and not found in nodes/rels, it should be null
	// This handles cases like OPTIONAL MATCH where the variable might not exist
	isValidIdentifier := true
	for i, ch := range expr {
		if i == 0 {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
				isValidIdentifier = false
				break
			}
		} else {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
				isValidIdentifier = false
				break
			}
		}
	}
	if isValidIdentifier && len(expr) > 0 {
		// This looks like an unresolved variable reference - return null
		return nil
	}

	// Check if this is an aggregation function - they should not be evaluated in expression context
	exprLower := strings.ToLower(expr)
	if strings.HasPrefix(exprLower, "count(") ||
		strings.HasPrefix(exprLower, "sum(") ||
		strings.HasPrefix(exprLower, "avg(") ||
		strings.HasPrefix(exprLower, "min(") ||
		strings.HasPrefix(exprLower, "max(") ||
		strings.HasPrefix(exprLower, "collect(") {
		// Aggregation functions must be handled by aggregation logic, not per-row evaluation
		return nil
	}

	// Unknown - return as string (for string literals without quotes, etc.)
	return expr
}
