package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateStringConcatWithContext handles string concatenation with + operator.
func (e *StorageExecutor) evaluateStringConcatWithContext(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) string {
	var result strings.Builder

	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)

	for _, part := range parts {
		val := e.evaluateExpressionWithContext(ctx, part, nodes, rels)
		result.WriteString(fmt.Sprintf("%v", val))
	}

	return result.String()
}

// tryCallPluginFunction attempts to call a function from the plugin system.
// Returns the result and true if the function was handled, or nil and false if not found.
// This is GENERIC - works for any plugin function (not specific to any plugin).
func (e *StorageExecutor) tryCallPluginFunction(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, bool) {
	// Plugin lookup must be configured
	if PluginFunctionLookup == nil {
		return nil, false
	}

	// Extract function name from expression (e.g., "myplugin.func([1,2,3])" -> "myplugin.func")
	lowerExpr := strings.ToLower(expr)
	parenIdx := strings.Index(lowerExpr, "(")
	if parenIdx == -1 {
		return nil, false
	}
	funcName := lowerExpr[:parenIdx]

	// Look up in plugin registry
	handler, found := PluginFunctionLookup(funcName)
	if !found {
		return nil, false
	}

	// Parse arguments
	argsStr := strings.TrimSpace(expr[parenIdx+1 : len(expr)-1])
	args := e.splitFunctionArgs(argsStr)

	// Evaluate each argument
	var evalArgs []interface{}
	for _, arg := range args {
		evalArgs = append(evalArgs, e.evaluateExpressionWithContext(ctx, arg, nodes, rels))
	}

	// Call the plugin function
	result, err := callPluginHandler(handler, evalArgs)
	if err != nil {
		// Log error but don't fail - fall back to built-in if available
		return nil, false
	}

	return result, true
}

// callPluginHandler invokes a plugin function handler with the given arguments.
func callPluginHandler(handler interface{}, args []interface{}) (interface{}, error) {
	if handler == nil {
		return nil, fmt.Errorf("nil handler")
	}

	// Type-based dispatch for common function signatures
	switch fn := handler.(type) {
	// No args
	case func() interface{}:
		return fn(), nil
	case func() string:
		return fn(), nil
	case func() int64:
		return fn(), nil
	case func() float64:
		return fn(), nil

	// Single arg functions
	case func(interface{}) interface{}:
		if len(args) >= 1 {
			return fn(args[0]), nil
		}
	case func([]interface{}) interface{}:
		if len(args) >= 1 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list), nil
			}
		}
	case func([]interface{}) float64:
		if len(args) >= 1 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list), nil
			}
		}
	case func(string) string:
		if len(args) >= 1 {
			if s, ok := args[0].(string); ok {
				return fn(s), nil
			}
		}
	case func(float64) float64:
		if len(args) >= 1 {
			if f, ok := toFloat64(args[0]); ok {
				return fn(f), nil
			}
		}
	case func([]float64) []float64:
		if len(args) >= 1 {
			if list, ok := toFloat64Slice(args[0]); ok {
				return fn(list), nil
			}
		}

	// Two arg functions
	case func(interface{}, interface{}) interface{}:
		if len(args) >= 2 {
			return fn(args[0], args[1]), nil
		}
	case func([]interface{}, interface{}) bool:
		if len(args) >= 2 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list, args[1]), nil
			}
		}
	case func([]interface{}, []interface{}) []interface{}:
		if len(args) >= 2 {
			list1, ok1 := args[0].([]interface{})
			list2, ok2 := args[1].([]interface{})
			if ok1 && ok2 {
				return fn(list1, list2), nil
			}
		}
	case func(string, string) string:
		if len(args) >= 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func(string, string) int:
		if len(args) >= 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func(string, string) float64:
		if len(args) >= 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func([]float64, []float64) float64:
		if len(args) >= 2 {
			list1, ok1 := toFloat64Slice(args[0])
			list2, ok2 := toFloat64Slice(args[1])
			if ok1 && ok2 {
				return fn(list1, list2), nil
			}
		}

	// Three arg functions
	case func(string, string, string) string:
		if len(args) >= 3 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			s3, ok3 := args[2].(string)
			if ok1 && ok2 && ok3 {
				return fn(s1, s2, s3), nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported function signature")
}

// NOTE: Logical, Comparison, and Arithmetic operators moved to operators.go

// evaluateMapLiteral parses and evaluates a map literal like { key: value, ... }
// where values can be expressions that reference variables in context.
func (e *StorageExecutor) evaluateMapLiteral(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) map[string]interface{} {
	return e.evaluateMapLiteralFull(ctx, expr, nodes, rels, nil, nil, nil, 0)
}

// evaluateMapLiteralFull parses and evaluates a map literal with full path context
func (e *StorageExecutor) evaluateMapLiteralFull(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) map[string]interface{} {
	result := make(map[string]interface{})

	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "{") || !strings.HasSuffix(expr, "}") {
		return result
	}

	inner := strings.TrimSpace(expr[1 : len(expr)-1])
	if inner == "" {
		return result
	}

	// Split by commas, respecting nesting and quoted values.
	pairs := splitTopLevelComma(inner)

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Find the first colon (key: value)
		colonIdx := strings.Index(pair, ":")
		if colonIdx == -1 {
			continue
		}

		// Keep map-literal key normalization aligned with property/map parsing:
		// supports backticked and quoted keys like `foo`, 'foo', "foo".
		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		valueExpr := strings.TrimSpace(pair[colonIdx+1:])

		// Evaluate the value expression in context with full path info
		value := e.evaluateExpressionWithContextFull(ctx, valueExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		result[key] = value
	}

	return result
}
