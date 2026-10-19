package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// errUnsupportedPluginSignature is a plugin handler whose Go signature
// callPluginHandler can't call.
var errUnsupportedPluginSignature = errors.New("unsupported function signature")

// pluginFunctionHandler is the one plugin function lookup, for the
// unknown-function check and both evaluators: the loaded plugin's handler
// for a dotted function name, matched as written. Plugin function names are
// case-sensitive, as in Neo4j (apoc.text.camelCase, not apoc.text.camelcase;
// #1163).
func pluginFunctionHandler(name string) (interface{}, bool) {
	if PluginFunctionLookup == nil || !strings.Contains(name, ".") {
		return nil, false
	}
	// Call readers have already decoded the identifier components. Reparsing
	// this key would interpret literal spaces/comment characters as syntax.
	return PluginFunctionLookup(name)
}

// callPluginFunction calls a plugin function's handler, for both evaluators,
// with the statement error a failed call is. A plugin function overrides a
// built-in of the same name; when its handler can't take a call, the
// built-in answers instead: handled is false (#1163).
func callPluginFunction(ctx context.Context, name string, handler interface{}, args []interface{}) (value interface{}, handled bool, err error) {
	if nativeAPOCForeignGraphHandler(name, handler) {
		return nil, false, nil
	}
	result, err := callPluginHandlerContext(ctx, handler, args)
	if errors.Is(err, errUnsupportedPluginSignature) {
		if _, builtIn := builtInCypherFunctions[lowerASCII(normalizeProjectionColumnName(strings.TrimSpace(name)))]; builtIn {
			return nil, false, nil
		}
	}
	if err != nil {
		return nil, true, functionEvaluationError(err)
	}
	return result, true, nil
}

// tryCallPluginFunction calls a plugin function from the main evaluator.
// Returns the result and true if a plugin handled the call, or nil and false
// if no plugin has the function (or a built-in answers it instead). A call
// the plugin fails is the statement's error, as in the row evaluator.
func (e *StorageExecutor) tryCallPluginFunction(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (interface{}, bool) {
	funcName, args, valid := parseFunctionCallArguments(expr)
	if !valid {
		return nil, false
	}
	handler, found := pluginFunctionHandler(funcName)
	if !found && !isNativeAPOCFunction(funcName) {
		return nil, false
	}


	// Evaluate each argument
	var evalArgs []interface{}
	for _, arg := range args {
		evalArgs = append(evalArgs, e.evaluateExpressionWithContextFull(ctx, arg, nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
		if getExpressionFailure(ctx) != nil {
			return nil, true
		}
	}

	var result interface{}
	var handled bool
	var err error
	if found {
		result, handled, err = callPluginFunction(ctx, funcName, handler, evalArgs)
	}
	if !handled && err == nil {
		result, handled, err = e.evaluateNativeAPOCFunction(ctx, funcName, evalArgs)
	}
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, true
	}
	return result, handled
}

// callPluginHandler invokes a plugin function handler with the given arguments.
func callPluginHandler(handler interface{}, args []interface{}) (interface{}, error) {
	return callPluginHandlerContext(context.Background(), handler, args)
}

func callPluginHandlerContext(ctx context.Context, handler interface{}, args []interface{}) (result interface{}, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("plugin handler panic: %v", recovered)
		}
	}()

	if handler == nil {
		// Plugin dispatch consumes this registration invariant and falls back to
		// built-in function handling; it is never returned by query execution.
		return nil, fmt.Errorf("nil handler")
	}

	// Type-based dispatch for common function signatures
	switch fn := handler.(type) {
	// No args
	case func() interface{}:
		if len(args) == 0 {
			return fn(), nil
		}
	case func() string:
		if len(args) == 0 {
			return fn(), nil
		}
	case func() int64:
		if len(args) == 0 {
			return fn(), nil
		}
	case func() float64:
		if len(args) == 0 {
			return fn(), nil
		}

	// Single arg functions
	case func(interface{}) interface{}:
		if len(args) == 1 {
			return fn(args[0]), nil
		}
	case func([]interface{}) interface{}:
		if len(args) == 1 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list), nil
			}
		}
	case func([]interface{}) float64:
		if len(args) == 1 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list), nil
			}
		}
	case func(string) string:
		if len(args) == 1 {
			if s, ok := args[0].(string); ok {
				return fn(s), nil
			}
		}
	case func(float64) float64:
		if len(args) == 1 {
			if f, ok := toFloat64(args[0]); ok {
				return fn(f), nil
			}
		}
	case func([]float64) []float64:
		if len(args) == 1 {
			if list, ok := toFloat64Slice(args[0]); ok {
				return fn(list), nil
			}
		}

	// Two arg functions
	case func(interface{}, interface{}) interface{}:
		if len(args) == 2 {
			return fn(args[0], args[1]), nil
		}
	case func([]interface{}, interface{}) bool:
		if len(args) == 2 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list, args[1]), nil
			}
		}
	case func([]interface{}, []interface{}) []interface{}:
		if len(args) == 2 {
			list1, ok1 := args[0].([]interface{})
			list2, ok2 := args[1].([]interface{})
			if ok1 && ok2 {
				return fn(list1, list2), nil
			}
		}
	case func(string, string) string:
		if len(args) == 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func(string, string) int:
		if len(args) == 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func(string, string) float64:
		if len(args) == 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func([]float64, []float64) float64:
		if len(args) == 2 {
			list1, ok1 := toFloat64Slice(args[0])
			list2, ok2 := toFloat64Slice(args[1])
			if ok1 && ok2 {
				return fn(list1, list2), nil
			}
		}

	// Three arg functions
	case func(string, string, string) string:
		if len(args) == 3 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			s3, ok3 := args[2].(string)
			if ok1 && ok2 && ok3 {
				return fn(s1, s2, s3), nil
			}
		}
	}

	return invokeTypedPlugin(ctx, handler, args)
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
	keys := make([]string, 0)

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

		colonIdx := findTopLevelMapKeyValueSeparator(pair)
		if colonIdx == -1 {
			continue
		}

		// Keep map-literal key normalization aligned with property/map parsing:
		// supports backticked and quoted keys like `foo`, 'foo', "foo".
		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		valueExpr := strings.TrimSpace(pair[colonIdx+1:])

		// Evaluate the value expression in context with full path info
		value := e.evaluateExpressionWithContextFull(ctx, valueExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if _, exists := result[key]; !exists {
			keys = append(keys, key)
		}
		result[key] = value
	}
	if failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure); ok {
		failure.recordMapKeyOrder(result, keys)
	}

	return result
}
