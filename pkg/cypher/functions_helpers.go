package cypher

// Author: Codex NOR-184 lane: checked callable consumption, existing context policy.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/apoc/registry"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// errUnsupportedPluginSignature is a plugin handler whose Go signature
// callPluginHandler can't call.
var errUnsupportedPluginSignature = registry.ErrUnsupportedSignature

// pluginFunctionHandler is the one plugin function lookup, for the
// unknown-function check and both evaluators: the loaded plugin's handler
// for a dotted function name, matched as written. Plugin function names are
// case-sensitive, as in Neo4j (apoc.text.camelCase, not apoc.text.camelcase;
// #1163).
func pluginFunctionHandler(name string) (*PluginFunctionHandler, bool) {
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
func callPluginFunction(ctx context.Context, name string, handler *PluginFunctionHandler, args []interface{}) (value interface{}, handled bool, err error) {
	if nativeAPOCForeignGraphHandler(name, handler) {
		return nil, false, nil
	}
	if bundledAPOCMapHandler(name, handler, len(args)) {
		if value, handled, err := apocMapNullResult(name, args); handled {
			return value, true, err
		}
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
	var callFacts1 functionCallFacts
	valid := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts1)
	funcName, argsStr := callFacts1.name, callFacts1.inner
	if !valid {
		return nil, false
	}
	handler, found := pluginFunctionHandler(funcName)
	if !found && !isNativeAPOCFunction(funcName) {
		return nil, false
	}
	args := e.splitFunctionArgs(argsStr)

	// Evaluate each argument
	var evalArgs []interface{}
	for _, arg := range args {
		value, err := e.checkedPropertyExpression(ctx, arg, e.entityRowWithPaths(ctx, nodes, rels, paths))
		if err != nil {
			return nil, true
		}
		evalArgs = append(evalArgs, value)
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

func callPluginHandlerContext(ctx context.Context, handler *PluginFunctionHandler, args []interface{}) (result interface{}, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("plugin handler panic: %v", recovered)
		}
		result = cypherfn.FunctionResult(result)
	}()

	if handler == nil {
		// Plugin dispatch consumes this registration invariant and falls back to
		// built-in function handling; it is never returned by query execution.
		return nil, fmt.Errorf("nil handler")
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
	value, err := e.materializePropertyMap(ctx, expr, e.entityRowWithPaths(ctx, nodes, rels, paths), true, true)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	return value
}
