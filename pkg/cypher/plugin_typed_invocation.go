package cypher

// Author: Codex NOR-184 lane: opaque checked results; context binding unchanged.

import (
	"context"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/apoc/registry"
)

var pluginContextType = reflect.TypeOf((*context.Context)(nil)).Elem()

// invokeTypedPlugin applies the query value domain and binds the caller's
// context. Argument conversion and execution belong to the checked callable.
func invokeTypedPlugin(ctx context.Context, handler *PluginFunctionHandler, args []interface{}) (interface{}, error) {
	if handler.Signature() == nil {
		return nil, errUnsupportedPluginSignature
	}
	typ := handler.Signature()
	// Preserve the query adapter's existing value domain. The shared Go
	// callable also transports lifecycle capabilities that Cypher cannot emit.
	for i := 0; i < typ.NumOut(); i++ {
		if kind := typ.Out(i).Kind(); kind == reflect.Func || kind == reflect.Chan || kind == reflect.UnsafePointer {
			return nil, errUnsupportedPluginSignature
		}
	}
	offset := 0
	if typ.NumIn() > 0 && typ.In(0) == pluginContextType {
		offset = 1
	}
	var prefix []reflect.Value
	if offset == 1 {
		prefix = []reflect.Value{reflect.ValueOf(ctx)}
	}
	return handler.CallWithPolicy(args, prefix, registry.CypherArguments)
}

// Context-taking plugin calls need the row evaluator to retain the statement's
// context, just as clocks and subqueries do. This is checked while preparing
// an expression; ordinary handlers do not force a context-bearing row copy.
func containsContextPluginCall(expression string) bool {
	if PluginFunctionLookup == nil || !strings.Contains(expression, ".") || !strings.Contains(expression, "(") {
		return false
	}
	found := false
	_ = forEachFunctionCall(expression, func(name, _ string) error {
		if handler, ok := pluginFunctionHandler(name); ok && handler != nil {
			typ := handler.Signature()
			if typ != nil && typ.Kind() == reflect.Func && typ.NumIn() > 0 && typ.In(0) == pluginContextType {
				found = true
			}
		}
		return nil
	})
	return found
}
