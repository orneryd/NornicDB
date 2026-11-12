package cypher

// Author: Codex NOR-184 lane: opaque checked results; context binding unchanged.

import (
	"context"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/internal/queryprofile"
)

var pluginContextType = reflect.TypeOf((*context.Context)(nil)).Elem()

// invokeTypedPlugin applies the query value domain and binds the caller's
// context. Argument conversion and execution belong to the checked callable.
//
// The opaque-callback report belongs to this adapter because it runs at the
// handler's invocation boundary. Whether the implementation ran comes from
// that boundary's phase receipt (registry.InvocationStarted), never from
// matching an error value: a declaration the query cannot express (no
// invocable signature, a lifecycle value it cannot emit) and a call the
// boundary rejects before entering the implementation (argument policy,
// argument count, argument binding) are never reported, so the built-in
// function that answers instead is not charged with an unobserved read. An
// implementation that ran is reported once, including when it returns another
// call's rejection or panics into the existing recovery. The report uses the
// policy the callable was admitted with - a canonical implementation never
// appears as an opaque callback, and a custom one does as soon as it actually
// runs.
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
	opaque := handler.ReadObservation() == registry.OpaqueReads
	returned := false
	defer func() {
		// A call that panicked at the boundary still entered the
		// implementation, so one report is made before the panic reaches the
		// existing recovery. The panic itself is neither recovered nor changed
		// here.
		if !returned && opaque {
			queryprofile.Callback(ctx, "native function callback", "")
		}
	}()
	value, err := handler.CallWithPolicy(args, prefix, registry.CypherArguments)
	returned = true
	if opaque && registry.InvocationStarted(err) {
		queryprofile.Callback(ctx, "native function callback", "")
	}
	return value, err
}

// Context-taking plugin calls need the row evaluator to retain the statement's
// context, just as clocks and subqueries do. This is checked while preparing
// an expression; ordinary handlers do not force a context-bearing row copy.
func containsContextPluginCall(ctx context.Context, expression string) bool {
	if PluginFunctionLookup == nil || !strings.Contains(expression, ".") || !strings.Contains(expression, "(") {
		return false
	}
	found := false
	_ = forEachFunctionCall(ctx, expression, func(name, _ string) error {
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
