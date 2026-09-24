package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// valueBindingsKey carries the non-entity variables (maps, lists, scalars) in
// scope for one expression evaluation: UNWIND / WITH row values in SET,
// comprehension and reduce variables, YIELD values and aggregate placeholders.
//
// Nodes and relationships stay in the evaluator's node / relationship maps;
// every other value lives here with its real Go value, so keys(), properties(),
// labels(), property access and type checks see the value itself. (Such values
// used to be passed as stand-in nodes {"value": v}, which functions over the
// variable then read as a node, and which could not be told apart from a real
// node whose only property is named "value".)
type valueBindingsKey struct{}

// withValueBindings returns ctx with bindings as the value scope. bindings
// replaces any outer scope, so callers that nest pass a map that already holds
// the outer bindings (see valueBindingsLayer).
func withValueBindings(ctx context.Context, bindings map[string]interface{}) context.Context {
	return context.WithValue(ctx, valueBindingsKey{}, bindings)
}

// valueBindingsFromContext returns the value scope of ctx, or nil.
func valueBindingsFromContext(ctx context.Context) map[string]interface{} {
	if ctx == nil {
		return nil
	}
	bindings, _ := ctx.Value(valueBindingsKey{}).(map[string]interface{})
	return bindings
}

// valueBindingsLayer returns a new value scope holding the outer scope of ctx
// plus room for extra names, for a nested scope (a comprehension inside SET)
// whose own variables shadow outer ones.
func valueBindingsLayer(ctx context.Context, extra int) map[string]interface{} {
	outer := valueBindingsFromContext(ctx)
	layer := make(map[string]interface{}, len(outer)+extra)
	for name, value := range outer {
		layer[name] = value
	}
	return layer
}

// boundValue resolves a non-entity variable: the value scope of ctx first,
// then the Fabric record bindings of a correlated executor.
func (e *StorageExecutor) boundValue(ctx context.Context, name string) (interface{}, bool) {
	if bindings := valueBindingsFromContext(ctx); bindings != nil {
		if value, ok := bindings[name]; ok {
			return value, true
		}
	}
	if value, ok := e.fabricRecordBindings[name]; ok {
		return value, true
	}
	return nil, false
}

// bindEvaluationValue binds name to value for one evaluation: nodes and
// relationships go to nodes / rels, anything else to values. The name is
// removed from the other scopes so the binding shadows an outer variable of
// the same name.
func bindEvaluationValue(name string, value interface{}, nodes map[string]*storage.Node, rels map[string]*storage.Edge, values map[string]interface{}) {
	delete(nodes, name)
	delete(rels, name)
	delete(values, name)
	switch entity := value.(type) {
	case *storage.Node:
		if entity != nil {
			nodes[name] = entity
			return
		}
		value = nil
	case *storage.Edge:
		if entity != nil {
			rels[name] = entity
			return
		}
		value = nil
	}
	values[name] = value
}
