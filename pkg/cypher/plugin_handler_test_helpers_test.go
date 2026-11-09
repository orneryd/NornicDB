package cypher

// Author: Codex NOR-184 lane: migrate fixtures to admitted callable publication.

// Author: Codex NOR-184 lane. Raw functions enter tests through real admission.

import (
	"context"
	"github.com/orneryd/nornicdb/apoc/registry"
)

func mustCallableForTest(function interface{}) *registry.Callable {
	handler, err := registry.Compile(function)
	if err != nil {
		panic(err)
	}
	return handler
}

func checkedTestLookup(lookup func(string) (interface{}, bool)) func(string) (*PluginFunctionHandler, bool) {
	if lookup == nil {
		return nil
	}
	return func(name string) (*PluginFunctionHandler, bool) {
		function, found := lookup(name)
		if !found {
			return nil, false
		}
		if function == nil {
			// Some metadata-only fixtures deliberately report presence without a
			// handler; preserve that condition without admitting a nil function.
			return nil, true
		}
		return mustCallableForTest(function), true
	}
}

func callPluginHandler(function interface{}, args []interface{}) (interface{}, error) {
	return callTestPluginHandlerContext(context.Background(), function, args)
}

func callTestPluginHandlerContext(ctx context.Context, function interface{}, args []interface{}) (interface{}, error) {
	handler, err := registry.Compile(function)
	if err != nil {
		return nil, err
	}
	return callPluginHandlerContext(ctx, handler, args)
}
