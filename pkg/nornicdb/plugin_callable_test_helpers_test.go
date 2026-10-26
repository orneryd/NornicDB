package nornicdb

// Author: Codex NOR-184 lane. Fixtures use the real registration admission gate.

import "github.com/orneryd/nornicdb/apoc/registry"

func mustCallableForTest(function interface{}) *registry.Callable {
	handler, err := registry.Compile(function)
	if err != nil {
		panic(err)
	}
	return handler
}
