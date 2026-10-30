//go:build ignore

package main

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc"
)

func TestAPOCPluginSeparatesProceduresFromFunctions(t *testing.T) {
	functions := Plugin.Functions()
	for _, name := range []string{"apoc.cypher.doIt", "apoc.path.expandConfig", "apoc.load.jdbc", "apoc.neighbors.toHop"} {
		if _, exists := functions[name]; exists {
			t.Errorf("procedure %s must not be exported as scalar function", name)
		}
	}
	for _, name := range []string{"apoc.math.sigmoid", "apoc.text.camelCase", "apoc.coll.sumLongs"} {
		if _, exists := functions[name]; !exists {
			t.Errorf("genuine function %s must remain exported", name)
		}
	}
}

// The plugin doesn't export a function the query engine has built in: a
// loaded plugin function would override the built-in (apoc.map.removeKeys's
// handler turns a null map into {}, where the built-in fails the call).
func TestAPOCPluginLeavesBuiltInFunctionsToTheEngine(t *testing.T) {
	functions := Plugin.Functions()
	for name := range functions {
		if apoc.IsBuiltInFunction(name) {
			t.Errorf("built-in function %s must not be exported", name)
		}
	}
	for _, name := range []string{"apoc.map.removeKeys", "apoc.map.removeKey", "apoc.coll.sum", "apoc.node.id", "apoc.util.md5", "apoc.util.sha1", "apoc.util.sha256"} {
		if _, exists := functions[name]; exists {
			t.Errorf("built-in function %s must not be exported", name)
		}
	}
}
