//go:build ignore

package main

import "testing"

func TestAPOCPluginSeparatesProceduresFromFunctions(t *testing.T) {
	functions := Plugin.Functions()
	for _, name := range []string{"apoc.cypher.doIt", "apoc.path.expandConfig", "apoc.load.jdbc", "apoc.neighbors.toHop"} {
		if _, exists := functions[name]; exists {
			t.Errorf("procedure %s must not be exported as scalar function", name)
		}
	}
	for _, name := range []string{"apoc.math.sigmoid", "apoc.text.camelCase", "apoc.coll.sum"} {
		if _, exists := functions[name]; !exists {
			t.Errorf("genuine function %s must remain exported", name)
		}
	}
}
