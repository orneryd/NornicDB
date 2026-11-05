package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"testing"
)

func TestFabricQueryStructureProjectionSourceMetadata(t *testing.T) {
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("projection source metadata oracle requires --ref neo-ref")
	}
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, word := range []string{"union", "use", "return", "with", "create", "call", "all", "offset"} {
				for _, quoted := range []bool{false, true} {
					name := word
					if quoted {
						name = "`" + name + "`"
					}
					for _, expression := range []string{"coalesce(" + name + ",0)", name + " + 1"} {
						t.Run(expression, func(t *testing.T) {
							body := "WITH 7 AS " + name + " RETURN " + expression
							fabricCompareGrammarCase(t, base, driver, "USE comp.one "+body, body)
						})
					}
				}
			}
		})
	}
}
