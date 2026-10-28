package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"testing"
)

func TestUseGraphByNameAllModifierContract(t *testing.T) {
	driver := fabricCallRowsReference(t)
	for _, normalization := range []bool{true, false} {
		t.Run(map[bool]string{true: "normalized", false: "original"}[normalization], func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			base := newFabricCallRowsExecutor(t)
			fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS all CALL { WITH all USE graph.byName(all) RETURN 7 AS k } RETURN k", nil, "", "Insufficient parameters")
			if driver != nil {
				fabricCompareGrammarCase(t, base, driver, "USE comp WITH 'comp.one' AS all CALL { WITH all USE graph.byName(all) RETURN 7 AS k } RETURN k", "WITH 'comp.one' AS all RETURN graph.byName(all)", "Insufficient parameters")
			}
			fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS `all` CALL { WITH `all` USE graph.byName(`all`) RETURN 7 AS k } RETURN k", []string{"k"}, "[[7]]")
			if driver != nil {
				fabricCompareGrammarCase(t, base, driver, "USE comp WITH 'comp.one' AS `all` CALL { WITH `all` USE graph.byName(`all`) RETURN 7 AS k } RETURN k", "WITH 'comp.one' AS `all` CALL { WITH `all` RETURN 7 AS k } RETURN k")
			}
		})
	}
}
