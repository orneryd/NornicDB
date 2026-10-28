package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"testing"
)

func TestDistinctSelectorDiagnosticContexts(t *testing.T) {
	driver := fabricCallRowsReference(t)
	for _, norm := range []bool{true, false} {
		t.Run(map[bool]string{true: "normalized", false: "original"}[norm], func(t *testing.T) {
			restore := func() {}
			if !norm {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			base := newFabricCallRowsExecutor(t)
			query := "USE comp WITH 'comp.one' AS distinct CALL { WITH distinct USE graph.byName(distinct) RETURN 7 AS k } RETURN k"
			fabricAssertGrammarCase(t, base, query, nil, "", "Insufficient parameters")
			if driver != nil {
				fabricCompareGrammarCase(t, base, driver, query, "WITH 'comp.one' AS distinct RETURN graph.byName(distinct)", "Insufficient parameters", "Invalid use of DISTINCT with function 'byName'")
			}
			fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS `distinct` CALL { WITH `distinct` USE graph.byName(`distinct`) RETURN 7 AS k } RETURN k", []string{"k"}, "[[7]]")
			fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS all CALL { WITH all USE graph.byName(DISTINCT all) RETURN 7 AS k } RETURN k", []string{"k"}, "[[7]]")
		})
	}
}
