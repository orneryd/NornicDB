package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"testing"
)

func TestUseLoneAllArgumentArityPublic(t *testing.T) {
	driver := fabricCallRowsReference(t)
	for _, norm := range []bool{true, false} {
		t.Run(fmt.Sprint("normalization_", norm), func(t *testing.T) {
			restore := func() {}
			if !norm {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, parser := range []string{"nornic", "antlr"} {
				t.Run(parser, func(t *testing.T) {
					old := config.GetParserType()
					config.SetParserType(parser)
					defer config.SetParserType(old)
					t.Run("unquoted", func(t *testing.T) {
						base := newFabricCallRowsExecutor(t)
						fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS all CALL { WITH all USE graph.byName(all) RETURN 7 AS k } RETURN k", nil, "", "Insufficient parameters")
						if driver != nil {
							fabricCompareGrammarCase(t, base, driver, "USE comp WITH 'comp.one' AS all CALL { WITH all USE graph.byName(all) RETURN 7 AS k } RETURN k", "WITH 'comp.one' AS all RETURN graph.byName(all)", "Insufficient parameters")
						}
					})
					t.Run("sibling_quoted_import", func(t *testing.T) {
						base := newFabricCallRowsExecutor(t)
						fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS all,1 AS `x x` CALL { WITH all,`x x` USE graph.byName(all) RETURN 7+`x x` AS k } RETURN k", nil, "", "Insufficient parameters")
						if driver != nil {
							fabricCompareGrammarCase(t, base, driver, "USE comp WITH 'comp.one' AS all,1 AS `x x` CALL { WITH all,`x x` USE graph.byName(all) RETURN 7+`x x` AS k } RETURN k", "WITH 'comp.one' AS all RETURN graph.byName(all)", "Insufficient parameters")
						}
					})
					t.Run("quoted", func(t *testing.T) {
						base := newFabricCallRowsExecutor(t)
						fabricAssertGrammarCase(t, base, "USE comp WITH 'comp.one' AS `all` CALL { WITH `all` USE graph.byName(`all`) RETURN 7 AS k } RETURN k", []string{"k"}, "[[7]]")
						if driver != nil {
							fabricCompareGrammarCase(t, base, driver, "USE comp WITH 'comp.one' AS `all` CALL { WITH `all` USE graph.byName(`all`) RETURN 7 AS k } RETURN k", "WITH 'comp.one' AS `all` CALL { WITH `all` RETURN 7 AS k } RETURN k")
						}
					})
				})
			}
		})
	}
}
