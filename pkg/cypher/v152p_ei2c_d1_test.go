package cypher

// Batch 2c item 3 (research only, untracked): report entry 24 D.1. Which
// rewrite of execute (executor_entry.go:118-801) removes the empty map entry
// of (b {key: a.key,}) before validateSyntax (:801). Each rewrite execute
// applies before :801 is applied here in the same order, the text after it is
// printed, and the map-key check validateStaticMapKeys (run by validateSyntax,
// executor_query_routing.go:496) and validateSyntax itself are run on it. The
// statements are also executed with query normalization off and on.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestZZEI2cD1MapComma(t *testing.T) {
	for _, q := range []string{
		"UNWIND [] AS row MATCH (a)-->(b {key: a.key,}) RETURN b AS out",
		"CREATE (:EIA {key:1})-[:R]->(:EIB {key:1}) WITH 1 AS one MATCH (a:EIA)-->(b {key: a.key,}) RETURN b.key AS out",
		"UNWIND [] AS row RETURN {key: 1,} AS out",
	} {
		t.Run(q, func(t *testing.T) {
			ctx := context.Background()
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2cd1"))
			check := func(stage, text string) {
				t.Logf("ZZEI2c D1 %-48s text=%q | validateStaticMapKeys=%v | validateSyntax=%v", stage, text, validateStaticMapKeys(ctx, text), e.validateSyntax(ctx, text, false, syntaxStatement))
			}
			text := q
			check("as sent", text)
			if canonical, rewrite, err := normalizeQueryText(ctx, text); err != nil {
				t.Logf("ZZEI2c D1 normalizeQueryText error %v", err)
			} else if rewrite != nil {
				text = canonical
			}
			check("normalizeQueryText (:342, :349)", text)
			if grouped, rw := canonicalizeNumericLiterals(text); rw != nil {
				text = grouped
			}
			check("canonicalizeNumericLiterals (:352)", text)
			if aliased, rw := canonicalizeFunctionAliases(text); rw != nil {
				text = aliased
			}
			check("canonicalizeFunctionAliases (:360)", text)
			if composed, rw, err := desugarQueryStructure(ctx, text, e); err != nil {
				t.Logf("ZZEI2c D1 desugarQueryStructure error %v", err)
			} else if rw != nil {
				text = composed
			}
			check("desugarQueryStructure (:432)", text)
			if stripped, ok := stripUnionBranchFinishes(ctx, text); ok {
				text = stripped
			}
			check("stripUnionBranchFinishes (:450)", text)
			if processed, _, _, err := e.preprocessShellCommands(ctx, text, nil); err != nil {
				t.Logf("ZZEI2c D1 preprocessShellCommands error %v", err)
			} else {
				text = processed
			}
			check("preprocessShellCommands (:488)", text)
			if canonical, names := canonicalizeLocalQuotedVariables(ctx, text, false); names != nil {
				text = canonical
			}
			check("canonicalizeLocalQuotedVariables (:572)", text)
			if desugared, rw := desugarStandaloneOrderClauses(ctx, text); rw != nil {
				text = desugared
			}
			check("desugarStandaloneOrderClauses (:588)", text)
			if desugared, rw, err := desugarLabelExpressions(ctx, text, nil, false); err != nil {
				t.Logf("ZZEI2c D1 desugarLabelExpressions error %v", err)
			} else if rw != nil {
				text = desugared
			}
			check("desugarLabelExpressions (:716, :722)", text)
			for _, normalized := range []bool{false, true} {
				func() {
					previous := config.IsCypherQueryNormalizationEnabled()
					config.SetCypherQueryNormalizationEnabled(normalized)
					defer config.SetCypherQueryNormalizationEnabled(previous)
					fresh := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), fmt.Sprintf("zzei2cd1n%v", normalized)))
					result, err := fresh.Execute(ctx, q, nil)
					t.Logf("ZZEI2c D1 Execute normalization=%v -> rows=%v err=%v", normalized, zzei2cRows(result), err)
				}()
			}
			require.True(t, true)
		})
	}
}
