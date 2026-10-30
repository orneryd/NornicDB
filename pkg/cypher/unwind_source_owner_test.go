package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestUnwindOriginalSourceViews reads UNWIND's list and variable from the
// clause's retained occurrence (parsePipelineIteration). The rewrite-stage
// predicate keeps its zero-allocation lexical form (startsUnwindBinding).
// Rewritten for the retained reader: Claude R17 implementation agent.
func TestUnwindOriginalSourceViews(t *testing.T) {
	for _, tc := range []struct{ q, expr, variable string }{
		{"UNWIND as AS x RETURN x", "as", "x"},
		{"UNWIND as AS as RETURN as", "as", "as"},
		{"UNWIND n.as AS x", "n.as", "x"},
		{"UNWIND as + [1] AS x", "as + [1]", "x"},
		{"UNWIND [{AS:1},['AS']] AS x", "[{AS:1},['AS']]", "x"},
		{"UNWIND fn(as) AS x", "fn(as)", "x"},
		{"UNWIND [7] AS `a b` RETURN `a b`", "[7]", "`a b`"},
		{"UNWIND [7] AS `a``b` RETURN `a``b`", "[7]", "`a``b`"},
		{"UNWIND/*list*/ as AS/*alias*/ x RETURN x", "as", "x"},
	} {
		t.Run(tc.q, func(t *testing.T) {
			expression, variable, ok := parsePipelineIteration(retainedClauseForTest(t.Context(), tc.q))
			require.True(t, ok)
			require.Equal(t, tc.expr, expression)
			require.Equal(t, tc.variable, variable)
		})
	}
	for _, q := range []string{"UNWIND as AS x RETURN x", "UNWIND [7] AS x RETURN x", "UNWIND [7] AS `a``b` RETURN 1", "UNWIND [7] AS `日本` RETURN 1", "UNWIND CASE WHEN true THEN as ELSE [] END AS x RETURN x"} {
		var ok bool
		require.Zero(t, testing.AllocsPerRun(100, func() { ok = startsUnwindBinding(q) }))
		require.True(t, ok)
	}
}
func TestUnwindASBindingPublicForms(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		for _, normalized := range []bool{true, false} {
			t.Run(parser+map[bool]string{true: "/normalized", false: "/raw"}[normalized], func(t *testing.T) {
				old := config.GetParserType()
				config.SetParserType(parser)
				defer config.SetParserType(old)
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
				config.SetCypherQueryNormalizationEnabled(normalized)
				for _, q := range []string{
					"WITH [7] AS as UNWIND as AS x RETURN x AS k",
					"WITH [7] AS x UNWIND x AS x RETURN x AS k", // Existing permissive UNWIND rebinding also applies to ordinary names.
					"WITH [7] AS case UNWIND case AS x RETURN x AS k",
					"WITH [7] AS when UNWIND when AS x RETURN x AS k",
					"WITH [7] AS then UNWIND then AS x RETURN x AS k",
					"WITH [7] AS else UNWIND else AS x RETURN x AS k",
					"UNWIND [7] AS as RETURN as AS k",
					"WITH [7] AS as UNWIND as AS as RETURN as AS k",
					"WITH {as:[7]} AS n UNWIND n.as AS x RETURN x AS k",
					"WITH [7] AS as UNWIND as + [] AS x RETURN x AS k",
					"UNWIND/*list*/ [7] AS/*alias*/ x RETURN x AS k",
					"UNWIND [7] AS x/*tail*/ RETURN x AS k",
					"WITH [7] AS as UNWIND CASE WHEN true THEN as ELSE [] END AS x RETURN x AS k",
					"WITH 7 AS as UNWIND CASE as WHEN 7 THEN [7] ELSE [] END AS x RETURN x AS k",
					"WITH 7 AS as UNWIND CASE WHEN as=7 THEN [7] ELSE [] END AS x RETURN x AS k",
					"WITH [7] AS as UNWIND CASE WHEN false THEN [] ELSE as END AS x RETURN x AS k",
				} {
					t.Run(q, func(t *testing.T) {
						e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unwind_roles"))
						r, err := e.Execute(t.Context(), q, nil)
						require.NoError(t, err)
						require.Equal(t, []string{"k"}, r.Columns)
						require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
					})
				}
				for _, q := range []string{"UNWIND [7] AS x WHERE true RETURN x", "UNWIND [7] AS x extra RETURN x"} {
					t.Run(q, func(t *testing.T) {
						e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unwind_roles"))
						_, err := e.Execute(t.Context(), q, nil)
						requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
					})
				}
			})
		}
	}
}

func TestUnwindImportedASFromFabricRecords(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		for _, normalized := range []bool{true, false} {
			t.Run(parser+map[bool]string{true: "/normalized", false: "/raw"}[normalized], func(t *testing.T) {
				old := config.GetParserType()
				config.SetParserType(parser)
				defer config.SetParserType(old)
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
				config.SetCypherQueryNormalizationEnabled(normalized)
				for _, q := range []string{"USE comp WITH [7] AS as CALL (as) { USE comp.one UNWIND as AS x RETURN x AS k } RETURN k", "USE comp WITH [7] AS as CALL { WITH as USE comp.one UNWIND/*list*/ as AS/*alias*/ x RETURN x AS k } RETURN k"} {
					t.Run(q, func(t *testing.T) {
						e := newFabricCallRowsExecutor(t)
						fabricRunModes(t, e, func(t *testing.T, run *StorageExecutor) {
							r, err := run.Execute(t.Context(), q, nil)
							require.NoError(t, err)
							require.Equal(t, []string{"k"}, r.Columns)
							require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
						})
					})
				}
			})
		}
	}
}

func TestCaseWordSourceView(t *testing.T) {
	for _, word := range []string{"CASE", "When", "unknown", "日本", "İ", "a\xff"} {
		source := word + " tail"
		want := word
		require.Equal(t, want, caseWordAt(source, 0))
		require.Zero(t, testing.AllocsPerRun(100, func() { caseWordAt(source, 0) }))
	}
}
