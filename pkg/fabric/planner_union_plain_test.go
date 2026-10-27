package fabric

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/stretchr/testify/require"
)

var unionPlainPlanSink Fragment
var unionPlainPartsSink []string

func TestPlan_UnionLexicalTokens(t *testing.T) {
	p := NewFabricPlanner(plannerUseCatalog())
	for _, body := range []string{
		"USE cmp.a RETURN 'plain' AS value /* UNION */",
		"USE cmp.a RETURN 'plain' AS value // UNION\n",
		"USE cmp.a RETURN 'UNION ALL' AS value",
		"USE cmp.a RETURN \"UNION\" AS value",
		"USE cmp.a RETURN 'plain' AS `UNION`",
	} {
		fragment, err := p.Plan(t.Context(), "USE cmp CALL { "+body+" } RETURN value", "cmp", cyphertext.RawSource)
		require.NoError(t, err, body)
		execs := collectPlannedExecs(fragment)
		require.Len(t, execs, 2, body)
		require.Equal(t, "cmp.a", execs[0].GraphName)
	}
}

func TestPlan_UnionPlainAllocations(t *testing.T) {
	p := NewFabricPlanner(plannerUseCatalog())
	// The narrow change must not exceed the composed parent budgets.
	// Check each non-UNION shape independently of result cardinality.
	ceilings := map[string]float64{"return": 14, "use": 19, "call": 34, "nested": 49}
	for _, shape := range unionPlainShapes() {
		t.Run(shape.name, func(t *testing.T) {
			var err error
			allocs := testing.AllocsPerRun(100, func() { unionPlainPlanSink, err = p.Plan(t.Context(), shape.query, "cmp", cyphertext.RawSource) })
			require.NoError(t, err)
			require.LessOrEqual(t, allocs, min(shape.baseAllocs, ceilings[shape.name]), "no-UNION planning must preserve the composed allocation ratchet")
			t.Logf("allocs=%g baseline=%g", allocs, shape.baseAllocs)
		})
	}
}

func TestSplitTopLevelUnion_NoUnionAllocations(t *testing.T) {
	for _, query := range []string{"RETURN 'plain' AS value", "RETURN 'UNION' AS value /* UNION */", "RETURN 'plain' AS value // UNION\n"} {
		allocs := testing.AllocsPerRun(100, func() {
			unionPlainPartsSink, _, _, _ = splitTopLevelUnion(query, nil)
		})
		require.Zero(t, allocs, query)
	}
}

func unionPlainShapes() []struct {
	name       string
	query      string
	baseAllocs float64
} {
	return []struct {
		name       string
		query      string
		baseAllocs float64
	}{
		{"return", "RETURN 'plain' AS value", 14},
		{"use", "USE cmp.a RETURN 'plain' AS value", 19},
		{"call", "USE cmp CALL { USE cmp.a RETURN 'plain' AS value } RETURN value", 34},
		{"nested", "USE cmp CALL { CALL { USE cmp.a RETURN 'plain' AS value } RETURN value } RETURN value", 49},
	}
}

func BenchmarkFabricUnionPlainPlanning(b *testing.B) {
	p := NewFabricPlanner(plannerUseCatalog())
	for _, shape := range unionPlainShapes() {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fragment, err := p.Plan(b.Context(), shape.query, "cmp", cyphertext.RawSource)
				if err != nil {
					b.Fatal(err)
				}
				unionPlainPlanSink = fragment
			}
		})
	}
}
