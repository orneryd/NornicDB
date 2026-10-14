package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Integral defaults must arrive through the shared Integer reader as exact
// Long values; a stored Float remains a Float even when it has no fraction.
func TestGDSLouvainTypedSeedsAndDefaults(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stored       interface{}
		defaultValue string
		want         []int64
		wantError    bool
	}{
		{"integer_default", int64(2), "2", []int64{2, 2}, false},
		{"integral_double_default", int64(2), "2.0", []int64{2, 2}, false},
		{"all_integer_default", nil, "2", []int64{2, 2}, false},
		{"all_float_default", nil, "2.0", nil, true},
		{"exact_long_and_default", int64(9007199254740993), "2.0", []int64{9007199254740993, 2}, false},
		{"integral_float_seed", 2.0, "2.0", nil, true},
		{"fractional_float_seed", 2.5, "2.0", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graphProjections = make(map[string]*GraphProjection)
			t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
			properties := map[string]interface{}{}
			if tc.stored != nil {
				properties["seed"] = tc.stored
			}
			_, run := newGDSEngineGraph(t, "typedseed", gdsNode("0", "N", properties), gdsNode("1", "N", nil))
			_, err := run("MATCH (n:N) CREATE (n)-[:R]->(n)")
			require.NoError(t, err)
			_, err = run(fmt.Sprintf("CALL gds.graph.project('typedseed',{N:{properties:{seed:{defaultValue:%s}}}},'R')", tc.defaultValue))
			require.NoError(t, err)
			result, err := run("CALL gds.louvain.stream('typedseed',{seedProperty:'seed',concurrency:1})")
			if tc.wantError {
				require.ErrorContains(t, err, "Provided seeding property `seed` does not comprise exclusively of long values")
				return
			}
			require.NoError(t, err)
			var got []int64
			for _, row := range result.Rows {
				got = append(got, row[1].(int64))
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestGDSLouvainExplicitEmptyWeightProperty(t *testing.T) {
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	_, run := newGDSEngineGraph(t, "emptyweight", gdsNode("0", "N", nil))
	_, err := run("MATCH (n:N) CREATE (n)-[:R {w:1.0}]->(n)")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('emptyweight','N',{R:{properties:'w'}})")
	require.NoError(t, err)
	_, err = run("CALL gds.louvain.stream('emptyweight',{relationshipWeightProperty:''})")
	require.ErrorContains(t, err, "Relationship weight property `` not found")
}
