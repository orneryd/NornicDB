package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// GDS 2.13.12 retains the exact Long array during import and permits FastRP's
// Double-array read to round its elements. The reference returns the same
// embedding for the large Long as for the boundary-valued Long and Double
// controls below, so each case must match the boundary Long embedding.
func TestGDSStoredArrayFastRPReference(t *testing.T) {
	embeddings := make(map[string]map[string][]float64)
	for _, tc := range []struct {
		name          string
		first, second interface{}
	}{
		{"boundary Long", []int64{9007199254740992}, []int64{9007199254740992}},
		{"large Long", []int64{9007199254740993}, []int64{9007199254740992}},
		{"Double", []float64{9007199254740992}, []float64{9007199254740992}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := reviewGraph(t, []map[string]interface{}{{"v": tc.first}, {"v": tc.second}}, []reviewEdge{{"0", "1", "R", nil}}, "{N:{properties:'v'}}", "'R'")
			projection, err := e.graphProjection("rev")
			require.NoError(t, err)
			require.Equal(t, tc.first, projection.NodeProperties["0"]["v"])
			for _, explicit := range []bool{false, true} {
				if explicit {
					_, err = e.Execute(context.Background(), "BEGIN", nil)
					require.NoError(t, err)
				}
				result, runErr := e.Execute(context.Background(), "CALL gds.fastRP.stream('rev',{embeddingDimension:4,randomSeed:42,iterationWeights:[1.0],propertyRatio:1.0,featureProperties:['v'],concurrency:1})", nil)
				if explicit {
					_, err = e.Execute(context.Background(), "ROLLBACK", nil)
					require.NoError(t, err)
				}
				require.NoError(t, runErr)
				require.Len(t, result.Rows, 2)
				got := make(map[string][]float64)
				for _, row := range result.Rows {
					id, ok := row[0].(string)
					require.True(t, ok)
					vector, ok := row[1].([]float64)
					require.True(t, ok)
					got[id] = vector
				}
				// Only node 0 has a relationship to propagate along.
				require.NotEqual(t, []float64{0, 0, 0, 0}, got["0"])
				require.Equal(t, []float64{0, 0, 0, 0}, got["1"])
				if want, seen := embeddings["boundary Long"]; seen {
					require.Equal(t, want, got)
				}
				embeddings[tc.name] = got
			}
			// Reads must not replace the exact imported array with rounded values.
			require.Equal(t, tc.first, projection.NodeProperties["0"]["v"])
		})
	}
	require.Len(t, embeddings, 3)
}
