package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// degrees(), radians() and point.distance return Neo4j 5.26.30's values
// (#907): one conversion constant, each coordinate converted before it is
// differenced, and a 3D distance measured at the points' average height.
// Neo4j computes in Java; Go's math functions agree to every published digit
// but differ in the low bits of trigonometric results (~1e-13 relative), so
// the values are compared with a 1e-12 relative tolerance — tight enough
// that a wrong formula or constant fails, loose enough for the documented
// cross-runtime difference.
func TestAngleAndDistanceMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "angle_distance"))
	for query, want := range map[string]float64{
		"RETURN radians(7.280885568782862) AS v":                                              0.12707542563620103,
		"RETURN degrees(1.0250403389434113) AS v":                                             58.730485252116864,
		"UNWIND [1.0250403389434113] AS x WITH x WHERE degrees(x) > 0 RETURN degrees(x) AS v": 58.730485252116864,
		"UNWIND [7.280885568782862] AS x WITH x WHERE radians(x) > 0 RETURN radians(x) AS v":  0.12707542563620103,
		"RETURN point.distance(point({latitude: 54.18730599173656, longitude: -2.4584680978005053}), point({latitude: 54.19263376956409, longitude: -2.466476627183425})) AS v":                                                  789.8341097568797,
		"RETURN point.distance(point({latitude: 3.32938205299007, longitude: 120.3808077564546, height: 2046.593427342866}), point({latitude: 70.22465845821, longitude: 123.85477823540043, height: 1464.4138125974994})) AS v": 7453105.704511136,
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		got, ok := result.Rows[0][0].(float64)
		require.True(t, ok, query)
		require.InEpsilonf(t, want, got, 1e-12, query)
	}
	// In a SET value and a WHERE, in that order.
	for _, query := range []string{
		"CREATE (n:AngleProbe) SET n.d = degrees(1.0250403389434113) RETURN n.d AS v",
		"MATCH (n:AngleProbe) WHERE degrees(1.0250403389434113) > 1 RETURN n.d AS v",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		got, ok := result.Rows[0][0].(float64)
		require.True(t, ok, query)
		require.InEpsilonf(t, 58.730485252116864, got, 1e-12, query)
	}
}
