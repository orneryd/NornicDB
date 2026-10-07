package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestAggregateValueTypes pins Neo4j's aggregate value rules: sum and avg take
// numbers or durations but never both, stDev, stDevP and the percentiles take
// numbers only, and any other value is a TypeError. Values come from UNWIND
// and from stored properties, which aggregate on different paths.
func TestAggregateValueTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:AggMixed {v: 1}), (:AggMixed {v: 'a'}), (:AggDur {v: duration('P1D')}), (:AggDur {v: duration('P2D')}), (:AggBoth {v: 1}), (:AggBoth {v: duration('P1D')})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"UNWIND [1, 'a'] AS x RETURN sum(x) AS v",
		"UNWIND [1, 'a'] AS x RETURN avg(x) AS v",
		"UNWIND [null, 'a'] AS x RETURN sum(x) AS v",
		"UNWIND [1, duration('P1D')] AS x RETURN sum(x) AS v",
		"UNWIND [duration('P1D'), 1] AS x RETURN avg(x) AS v",
		"UNWIND [1, duration('P1D')] AS x RETURN stDev(x) AS v",
		"UNWIND [1, 'a'] AS x RETURN stDevP(x) AS v",
		"UNWIND [1, 'a'] AS x RETURN percentileCont(x, 0.5) AS v",
		"UNWIND [1, 'a'] AS x RETURN percentileDisc(x, 0.5) AS v",
		"MATCH (n:AggMixed) RETURN sum(n.v) AS v",
		"MATCH (n:AggMixed) RETURN avg(n.v) AS v",
		"MATCH (n:AggMixed) RETURN stDev(n.v) AS v",
		"MATCH (n:AggMixed) RETURN percentileCont(n.v, 0.5) AS v",
		"MATCH (n:AggBoth) RETURN sum(n.v) AS v",
		"MATCH (n:AggDur) RETURN stDevP(n.v) AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, statusText(err), "Neo.ClientError.Statement.TypeError", query)
	}
	for query, want := range map[string]string{
		"UNWIND [duration('P1D'), duration('P2D')] AS x RETURN sum(x) AS v":                                  "P3D",
		"UNWIND [duration('P1D'), duration('P2D')] AS x RETURN avg(x) AS v":                                  "P1DT12H",
		"UNWIND [duration('P1D'), duration('P1D'), duration('P2D')] AS x RETURN sum(DISTINCT x) AS v":        "P3D",
		"UNWIND [duration('P1M'), duration('PT10H28M7S'), null] AS x RETURN avg(x) AS v":                     "P15DT10H28M36.5S",
		"UNWIND [duration('PT1S'), duration('PT2S'), duration('PT4S')] AS x RETURN avg(x) AS v":              "PT2.333333333S",
		"MATCH (n:AggDur) RETURN sum(n.v) AS v":                                                              "P3D",
		"MATCH (n:AggDur) RETURN avg(n.v) AS v":                                                              "P1DT12H",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		duration, ok := asCypherDuration(result.Rows[0][0])
		require.True(t, ok, "%s: %#v", query, result.Rows[0][0])
		require.Equal(t, want, duration.String(), query)
	}
	result, err := exec.Execute(ctx, "UNWIND [1, 2, null] AS x RETURN sum(x) AS s, avg(x) AS a", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), 1.5}}, result.Rows)
}

// TestFunctionNullAndPointValues pins Neo4j's values of isEmpty(null),
// point.withinBBox with an argument that isn't a point, and rtrim with
// characters, which never removes a first one-byte character.
func TestFunctionNullAndPointValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN isEmpty(null) AS v": nil,
		"RETURN point.withinBBox(point({x: 1, y: 1}), 'x', point({x: 2, y: 2})) AS v":                                      nil,
		"RETURN point.withinBBox(null, point({x: 0, y: 0}), point({x: 2, y: 2})) AS v":                                     nil,
		"RETURN point.withinBBox({x: 1, y: 1}, point({x: 0, y: 0}), point({x: 2, y: 2})) AS v":                             nil,
		"RETURN point.withinBBox(point({x: 1, y: 1}), point({longitude: 0, latitude: 0}), point({x: 2, y: 2})) AS v":       nil,
		"RETURN point.withinBBox(point({x: 1, y: 1}), point({x: 0, y: 0}), point({x: 2, y: 2})) AS v":                      true,
		"RETURN point.withinBBox(point({x: 3, y: 1}), point({x: 0, y: 0}), point({x: 2, y: 2})) AS v":                      false,
		"RETURN [rtrim('xx', 'x'), rtrim('ab', 'ab'), rtrim('', 'x'), rtrim('éé', 'é'), rtrim('xax', 'x'), ltrim('xx', 'x')] AS v": []interface{}{"x", "a", "", "", "xa", ""},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}
