package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPointValueHelpers(t *testing.T) {
	point3D := CypherPoint{SRID: 9157, X: 1, Y: 2, Z: 3}
	require.Equal(t, []float64{1, 2, 3}, point3D.Coordinates())
	require.Equal(t, "point({srid:9157, x:1.0, y:2.0, z:3.0})", point3D.String())
	require.Equal(t, "https://spatialreference.org/ref/sr-org/9157/ogcwkt/", point3D.CRSHref())
	require.Equal(t, "point", point3D.PropertyValueKind())

	pointer := &CypherPoint{SRID: 7203, X: 1, Y: 2}
	got, ok := pointValue(pointer)
	require.True(t, ok)
	require.Equal(t, *pointer, got)
	_, ok = pointValue((*CypherPoint)(nil))
	require.False(t, ok)
	require.Equal(t, "point({x: 1.0, y: 2.0, crs: 'cartesian'})", formatCypherValueString(pointer))

	require.Equal(t, `String("a")`, neo4jValueDescription("a"))
	require.Equal(t, "Boolean(true)", neo4jValueDescription(true))
	require.Equal(t, "Long(1)", neo4jValueDescription(int64(1)))
	require.Equal(t, "Double(1.5)", neo4jValueDescription(1.5))
	require.Equal(t, "[1]", neo4jValueDescription([]interface{}{1}))

	require.Equal(t, 170.0, wrapLongitude(-190))
	require.Equal(t, -160.0, wrapLongitude(200))

	_, ok = NewCypherPoint(1234, 1, 2)
	require.False(t, ok)
	_, ok = NewCypherPoint(7203, 1, 2, 3)
	require.False(t, ok)
	created, ok := NewCypherPoint(4979, 1, 2, 3)
	require.True(t, ok)
	require.Equal(t, CypherPoint{SRID: 4979, X: 1, Y: 2, Z: 3}, created)

	for _, kind := range []string{CypherDate{}.PropertyValueKind(), CypherLocalTime{}.PropertyValueKind(), CypherTime{}.PropertyValueKind(), CypherLocalDateTime{}.PropertyValueKind(), CypherDateTime{}.PropertyValueKind(), (&CypherDuration{}).PropertyValueKind()} {
		require.NotEmpty(t, kind)
	}
}

func TestPointConstructionErrors(t *testing.T) {
	for _, fields := range []map[string]interface{}{
		{"x": 1, "y": 2, "z": "a"},
		{"latitude": 1, "longitude": "a"},
		{"latitude": 1, "longitude": 2, "height": "a"},
		{"latitude": 1, "longitude": 2, "z": "a"},
	} {
		_, _, err := newPointFromMap(fields)
		require.Error(t, err, fields)
	}
	point, ok, err := newPointFromMap(map[string]interface{}{"x": 1, "y": 2, "z": nil})
	require.NoError(t, err)
	require.False(t, ok, "a null z is a null point: %v", point)
	_, ok, err = newPointFromMap(map[string]interface{}{"latitude": 1, "longitude": 2, "height": nil})
	require.NoError(t, err)
	require.False(t, ok)
	_, _, err = newPointFromMap(map[string]interface{}{"x": 1, "y": 2, "crs": 1})
	require.ErrorContains(t, err, "Unknown coordinate reference system: 1")
}

func TestPointFieldsAndComparisons(t *testing.T) {
	geographic := CypherPoint{SRID: 4326, X: 2, Y: 1}
	_, isPoint, err := evaluatePointProperty(geographic, "height")
	require.True(t, isPoint)
	require.ErrorContains(t, err, "Field: height is not available on point")
	value, _, err := evaluatePointProperty(geographic, "crs")
	require.NoError(t, err)
	require.Equal(t, "wgs-84", value)
	_, isPoint, _ = evaluatePointProperty(map[string]interface{}{}, "x")
	require.False(t, isPoint)

	equal, isPoint := comparePointValues(geographic, "a")
	require.True(t, isPoint)
	require.False(t, equal)
	_, isPoint = comparePointValues(1, 2)
	require.False(t, isPoint)
	_, ordered := comparePointOrdering(geographic, 1)
	require.False(t, ordered)
	order, _ := comparePointOrdering(CypherPoint{SRID: 7203, X: 1, Y: 2}, CypherPoint{SRID: 7203, X: 1, Y: 1})
	require.Equal(t, 1, order)
	order, _ = comparePointOrdering(CypherPoint{SRID: 7203, X: 1, Y: 1}, CypherPoint{SRID: 7203, X: 1, Y: 1})
	require.Equal(t, 0, order)

	for op, want := range map[string]bool{"=": false, "<>": true, "<": false} {
		require.Equal(t, want, compareWithOperator(CypherPoint{SRID: 7203, X: 1, Y: 1}, CypherPoint{SRID: 7203, X: 2, Y: 1}, op), op)
	}

	distance, ok := pointDistance(CypherPoint{SRID: 4979, X: 0, Y: 0, Z: 0}, CypherPoint{SRID: 4979, X: 0, Y: 0, Z: 3})
	require.True(t, ok)
	require.Equal(t, 3.0, distance)

	_, ok = pointWithinBBox(CypherPoint{SRID: 7203}, CypherPoint{SRID: 4326}, CypherPoint{SRID: 7203})
	require.False(t, ok)
	inside, _ := pointWithinBBox(CypherPoint{SRID: 4326, X: 179, Y: 0}, CypherPoint{SRID: 4326, X: 170, Y: -1}, CypherPoint{SRID: 4326, X: -170, Y: 1})
	require.True(t, inside, "a box across the antimeridian")
	inside, _ = pointWithinBBox(CypherPoint{SRID: 9157, X: 1, Y: 1, Z: 5}, CypherPoint{SRID: 9157}, CypherPoint{SRID: 9157, X: 2, Y: 2, Z: 2})
	require.False(t, inside, "z is outside")

	require.Equal(t, 11, cypherSortRank(&CypherDuration{}))
	require.Equal(t, 15, cypherSortRank(math.NaN()))
	require.Equal(t, []float64{1, 2}, CypherPoint{SRID: 7203, X: 1, Y: 2}.Coordinates())
}

func TestPointStoredForm(t *testing.T) {
	data, err := msgpack.Marshal(CypherPoint{SRID: 4979, X: 1, Y: 2, Z: 3})
	require.NoError(t, err)
	var decoded CypherPoint
	require.NoError(t, msgpack.Unmarshal(data, &decoded))
	require.Equal(t, CypherPoint{SRID: 4979, X: 1, Y: 2, Z: 3}, decoded)

	require.ErrorContains(t, msgpack.Unmarshal([]byte{0xd4, 47, 0}, &decoded), "want 28")
	require.Error(t, msgpack.Unmarshal([]byte{0xc7, 28, 47, 0}, &decoded), "a truncated value")

	unknown := append([]byte{0xc7, 28, 47}, make([]byte, 28)...)
	require.ErrorContains(t, msgpack.Unmarshal(unknown, &decoded), "unknown SRID 0")
}

func TestPointOfAnEntityAndMixedSpatialArguments(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()
	nodes := map[string]*storage.Node{"n": {ID: "n1", Properties: map[string]interface{}{"x": 1.0, "y": 2.0}}}
	rels := map[string]*storage.Edge{"r": {ID: "r1", Properties: map[string]interface{}{"latitude": 1.0, "longitude": 2.0}}}
	require.Equal(t, CypherPoint{SRID: 7203, X: 1, Y: 2}, e.evaluateExpressionWithContext(ctx, "point(n)", nodes, rels))
	require.Equal(t, CypherPoint{SRID: 4326, X: 2, Y: 1}, e.evaluateExpressionWithContext(ctx, "point(r)", nodes, rels))
	require.Nil(t, e.evaluateExpressionWithContext(ctx, "point(1)", nodes, rels))
	require.Nil(t, e.evaluateExpressionWithContext(ctx, "distance(point({x: 0, y: 0}), point({latitude: 0, longitude: 1}))", nodes, rels))
	require.Nil(t, e.evaluateExpressionWithContext(ctx, "withinBBox(point({x: 1, y: 1}), point({x: 0, y: 0}), point({latitude: 2, longitude: 2}))", nodes, rels))
}
