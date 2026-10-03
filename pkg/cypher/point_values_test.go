package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expected values below are Neo4j 5.26.30's (#817).

func TestPointValuesMatchNeo4j(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	scalar := func(query string) interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		if !assert.NoError(t, err, query) || !assert.Len(t, result.Rows, 1, query) {
			return "<failed>"
		}
		return result.Rows[0][0]
	}

	for query, want := range map[string]interface{}{
		"RETURN toString(point({x: 1, y: 2}))":                                      "point({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"RETURN toString(point({latitude: 1.5, longitude: 2.5}))":                   "point({x: 2.5, y: 1.5, crs: 'wgs-84'})",
		"RETURN toString(point({x: 1, y: 2, z: 3}))":                                "point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})",
		"RETURN toString(point({latitude: 1, longitude: 2, height: 3}))":            "point({x: 2.0, y: 1.0, z: 3.0, crs: 'wgs-84-3d'})",
		"RETURN toString(point({x: 1, y: 2, latitude: 3}))":                         "point({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"RETURN toString(point({x: 1, y: 2, crs: 'WGS-84'}))":                       "point({x: 1.0, y: 2.0, crs: 'wgs-84'})",
		"RETURN toString(point({x: 1, y: 2, srid: 4326}))":                          "point({x: 1.0, y: 2.0, crs: 'wgs-84'})",
		"RETURN toString(point({latitude: 0, longitude: 200}))":                     "point({x: -160.0, y: 0.0, crs: 'wgs-84'})",
		"RETURN toString(point({x: 1.5, y: 2, height: 3}))":                         "point({x: 1.5, y: 2.0, crs: 'cartesian'})",
		"RETURN toString(point({latitude: 1, longitude: 2, z: 3}))":                 "point({x: 2.0, y: 1.0, z: 3.0, crs: 'wgs-84-3d'})",
		"RETURN point({x: null, y: 1})":                                             nil,
		"RETURN point(null)":                                                        nil,
		"WITH point({x: 1, y: 2, z: 3}) AS p RETURN [p.x, p.y, p.z, p.crs, p.srid]": []interface{}{1.0, 2.0, 3.0, "cartesian-3d", int64(9157)},
		"WITH point({latitude: 1.5, longitude: 2.5, height: 10}) AS p RETURN [p.latitude, p.longitude, p.height, p.x, p.y, p.z, p.crs, p.srid]": []interface{}{1.5, 2.5, 10.0, 2.5, 1.5, 10.0, "wgs-84-3d", int64(4979)},
		"RETURN point.distance(point({x: 0, y: 0}), point({x: 3, y: 4}))":                                                                       5.0,
		"RETURN point.distance(point({latitude: 0, longitude: 0}), point({latitude: 0, longitude: 1}))":                                         111319.54315315113,
		"RETURN point.distance(point({x: 0, y: 0}), point({latitude: 0, longitude: 1}))":                                                        nil,
		"RETURN point.withinBBox(point({x: 1, y: 1}), point({x: 0, y: 0}), point({x: 2, y: 2}))":                                                true,
		"RETURN point({x: 1, y: 2}) = point({x: 1.0, y: 2.0})":                                                                                  true,
		"RETURN point({x: 1, y: 2}) = point({x: 1, y: 2, crs: 'cartesian'})":                                                                    true,
		"RETURN point({x: 1, y: 2}) = point({latitude: 2, longitude: 1})":                                                                       false,
		"RETURN point({x: 1, y: 2}) <> point({x: 1, y: 3})":                                                                                     true,
		"RETURN point({x: 1, y: 2}) < point({x: 2, y: 2})":                                                                                      nil,
		"RETURN point({x: 1, y: 1}) IN [point({x: 1.0, y: 1.0})]":                                                                               true,
		"RETURN valueType(point({x: 1, y: 1}))":                                                                                                 "POINT NOT NULL",
		"RETURN [point({x: 1, y: 1})][0].crs":                                                                                                   "cartesian",
	} {
		assert.Equal(t, want, scalar(query), query)
	}

	result, err := exec.Execute(ctx, "UNWIND [point({x: 2, y: 1}), point({latitude: 1, longitude: 1}), point({x: 1, y: 5}), point({x: 1, y: 2, z: 0})] AS p RETURN p ORDER BY p", nil)
	require.NoError(t, err)
	var order []string
	for _, row := range result.Rows {
		order = append(order, fmt.Sprint(row[0]))
	}
	require.Equal(t, []string{"point({srid:4326, x:1.0, y:1.0})", "point({srid:7203, x:1.0, y:5.0})", "point({srid:7203, x:2.0, y:1.0})", "point({srid:9157, x:1.0, y:2.0, z:0.0})"}, order)

	result, err = exec.Execute(ctx, "UNWIND [point({x: 1, y: 1}), point({x: 1.0, y: 1.0}), point({latitude: 1, longitude: 1})] AS p RETURN DISTINCT p", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	for query, want := range map[string]string{
		"RETURN point({x: 1})":                                        "Neo.ClientError.Statement.SyntaxError",
		"RETURN point({latitude: 100, longitude: 0})":                 "Cannot create WGS84 point with invalid coordinate: [0.0, 100.0]. Valid range for Y coordinate is [-90, 90].",
		"RETURN point({x: 'a', y: 1})":                                `Cannot assign String("a") to field 'a'`,
		"RETURN point({x: 1, y: 2, crs: 'nope'})":                     "Unknown coordinate reference system: nope",
		"RETURN point({x: 1, y: 2, srid: 1234})":                      "Unknown coordinate reference system: code=1234",
		"RETURN point({x: 1, y: 2, z: 3, crs: 'cartesian'})":          "Cannot create point with 2D coordinate reference system and 3 coordinates",
		"RETURN point({x: 1, y: 2, crs: 'cartesian-3d'})":             "Cannot create point with 3D coordinate reference system and 2 coordinates",
		"RETURN point({latitude: 1, longitude: 2, crs: 'cartesian'})": "Geographic points does not support coordinate reference system: cartesian",
		"RETURN point({x: 1, y: 2, crs: 'wgs-84', srid: 7203})":       "Cannot specify both CRS and SRID",
		"RETURN point({x: 1, y: 2}).latitude":                         "Field: latitude is not available on cartesian point: point({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"RETURN point({x: 1, y: 2}).z":                                "Field: z is not available on point: point({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"RETURN point({x: 1, y: 2}).foo":                              "No such field: foo",
	} {
		_, err := exec.Execute(ctx, query, nil)
		if assert.Error(t, err, query) {
			assert.Contains(t, err.Error()+" "+statusText(err), want, query)
		}
	}
}

func TestPointPropertiesAreStored(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:P {loc: point({x: 1, y: 2}), w: point({latitude: 1.5, longitude: 2.5, height: 3}), pts: [point({x: 1, y: 1}), point({x: 2, y: 2})]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (n:Q) SET n.loc = point({x: 3, y: 4})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:P) RETURN n.loc, n.w, n.pts, n.loc.x, point.distance(n.loc, point({x: 1, y: 5}))", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	require.Equal(t, CypherPoint{SRID: 7203, X: 1, Y: 2}, row[0])
	require.Equal(t, CypherPoint{SRID: 4979, X: 2.5, Y: 1.5, Z: 3}, row[1])
	require.Equal(t, []interface{}{CypherPoint{SRID: 7203, X: 1, Y: 1}, CypherPoint{SRID: 7203, X: 2, Y: 2}}, row[2])
	require.Equal(t, 1.0, row[3])
	require.Equal(t, 3.0, row[4])

	result, err = exec.Execute(ctx, "MATCH (n:Q) WHERE n.loc = point({x: 3, y: 4}) RETURN toString(n.loc)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"point({x: 3.0, y: 4.0, crs: 'cartesian'})"}}, result.Rows)

	_, err = exec.Execute(ctx, "CREATE (:Q {bad: [point({x: 1, y: 1}), 1]})", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Q {bad: [point({x: 1, y: 1}), point({latitude: 1, longitude: 1})]})", nil)
	require.ErrorContains(t, err, "Collections containing point values with different CRS can not be stored in properties.")
}
