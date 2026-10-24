package cypher

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// fabricRemoteImportExecutor is the Fabric executor with the Bolt
// (comp.remote), HTTP (comp.http) and local (comp.one) constituents.
func fabricRemoteImportExecutor(t *testing.T) (*StorageExecutor, neo4j.DriverWithContext) {
	t.Helper()
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("requires --ref neo-ref")
	}
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	require.NotEmpty(t, httpURI, "requires the reference HTTP endpoint")
	exec := fabricDispatchRemoteExecutor(t)
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, mgr.AddConstituent("comp", multidb.ConstituentRef{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI}))
	return exec, driver
}

// fabricRunModes runs fn in autocommit and in an explicit composite
// transaction.
func fabricRunModes(t *testing.T, exec *StorageExecutor, fn func(t *testing.T, runner *StorageExecutor)) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			runner := exec
			if explicit {
				runner = fabricCallRowsCompositeExecutor(t, exec)
				_, err := runner.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = runner.Execute(context.Background(), "ROLLBACK", nil) }()
			}
			fn(t, runner)
		})
	}
}

// Every temporal, point and duration value a caller passes as a parameter,
// as a value or a pointer, bare or in a list or a map, null too, reaches a
// Bolt or HTTP participant as its Cypher type: imported by a scope clause or
// a leading WITH, forwarded as the body's own parameter, and returned from
// the participant for NornicDB to read. The participant reads the value's
// components and its text, and they equal what Neo4j reads from the same
// value passed by its driver.
func TestFabricRemoteTypedParameters(t *testing.T) {
	exec, driver := fabricRemoteImportExecutor(t)
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	// The Go driver sends a fixed-offset DateTime for a zone named "Offset".
	offset := func(seconds int) *time.Location { return time.FixedZone("Offset", seconds) }
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	clock := time.Date(1970, 1, 1, 12, 34, 56, 123456789, time.UTC)
	local := time.Date(2024, 2, 29, 12, 34, 56, 123456789, time.UTC)
	zoned := time.Date(2024, 2, 29, 12, 34, 56, 123456789, berlin)
	historical := time.Date(1850, 1, 1, 12, 0, 0, 987654321, paris)
	fixed := time.Date(2024, 2, 29, 12, 34, 56, 123456789, offset(2*3600+5*60+59))
	utc := time.Date(2024, 2, 29, 12, 34, 56, 123456789, offset(0))
	plain := time.Date(2024, 2, 29, 12, 34, 56, 123456789, offset(2*3600))
	clockAt := func(hour, minute, second, seconds int) time.Time {
		return time.Date(1970, 1, 1, hour, minute, second, 123456789, offset(seconds))
	}
	const (
		dateProbe     = "[$E.year, $E.month, $E.day, toString($E)]"
		clockProbe    = "[$E.hour, $E.minute, $E.second, $E.nanosecond, toString($E)]"
		timeProbe     = "[$E.hour, $E.minute, $E.second, $E.nanosecond, $E.offset, toString($E)]"
		localProbe    = "[$E.year, $E.month, $E.day, $E.hour, $E.nanosecond, toString($E)]"
		instantProbe  = "[$E.epochSeconds, $E.nanosecond, $E.timezone, $E.offset, toString($E)]"
		durationProbe = "[$E.months, $E.days, $E.seconds, $E.nanosecondsOfSecond, toString($E)]"
		pointProbe    = "[$E.x, $E.y, $E.srid, toString($E)]"
		point3DProbe  = "[$E.x, $E.y, $E.z, $E.srid, toString($E)]"
		nullProbe     = "[$E IS NULL]"
	)
	type kind struct {
		name            string
		value, refValue interface{}
		probe           string
	}
	kinds := []kind{
		{"date", CypherDate{Time: day}, dbtype.Date(day), dateProbe},
		{"localtime", CypherLocalTime{Time: clock}, dbtype.LocalTime(clock), clockProbe},
		{"time", CypherTime{Time: clockAt(12, 34, 56, 3600)}, dbtype.Time(clockAt(12, 34, 56, 3600)), timeProbe},
		{"time_utc", CypherTime{Time: clockAt(12, 34, 56, 0)}, dbtype.Time(clockAt(12, 34, 56, 0)), timeProbe},
		{"time_offset_seconds", CypherTime{Time: clockAt(0, 1, 2, -2670)}, dbtype.Time(clockAt(0, 1, 2, -2670)), timeProbe},
		{"localdatetime", CypherLocalDateTime{Time: local}, dbtype.LocalDateTime(local), localProbe},
		{"datetime_zone", CypherDateTime{Time: zoned, ZoneID: "Europe/Berlin"}, zoned, instantProbe},
		{"datetime_historical_zone", CypherDateTime{Time: historical, ZoneID: "Europe/Paris"}, historical, instantProbe},
		{"datetime_offset", CypherDateTime{Time: plain}, plain, instantProbe},
		{"datetime_offset_seconds", CypherDateTime{Time: fixed}, fixed, instantProbe},
		{"datetime_utc", CypherDateTime{Time: utc}, utc, instantProbe},
		{"go_time_zone", zoned, zoned, instantProbe},
		{"go_time_offset", plain, plain, instantProbe},
		{"duration", CypherDuration{Months: 14, Days: 3, Seconds: 14706, Nanos: 123456789}, dbtype.Duration{Months: 14, Days: 3, Seconds: 14706, Nanos: 123456789}, durationProbe},
		{"duration_components", CypherDuration{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6, Nanos: 7}, dbtype.Duration{Months: 14, Days: 3, Seconds: 4*3600 + 5*60 + 6, Nanos: 7}, durationProbe},
		{"point_cartesian", CypherPoint{SRID: 7203, X: 1.25, Y: 2.5}, dbtype.Point2D{SpatialRefId: 7203, X: 1.25, Y: 2.5}, pointProbe},
		{"point_wgs84", CypherPoint{SRID: 4326, X: 12.5, Y: 55.75}, dbtype.Point2D{SpatialRefId: 4326, X: 12.5, Y: 55.75}, pointProbe},
		{"point_cartesian_3d", CypherPoint{SRID: 9157, X: 1.25, Y: 2.5, Z: 3.75}, dbtype.Point3D{SpatialRefId: 9157, X: 1.25, Y: 2.5, Z: 3.75}, point3DProbe},
		{"point_wgs84_3d", CypherPoint{SRID: 4979, X: 12.5, Y: 55.75, Z: 100.5}, dbtype.Point3D{SpatialRefId: 4979, X: 12.5, Y: 55.75, Z: 100.5}, point3DProbe},
	}
	for _, k := range kinds[:len(kinds):len(kinds)] {
		pointer := reflect.New(reflect.TypeOf(k.value))
		pointer.Elem().Set(reflect.ValueOf(k.value))
		kinds = append(kinds, kind{k.name + "_pointer", pointer.Interface(), k.refValue, k.probe})
	}
	kinds = append(kinds,
		kind{"null", nil, nil, nullProbe},
		kind{"null_time_pointer", (*time.Time)(nil), nil, nullProbe},
		kind{"null_datetime_pointer", (*CypherDateTime)(nil), nil, nullProbe},
		kind{"null_date_pointer", (*CypherDate)(nil), nil, nullProbe},
		kind{"null_duration_pointer", (*CypherDuration)(nil), nil, nullProbe},
		kind{"null_point_pointer", (*CypherPoint)(nil), nil, nullProbe},
	)
	// A shape places the value in the parameter, and element reads it back.
	shapes := []struct {
		name    string
		wrap    func(interface{}) interface{}
		element string
	}{
		{"bare", func(v interface{}) interface{} { return v }, "v"},
		{"list", func(v interface{}) interface{} { return []interface{}{v, nil} }, "v[0]"},
		{"map", func(v interface{}) interface{} { return map[string]interface{}{"k": []interface{}{v}, "n": nil} }, "v.k[0]"},
	}
	// A path takes the parameter into the participant (%[1]s) and reads it
	// with the probe (%[2]s).
	paths := []struct{ name, query string }{
		{"scope_import", "WITH $v AS v CALL (v) { USE comp.%[1]s RETURN %[2]s AS p } RETURN p"},
		{"with_import", "WITH $v AS v CALL { WITH v USE comp.%[1]s RETURN %[2]s AS p } RETURN p"},
		{"forwarded", "CALL { USE comp.%[1]s WITH $v AS v RETURN %[2]s AS p } RETURN p"},
		{"returned", "CALL { USE comp.%[1]s WITH $v AS v RETURN v } RETURN %[2]s AS p"},
	}
	for _, k := range kinds {
		for _, shape := range shapes {
			probe := strings.ReplaceAll(k.probe, "$E", shape.element)
			reference := "WITH $v AS v RETURN " + probe + " AS p"
			ref, err := neo4j.ExecuteQuery(context.Background(), driver, reference, map[string]interface{}{"v": shape.wrap(k.refValue)}, neo4j.EagerResultTransformer)
			require.NoError(t, err, "Neo4j %s/%s: %s", k.name, shape.name, reference)
			rows := make([][]interface{}, 0, len(ref.Records))
			for _, record := range ref.Records {
				rows = append(rows, record.Values)
			}
			want := fabricTypedRows(rows)
			params := map[string]interface{}{"v": shape.wrap(k.value)}
			for _, path := range paths {
				for _, destination := range []string{"remote", "http"} {
					t.Run(k.name+"/"+shape.name+"/"+path.name+"/"+destination, func(t *testing.T) {
						query := "USE comp " + fmt.Sprintf(path.query, destination, probe)
						fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
							result, err := runner.Execute(context.Background(), query, params)
							require.NoError(t, err, "Fabric query: %s", query)
							require.Equal(t, want, fabricTypedRows(result.Rows), "Fabric query: %s", query)
						})
					})
				}
			}
		}
	}
}

// A CALL scope clause is "*" alone or a list of variables the enclosing
// query defines. Anything else is Neo4j's syntax error, and the body's write
// never runs, whichever constituent it targets: an expression, a literal or
// a parameter, an empty item, or a name nothing defines, such as an unquoted
// null or true, which Neo4j reads as a variable's name. Where the enclosing
// query defines it (WITH 3 AS `null`), that name, plain or quoted, imports it.
func TestFabricScopedCallScopeClause(t *testing.T) {
	exec, driver := fabricRemoteImportExecutor(t)
	const label = "FabricScopeClause"
	unwritten := func(t *testing.T, params map[string]interface{}) {
		t.Helper()
		written, err := neo4j.ExecuteQuery(context.Background(), driver, "MATCH (n:"+label+" {run: $run}) RETURN count(n) AS c", params, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		require.EqualValues(t, 0, written.Records[0].Values[0], "remote writes")
		local, err := exec.Execute(context.Background(), "USE comp CALL { USE comp.one MATCH (n:"+label+" {run: $run}) RETURN count(n) AS c } RETURN c", params)
		require.NoError(t, err)
		require.Equal(t, fabricTypedRows([][]interface{}{{int64(0)}}), fabricTypedRows(local.Rows), "local writes")
	}
	invalid := []string{
		"1", "1.5", "-1", "'x'", "x + 1", "x, 1", "x, *", "*, x", "x AS y", "x.foo", "$x", "[x]", "{x: x}",
		"x,", ",x", "x,,x", " , ", "x, /* c */ ,",
		"null", "Null", "true", "TRUE", "false", "NaN", "Infinity", "missing", "x, missing",
	}
	for _, scope := range invalid {
		params := map[string]interface{}{"run": fmt.Sprintf("scope-%d", time.Now().UnixNano())}
		reference := "WITH 1 AS x CALL (" + scope + ") { CREATE (:" + label + " {run: $run}) } RETURN x"
		_, err := neo4j.ExecuteQuery(context.Background(), driver, reference, params, neo4j.EagerResultTransformer)
		require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError", "Neo4j: %s", reference)
		for _, destination := range []string{"remote", "http", "one"} {
			t.Run("invalid/"+scope+"/"+destination, func(t *testing.T) {
				query := "USE comp WITH 1 AS x CALL (" + scope + ") { USE comp." + destination + " CREATE (:" + label + " {run: $run}) } RETURN x"
				fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
					_, err := runner.Execute(context.Background(), query, params)
					require.Error(t, err, "Fabric query: %s", query)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%v", err)
				})
				unwritten(t, params)
			})
		}
	}
	const defined = "WITH 1 AS x, 3 AS `null`, 4 AS `NULL`, 5 AS `true`, 6 AS `nan`, 7 AS `NaN`, 8 AS `Infinity` "
	for _, tc := range []struct{ scope, body string }{
		{"", "RETURN 2 AS y"},
		{"*", "RETURN x + 1 AS y"},
		{"x", "RETURN x + 1 AS y"},
		{"/* a */ x /* b */", "RETURN x + 1 AS y"},
		{"`x`", "RETURN x + 1 AS y"},
		{"x, x", "RETURN x + 1 AS y"},
		{"null", "RETURN `null` + 1 AS y"},
		{"`null`", "RETURN `null` + 1 AS y"},
		{"NULL", "RETURN `NULL` + 1 AS y"},
		{"x, true", "RETURN x + `true` AS y"},
		{"nan", "RETURN `nan` + 1 AS y"},
		{"NaN", "RETURN `NaN` + 1 AS y"},
		{"Infinity", "RETURN `Infinity` + 1 AS y"},
	} {
		reference := defined + "CALL (" + tc.scope + ") { " + tc.body + " } RETURN x, y"
		ref, err := neo4j.ExecuteQuery(context.Background(), driver, reference, nil, neo4j.EagerResultTransformer)
		require.NoError(t, err, "Neo4j: %s", reference)
		rows := make([][]interface{}, 0, len(ref.Records))
		for _, record := range ref.Records {
			rows = append(rows, record.Values)
		}
		want := fabricTypedRows(rows)
		for _, destination := range []string{"remote", "http", "one"} {
			t.Run("valid/"+tc.scope+"/"+destination, func(t *testing.T) {
				query := "USE comp " + defined + "CALL (" + tc.scope + ") { USE comp." + destination + " " + tc.body + " } RETURN x, y"
				fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
					result, err := runner.Execute(context.Background(), query, nil)
					require.NoError(t, err, "Fabric query: %s", query)
					require.Equal(t, want, fabricTypedRows(result.Rows), "Fabric query: %s", query)
				})
			})
		}
	}
}
