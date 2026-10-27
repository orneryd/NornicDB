package cypher

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Every value kind keeps its type through the remote value codec: a value
// that starts on a local constituent, a Bolt participant or an HTTP
// participant, bare or nested in a list or in a map's list, is imported into
// a Bolt or HTTP participant and returned from it. The participant reads the
// value's components (rp), and so does NornicDB from what it returned (lp);
// both equal the components Neo4j reads from the same value on one database,
// compared by kind and exact value (fabricTypedRows), so an integer that
// came back as a float, or lost precision, fails.
func TestFabricRemoteValueRoundTrip(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("requires --ref neo-ref")
	}
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	require.NotEmpty(t, httpURI, "requires the reference HTTP endpoint")
	// probe reads the components of $E, the element holding the value.
	kinds := []struct{ name, expr, probe string }{
		{"null", "null", "[$E IS NULL]"},
		{"boolean", "true", "[$E]"},
		{"integer", "-42", "[$E, $E / 5, toString($E)]"},
		{"integer_above_2p53", "9007199254740993", "[$E, $E - 9007199254740992, toString($E)]"},
		{"integer_below_minus_2p53", "-9007199254740993", "[$E, $E + 9007199254740992, toString($E)]"},
		{"integer_max", "9223372036854775807", "[$E, $E - 9223372036854775806, toString($E)]"},
		{"integer_min", "-9223372036854775807 - 1", "[$E, $E + 9223372036854775807, toString($E)]"},
		{"float", "2.5", "[$E, $E / 5, toString($E)]"},
		{"float_integral", "-42.0", "[$E, $E / 5, toString($E)]"},
		{"string", "'a\\'b`c'", "[$E]"},
		{"list", "[1, 'x', null]", "[$E]"},
		{"map", "{`odd``key`: 1, b: [true]}", "[$E.`odd``key`, $E.b]"},
		{"date", "date('2024-02-29')", "[$E.year, $E.month, $E.day]"},
		{"time", "time('12:34:56.000000123+01:00')", "[$E.hour, $E.minute, $E.second, $E.nanosecond, $E.offset]"},
		{"localtime", "localtime('12:34:56.000000123')", "[$E.hour, $E.minute, $E.second, $E.nanosecond]"},
		{"datetime_offset", "datetime('2024-02-29T12:34:56.000000123+01:00')", "[$E.year, $E.month, $E.day, $E.hour, $E.nanosecond, $E.offset]"},
		{"datetime_zone", "datetime('2024-02-29T12:34:56[Europe/Berlin]')", "[$E.year, $E.hour, $E.timezone, $E.offset]"},
		{"localdatetime", "localdatetime('2024-02-29T12:34:56.5')", "[$E.year, $E.day, $E.hour, $E.millisecond]"},
		{"duration", "duration('P1Y2M3DT4H5M6.5S')", "[$E.months, $E.days, $E.hours, $E.milliseconds]"},
		// Neo4j's text forms reject an offset with seconds; its map forms take it.
		{"time_offset_seconds", "time({hour: 12, minute: 34, second: 56, timezone: '+02:05:59'})", "[$E.hour, $E.offset, toString($E)]"},
		{"datetime_offset_seconds", "datetime({year: 2024, month: 1, day: 1, hour: 12, timezone: '+02:05:59'})", "[$E.year, $E.hour, $E.offset, toString($E)]"},
		{"datetime_lmt_zone", "datetime({year: 1850, month: 1, day: 1, hour: 12, timezone: 'Europe/Paris'})", "[$E.year, $E.hour, $E.timezone, $E.offset, toString($E)]"},
		{"point_cartesian", "point({x: 1.25, y: 2.5})", "[$E.x, $E.y, $E.srid]"},
		{"point_cartesian_3d", "point({x: 1.25, y: 2.5, z: 3.75})", "[$E.x, $E.y, $E.z, $E.srid]"},
		{"point_wgs84", "point({longitude: 1.25, latitude: 2.5})", "[$E.longitude, $E.latitude, $E.srid]"},
		{"point_wgs84_3d", "point({longitude: 1.25, latitude: 2.5, height: 3.75})", "[$E.longitude, $E.latitude, $E.height, $E.srid]"},
	}
	// Each depth wraps the value and names the element that holds it.
	depths := []struct {
		name          string
		wrap, element func(string) string
	}{
		{"bare", func(x string) string { return x }, func(v string) string { return v }},
		{"in_list", func(x string) string { return "[" + x + "]" }, func(v string) string { return v + "[0]" }},
		{"in_map_list", func(x string) string { return "{k: [" + x + "]}" }, func(v string) string { return v + ".k[0]" }},
		{"in_map_list_map", func(x string) string { return "{a: [], k: [{n: " + x + "}], t: {}}" }, func(v string) string { return v + ".k[0].n" }},
	}
	statement := func(origin, destination, expr, probe string, wrap, element func(string) string) string {
		value := wrap(expr)
		source := "WITH " + value + " AS v "
		if origin != "local" {
			source = "CALL { USE comp." + origin + " RETURN " + value + " AS v } "
		}
		return "USE comp " + source + "CALL { WITH v USE comp." + destination + " RETURN v AS w, " + strings.ReplaceAll(probe, "$E", element("v")) +
			" AS rp } RETURN " + strings.ReplaceAll(probe, "$E", element("w")) + " AS lp, rp"
	}
	strip := strings.NewReplacer("USE comp.remote ", "", "USE comp.http ", "", "USE comp ", "")
	for _, normalization := range []string{"on", "off"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			if normalization == "off" {
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
			}
			exec := fabricDispatchRemoteExecutor(t)
			mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
			require.NoError(t, mgr.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI}))
			for _, kind := range kinds {
				for _, depth := range depths {
					var want []interface{}
					for _, origin := range []string{"local", "remote", "http"} {
						for _, destination := range []string{"remote", "http"} {
							query := statement(origin, destination, kind.expr, kind.probe, depth.wrap, depth.element)
							for _, explicit := range []bool{false, true} {
								t.Run(fmt.Sprintf("%s/%s/from_%s/to_%s/explicit=%v", kind.name, depth.name, origin, destination, explicit), func(t *testing.T) {
									ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
									defer cancel()
									if want == nil {
										ref, err := neo4j.ExecuteQuery(ctx, driver, strip.Replace(query), nil, neo4j.EagerResultTransformer)
										require.NoError(t, err, "Neo4j query: %s", strip.Replace(query))
										rows := make([][]interface{}, 0, len(ref.Records))
										for _, record := range ref.Records {
											rows = append(rows, record.Values)
										}
										want = fabricTypedRows(rows)
									}
									runner := exec
									if explicit {
										runner = fabricCallRowsCompositeExecutor(t, exec)
										_, err := runner.Execute(ctx, "BEGIN", nil)
										require.NoError(t, err)
										defer func() { _, _ = runner.Execute(context.Background(), "ROLLBACK", nil) }()
									}
									result, err := runner.Execute(ctx, query, nil)
									require.NoError(t, err, "Fabric query: %s", query)
									require.Equal(t, want, fabricTypedRows(result.Rows), "Fabric query: %s", query)
								})
							}
						}
					}
				}
			}
		})
	}
}

// fabricTypedRows is rows with every value tagged with its kind, so rows
// compare by kind and exact value: an integer never equals a float, and an
// int64 is compared exactly.
func fabricTypedRows(rows [][]interface{}) []interface{} {
	out := make([]interface{}, len(rows))
	for i, row := range rows {
		out[i] = fabricTypedValue(row)
	}
	return out
}

func fabricTypedValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("integer %d", rv.Int())
	case reflect.Float32, reflect.Float64:
		return "float " + strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.String:
		return "string " + rv.String()
	case reflect.Bool:
		return fmt.Sprintf("boolean %v", rv.Bool())
	case reflect.Slice, reflect.Array:
		items := make([]interface{}, rv.Len())
		for i := range items {
			items[i] = fabricTypedValue(rv.Index(i).Interface())
		}
		return items
	case reflect.Map:
		entries := make(map[string]interface{}, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			entries[fmt.Sprint(iter.Key().Interface())] = fabricTypedValue(iter.Value().Interface())
		}
		return entries
	}
	return fmt.Sprintf("%T %v", value, value)
}
