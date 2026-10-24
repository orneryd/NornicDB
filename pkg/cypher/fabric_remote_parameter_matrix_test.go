package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// fabricMatrixOutcome is a statement's rows, or its error's Neo4j status.
type fabricMatrixOutcome struct {
	rows interface{}
	code string
	err  error
}

func fabricMatrixNeo4j(t *testing.T, driver neo4j.DriverWithContext, query string, params map[string]interface{}) fabricMatrixOutcome {
	t.Helper()
	result, err := neo4j.ExecuteQuery(context.Background(), driver, query, params, neo4j.EagerResultTransformer)
	if err != nil {
		var neo4jErr *neo4j.Neo4jError
		require.ErrorAs(t, err, &neo4jErr, "Neo4j: %s", query)
		return fabricMatrixOutcome{code: neo4jErr.Code, err: err}
	}
	rows := make([][]interface{}, 0, len(result.Records))
	for _, record := range result.Records {
		rows = append(rows, record.Values)
	}
	return fabricMatrixOutcome{rows: fabricTypedRows(rows)}
}

// requireFabricMatrixOutcome checks that NornicDB returned want's rows, or
// failed with want's status.
func requireFabricMatrixOutcome(t *testing.T, want fabricMatrixOutcome, result *ExecuteResult, err error, query string) {
	t.Helper()
	if want.err != nil {
		require.Error(t, err, "Fabric query: %s (Neo4j: %v)", query, want.err)
		code, _ := nornicerrors.Neo4jStatus(err)
		if code != want.code {
			require.Contains(t, err.Error(), want.code, "Fabric query: %s", query)
		}
		return
	}
	require.NoError(t, err, "Fabric query: %s", query)
	require.Equal(t, want.rows, fabricTypedRows(result.Rows), "Fabric query: %s", query)
}

// Every value a caller passes reaches a Bolt or HTTP participant as its
// Cypher type in every position a parameter can take: returned, as a
// function argument, inside a list or a map, in WHERE, unwound from a batch
// of rows, as a node's or relationship's property map in CREATE, in MERGE,
// in SET = and SET +=, and imported by a scope clause; in autocommit and in
// an explicit transaction. Each statement's rows, or its error, equal what
// Neo4j gives for the same statement and the same value sent by its driver.
func TestFabricRemoteParameterPositions(t *testing.T) {
	exec, driver := fabricRemoteImportExecutor(t)
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	offset := func(seconds int) *time.Location { return time.FixedZone("Offset", seconds) }
	zoned := time.Date(2024, 2, 29, 12, 34, 56, 123456789, berlin)
	historical := time.Date(1850, 1, 1, 12, 0, 0, 987654321, paris)
	fixed := time.Date(2024, 2, 29, 12, 34, 56, 0, offset(2*3600+5*60+59))
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	clock := time.Date(1970, 1, 1, 0, 1, 2, 123456789, offset(-2670))
	local := time.Date(2024, 2, 29, 12, 34, 56, 5, time.UTC)
	date := CypherDate{Time: day}
	duration := CypherDuration{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6, Nanos: 7}
	point := CypherPoint{SRID: 4979, X: 12.5, Y: 55.75, Z: 100.5}
	const (
		scalarProbe = "[valueType($E), toString($E)]"
		listProbe   = "[valueType($E), [e IN $E | toString(e)]]"
		mapProbe    = "[valueType($E), toString($E.d), toString($E.n)]"
	)
	kinds := []struct {
		name            string
		value, refValue interface{}
		probe           string
	}{
		{"date", date, dbtype.Date(day), scalarProbe},
		{"date_pointer", &date, dbtype.Date(day), scalarProbe},
		{"localtime", CypherLocalTime{Time: local}, dbtype.LocalTime(local), scalarProbe},
		{"time_offset_seconds", CypherTime{Time: clock}, dbtype.Time(clock), scalarProbe},
		{"localdatetime", CypherLocalDateTime{Time: local}, dbtype.LocalDateTime(local), scalarProbe},
		{"datetime_zone", CypherDateTime{Time: zoned, ZoneID: "Europe/Berlin"}, zoned, scalarProbe},
		{"datetime_historical_zone", CypherDateTime{Time: historical, ZoneID: "Europe/Paris"}, historical, scalarProbe},
		{"datetime_offset_seconds", CypherDateTime{Time: fixed}, fixed, scalarProbe},
		{"go_time_pointer", &zoned, zoned, scalarProbe},
		{"duration", duration, dbtype.Duration{Months: 14, Days: 3, Seconds: 4*3600 + 5*60 + 6, Nanos: 7}, scalarProbe},
		{"duration_pointer", &duration, dbtype.Duration{Months: 14, Days: 3, Seconds: 4*3600 + 5*60 + 6, Nanos: 7}, scalarProbe},
		{"point_3d", point, dbtype.Point3D{SpatialRefId: 4979, X: 12.5, Y: 55.75, Z: 100.5}, scalarProbe},
		{"point_pointer", &point, dbtype.Point3D{SpatialRefId: 4979, X: 12.5, Y: 55.75, Z: 100.5}, scalarProbe},
		{"null", nil, nil, scalarProbe},
		{"null_time_pointer", (*time.Time)(nil), nil, scalarProbe},
		{"null_point_pointer", (*CypherPoint)(nil), nil, scalarProbe},
		{"null_list", []interface{}(nil), nil, listProbe},
		{"null_string_list", []string(nil), nil, listProbe},
		{"null_map", map[string]interface{}(nil), nil, mapProbe},
		{"nan", math.NaN(), math.NaN(), scalarProbe},
		{"infinity", math.Inf(1), math.Inf(1), scalarProbe},
		{"negative_infinity", math.Inf(-1), math.Inf(-1), scalarProbe},
		{"integer_above_2p53", int64(9007199254740993), int64(9007199254740993), scalarProbe},
		{"integer_below_minus_2p53", int64(-9007199254740993), int64(-9007199254740993), scalarProbe},
		{"integral_float", -42.0, -42.0, scalarProbe},
		{"datetimes_with_null", []interface{}{CypherDateTime{Time: zoned, ZoneID: "Europe/Berlin"}, nil, &zoned}, []interface{}{zoned, nil, zoned}, listProbe},
		{"floats_with_nan", []interface{}{1.5, math.NaN(), math.Inf(-1)}, []interface{}{1.5, math.NaN(), math.Inf(-1)}, listProbe},
		{"mixed_list", []interface{}{date, int64(1), "x", duration}, []interface{}{dbtype.Date(day), int64(1), "x", dbtype.Duration{Months: 14, Days: 3, Seconds: 4*3600 + 5*60 + 6, Nanos: 7}}, listProbe},
		{"map", map[string]interface{}{"d": date, "n": int64(1)}, map[string]interface{}{"d": dbtype.Date(day), "n": int64(1)}, mapProbe},
	}
	const label = "FabricParameterPosition"
	defer func() {
		_, _ = neo4j.ExecuteQuery(context.Background(), driver, "MATCH (n:"+label+") DETACH DELETE n", nil, neo4j.EagerResultTransformer)
	}()
	// A position's body reads the value back with the probe at $R.
	positions := []struct{ name, body, read string }{
		{"return", "RETURN $P AS p", "$v"},
		{"function_argument", "RETURN $P AS p", "coalesce($v, $v)"},
		{"in_list", "RETURN $P AS p", "[$v, 1][0]"},
		{"in_map", "RETURN $P AS p", "{k: $v}.k"},
		{"where", "UNWIND [1] AS one WITH one WHERE $v IS NULL OR $v IS NOT NULL RETURN $P AS p", "$v"},
		{"unwind_rows", "UNWIND $rows AS row RETURN $P AS p", "row.k"},
		{"create_property_map", "CREATE (n:" + label + " $props) RETURN $P AS p", "n.k"},
		{"create_relationship_property_map", "CREATE (:" + label + " {run: $run})-[r:FABRIC_PARAMETER_POSITION $props]->(:" + label + " {run: $run}) RETURN $P AS p", "r.k"},
		{"merge", "MERGE (n:" + label + " {run: $run, k: $v}) RETURN $P AS p", "n.k"},
		{"set_equals", "CREATE (n:" + label + ") SET n = $props RETURN $P AS p", "n.k"},
		{"set_plus", "CREATE (n:" + label + " {run: $run}) SET n += $props RETURN $P AS p", "n.k"},
		{"scope_import", "", "v"},
	}
	params := func(run string, value interface{}) map[string]interface{} {
		return map[string]interface{}{
			"run":   run,
			"v":     value,
			"rows":  []interface{}{map[string]interface{}{"k": value}, map[string]interface{}{"k": value}},
			"props": map[string]interface{}{"run": run, "k": value},
		}
	}
	for _, k := range kinds {
		for _, position := range positions {
			probe := strings.ReplaceAll(k.probe, "$E", position.read)
			body := strings.Replace(position.body, "$P", probe, 1)
			reference := body
			if position.name == "scope_import" {
				reference = "WITH $v AS v RETURN " + probe + " AS p"
			}
			run := fmt.Sprintf("position-%d", time.Now().UnixNano())
			want := fabricMatrixNeo4j(t, driver, reference, params(run, k.refValue))
			for _, destination := range []string{"remote", "http"} {
				query := "USE comp CALL { USE comp." + destination + " " + body + " } RETURN p"
				if position.name == "scope_import" {
					query = "USE comp WITH $v AS v CALL (v) { USE comp." + destination + " RETURN " + probe + " AS p } RETURN p"
				}
				t.Run(k.name+"/"+position.name+"/"+destination, func(t *testing.T) {
					fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
						result, err := runner.Execute(context.Background(), query, params(run+"-"+destination, k.value))
						requireFabricMatrixOutcome(t, want, result, err, query)
					})
				})
			}
		}
	}
}
