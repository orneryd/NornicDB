package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNeo4j5FunctionsMatchNeo4j pins the #698 functions to Neo4j 5.26.30's
// results for the same statements.
func TestNeo4j5FunctionsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo5fns"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN radians(180) AS v":                                   3.141592653589793,
		"RETURN isNaN(0.0/0.0) AS v":                                 true,
		"RETURN isNaN(1) AS v":                                       false,
		"RETURN isNaN(1.0 % 0) AS v":                                 true,
		"UNWIND [0.0] AS z RETURN isNaN(0.0 / z) AS v":               true,
		"UNWIND [0.0] AS z RETURN isNaN(1 % z) AS v":                 true,
		"RETURN isNaN(null) AS v":                                    nil,
		"RETURN char_length('héllo') AS v":                           int64(5),
		"RETURN character_length('abc') AS v":                        int64(3),
		"RETURN char_length(null) AS v":                              nil,
		"RETURN upper('aé') AS v":                                    "AÉ",
		"RETURN lower('AÉ') AS v":                                    "aé",
		"RETURN btrim('  a  ') AS v":                                 "a",
		"RETURN btrim('xyaxy', 'xy') AS v":                           "a",
		"RETURN btrim(null) AS v":                                    nil,
		"RETURN btrim('a', null) AS v":                               nil,
		"RETURN ltrim('xxa', 'x') AS v":                              "a",
		"RETURN rtrim('axx', 'x') AS v":                              "a",
		"RETURN trim(BOTH 'x' FROM 'xax') AS v":                      "a",
		"RETURN trim(LEADING 'x' FROM 'xxa') AS v":                   "a",
		"RETURN trim(TRAILING 'x' FROM 'axx') AS v":                  "a",
		"RETURN trim(BOTH FROM '  a  ') AS v":                        "a",
		"RETURN trim('x' FROM 'xax') AS v":                           "a",
		"RETURN trim(LEADING FROM '  a') AS v":                       "a",
		"RETURN trim(FROM ' a ') AS v":                               "a",
		"RETURN trim(null FROM 'a') AS v":                            nil,
		"RETURN trim(BOTH 'x' FROM null) AS v":                       nil,
		"RETURN normalize('Å') = 'Å' AS v":                          true,
		"RETURN size(normalize('Å', NFD)) AS v":                      int64(2),
		"RETURN normalize(null) AS v":                                nil,
		"RETURN toIntegerList(['1', 2, 'x', null, 1.9, true]) AS v":  []interface{}{int64(1), int64(2), nil, nil, int64(1), int64(1)},
		"RETURN toFloatList(['1.5', 2, 'x', null]) AS v":             []interface{}{1.5, 2.0, nil, nil},
		"RETURN toStringList([1, 1.5, true, null, 'a']) AS v":        []interface{}{"1", "1.5", "true", nil, "a"},
		"RETURN toBooleanList(['true', 'x', 1, 0, null, true]) AS v": []interface{}{true, nil, true, false, nil, true},
		"RETURN toIntegerList(null) AS v":                            nil,
		"RETURN nullIf(1, 1) AS v":                                   nil,
		"RETURN nullIf(1, 2) AS v":                                   int64(1),
		"RETURN nullIf(null, 1) AS v":                                nil,
		"RETURN nullIf(1, 1.0) AS v":                                 nil,
		"RETURN valueType(1) AS v":                                   "INTEGER NOT NULL",
		"RETURN valueType(1.5) AS v":                                 "FLOAT NOT NULL",
		"RETURN valueType('a') AS v":                                 "STRING NOT NULL",
		"RETURN valueType(null) AS v":                                "NULL",
		"RETURN valueType(true) AS v":                                "BOOLEAN NOT NULL",
		"RETURN valueType([1,2]) AS v":                               "LIST<INTEGER NOT NULL> NOT NULL",
		"RETURN valueType([1,'a']) AS v":                             "LIST<STRING NOT NULL | INTEGER NOT NULL> NOT NULL",
		"RETURN valueType([]) AS v":                                  "LIST<NOTHING> NOT NULL",
		"RETURN valueType([1,null]) AS v":                            "LIST<INTEGER> NOT NULL",
		"RETURN valueType([null]) AS v":                              "LIST<NULL> NOT NULL",
		"RETURN valueType({a:1}) AS v":                               "MAP NOT NULL",
		"RETURN valueType([[1]]) AS v":                               "LIST<LIST<INTEGER NOT NULL> NOT NULL> NOT NULL",
		"RETURN valueType([[1], ['a']]) AS v":                        "LIST<LIST<STRING NOT NULL> NOT NULL | LIST<INTEGER NOT NULL> NOT NULL> NOT NULL",
		"RETURN valueType([[1,null], [1]]) AS v":                     "LIST<LIST<INTEGER> NOT NULL> NOT NULL",
		"RETURN valueType([[], [1]]) AS v":                           "LIST<LIST<INTEGER NOT NULL> NOT NULL> NOT NULL",
		"RETURN valueType([1.5, 2]) AS v":                            "LIST<INTEGER NOT NULL | FLOAT NOT NULL> NOT NULL",
		"RETURN valueType(date()) AS v":                              "DATE NOT NULL",
		"RETURN valueType(datetime()) AS v":                          "ZONED DATETIME NOT NULL",
		"RETURN valueType(localdatetime()) AS v":                     "LOCAL DATETIME NOT NULL",
		"RETURN valueType(time()) AS v":                              "ZONED TIME NOT NULL",
		"RETURN valueType(localtime()) AS v":                         "LOCAL TIME NOT NULL",
		"RETURN valueType(duration('P1D')) AS v":                     "DURATION NOT NULL",
		"CREATE (n) RETURN valueType(n) AS v":                        "NODE NOT NULL",
		"CREATE ()-[r:R]->() RETURN valueType(r) AS v":               "RELATIONSHIP NOT NULL",
		"CREATE p=()-[:R]->() RETURN valueType(p) AS v":              "PATH NOT NULL",
	} {
		result, err := exec.Execute(ctx, query, nil)
		{
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1, query)
			require.Equal(t, want, result.Rows[0][0], query)
		}
	}
	for query, message := range map[string]string{
		"RETURN trim(BOTH 'xy' FROM 'xyax') AS v": "must be of length 1",
		"RETURN toIntegerList(1) AS v":            "Type mismatch",
		"RETURN radians('a') AS v":                "Type mismatch",
		"RETURN char_length(1) AS v":              "Type mismatch",
		"RETURN isNaN('a') AS v":                  "Type mismatch",
		"RETURN day(date()) AS v":                 "unknown function",
		"RETURN isNaN(1 % 0) AS v":                "/ by zero",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
	// Argument errors from values known only at run time fail the statement
	// too, as in Neo4j, rather than evaluating to null.
	for query, message := range map[string]string{
		"UNWIND [$s] AS x RETURN radians(x) AS v":                   "Type mismatch",
		"UNWIND [$s] AS x RETURN isNaN(x) AS v":                     "Type mismatch",
		"UNWIND [$s] AS x RETURN toIntegerList(x) AS v":             "Type mismatch",
		"UNWIND [$i] AS x RETURN char_length(x) AS v":               "Type mismatch",
		"UNWIND [$s] AS x RETURN trim(BOTH x FROM 'xyax') AS v":     "must be of length 1",
		"UNWIND [$s] AS x RETURN toUpper(trim(BOTH x FROM x)) AS v": "must be of length 1",
		"UNWIND [$s] AS x RETURN 1 + radians(x) AS v":               "Type mismatch",
	} {
		_, err := exec.Execute(ctx, query, map[string]interface{}{"s": "xy", "i": int64(1)})
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
}

// TestFunctionCatalogIsTheOneTable: SHOW FUNCTIONS lists every listed
// catalog entry and nothing else, and the unknown-function check accepts
// exactly the catalog's names (#698).
func TestShowPointFunctionInventory(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW FUNCTIONS YIELD name, signature WHERE name IN ['point.distance', 'point.withinBBox'] RETURN name, signature ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"point.distance", "point.distance(from :: POINT, to :: POINT) :: FLOAT"},
		{"point.withinBBox", "point.withinBBox(point :: POINT, lowerLeft :: POINT, upperRight :: POINT) :: BOOLEAN"},
	}, result.Rows)
	result, err = executor.Execute(ctx, "RETURN point.distance(point({x:0,y:0}), point({x:3,y:4})) AS distance, point.withinBBox(point({x:1,y:1}), point({x:0,y:0}), point({x:2,y:2})) AS within", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{float64(5), true}}, result.Rows)
}

func TestCSVMetadataFunctionsOutsideLoadCSV(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "RETURN file() AS source, linenumber() AS line", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, nil}}, result.Rows)
	for _, name := range []string{"file", "linenumber"} {
		_, err := executor.Execute(ctx, "RETURN "+name+"(1) AS value", nil)
		require.Error(t, err)
	}
}

func TestSharedFunctionInventoryMatchesPinnedNeo4j(t *testing.T) {
	endpoint := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if endpoint == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare the shared function inventory")
	}
	query := "SHOW FUNCTIONS YIELD name, signature, category, description, isBuiltIn, argumentDescription, returnDescription, aggregating, isDeprecated, deprecatedBy RETURN name, signature, category, description, isBuiltIn, argumentDescription, returnDescription, aggregating, isDeprecated, deprecatedBy ORDER BY name, signature"
	payload, err := json.Marshal(map[string]interface{}{"statements": []map[string]string{{"statement": query}}})
	require.NoError(t, err)
	client := &http.Client{Timeout: 15 * time.Second}
	response, err := client.Post(strings.TrimRight(endpoint, "/")+"/db/neo4j/tx/commit", "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var reference struct {
		Results []struct {
			Data []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []interface{} `json:"errors"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reference))
	require.Empty(t, reference.Errors)
	require.Len(t, reference.Results, 1)
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, query, nil)
	require.NoError(t, err)
	names := make(map[string]map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		name := row[0].(string)
		if names[name] == nil {
			names[name] = make(map[string][]interface{})
		}
		names[name][row[1].(string)] = row
	}
	for _, row := range reference.Results[0].Data {
		require.Len(t, row.Row, 10)
		name, signature := row.Row[0].(string), row.Row[1].(string)
		t.Run(signature, func(t *testing.T) {
			require.Contains(t, names, name)
			require.Contains(t, names[name], signature)
			require.Equal(t, row.Row, names[name][signature])
		})
	}
}

func TestFunctionCatalogIsTheOneTable(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "fncatalog"))
	result, err := exec.Execute(context.Background(), "SHOW FUNCTIONS YIELD name", nil)
	require.NoError(t, err)
	listed := map[string]bool{}
	for _, row := range result.Rows {
		listed[row[0].(string)] = true
	}
	count := 0
	for _, function := range cypherFunctionCatalog {
		if function.listed {
			count++
			require.True(t, listed[function.name], function.name)
		}
		_, known := builtInCypherFunctions[strings.ToLower(function.name)]
		require.True(t, known, function.name)
	}
	require.Len(t, result.Rows, count)
	names := map[string]bool{}
	for _, function := range cypherFunctionCatalog {
		names[strings.ToLower(function.name)] = true
	}
	require.Len(t, builtInCypherFunctions, len(names))

	// A function with several signatures lists one row per signature.
	rounds, err := exec.Execute(context.Background(), "SHOW FUNCTIONS YIELD name, signature WHERE name = 'round' RETURN signature", nil)
	require.NoError(t, err)
	require.Len(t, rounds.Rows, 3)
}

// TestTemporalConstructorErrorsMatchNeo4j: a temporal constructor that can't
// build a value from its input fails the statement with Neo4j 5.26.30's
// error, on every route, instead of evaluating to null.
func TestTemporalConstructorErrorsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporalerr"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:TemporalErr {a: 1})", nil)
	require.NoError(t, err)
	for query, want := range map[string]string{
		"RETURN date('x') AS v":                                        "SyntaxError: Text cannot be parsed to a Date",
		"RETURN datetime('x') AS v":                                    "SyntaxError: Text cannot be parsed to a DateTime",
		"RETURN localdatetime('x') AS v":                               "SyntaxError: Text cannot be parsed to a LocalDateTime",
		"RETURN time('x') AS v":                                        "SyntaxError: Text cannot be parsed to a Time",
		"WITH 'x' AS s RETURN localtime(s) AS v":                       "SyntaxError: Text cannot be parsed to a LocalTime",
		"UNWIND ['x'] AS s RETURN duration(s) AS v":                    "SyntaxError: Text cannot be parsed to a Duration",
		"RETURN datetime('2020-02-30T10:00') AS v":                     "SyntaxError",
		"RETURN datetime(1) AS v":                                      "ProcedureCallFailed: Invalid call signature for DateTimeFunction: Provided input was [Long(1)]",
		"RETURN time(true) AS v":                                       "ProcedureCallFailed: Invalid call signature for TimeFunction: Provided input was [Boolean('true')]",
		"RETURN date({year: 'x'}) AS v":                                "Neo.DatabaseError.Statement.ExecutionFailed",
		"CREATE (n:TemporalErr {d: datetime('x')}) RETURN n.d":         "Text cannot be parsed to a DateTime",
		"MATCH (n:TemporalErr) WHERE datetime('x') IS NULL RETURN n.a": "Text cannot be parsed to a DateTime",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, statusText(err), want, query)
	}
	result, err := exec.Execute(ctx, "MATCH (n:TemporalErr) WHERE n.d IS NOT NULL RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "the failed CREATE stores nothing")
	result, err = exec.Execute(ctx, "RETURN datetime(null) AS v, date('2020-01-01') IS NOT NULL AS w", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, true}}, result.Rows)
}
