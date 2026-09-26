package cypher

import (
	"context"
	"strings"
	"testing"

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
	require.Len(t, builtInCypherFunctions, len(cypherFunctionCatalog))
}
