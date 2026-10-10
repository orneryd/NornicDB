package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// neo4jExpressionBehaviour is one recorded statement sequence: the result
// (columns and rows, or the error's status code) the reference gave for the
// last statement.
type neo4jExpressionBehaviour struct {
	Source     string                 `json:"source"`
	Reference  string                 `json:"reference"`
	Statements []string               `json:"statements"`
	Params     map[string]interface{} `json:"params"`
	Expected   string                 `json:"expected"`
}

// TestNeo4jExpressionBehaviours runs, through the executor, the expression,
// predicate, function, aggregation and pattern behaviours the removed ANTLR
// package evaluator was tested for (testdata/neo4j_expression_behaviours.json;
// "source" names the removed test). Each expected result was recorded on
// Neo4j 5.26.30 in a rolled-back transaction, or for the APOC functions
// (no APOC on the reference server) is APOC's documented result.
func TestNeo4jExpressionBehaviours(t *testing.T) {
	raw, err := os.ReadFile("testdata/neo4j_expression_behaviours.json")
	require.NoError(t, err)
	var cases []neo4jExpressionBehaviour
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.NotEmpty(t, cases)
	for _, tc := range cases {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j_expression_behaviours"))
		params := make(map[string]interface{}, len(tc.Params))
		for name, value := range tc.Params {
			// JSON numbers are floats; a whole one is an integer parameter.
			if number, isFloat := value.(float64); isFloat && number == math.Trunc(number) {
				value = int64(number)
			}
			params[name] = value
		}
		got := ""
		for index, statement := range tc.Statements {
			result, err := exec.Execute(context.Background(), statement, params)
			if err != nil {
				var classified interface{ BoltErrorCode() string }
				code := "Neo.ClientError.Statement.SyntaxError" // errors.Neo4jStatus's default
				if errors.As(err, &classified) {
					code = classified.BoltErrorCode()
				}
				got = "ERR " + code
				break
			}
			if index == len(tc.Statements)-1 {
				columns := make([]interface{}, len(result.Columns))
				for i, column := range result.Columns {
					columns[i] = column
				}
				got = canonicalRecordedValue(map[string]interface{}{"cols": columns, "rows": result.Rows})
			}
		}
		require.Equal(t, tc.Expected, got, "%s (%s): %v", tc.Source, tc.Reference, tc.Statements)
	}
}

// canonicalRecordedValue writes a result as the recordings do (Python's
// json.dumps(sort_keys=True, ensure_ascii=False) of the Neo4j driver's
// values): a float always with a fraction (5.0), NaN and Infinity by name,
// temporal values and points as their text, map keys sorted.
func canonicalRecordedValue(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(typed)
	case string:
		var out bytes.Buffer
		encoder := json.NewEncoder(&out)
		encoder.SetEscapeHTML(false)
		_ = encoder.Encode(typed)
		return strings.TrimSuffix(out.String(), "\n")
	case float64:
		switch {
		case math.IsNaN(typed):
			return "NaN"
		case math.IsInf(typed, 1):
			return "Infinity"
		case math.IsInf(typed, -1):
			return "-Infinity"
		case typed == math.Trunc(typed) && math.Abs(typed) < 1e16:
			return strconv.FormatFloat(typed, 'f', 1, 64)
		}
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case int, int64, int32:
		return strconv.FormatInt(toInt64ForCanonical(typed), 10)
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = canonicalRecordedValue(key) + ": " + canonicalRecordedValue(typed[key])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	if items, isList := cypherListValue(value); isList {
		parts := make([]string, len(items))
		for i, item := range items {
			parts[i] = canonicalRecordedValue(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	if rows, isRows := value.([][]interface{}); isRows {
		parts := make([]string, len(rows))
		for i, row := range rows {
			parts[i] = canonicalRecordedValue(row)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return canonicalRecordedValue(formatCypherValueString(value))
}

func toInt64ForCanonical(value interface{}) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	}
	return value.(int64)
}
