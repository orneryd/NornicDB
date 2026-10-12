package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestTemporalPatternsMatchNeo4j replays Neo4j 2026.09's answers
// (testdata/temporal_patterns_neo4j.json, from
// scripts/gen_temporal_pattern_cases.py) for format() with every pattern
// letter at every length, duration patterns, literals and optional sections,
// and for the temporal constructors' pattern form. An answer is =text, null,
// or !status-code[: message].
func TestTemporalPatternsMatchNeo4j(t *testing.T) {
	raw, err := os.ReadFile("testdata/temporal_patterns_neo4j.json")
	require.NoError(t, err)
	var cases struct {
		Values map[string]string
		Format [][]*string
		Parse  [][]string
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_patterns"))
	ctx := context.Background()
	answer := func(query string, params map[string]interface{}) string {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, params)
		if err != nil {
			var classified interface{ BoltErrorCode() string }
			if errors.As(err, &classified) {
				return "!" + classified.BoltErrorCode() + ": " + err.Error()
			}
			return "!" + err.Error()
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			return fmt.Sprintf("rows %v", result.Rows)
		}
		if result.Rows[0][0] == nil {
			return "null"
		}
		return fmt.Sprint("=", result.Rows[0][0])
	}
	var mismatches []string
	check := func(label, want, got string) {
		if strings.HasPrefix(want, "!") {
			code, message, _ := strings.Cut(want[1:], ": ")
			if strings.HasPrefix(got, "!"+code+": ") && strings.Contains(got, message) {
				return
			}
		} else if want == got {
			return
		}
		mismatches = append(mismatches, fmt.Sprintf("%s\n    want %s\n    got  %s", label, want, got))
	}
	for _, row := range cases.Format {
		value := cases.Values[*row[0]]
		if row[1] == nil {
			check("format("+value+")", *row[2], answer("RETURN format("+value+") AS v", nil))
			continue
		}
		check(fmt.Sprintf("format(%s, %q)", value, *row[1]), *row[2],
			answer("RETURN format("+value+", $p) AS v", map[string]interface{}{"p": *row[1]}))
	}
	for _, row := range cases.Parse {
		check(fmt.Sprintf("%s(%q, %q)", row[0], row[1], row[2]), row[3],
			answer("RETURN toString("+row[0]+"($t, $p)) AS v", map[string]interface{}{"t": row[1], "p": row[2]}))
	}
	if len(mismatches) > 0 {
		t.Fatalf("%d of %d answers differ from Neo4j:\n%s", len(mismatches), len(cases.Format)+len(cases.Parse),
			strings.Join(mismatches[:min(len(mismatches), 60)], "\n"))
	}
}

// format() and the pattern constructors' nulls, arities and argument types,
// and NornicDB's printf form of format(), which takes a template string.
func TestTemporalPatternArguments(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_pattern_arguments"))
	ctx := context.Background()
	rows := func(query string, params map[string]interface{}) [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{{nil, nil, nil, nil, nil}},
		rows("CYPHER 25 RETURN format(null) AS a, format(date('2020-01-01'), null) AS b, date(null, 'yyyy') AS c, date('2020', null) AS d, date({year: 2020}, null) AS e", nil))
	require.Equal(t, [][]interface{}{{"x=1 y=a", "plain"}}, rows("RETURN format('x=%d y=%s', 1, 'a') AS a, format('plain') AS b", nil))
	require.Equal(t, [][]interface{}{{"13:45:00", "1986-11-18T00:00:00Z"}},
		rows("CYPHER 25 RETURN toString(local_time('13-45', 'HH-mm')) AS a, toString(zoned_datetime('11/18/1986', 'MM/dd/yyyy')) AS b", nil))
	require.Equal(t, [][]interface{}{{"1986"}}, rows("CYPHER 25 WITH '1986-11-18' AS text, 'yyyy-MM-dd' AS pattern RETURN format(date(text, pattern), 'yyyy') AS v", nil))
	require.Equal(t, [][]interface{}{{"1986-11-18"}}, rows("CYPHER 25 UNWIND ['1986-11-18'] AS text RETURN toString(date(text, $p)) AS v", map[string]interface{}{"p": "yyyy-MM-dd"}))
	for query, code := range map[string]string{
		"RETURN format() AS v":                                      "Neo.ClientError.Statement.SyntaxError",
		"RETURN format(date('2020-01-01'), 'yyyy', 1) AS v":         "Neo.ClientError.Statement.SyntaxError",
		"RETURN format(1, 'yyyy') AS v":                             "Neo.ClientError.Statement.SyntaxError",
		"RETURN format(date('2020-01-01'), 1) AS v":                 "Neo.ClientError.Statement.SyntaxError",
		`RETURN format(duration('P1D'), "y'") AS v`:                 "Neo.ClientError.Statement.ArgumentError",
		"RETURN format(date('2020-01-01'), 'HH') AS v":              "Neo.ClientError.Statement.ArgumentError",
		"RETURN date({year: 2020}, 'yyyy') AS v":                    "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN date(1, 'yyyy') AS v":                               "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN date('2020', 1) AS v":                               "Neo.ClientError.Statement.SyntaxError",
		"RETURN date('2020', 'yyyy') AS v":                          "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [1] AS x RETURN localtime('13', 'mm') AS v":         "Neo.ClientError.Statement.SyntaxError",
		"WITH 'x' AS p RETURN datetime('2020-01-01', p + '{') AS v": "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	_, err := exec.Execute(ctx, "CYPHER 25 RETURN date({year: 2020}, 'yyyy') AS v", nil)
	require.ErrorContains(t, err, "A pattern can only be used in conjunction with a `STRING` input.")
	_, err = exec.Execute(ctx, `CYPHER 25 RETURN format(duration('P1D'), "y'") AS v`, nil)
	require.ErrorContains(t, err, "Pattern parsing failed. Make sure that an even number of escapes are used in the pattern.")
}
