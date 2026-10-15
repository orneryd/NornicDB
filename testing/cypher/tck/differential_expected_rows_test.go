package tck

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/testing/cypher/differential"
	"github.com/stretchr/testify/require"
)

func TestDifferentialExpectedValueNormalization(t *testing.T) {
	executor := boltDifferentialExecutor{}
	for _, test := range []struct {
		json   string
		actual any
	}{
		{"9007199254740993", int64(9007199254740993)},
		{"9223372036854775807", int64(9223372036854775807)},
		{"-9223372036854775808", int64(-9223372036854775808)},
		{"-0", int64(0)},
		{"1.0", float64(1)},
		{"1.0005e3", float64(1000.5)},
	} {
		t.Run(test.json, func(t *testing.T) {
			got, err := executor.NormalizeExpectedValue(json.Number(test.json))
			require.NoError(t, err)
			require.Equal(t, differentialOutcomeValue(test.actual), got)
		})
	}
	integer, err := executor.NormalizeExpectedValue(json.Number("1"))
	require.NoError(t, err)
	decimal, err := executor.NormalizeExpectedValue(json.Number("1.0"))
	require.NoError(t, err)
	require.NotEqual(t, integer, decimal)
	nested, err := executor.NormalizeExpectedValue(map[string]any{"nested": []any{json.Number("9007199254740993"), json.Number("1.0"), true, nil}})
	require.NoError(t, err)
	require.Equal(t,
		differentialOutcomeValue(map[string]any{"nested": []any{int64(9007199254740993), float64(1), true, nil}}),
		nested)
}

func TestDifferentialExpectedIntegerOverflowDoesNotBecomeFloat(t *testing.T) {
	executor := boltDifferentialExecutor{}
	for _, literal := range []string{"9223372036854775808", "9223372036854775809", "-9223372036854775809", "18446744073709551615", "1e999", "invalid"} {
		t.Run(literal, func(t *testing.T) {
			for _, input := range []any{json.Number(literal), []any{json.Number(literal)}, map[string]any{"nested": []any{json.Number(literal)}}} {
				_, err := executor.NormalizeExpectedValue(input)
				require.Error(t, err, "an invalid oracle must not accept a rounded value")
			}
		})
	}
}

type expectedBoltServer struct {
	boltDifferentialExecutor
	answer differential.Outcome
}

func (server expectedBoltServer) Execute(context.Context, string) differential.Outcome {
	return server.answer
}
func (server expectedBoltServer) Reset(context.Context) error { return nil }

func TestDifferentialExpectedOverflowRejectsMatchingRoundedOutcomes(t *testing.T) {
	for _, literal := range []string{"9223372036854775809", "-9223372036854775809"} {
		rounded, err := json.Number(literal).Float64()
		require.NoError(t, err)
		for _, nested := range []bool{false, true} {
			var input, answer any = json.Number(literal), rounded
			if nested {
				input = map[string]any{"n": []any{input}}
				answer = map[string]any{"n": []any{answer}}
			}
			expected := [][]any{{input}}
			issues := []differential.IssueCase{{Issue: 12, Statements: []differential.IssueStatement{{ID: "overflow", Query: "RETURN value AS v", ExpectedRows: &expected}}}}
			server := expectedBoltServer{answer: differential.Outcome{Columns: []string{"v"}, Rows: [][]any{{differentialOutcomeValue(answer)}}}}
			_, err = differential.RunIssues(context.Background(), issues, &differential.Pair{Neo4j: server, NornicDB: server}, differential.Options{StatementTimeout: time.Second})
			require.ErrorContains(t, err, "expected_rows")
		}
	}
}

func TestDifferentialExpectedRowsDistinguishTypedValuesFromMaps(t *testing.T) {
	for _, test := range []struct {
		name      string
		input     any
		actual    any
		wantError bool
	}{
		{"integer versus shaped map", json.Number("1"), map[string]any{"Integer": "1"}, true},
		{"float versus shaped map", json.Number("1.0"), map[string]any{"Float": "1"}, true},
		{"nested integer versus shaped map", []any{json.Number("1")}, []any{map[string]any{"Integer": "1"}}, true},
		{"ordinary shaped map remains valid", map[string]any{"Integer": "1"}, map[string]any{"Integer": "1"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected := [][]any{{test.input}}
			issues := []differential.IssueCase{{Issue: 12, Statements: []differential.IssueStatement{{ID: "typed-map", Query: "RETURN value AS v", ExpectedRows: &expected}}}}
			server := expectedBoltServer{answer: differential.Outcome{Columns: []string{"v"}, Rows: [][]any{{differentialOutcomeValue(test.actual)}}}}
			_, err := differential.RunIssues(context.Background(), issues, &differential.Pair{Neo4j: server, NornicDB: server}, differential.Options{StatementTimeout: time.Second})
			if test.wantError {
				require.ErrorContains(t, err, "expected_rows")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// expectedBoltJSONValue converts only JSON number syntax to the corresponding
// native Bolt input type. Range errors remain errors; integer tokens never fall
// back to float. The existing outcome normalizer owns all result type encoding.
func expectedBoltJSONValue(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") {
			return typed.Float64()
		}
		return typed.Int64()
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			converted, err := expectedBoltJSONValue(item)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := expectedBoltJSONValue(item)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return value, nil
	}
}
