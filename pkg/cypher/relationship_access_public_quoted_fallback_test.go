package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A quoted property identifier may contain a closing parenthesis. It remains
// one argument token through every evaluator and cannot terminate a call.
func TestRelationshipAccessPublicQuotedFallbackCoverage(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_quoted_fallback"))
	exec.cache = nil
	properties := map[string]interface{}{}
	for name, value := range map[string]interface{}{
		"zero": 0.0, "one": 1.0, "four": 4.0, "fraction": 1.2, "halfpi": math.Pi / 2,
		"text": "hit|other", "list": []interface{}{"first", "last"},
	} {
		properties[name] = value
		properties[name+")key"] = value
	}
	_, err := exec.Execute(context.Background(), "CREATE (a:QuotedFallback) SET a += $properties", map[string]interface{}{"properties": properties})
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "MATCH (a:QuotedFallback) CREATE (a)-[:QUOTED_FALLBACK {key:'hit'}]->()", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, expression string
		want             interface{}
	}{
		{"sin", "sin(%s)", 0.0},
		{"cos", "cos(%s)", 1.0},
		{"tan", "tan(%s)", 0.0},
		{"asin", "asin(%s)", 0.0},
		{"acos", "acos(%s)", 0.0},
		{"atan", "atan(%s)", 0.0},
		{"atan2", "atan2(%s,1)", 0.0},
		{"exp", "exp(%s)", 1.0},
		{"log", "log(%s)", 0.0},
		{"log10", "log10(%s)", 0.0},
		{"sqrt", "sqrt(%s)", 2.0},
		{"degrees", "degrees(%s)", 0.0},
		{"haversin", "haversin(%s)", 0.0},
		{"sinh", "sinh(%s)", 0.0},
		{"cosh", "cosh(%s)", 1.0},
		{"tanh", "tanh(%s)", 0.0},
		{"cot", "cot(%s)", 0.0},
		{"coth", "coth(%s)", 1.3130352854993312},
		{"power", "power(%s,3)", 1.0},
		{"ceil", "ceil(%s)", 2.0},
		{"floor", "floor(%s)", 1.0},
		{"round", "round(%s)", 1.0},
		{"tail", "tail(%s)", []interface{}{"last"}},
		{"replace", "replace(%s,'hit','ok')", "ok|other"},
		{"split", "split(%s,'|')", []interface{}{"hit", "other"}},
	} {
		property := "zero"
		switch tc.name {
		case "acos", "log", "log10", "power", "coth":
			property = "one"
		case "sqrt":
			property = "four"
		case "cot":
			property = "halfpi"
		case "ceil", "floor", "round":
			property = "fraction"
		case "tail":
			property = "list"
		case "replace", "split":
			property = "text"
		}
		for _, quoted := range []bool{false, true} {
			key := property
			if quoted {
				key = "`" + property + ")key`"
			}
			for _, mode := range []string{"", "PROFILE ", "call"} {
				t.Run(fmt.Sprintf("%s/quoted=%v/%s", tc.name, quoted, mode), func(t *testing.T) {
					query := "MATCH (a:QuotedFallback)-[r:QUOTED_FALLBACK {key:'hit'}]->() RETURN " + fmt.Sprintf(tc.expression, "a."+key) + " AS value"
					if mode == "call" {
						query = "CALL { " + query + " } RETURN value"
					} else {
						query = mode + query
					}
					result, err := exec.Execute(context.Background(), query, nil)
					require.NoError(t, err, query)
					require.Len(t, result.Rows, 1)
					if want, ok := tc.want.(float64); ok {
						value, ok := result.Rows[0][0].(float64)
						require.True(t, ok, "%s: %T", query, result.Rows[0][0])
						require.InDelta(t, want, value, 1e-12)
					} else {
						require.Equal(t, tc.want, result.Rows[0][0])
					}
				})
			}
		}
	}
}
