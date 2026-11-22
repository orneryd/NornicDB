package cypher

import (
	"encoding/json"
	"os"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fresh Neo4j5.26.30 empty-input RUN evidence, not the missing historical120.
func TestNOR79ReferenceAdmissionBoundary(t *testing.T) {
	data, err := os.ReadFile("testdata/nor79_reference_admission.json")
	require.NoError(t, err)
	var probes []struct {
		ID, Query, Parameter, Paramname string
		ExitCode                        int `json:"exit_code"`
	}
	require.NoError(t, json.Unmarshal(data, &probes))
	require.Len(t, probes, 56)
	values := map[string]interface{}{"1.5": 1.5, "1.0": 1.0, "1": int64(1), "5": int64(5), "'text'": "text", "true": true, "{k:1}": map[string]interface{}{"k": int64(1)}, "null": nil, "[1,2]": []interface{}{int64(1), int64(2)}, "[1.5]": []interface{}{1.5}, "{v:1.5}": map[string]interface{}{"v": 1.5}}
	forEachParser(t, func(t *testing.T) {
		for _, probe := range probes {
			t.Run(probe.ID, func(t *testing.T) {
				exec := norStaticFreshExecutor(t)
				var params map[string]interface{}
				if probe.Parameter != "" {
					name := probe.Paramname
					if name == "" {
						name = "p"
					}
					value, exists := values[probe.Parameter]
					require.True(t, exists, "unrecognized reference parameter expression")
					params = map[string]interface{}{name: value}
				}
				result, err := exec.Execute(t.Context(), probe.Query, params)
				if probe.ExitCode == 0 {
					require.NoError(t, err, probe.Query)
					require.NotNil(t, result)
					require.Empty(t, result.Rows)
				} else {
					require.Error(t, err, probe.Query)
					code, message := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					require.Contains(t, message, "Type mismatch")
				}
			})
		}
	})
}

func TestNOR79NumericParameterProofAndListBoundary(t *testing.T) {
	type namedFloat float64
	type namedInteger int64
	for _, value := range []interface{}{float32(1.5), float64(1.5), float64(1), namedFloat(1.5)} {
		require.False(t, staticParameterOperand(value).known(), "%T", value)
		require.Empty(t, parameterPropertyAccessType(value))
		require.NoError(t, validateListOperands(t.Context(), "RETURN [x IN $p | x]", map[string]interface{}{"p": value}))
	}
	for _, value := range []interface{}{int32(1), int64(1), namedInteger(1)} {
		require.Equal(t, "Integer", staticParameterOperand(value).typeName())
		require.ErrorContains(t, validateListOperands(t.Context(), "RETURN [x IN $p | x]", map[string]interface{}{"p": value}), "Integer")
	}
	require.Equal(t, "Float", staticLiteralType(t.Context(), "1.5", scalarLiteralOnly))
	// Preserve the existing list-owner contract for kinds outside numeric metadata.
	for _, test := range []struct {
		name  string
		value interface{}
		want  string
	}{
		{"date", CypherDate{}, "Date"}, {"duration", CypherDuration{}, "Duration"},
		{"point", CypherPoint{}, "Point"}, {"node", &storage.Node{}, "Node"},
		{"relationship", &storage.Edge{}, "Relationship"}, {"path", PathResult{}, "Path"},
		{"map", map[string]interface{}{}, "Map, Node or Relationship"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateListOperands(t.Context(), "RETURN [x IN $p | x]", map[string]interface{}{"p": test.value})
			require.Error(t, err)
			require.Contains(t, err.Error(), test.want)
		})
	}
}
