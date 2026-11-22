package cypher

import (
	"encoding/json"
	"os"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// These 92 exact RUN probes were freshly checked on Neo4j 5.26.30.
// They establish empty-input admission only, not scalar runtime iteration.
// Provenance and the upstream timestamp property rewrite are in OpenSpec.
func TestNOR81ReferenceAdmissionBoundary(t *testing.T) {
	data, err := os.ReadFile("testdata/nor81_reference_admission.json")
	require.NoError(t, err)
	var probes []struct {
		Name, Query        string
		Parameter, Accepts bool
	}
	require.NoError(t, json.Unmarshal(data, &probes))
	require.Len(t, probes, 92)
	forEachParser(t, func(t *testing.T) {
		for _, probe := range probes {
			t.Run(probe.Name, func(t *testing.T) {
				exec := norStaticFreshExecutor(t)
				var params map[string]interface{}
				if probe.Parameter {
					params = map[string]interface{}{"p": int64(1)}
				}
				result, err := exec.Execute(t.Context(), probe.Query, params)
				if probe.Accepts {
					require.NoError(t, err, probe.Query)
					require.NotNil(t, result)
					require.Empty(t, result.Rows, "empty input must stay empty")
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
