package cypher

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The historical oracle pins verdicts only. These exact Neo4j 5.26.30 RUN
// records additionally establish the MATCH diagnostic class, including literal,
// entity and subquery controls. RUN evidence is projected through both parsers,
// transactions and statement modes; it is not separate upstream mode evidence.
func TestNOR79ReferenceMatchBoundary(t *testing.T) {
	nor79ReferenceScopeBoundary(t, "testdata/nor79_reference_match.json", 49, "Type mismatch")
}

func TestNOR79ReferenceBooleanBoundary(t *testing.T) {
	nor79ReferenceScopeBoundary(t, "testdata/nor79_reference_boolean.json", 34, "Coercion of list to boolean")
}

func nor79ReferenceScopeBoundary(t *testing.T, path string, count int, messageClass string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var probes []struct {
		ID, Query, Output string
		Parameters        map[string]string
		ExitCode          int `json:"exit_code"`
	}
	require.NoError(t, json.Unmarshal(data, &probes))
	require.Len(t, probes, count)
	values := map[string]interface{}{"5": int64(5), "1.5": 1.5, "'a'": "a", "[1]": []interface{}{int64(1)}, "{a:1}": map[string]interface{}{"a": int64(1)}, "null": nil, "true": true}
	values["['a']"] = []interface{}{"a"}
	values["[true]"] = []interface{}{true}
	values["[]"] = []interface{}{}
	values["[null]"] = []interface{}{nil}
	values["[[1]]"] = []interface{}{[]interface{}{int64(1)}}
	values["[1.5]"] = []interface{}{1.5}
	values["[1,true]"] = []interface{}{int64(1), true}
	forEachParser(t, func(t *testing.T) {
		for _, execution := range norStaticExecutions() {
			for _, mode := range norStaticTransactionModes() {
				for _, probe := range probes {
					t.Run(execution.kind+"/"+mode+"/"+probe.ID, func(t *testing.T) {
						params := make(map[string]interface{}, len(probe.Parameters))
						for key, expression := range probe.Parameters {
							value, found := values[expression]
							require.True(t, found, "reference parameter %q", expression)
							params[key] = value
						}
						if probe.ExitCode != 0 {
							require.Contains(t, probe.Output, messageClass)
						}
						exec := norStaticFreshExecutor(t)
						if mode == "tx" {
							_, err := exec.Execute(t.Context(), "BEGIN", nil)
							require.NoError(t, err)
						}
						result, err := exec.Execute(t.Context(), execution.prefix+probe.Query, params)
						norStaticRollback(t, t.Context(), exec, mode)
						mismatch, detail := norClassifyVerdict(execution.kind, result, err, probe.ExitCode == 0, messageClass)
						require.Equal(t, norVerdictMatches, mismatch, "%s: %s", probe.Query, detail)
					})
				}
			}
		}
	})
}
