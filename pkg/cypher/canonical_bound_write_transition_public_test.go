package cypher

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type boundWriteTransitionObservation struct {
	Parser                                        string `json:"parser"`
	ID, Input, Mode, Query, Code, Detail, Message string
	Columns                                       []string
	Rows                                          [][]interface{}
}

// The expected rows contain the exact release observations with separately
// justified class/context corrections. Every mode is a subtest, so a red RUN
// cannot prevent the old product's EXPLAIN and PROFILE witnesses from running.
func TestCanonicalBoundWriteTransitionPublic(t *testing.T) {
	raw, err := os.ReadFile("testdata/bound_write_transition_population.json")
	require.NoError(t, err)
	var population struct {
		ExpectedPublic []boundWriteTransitionObservation `json:"expectedPublic"`
	}
	require.NoError(t, json.Unmarshal(raw, &population))
	parser := "default"
	if os.Getenv("NORNICDB_PARSER") == "antlr" {
		parser = "antlr"
	}
	count := 0
	for _, expected := range population.ExpectedPublic {
		if expected.Parser != parser {
			continue
		}
		count++
		t.Run(expected.ID+"/"+expected.Input+"/"+expected.Mode, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, "CREATE (:Rule73Transition {x:0})", nil)
			require.NoError(t, err)
			result, failure := exec.Execute(ctx, expected.Query, nil)
			code, detail, message := boundRuleError(failure)
			got := boundWriteTransitionObservation{Parser: parser, ID: expected.ID, Input: expected.Input, Mode: expected.Mode, Query: expected.Query, Code: code, Detail: detail, Message: message}
			if result != nil {
				got.Columns, got.Rows = result.Columns, result.Rows
			}
			encoded, err := json.Marshal(got)
			require.NoError(t, err)
			t.Logf("WRITE_TRANSITION_ASSERT %s", encoded)
			require.Equal(t, expected.Code, code)
			require.Equal(t, expected.Detail, detail)
			require.Equal(t, expected.Message, message)
			if failure != nil {
				return
			}
			// Known-empty public results do not distinguish nil backing slices
			// from allocated empty slices; raw Facts has the separate knownness test.
			if len(expected.Columns) == 0 {
				require.Empty(t, got.Columns)
			} else {
				require.Equal(t, expected.Columns, got.Columns)
			}
			if len(expected.Rows) == 0 {
				require.Empty(t, got.Rows)
			} else {
				actualRows, err := json.Marshal(got.Rows)
				require.NoError(t, err)
				wantRows, err := json.Marshal(expected.Rows)
				require.NoError(t, err)
				require.JSONEq(t, string(wantRows), string(actualRows))
			}
			if expected.Mode == "EXPLAIN" || strings.Contains(expected.Query, "UNWIND [] AS __transition_input") {
				require.Empty(t, got.Rows)
			}
		})
	}
	require.Equal(t, 3402, count, "every writer/follower, input and mode must have a public witness")
}
