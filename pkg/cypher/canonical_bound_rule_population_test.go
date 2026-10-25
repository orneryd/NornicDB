package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

type boundRuleCase struct {
	ID        string                 `json:"id"`
	Query     string                 `json:"query"`
	Rules     []string               `json:"rules"`
	Admission bool                   `json:"admission"`
	Params    map[string]interface{} `json:"params"`
}
type boundRuleObservation struct {
	ID      string   `json:"id"`
	Input   string   `json:"input"`
	Mode    string   `json:"mode"`
	Code    string   `json:"code"`
	Detail  string   `json:"detail"`
	Message string   `json:"message"`
	Columns []string `json:"columns"`
	Rows    int      `json:"rows"`
}
type boundRulePopulation struct {
	Cases               []boundRuleCase        `json:"cases"`
	Observations        []boundRuleObservation `json:"observations"`
	ANTLRObservations   []boundRuleObservation `json:"antlrObservations"`
	PreChange           boundRulePreChange     `json:"preChange"`
	AdditionalPreChange []boundRulePreChange   `json:"additionalPreChange"`
}

func readBoundRulePopulation(t *testing.T) boundRulePopulation {
	t.Helper()
	data, err := os.ReadFile("testdata/bound_validation_rules.json")
	require.NoError(t, err)
	var population boundRulePopulation
	require.NoError(t, json.Unmarshal(data, &population))
	if os.Getenv("NORNICDB_PARSER") == "antlr" {
		population.Observations = population.ANTLRObservations
	}
	return population
}
func boundRuleError(err error) (code, detail, message string) {
	if err == nil {
		return
	}
	message = err.Error()
	var classified interface {
		BoltErrorCode() string
		BoltErrorDetail() string
	}
	if errors.As(err, &classified) {
		code = classified.BoltErrorCode()
		detail = classified.BoltErrorDetail()
	}
	return
}

// This recorder is run on the exact release with only this test/data overlaid.
// Its receipts pin accepted behavior and diagnostics, including empty input.
func TestCanonicalBoundRuleReleaseOracle(t *testing.T) { observeBoundRules(t, false) }
func TestCanonicalBoundRulePublic(t *testing.T)        { observeBoundRules(t, true) }

func TestCanonicalBoundDiagnosticContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A cancellation after grammar retention stays the terminal outcome; the
	// diagnostic adapter must not parse another header under Background.
	err := syntaxAdmissionError(ctx, "CALL (1 AS x) { RETURN x } RETURN x", RawSource, context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
}
func observeBoundRules(t *testing.T, compare bool) {
	population := readBoundRulePopulation(t)
	expected := map[string]boundRuleObservation{}
	exactDiagnostic := map[string]bool{"call_import_only": true, "call_scoped_expression": true, "reduce_syntax": true}
	for _, rule := range population.PreChange.Removals {
		for _, id := range rule.Cases {
			exactDiagnostic[id] = true
		}
	}
	for _, baseline := range population.AdditionalPreChange {
		for _, rule := range baseline.Removals {
			for _, id := range rule.Cases {
				exactDiagnostic[id] = true
			}
		}
	}
	for _, sample := range population.Observations {
		expected[sample.ID+"/"+sample.Input+"/"+sample.Mode] = sample
	}
	if compare {
		require.NotEmpty(t, expected, "release receipts must populate the oracle")
	}
	for _, sample := range population.Cases {
		for _, input := range []struct{ name, prefix string }{{"ordinary", ""}, {"empty", "UNWIND [] AS __rule_input "}, {"one", "UNWIND [0] AS __rule_input "}} {
			t.Run(sample.ID+"/"+input.name, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, "CREATE (:RuleNode {id:1})-[:RuleRel]->(:RuleNode {id:2})", nil)
				require.NoError(t, err)
				query := sample.Query
				version := ""
				if strings.HasPrefix(query, "CYPHER 25 ") {
					query = strings.TrimPrefix(query, "CYPHER 25 ")
					version = "CYPHER 25 "
				}
				for _, mode := range []string{"RUN", "EXPLAIN", "PROFILE"} {
					text := version + input.prefix + query
					if mode != "RUN" {
						text = mode + " " + text
					}
					result, err := exec.Execute(ctx, text, sample.Params)
					got := boundRuleObservation{ID: sample.ID, Input: input.name, Mode: mode}
					got.Code, got.Detail, got.Message = boundRuleError(err)
					if result != nil {
						got.Columns = result.Columns
						got.Rows = len(result.Rows)
					}
					if compare {
						key := sample.ID + "/" + input.name + "/" + mode
						want, known := expected[key]
						require.True(t, known, "missing release leaf %s", key)
						require.Equal(t, want.Message == "", got.Message == "", key)
						require.Equal(t, want.Code, got.Code, key)
						require.Equal(t, want.Detail, got.Detail, key)
						if exactDiagnostic[sample.ID] {
							require.Equal(t, want.Message, got.Message, key)
						}
						if want.Message != "" {
							require.NotEmpty(t, got.Message, key)
						}
						if want.Message == "" {
							require.Equal(t, want.Columns, got.Columns, key)
							require.Equal(t, want.Rows, got.Rows, key)
						}
					} else {
						encoded, err := json.Marshal(got)
						require.NoError(t, err)
						t.Logf("BOUND_RULE_ORACLE %s", encoded)
					}
				}
			})
		}
	}
}

func TestCanonicalBoundRuleAdmission(t *testing.T) {
	for _, sample := range readBoundRulePopulation(t).Cases {
		if !sample.Admission {
			continue
		}
		t.Run(sample.ID, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			var failure error
			exec.StatementFacts(sample.Query, resultmetadata.KnownScope(nil), strings.HasPrefix(sample.Query, "CYPHER 25"), RawSource).Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
			require.Error(t, failure, "declarations must be rejected before fact/header publication")
			_, err := exec.StatementResultColumnsContext(ctx, sample.Query, RawSource)
			require.Error(t, err)
		})
	}
}

func boundRuleAdmissionError(ctx context.Context, source string) error {
	_, err := (&StorageExecutor{}).admitBoundStatement(ctx, source)
	return err
}
