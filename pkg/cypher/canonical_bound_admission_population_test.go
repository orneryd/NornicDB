package cypher

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). The registry is the procedure declaration
// population. Every finite range boundary must be admitted before a handler
// runs, including when the input has no rows and when no result is executed.
func TestCanonicalBoundProcedureArityPopulation(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	procedures := ListRegisteredProcedures()
	checked := 0
	for _, spec := range procedures {
		registered, found := globalProcedureRegistry.Get(spec.Name)
		if !found || registered.User {
			continue
		}
		var boundaries []struct {
			name, message string
			count         int
		}
		if spec.MinArgs > 0 {
			count := spec.MinArgs - 1
			boundaries = append(boundaries, struct {
				name, message string
				count         int
			}{"minimum", fmt.Sprintf("procedure %s requires at least %d arguments, got %d", spec.Name, spec.MinArgs, count), count})
		}
		if spec.MaxArgs >= 0 {
			count := spec.MaxArgs + 1
			boundaries = append(boundaries, struct {
				name, message string
				count         int
			}{"maximum", fmt.Sprintf("procedure %s accepts at most %d arguments, got %d", spec.Name, spec.MaxArgs, count), count})
		}
		for _, boundary := range boundaries {
			checked++
			arguments := make([]string, boundary.count)
			for i := range arguments {
				arguments[i] = "null"
			}
			for _, input := range []string{"", "UNWIND [] AS __arity_input ", "UNWIND [0] AS __arity_input "} {
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(spec.Name+"/"+boundary.name+"/"+input+"/"+mode, func(t *testing.T) {
						executor, ctx := newUnitExecutor(t)
						query := mode + input + "CALL " + spec.Name + "(" + strings.Join(arguments, ",") + ") YIELD * RETURN 1 AS __arity_proof"
						_, err := executor.Execute(ctx, query, nil)
						require.Error(t, err, "%s", query)
						var status interface {
							BoltErrorCode() string
							BoltErrorDetail() string
						}
						require.True(t, errors.As(err, &status), "%s: %v", query, err)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.BoltErrorCode(), query)
						require.Equal(t, "InvalidNumberOfArguments", status.BoltErrorDetail(), query)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError: "+boundary.message, err.Error(), query)
					})
				}
			}
		}
	}
	require.Positive(t, checked)
	t.Logf("DECLARED_PROCEDURE_ARITY_BOUNDARIES %d", checked)
}

// Author: #1640 lane (Codex). This is an evidence recorder for the compiled
// census's typed-role candidates, not a passing regression claim. The retained
// queries cross local/outer operand identities and generated dynamic-token
// expressions. Its observations decide the public regression expectations.
func TestCanonicalBoundTypeRoleProbe(t *testing.T) {
	previous := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
	for _, normalized := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalized)
		for _, sample := range []struct{ name, query string }{
			{"pattern_projection_self_token", "RETURN [(n)-->() | n:$(n)] AS xs"},
			{"pattern_projection_string_token", "RETURN [(n)-->() | n:$('TypedRoleNode')] AS xs"},
			{"lexical_label_string", "WITH 1 AS value RETURN [value IN ['TypedRoleNode'] | COUNT { MATCH (n:$(value)) RETURN n }] AS xs"},
			{"lexical_label_integer", "WITH 'TypedRoleNode' AS value RETURN [value IN [1] | COUNT { MATCH (n:$(value)) RETURN n }] AS xs"},
			{"foreach_label_string", "WITH 1 AS value FOREACH (value IN ['TypedRoleNode'] | CREATE (:$(value)))"},
			{"foreach_label_integer", "WITH 'TypedRoleNode' AS value FOREACH (value IN [1] | CREATE (:$(value)))"},
			{"weak_label_integer", "WITH 7 AS value CALL { MATCH (n:$(value)) RETURN count(*) AS c } RETURN c"},
		} {
			for _, input := range []string{"", "UNWIND [] AS __role_input ", "UNWIND [0] AS __role_input "} {
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(fmt.Sprintf("normalization=%v/%s/%s/%s", normalized, sample.name, input, mode), func(t *testing.T) {
						executor, ctx := newUnitExecutor(t)
						_, err := executor.Execute(ctx, "CREATE (:TypedRoleNode)-[:TypedRoleRel]->(:TypedRoleNode)", nil)
						require.NoError(t, err)
						query := mode + input + sample.query
						result, err := executor.Execute(ctx, query, nil)
						observation := struct {
							Name, Query, Input, Mode, Code, Detail, Message string
							Normalized                                      bool
							Columns                                         []string
							Rows                                            [][]interface{}
						}{Name: sample.name, Query: query, Input: input, Mode: mode, Normalized: normalized}
						if err != nil {
							observation.Message = err.Error()
							var status interface {
								BoltErrorCode() string
								BoltErrorDetail() string
							}
							if errors.As(err, &status) {
								observation.Code = status.BoltErrorCode()
								observation.Detail = status.BoltErrorDetail()
							}
						}
						if result != nil {
							observation.Columns = result.Columns
							observation.Rows = result.Rows
						}
						encoded, failure := json.Marshal(observation)
						require.NoError(t, failure)
						t.Logf("BOUND_TYPE_ROLE_OBSERVATION %s", encoded)
					})
				}
			}
		}
	}
}
