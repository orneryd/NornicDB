package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A dynamic label or property key whose value the request itself supplies is
// typed when that request is admitted, as Neo4j 5.26.30 compiles it with the
// statement: a value the token can't take is the compile's SyntaxError before
// a row exists, so a MATCH that selects no row and EXPLAIN or PROFILE report
// it too. The value a row computes stays the per-row reader's TypeError
// (TestDynamicTokenParameterAdmissionKeepsMissingParameterAndRuntimeValues).
func TestDynamicTokenParameterAdmissionRejectsKnownMismatchBeforeAnyRow(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, testCase := range []struct {
			name    string
			query   string
			params  map[string]interface{}
			message string
		}{
			{
				name:    "set dynamic key",
				query:   "MATCH (n:Absent) SET n[$k] = 1",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "set parenthesized dynamic key",
				query:   "MATCH (n:Absent) SET n[($k)] = 1",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "set dynamic label",
				query:   "MATCH (n:Absent) SET n:$($l)",
				params:  map[string]interface{}{"l": int64(1)},
				message: "Type mismatch for parameter 'l': expected String or List<String> but was Integer",
			},
			{
				name:    "set IS dynamic label",
				query:   "MATCH (n:Absent) SET n IS $($l)",
				params:  map[string]interface{}{"l": int64(1)},
				message: "Type mismatch for parameter 'l': expected String or List<String> but was Integer",
			},
			{
				name:    "remove dynamic key",
				query:   "MATCH (n:Absent) REMOVE n[$k]",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "remove dynamic label",
				query:   "MATCH (n:Absent) REMOVE n:$($l)",
				params:  map[string]interface{}{"l": int64(1)},
				message: "Type mismatch for parameter 'l': expected String or List<String> but was Integer",
			},
			{
				name:    "merge on create dynamic key",
				query:   "MERGE (m:Absent {id: 1}) ON CREATE SET m[$k] = 1",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "merge on match dynamic label",
				query:   "MERGE (m:Absent {id: 1}) ON MATCH SET m:$($l)",
				params:  map[string]interface{}{"l": int64(1)},
				message: "Type mismatch for parameter 'l': expected String or List<String> but was Integer",
			},
			{
				name:    "foreach body dynamic key",
				query:   "MATCH (n:Absent) FOREACH (x IN [] | SET n[$k] = 1)",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "call subquery body dynamic key",
				query:   "CALL { MATCH (n:Absent) SET n[$k] = 1 RETURN 1 AS v } RETURN v",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "explain",
				query:   "EXPLAIN MATCH (n:Absent) SET n[$k] = 1",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
			{
				name:    "profile",
				query:   "PROFILE MATCH (n:Absent) SET n[$k] = 1",
				params:  map[string]interface{}{"k": int64(1)},
				message: "Type mismatch for parameter 'k': expected String but was Integer",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				exec, ctx := dynamicTokenParameterFixture(t)
				_, err := exec.Execute(ctx, testCase.query, testCase.params)
				require.Error(t, err, testCase.query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%s: %v", testCase.query, err)
				require.ErrorContains(t, err, testCase.message, testCase.query)
				// The statement wrote nothing: it never ran.
				requireCount(t, exec, ctx, "MATCH (n:Absent) RETURN count(n) AS c", int64(0))
			})
		}
	})
}

// Admission reads the request's own parameter values, so one statement text is
// admissible for a value its token takes and rejected for one it doesn't, in
// either order: neither the compile verdict (which is value-independent and
// shared through the statement caches) nor an earlier run's admission decides
// a later request.
func TestDynamicTokenParameterAdmissionIsPerRequestNotPerStatement(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, ctx := dynamicTokenParameterFixture(t)
		_, err := exec.Execute(ctx, "CREATE (:Admit {id: 1})", nil)
		require.NoError(t, err)

		const keyQuery = "MATCH (n:Admit {id: 1}) SET n[$k] = 1"
		requireCount(t, exec, ctx, "MATCH (n:Admit {id: 1}) RETURN 1 AS v", int64(1))
		// A value the key takes is admitted and stores the property.
		_, err = exec.Execute(ctx, keyQuery, map[string]interface{}{"k": "alpha"})
		require.NoError(t, err, keyQuery)
		requireCount(t, exec, ctx, "MATCH (n:Admit {id: 1}) RETURN n.alpha AS v", int64(1))
		// The same text with a value the key can't take is rejected.
		_, err = exec.Execute(ctx, keyQuery, map[string]interface{}{"k": int64(1)})
		require.Error(t, err, keyQuery)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, err.Error())
		// And the text is admissible again for another value it takes: the
		// rejected request stored nothing and poisoned nothing.
		_, err = exec.Execute(ctx, keyQuery, map[string]interface{}{"k": "beta"})
		require.NoError(t, err, keyQuery)
		requireCount(t, exec, ctx, "MATCH (n:Admit {id: 1}) RETURN n.beta AS v", int64(1))
		requireCount(t, exec, ctx, "MATCH (n:Admit {id: 1}) RETURN n.alpha AS v", int64(1))

		const labelQuery = "MATCH (n:Admit {id: 1}) SET n:$($l)"
		_, err = exec.Execute(ctx, labelQuery, map[string]interface{}{"l": "Tagged"})
		require.NoError(t, err, labelQuery)
		requireCount(t, exec, ctx, "MATCH (n:Tagged) RETURN count(n) AS c", int64(1))
		_, err = exec.Execute(ctx, labelQuery, map[string]interface{}{"l": int64(1)})
		require.Error(t, err, labelQuery)
		code, _ = nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, err.Error())
		_, err = exec.Execute(ctx, labelQuery, map[string]interface{}{"l": "Tagged2"})
		require.NoError(t, err, labelQuery)
		requireCount(t, exec, ctx, "MATCH (n:Tagged2) RETURN count(n) AS c", int64(1))

		// The statements a valid value reaches keep working: a MATCH that
		// selects no row writes nothing and reports nothing.
		_, err = exec.Execute(ctx, "MATCH (n:Absent) SET n[$k] = 1", map[string]interface{}{"k": "alpha"})
		require.NoError(t, err)
		// The MERGE action and FOREACH body the request admits write their
		// property.
		_, err = exec.Execute(ctx, "MERGE (m:MergeAdmit {id: 1}) ON CREATE SET m[$k] = 1", map[string]interface{}{"k": "created"})
		require.NoError(t, err)
		requireCount(t, exec, ctx, "MATCH (m:MergeAdmit {id: 1}) RETURN m.created AS v", int64(1))
		_, err = exec.Execute(ctx, "MATCH (n:Admit {id: 1}) FOREACH (x IN [1] | SET n[$k] = x)", map[string]interface{}{"k": "loop"})
		require.NoError(t, err)
		requireCount(t, exec, ctx, "MATCH (n:Admit {id: 1}) RETURN n.loop AS v", int64(1))
	})
}

// The correction is the request's own values only: a parameter the statement
// never supplied is ParameterMissing before any token check, and a value a row
// computes (a property, null, a list that isn't all strings) stays the run-time
// TypeError the dynamic token reader reports.
func TestDynamicTokenParameterAdmissionKeepsMissingParameterAndRuntimeValues(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, ctx := dynamicTokenParameterFixture(t)
		_, err := exec.Execute(ctx, "CREATE (:Admit {id: 1})", nil)
		require.NoError(t, err)

		for _, params := range []map[string]interface{}{
			nil,
			{"other": int64(1)},
		} {
			_, err := exec.Execute(ctx, "MATCH (n:Admit {id: 1}) SET n[$k] = 1", params)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.ParameterMissing", code, err.Error())
			require.ErrorContains(t, err, "Expected parameter(s): k")
		}

		for _, testCase := range []struct {
			name   string
			query  string
			params map[string]interface{}
		}{
			{
				name:  "row property as dynamic key",
				query: "MATCH (n:Admit {id: 1}) SET n[n.id] = 1",
			},
			{
				name:   "null parameter as dynamic key",
				query:  "MATCH (n:Admit {id: 1}) SET n[$k] = 1",
				params: map[string]interface{}{"k": nil},
			},
			{
				name:   "null parameter as dynamic label",
				query:  "MATCH (n:Admit {id: 1}) SET n:$($l)",
				params: map[string]interface{}{"l": nil},
			},
			{
				name:   "mixed list parameter as dynamic label",
				query:  "MATCH (n:Admit {id: 1}) SET n:$($l)",
				params: map[string]interface{}{"l": []interface{}{"A", int64(1)}},
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				_, err := exec.Execute(ctx, testCase.query, testCase.params)
				require.Error(t, err, testCase.query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.TypeError", code, "%s: %v", testCase.query, err)
			})
		}
	})
}

// dynamicTokenParameterFixture builds an executor over its own empty graph, so
// each case's assertions see only the entities it seeded.
func dynamicTokenParameterFixture(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_token_parameters")), context.Background()
}

func TestDynamicTokenParameterAdmissionBeforeFabricDispatch(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := newFabricCallRowsExecutor(t)
		for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
			query := prefix + "USE comp CALL { USE comp.one MATCH (n:Absent) SET n[$k]=1 RETURN 1 AS v } RETURN v"
			_, err := exec.Execute(t.Context(), query, map[string]any{"k": int64(1)})
			require.Error(t, err, query)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
			require.ErrorContains(t, err, "parameter 'k'", query)
		}
	})
}
