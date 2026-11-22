package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-144: a Cypher 25 conditional whose arm re-projects a variable the part
// before the conditional bound.
//
// Reported statement:
//
//	CYPHER 25 RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN kept, 2 AS extra
//
// The reference, Neo4j 2025.06.2 Community
// (image digest sha256:0347fe5b1126dd328cc68a54449c2cf83636e348e2da1547abf62b2dbfad67f2),
// answers it with columns kept, extra and the row [7, 2]. The wildcard
// spelling the first audit reported, RETURN *, 2 AS extra, is rejected by the
// same reference: "RETURN * is not allowed when there are no variables in
// scope". Both products reject it, so the wildcard stays rejected here and its
// rejection is part of this matrix.
//
// Every expectation below comes from that reference capture or from a
// conditional shape this package's tests already assert. Each case runs on
// both parsers and both normalization settings through the public Execute path,
// so a case answers the same way on all four.

// conditionalOutputCase is one matrix member and the source of its expectation.
type conditionalOutputCase struct {
	name string
	// query is the statement without the CYPHER 25 prefix the harness adds.
	query string
	// wantColumns and wantRows are the expected successful result.
	wantColumns []string
	wantRows    [][]interface{}
	// wantCode is the expected status code of a rejected statement.
	wantCode string
	// established names the reference capture entry or existing test the
	// expectation comes from.
	established string
}

// conditionalOutputCases is the NOR-144 matrix: the reference capture's cases
// 01 to 13 and the conditional shapes this package already supports.
func conditionalOutputCases() []conditionalOutputCase {
	l := func(values ...interface{}) []interface{} { return values }
	return []conditionalOutputCase{
		{
			name:        "reference_reported_star_in_conditional_branch",
			query:       "RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN *, 2 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 01-exact",
		},
		{
			name:        "reference_braced_star_in_conditional_branch",
			query:       "RETURN 7 AS kept NEXT WHEN kept = 7 THEN { RETURN *, 2 AS extra }",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 03-braced",
		},
		{
			name:        "reference_star_with_multiple_incoming_columns",
			query:       "RETURN 7 AS kept, 9 AS other NEXT WHEN kept = 7 THEN RETURN *, 2 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 04-multiple",
		},
		{
			name:        "reference_star_with_empty_input",
			query:       "UNWIND [] AS kept RETURN kept NEXT WHEN kept = 7 THEN RETURN *, 2 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 05-empty",
		},
		{
			name:        "reference_star_with_conflicting_alias",
			query:       "RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN *, 2 AS kept",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 06-conflict",
		},
		{
			name:        "reference_star_in_else_arm_of_false_condition",
			query:       "RETURN 7 AS kept NEXT WHEN kept = 8 THEN RETURN *, 2 AS extra ELSE RETURN *, 3 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 07-false-else",
		},
		{
			name:        "reference_star_in_untaken_branch_without_else",
			query:       "RETURN 7 AS kept NEXT WHEN kept = 8 THEN RETURN *, 2 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 08-false-no-else",
		},
		{
			name:        "reference_star_after_second_return_clause",
			query:       "RETURN 7 AS kept RETURN *, 2 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 09-standalone-star",
		},
		{
			name:        "reference_star_after_with",
			query:       "WITH 7 AS kept RETURN *, 2 AS extra",
			wantColumns: []string{"kept", "extra"},
			wantRows:    [][]interface{}{l(int64(7), int64(2))},
			established: "reference/neo4j-2025.06.json 10-valid-with-star",
		},
		{
			name:        "reference_star_branch_with_undefined_condition_variable",
			query:       "RETURN 7 AS kept NEXT WHEN missing = 7 THEN RETURN *, 2 AS extra",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "reference/neo4j-2025.06.json 11-missing",
		},
		{
			name:        "reference_single_branch_conditional",
			query:       "WHEN true THEN RETURN 2 AS extra",
			wantColumns: []string{"extra"},
			wantRows:    [][]interface{}{l(int64(2))},
			established: "reference/neo4j-2025.06.json 12-single",
		},
		{
			name:        "reference_star_with_added_column_after_next",
			query:       "RETURN 7 AS kept NEXT RETURN *, 2 AS extra",
			wantColumns: []string{"kept", "extra"},
			wantRows:    [][]interface{}{l(int64(7), int64(2))},
			established: "reference/neo4j-2025.06.json 13-next-simple",
		},
		{
			name:        "supported_when_then_else_single_column",
			query:       "WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x",
			wantColumns: []string{"x"},
			wantRows:    [][]interface{}{l(int64(1))},
			established: "TestQueryStructureMatchesNeo4j",
		},
		{
			name:        "supported_multi_arm_conditional",
			query:       "WHEN false THEN RETURN 1 AS x WHEN true THEN RETURN 2 AS x ELSE RETURN 3 AS x",
			wantColumns: []string{"x"},
			wantRows:    [][]interface{}{l(int64(2))},
			established: "TestQueryStructureMatchesNeo4j",
		},
		{
			name:        "supported_next_after_conditional",
			query:       "WHEN true THEN RETURN 1 AS x NEXT RETURN x + 1 AS y",
			wantColumns: []string{"y"},
			wantRows:    [][]interface{}{l(int64(2))},
			established: "TestQueryStructureMatchesNeo4j",
		},
		{
			name:        "supported_braced_arms_then_next",
			query:       "WHEN true THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x } NEXT RETURN x",
			wantColumns: []string{"x"},
			wantRows:    [][]interface{}{l(int64(1))},
			established: "TestQueryStructureMatchesNeo4j",
		},
		{
			name:        "supported_conditional_in_second_part",
			query:       "RETURN 1 AS a NEXT WHEN a = 1 THEN RETURN 'y' AS r ELSE RETURN 'n' AS r",
			wantColumns: []string{"r"},
			wantRows:    [][]interface{}{l("y")},
			established: "TestQueryStructureMatchesNeo4j",
		},
		{
			name:        "supported_wildcard_after_next_without_conditional",
			query:       "RETURN 1 AS a NEXT RETURN *",
			wantColumns: []string{"a"},
			wantRows:    [][]interface{}{l(int64(1))},
			established: "TestQueryStructureMatchesNeo4j",
		},
		{
			name:        "supported_invalid_conditional_arm_column_names",
			query:       "WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			established: "TestConditionalArmDiagnostics",
		},
	}
}

// TestConditionalOutputReferenceMatrix runs every matrix member through the
// public Execute path on both parsers and both normalization settings and
// checks it against the source recorded with it.
func TestConditionalOutputReferenceMatrix(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor144_matrix"))
		ctx := context.Background()
		for _, tc := range conditionalOutputCases() {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, "CYPHER 25 "+tc.query, nil)
				if tc.wantCode != "" {
					require.Error(t, err, "%s rejects this statement", tc.established)
					require.Equal(t, tc.wantCode, errorStatusCode(err), "expected the rejection recorded by %s: %v", tc.established, err)
					return
				}
				require.NoError(t, err, "expected the success recorded by %s", tc.established)
				require.Equal(t, tc.wantColumns, result.Columns, "columns recorded by %s", tc.established)
				require.ElementsMatch(t, tc.wantRows, result.Rows, "rows recorded by %s", tc.established)
			})
		}
	})
}

// TestConditionalArmReturnsInheritedVariable is the reported defect: the
// conditional's arm returns the variable the part before the conditional bound
// and adds a column, and the answer must be the reference's.
func TestConditionalArmReturnsInheritedVariable(t *testing.T) {
	l := func(values ...interface{}) []interface{} { return values }
	for _, tc := range []struct {
		name    string
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{
			name:    "reported statement",
			query:   "RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN kept, 2 AS extra",
			columns: []string{"kept", "extra"},
			rows:    [][]interface{}{l(int64(7), int64(2))},
		},
		{
			name:    "chosen else arm",
			query:   "RETURN 7 AS kept NEXT WHEN kept = 8 THEN RETURN kept, 1 AS extra ELSE RETURN kept, 2 AS extra",
			columns: []string{"kept", "extra"},
			rows:    [][]interface{}{l(int64(7), int64(2))},
		},
		{
			name:    "two inherited columns",
			query:   "RETURN 7 AS kept, 9 AS other NEXT WHEN kept = 7 THEN RETURN kept, other, 2 AS extra",
			columns: []string{"kept", "other", "extra"},
			rows:    [][]interface{}{l(int64(7), int64(9), int64(2))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachCypherParserSetting(t, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor144_output"))
				result, err := exec.Execute(context.Background(), "CYPHER 25 "+tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.columns, result.Columns)
				require.Equal(t, tc.rows, result.Rows)
			})
		})
	}
}

// An arm that returns another value under the inherited name still declares
// that name again, so the imported-variable conflict keeps its rejection
// (TestScopedImportExportExtensionBoundaries,
// TestCallSubqueryColumnsCannotRedeclareOuterVariables). Only the unchanged
// projection passes.
func TestConditionalArmInheritedVariableKeepsItsConflict(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor144_conflict"))
		ctx := context.Background()
		requireStatementCode(t, exec, "CYPHER 25 RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN 2 AS kept, 1 AS extra", "Neo.ClientError.Statement.SyntaxError")
		requireStatementCode(t, exec, "CYPHER 25 RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN kept + 1 AS kept, 1 AS extra", "Neo.ClientError.Statement.SyntaxError")

		// The one extension #907 already grants, unchanged by this fix: a CALL
		// subquery returns the outer variable it imported as itself.
		result, err := exec.Execute(ctx, "CYPHER 25 WITH 7 AS kept CALL (*) { RETURN kept, 2 AS extra } RETURN kept, extra", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"kept", "extra"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(7), int64(2)}}, result.Rows)
	})
}
