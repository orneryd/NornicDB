package cypher

// NOR-144 reproduction: a Cypher 25 conditional whose branch ends in RETURN *.
//
// Reported statement:
//
//	CYPHER 25 RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN *, 2 AS extra
//
// The statement is rejected on both parser paths, and the official Cypher 25
// reference says that rejection is correct: on Neo4j 2025.06.2 Community
// (image digest sha256:0347fe5b1126dd328cc68a54449c2cf83636e348e2da1547abf62b2dbfad67f2)
// the statement fails with Neo.ClientError.Statement.SyntaxError, "RETURN * is
// not allowed when there are no variables in scope". The proposed columns
// kept, extra and row [7, 2] are NOT the reference answer for the star form.
// They are the reference answer for the explicit spelling
// RETURN kept, 2 AS extra, and that spelling is where this package currently
// diverges (see TestNOR144ExplicitConditionalColumnReprojection). Neo4j 5.26
// cannot serve as the reference here.
//
// Every expectation below is taken from one of two sources: the parent-captured
// reference in
// openspec/changes/nor144-v151-reproduction/reference/neo4j-2025.06.json, or an
// existing supported package test. No expectation is derived from this
// implementation's own output. Each case runs through the public Execute path
// on both parser paths and both normalization settings, and the harness logs
// the observed columns, rows, error text and status code.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor144Observation is one public-Execute observation of one statement.
type nor144Observation struct {
	Parser        string          `json:"parser"`
	Normalization bool            `json:"normalization"`
	Query         string          `json:"query"`
	Columns       []string        `json:"columns,omitempty"`
	Rows          [][]interface{} `json:"rows,omitempty"`
	Error         string          `json:"error,omitempty"`
	Code          string          `json:"code,omitempty"`
}

// nor144Case is one matrix member and the source of its expectation.
type nor144Case struct {
	name string
	// query is the statement without the CYPHER 25 prefix the harness adds.
	query string
	// wantColumns and wantRows are the expected successful result.
	wantColumns []string
	wantRows    [][]interface{}
	// wantCode is the expected status code of a rejected statement.
	wantCode string
	// established names the reference capture entry or existing test that the
	// expectation comes from.
	established string
}

// nor144Cases is the reproduction matrix. The reference_* cases are cases 01
// to 13 of the reference capture; the supported_* cases are the conditional
// shapes this package's existing tests already assert. The explicit spelling of
// the reported query is not here: it is the open divergence, and its own test
// asserts it separately so the reproduced failure stays unambiguous.
func nor144Cases() []nor144Case {
	l := func(values ...interface{}) []interface{} { return values }
	return []nor144Case{
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

// nor144Execute runs one statement through the public Execute path under the
// given parser and normalization setting.
func nor144Execute(t *testing.T, parser string, normalization bool, query string) nor144Observation {
	t.Helper()
	previousParser := config.GetParserType()
	previousNormalization := config.IsCypherQueryNormalizationEnabled()
	config.SetParserType(parser)
	config.SetCypherQueryNormalizationEnabled(normalization)
	t.Cleanup(func() {
		config.SetParserType(previousParser)
		config.SetCypherQueryNormalizationEnabled(previousNormalization)
	})

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor144"))
	observation := nor144Observation{Parser: parser, Normalization: normalization, Query: query}
	result, err := exec.Execute(context.Background(), query, nil)
	if err != nil {
		observation.Error = err.Error()
		observation.Code = errorStatusCode(err)
	} else {
		observation.Columns = result.Columns
		observation.Rows = result.Rows
	}
	encoded, marshalErr := json.Marshal(observation)
	require.NoError(t, marshalErr)
	t.Logf("NOR144 observation %s", encoded)
	return observation
}

// nor144ObserveParsers runs one statement on both parsers under both
// normalization settings when normalize is true, and on both parsers with the
// process setting when it is false.
func nor144ObserveParsers(t *testing.T, query string, normalize bool) []nor144Observation {
	t.Helper()
	settings := []bool{config.IsCypherQueryNormalizationEnabled()}
	if normalize {
		settings = []bool{false, true}
	}
	observations := make([]nor144Observation, 0, 4)
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, normalization := range settings {
			observations = append(observations, nor144Execute(t, parser, normalization, query))
		}
	}
	return observations
}

// nor144RequireParserAgreement asserts that every observation reports the same
// status code, columns and rows. Error text is parser-owned and may differ.
func nor144RequireParserAgreement(t *testing.T, observations []nor144Observation) {
	t.Helper()
	first := observations[0]
	for _, observation := range observations[1:] {
		require.Equal(t, first.Code, observation.Code,
			"parser %s normalization %v disagrees on status code: %s", observation.Parser, observation.Normalization, observation.Error)
		require.Equal(t, first.Columns, observation.Columns,
			"parser %s normalization %v disagrees on columns", observation.Parser, observation.Normalization)
		require.Equal(t, first.Rows, observation.Rows,
			"parser %s normalization %v disagrees on rows", observation.Parser, observation.Normalization)
	}
}

// TestNOR144ConditionalStarReferenceMatrix records the current public behavior
// of every matrix member on both parsers and both normalization settings,
// asserts those runs agree, and checks each expectation against its recorded
// source.
func TestNOR144ConditionalStarReferenceMatrix(t *testing.T) {
	for _, tc := range nor144Cases() {
		t.Run(tc.name, func(t *testing.T) {
			observations := nor144ObserveParsers(t, "CYPHER 25 "+tc.query, true)
			nor144RequireParserAgreement(t, observations)
			first := observations[0]

			if tc.wantCode != "" {
				require.Equal(t, tc.wantCode, first.Code,
					"expected the rejection recorded by %s, got: %s", tc.established, first.Error)
				require.NotEmpty(t, first.Error, "a rejection carries an error message")
				return
			}
			require.Empty(t, first.Error, "expected the success recorded by %s", tc.established)
			if tc.wantColumns != nil {
				require.Equal(t, tc.wantColumns, first.Columns, "columns recorded by %s", tc.established)
			} else {
				require.NotEmpty(t, first.Columns, "a successful statement declares its columns")
			}
			require.ElementsMatch(t, tc.wantRows, first.Rows, "rows recorded by %s", tc.established)
		})
	}
}

// TestNOR144ExplicitConditionalColumnReprojection is the reproduced defect:
// the reference accepts the explicit spelling of the reported query and returns
// kept, extra / [7, 2], while this package rejects it with
//
//	Variable `kept` already declared in outer scope
//
// on both parsers. The conditional rewrite lowers the branch to a guarded
// CALL (*) subquery, and a nested CALL (*) that re-projects a variable of the
// enclosing statement is what the scope owner rejects; the minimized form is
// logged by TestNOR144LoweredFormBoundaryDiagnostic. No production change is
// part of this reproduction, so this assertion is expected to fail until the
// owner is corrected.
func TestNOR144ExplicitConditionalColumnReprojection(t *testing.T) {
	query := "CYPHER 25 RETURN 7 AS kept NEXT WHEN kept = 7 THEN RETURN kept, 2 AS extra"
	observations := nor144ObserveParsers(t, query, true)
	nor144RequireParserAgreement(t, observations)
	first := observations[0]

	require.Empty(t, first.Error,
		"reference/neo4j-2025.06.json 02-explicit returns kept, extra / [7, 2]; observed: %s", first.Error)
	require.Equal(t, []string{"kept", "extra"}, first.Columns)
	require.Equal(t, [][]interface{}{{int64(7), int64(2)}}, first.Rows)
}

// TestNOR144LoweredFormBoundaryDiagnostic records the rewrite stage's answer
// for the conditional cases and localizes the reproduced rejection to the
// lowered frame. The statements are lowered without the CYPHER 25 prefix
// because Execute reads the prefix before it lowers; the composition rewrite
// itself is version-independent.
//
// The minimized statements after the matrix are this reproduction's own
// spellings of the lowered frame, not reference cases; they are logged and
// checked for parser agreement only.
func TestNOR144LoweredFormBoundaryDiagnostic(t *testing.T) {
	for _, tc := range nor144Cases() {
		if !strings.Contains(strings.ToUpper(tc.query), "WHEN ") {
			continue
		}
		t.Run("lowering/"+tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor144_lowering"))
			rewritten, rewrite, err := desugarQueryStructure(context.Background(), tc.query, exec)
			if err != nil {
				t.Logf("NOR144 lowering name=%s error=%q code=%q", tc.name, err.Error(), errorStatusCode(err))
				return
			}
			require.NotNil(t, rewrite, "a conditional query is rewritten")
			t.Logf("NOR144 lowering name=%s rewritten=%q", tc.name, rewritten)
		})
	}

	for _, probe := range []struct{ name, query string }{
		{
			name:  "arm_candidate",
			query: "CALL (*) { RETURN 7 AS kept } RETURN kept, 2 AS extra",
		},
		{
			name:  "one_level",
			query: "WITH 7 AS kept CALL (*) { RETURN kept, 2 AS extra } RETURN *",
		},
		{
			name:  "one_level_with_filter",
			query: "WITH 7 AS kept CALL (*) { FILTER true RETURN kept, 2 AS extra } RETURN *",
		},
		{
			name:  "nested",
			query: "WITH 7 AS kept CALL (*) { CALL (*) { RETURN kept, 2 AS extra } RETURN kept, extra } RETURN *",
		},
		{
			name:  "nested_with_filter",
			query: "WITH 7 AS kept CALL (*) { FILTER true CALL (*) { RETURN kept, 2 AS extra } RETURN kept, extra } RETURN *",
		},
		{
			name:  "nested_renamed",
			query: "WITH 7 AS kept CALL (*) { FILTER true CALL (*) { RETURN kept AS k } RETURN k AS kept } RETURN *",
		},
	} {
		t.Run("probe/"+probe.name, func(t *testing.T) {
			observations := nor144ObserveParsers(t, "CYPHER 25 "+probe.query, false)
			nor144RequireParserAgreement(t, observations)
			if first := observations[0]; first.Error != "" {
				require.NotEmpty(t, first.Code, "a rejection carries a status code: %s", first.Error)
			}
		})
	}
}
