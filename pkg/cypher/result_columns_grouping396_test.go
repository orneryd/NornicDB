package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanonicalProjectionGeneratedMatrix(t *testing.T) {
	require.Len(t, statementfactstest.ProjectionCases(), 971)
	testCanonicalGroupingMatrix396(t, statementfactstest.ProjectionCases(), statementfactstest.NewProjectionOracle(t))
}

func testCanonicalGroupingMatrix396(t *testing.T, samples []statementfactstest.Case, oracle *statementfactstest.ProjectionOracle) {
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, sample := range samples {
				t.Run(sample.Name, func(t *testing.T) {
					// Establish the oracle even when a native assertion is red.
					if reference != nil {
						result, err := neo4j.ExecuteQuery(ctx, reference, "EXPLAIN "+oracle.ReferenceCase(sample).Query, nil, neo4j.EagerResultTransformer)
						if sample.Code != "" {
							require.Error(t, err)
							status, ok := err.(*neo4j.Neo4jError)
							require.True(t, ok, "%v", err)
							require.Equal(t, sample.Code, status.Code)
						} else {
							require.NoError(t, err)
							require.Equal(t, sample.Columns, result.Keys)
						}
					}
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
					if oracle != nil {
						_, err := executor.Execute(ctx, "CREATE (:ProjectionSemanticSentinel)", nil)
						require.NoError(t, err)
					}
					columns, err := executor.StatementResultColumns(sample.Query, RawSource)
					if sample.Code != "" {
						if err == nil {
							t.Errorf("raw columns accepted invalid query: %s", sample.Query)
						} else {
							requireStatusCode(t, err, sample.Code)
						}
					} else {
						require.NoError(t, err)
						require.Equal(t, sample.Columns, columns)
					}
					executor.StatementFacts(sample.Query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
						if sample.Code != "" {
							t.Errorf("facts accepted invalid query: %s", sample.Query)
						} else {
							statement.Columns.Visit(func(actual []string) { require.Equal(t, sample.Columns, actual) }, func() { t.Fatal("known facts have unknown columns") })
						}
					}, func() { t.Fatal("facts declined without a continuation") }, func(reason resultmetadata.Unresolved) {
						if sample.Code == "" {
							t.Fatalf("facts unexpectedly unresolved: %v", reason.Err)
						}
						requireStatusCode(t, reason.Err, sample.Code)
					})
					for _, explicit := range []bool{false, true} {
						for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
							t.Run(fmt.Sprintf("explicit=%v/mode=%s", explicit, prefix), func(t *testing.T) {
								referenceRows := oracle.Check(t, sample, prefix, explicit)
								if explicit {
									_, err := executor.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
									defer executor.Execute(ctx, "ROLLBACK", nil)
								}
								result, err := executor.Execute(ctx, prefix+sample.Query, nil)
								if sample.Code != "" {
									require.Error(t, err)
									requireStatusCode(t, err, sample.Code)
								} else {
									require.NoError(t, err)
									require.Equal(t, sample.Columns, result.Columns)
									oracle.CheckRows(t, prefix, referenceRows, result.Rows)
								}
							})
						}
					}
				})
			}
		})
	}
}

// The merged identity-projection fixture is changed only after this public
// differential establishes grouping's meaning, including order and controls.
func TestCanonicalGroupingPassThroughReference(t *testing.T) {
	oracle := statementfactstest.NewProjectionOracle(t)
	if oracle == nil {
		t.Skip("live Neo4j reference required for merged-fixture contract")
	}
	testCanonicalGroupingMatrix396(t, []statementfactstest.Case{
		{Name: "grouped_alias", Query: "WITH 7 AS x WITH (x) AS x ORDER BY (x) RETURN (x)", Columns: []string{"x"}},
		{Name: "plain_alias_control", Query: "WITH 7 AS x WITH x AS x ORDER BY x RETURN x", Columns: []string{"x"}},
		{Name: "grouped_unaliased", Query: "WITH 7 AS x WITH (x) ORDER BY ((x)) RETURN x", Columns: []string{"x"}},
		{Name: "grouped_empty", Query: "WITH 7 AS `` WITH ((``)) AS `` ORDER BY (``) RETURN ((``))", Columns: []string{""}},
		{Name: "grouped_keyword", Query: "WITH 7 AS `WHERE` WITH (((`WHERE`))) ORDER BY ((`WHERE`)) RETURN (`WHERE`)", Columns: []string{"WHERE"}},
		{Name: "literal_alias_control", Query: "WITH (1) AS x RETURN x", Columns: []string{"x"}},
		{Name: "compound_alias_control", Query: "WITH 7 AS x WITH (x + 0) AS x ORDER BY x RETURN x", Columns: []string{"x"}},
		{Name: "compound_display_control", Query: "WITH 7 AS x RETURN (x+1)", Columns: []string{"(x+1)"}},
		{Name: "literal_display_control", Query: "RETURN (1)", Columns: []string{"(1)"}},
		{Name: "renamed_control", Query: "WITH 7 AS x RETURN (x) AS y", Columns: []string{"y"}},
	}, oracle)
}

func TestCanonicalProjectionPermutationMatrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.ProjectionPermutationCases())
	require.Len(t, samples, 1344)
	testCanonicalGroupingMatrix396(t, samples, oracle)
}

func TestCanonicalNestedScopeMatrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.NestedScopeCases())
	testCanonicalGroupingMatrix396(t, samples, oracle)
}

func TestCanonicalNestedScope25Matrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.NestedScope25Cases())
	testCanonicalGroupingMatrix396(t, samples, oracle)
}
