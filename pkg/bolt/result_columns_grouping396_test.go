package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltProjectionGeneratedMatrix(t *testing.T) {
	testBoltGroupingMatrix396(t, statementfactstest.ProjectionCases(), statementfactstest.NewProjectionOracle(t))
}

func testBoltGroupingMatrix396(t *testing.T, samples []statementfactstest.Case, oracle *statementfactstest.ProjectionOracle) {
	ctx := context.Background()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "tenant"))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	if oracle != nil {
		_, err := neo4j.ExecuteQuery(ctx, driver, "CREATE (:ProjectionSemanticSentinel)", nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
	}
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, sample := range samples {
				for _, explicit := range []bool{false, true} {
					for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("%s/explicit=%v/mode=%s", sample.Name, explicit, prefix), func(t *testing.T) {
							referenceRows := oracle.Check(t, sample, prefix, explicit)
							session := driver.NewSession(ctx, neo4j.SessionConfig{})
							defer session.Close(ctx)
							var result neo4j.ResultWithContext
							var failure error
							if explicit {
								tx, err := session.BeginTransaction(ctx)
								require.NoError(t, err)
								defer tx.Rollback(ctx)
								result, failure = tx.Run(ctx, prefix+sample.Query, nil)
							} else {
								result, failure = session.Run(ctx, prefix+sample.Query, nil)
							}
							var columns []string
							var rows [][]any
							if failure == nil {
								columns, failure = result.Keys()
							}
							if failure == nil {
								var records []*neo4j.Record
								records, failure = result.Collect(ctx)
								for _, record := range records {
									rows = append(rows, record.Values)
								}
							}
							if sample.Code != "" {
								require.Error(t, failure)
								var status *neo4j.Neo4jError
								require.True(t, errors.As(failure, &status), "%v", failure)
								require.Equal(t, sample.Code, status.Code)
							} else {
								require.NoError(t, failure)
								require.Equal(t, sample.Columns, columns)
								oracle.CheckRows(t, prefix, referenceRows, rows)
							}
						})
					}
				}
			}
		})
	}
}

func TestBoltProjectionPermutationMatrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.ProjectionPermutationCases())
	require.Len(t, samples, 1344)
	testBoltGroupingMatrix396(t, samples, oracle)
}

func TestBoltNestedScopeMatrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.NestedScopeCases())
	testBoltGroupingMatrix396(t, samples, oracle)
}

func TestBoltNestedScope25Matrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.NestedScope25Cases())
	testBoltGroupingMatrix396(t, samples, oracle)
}
