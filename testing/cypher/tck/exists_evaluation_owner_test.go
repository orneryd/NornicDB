package tck

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/subqueryeval"
	"github.com/stretchr/testify/require"
)

func TestExistsSubqueryEvaluationOwnerBolt(t *testing.T) {
	var cases []differentialCase
	for _, tc := range subqueryeval.Cases() {
		cases = append(cases, differentialCase{Name: tc.Name, Setup: subqueryeval.Setup, Query: tc.Query, Parameters: map[string]any{"nonce": int64(1)}, NoEffects: true})
	}
	runDifferentialCases(t, cases)
}

// The Execute API is checked against the live reference too, using the same
// cases as the wire routes instead of relying only on pinned expected values.
func TestExistsSubqueryEvaluationOwnerExecute(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires live Neo4j reference")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())
	require.NoError(t, waitForReference(ctx, driver))
	ref := newDifferentialBackend(t, driver, "neo4j", AutocommitMode)
	resetDifferentialBackend(t, ctx, ref, "Neo4j")
	defer resetDifferentialBackend(t, context.Background(), ref, "Neo4j")
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "exists_execute_reference")
	defer store.Close()
	exec := cypher.NewStorageExecutor(store)
	for _, setup := range subqueryeval.Setup {
		executeDifferentialSetup(t, ctx, ref, "Neo4j", setup)
		_, err := exec.Execute(ctx, setup, nil)
		require.NoError(t, err)
	}
	for i, tc := range subqueryeval.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			params := map[string]any{"nonce": int64(i + 1)}
			expected, err := ref.Execute(ctx, tc.Query, params)
			require.NoError(t, err)
			actual, err := exec.Execute(ctx, tc.Query, params)
			require.NoError(t, err)
			require.Equal(t, expected.Columns, actual.Columns)
			require.Equal(t, expected.Rows, actual.Rows)
		})
	}
}
