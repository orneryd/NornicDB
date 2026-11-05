package cypher

import (
	"context"
	"os"
	"testing"

	fixturecontract "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"

	"github.com/stretchr/testify/require"
)

// Empty row streams must reach the final aggregation without evaluating n in
// a correlated CALL or in later OPTIONAL MATCH property maps.
func TestZeroRowCorrelatedCallAggregation(t *testing.T) {
	for _, fixture := range []string{"row_zero_aggregate.cypher", "row_zero_aggregate_minimal.cypher"} {
		query, err := os.ReadFile("testdata/" + fixture)
		require.NoError(t, err)
		for _, explicit := range []bool{false, true} {
			mode := "auto"
			if explicit {
				mode = "explicit"
			}
			for _, populated := range []bool{false, true} {
				name := "empty"
				if populated {
					name = "nonempty"
				}
				t.Run(fixture+"/"+mode+"/"+name, func(t *testing.T) {
					testCase := fixturecontract.ZeroRowCorrelatedCallCaseFor(populated)
					base := fixturecontract.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "zero_row_aggregate"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, fixturecontract.ZeroRowCorrelatedCallSeedCypher, nil)
					require.NoError(t, err)
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := exec.Execute(ctx, string(query), testCase.Params)
					require.NoError(t, err)
					fixturecontract.AssertZeroRowCorrelatedCallRows(t, result.Rows, testCase)
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
