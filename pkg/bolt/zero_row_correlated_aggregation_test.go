package bolt

import (
	"os"
	"testing"

	fixturecontract "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/stretchr/testify/require"
)

func TestBoltZeroRowCorrelatedCallAggregation(t *testing.T) {
	_, port := startTransactionLifecycleServer(t)
	seed := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, seed, fixturecontract.ZeroRowCorrelatedCallSeedCypher, nil)
	require.NoError(t, seed.Close())
	for _, fixture := range []string{"row_zero_aggregate.cypher", "row_zero_aggregate_minimal.cypher"} {
		query, err := os.ReadFile("../cypher/testdata/" + fixture)
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
					conn := openBoltTestConn(t, port)
					defer conn.Close()
					if explicit {
						beginExplicitTransaction(t, conn, nil)
					}
					records := runBoltQueryWithParams(t, conn, string(query), testCase.Params)
					fixturecontract.AssertZeroRowCorrelatedCallRows(t, records, testCase)
					if explicit {
						require.NoError(t, SendCommit(t, conn))
						require.NoError(t, ReadSuccess(t, conn))
					}
				})
			}
		}
	}
}
