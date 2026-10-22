package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reuse the merged comparison owner's independent Neo4j oracle. A hidden NaN
// import must stay out of DISTINCT keys while projected temporal values retain
// their overlap occurrence, recursively through lists and maps.
func TestScopedCallDistinctTemporalContracts(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	for _, tc := range temporalContractCases(t) {
		t.Run(tc.Name, func(t *testing.T) {
			values, err := exec.Execute(ctx, "RETURN "+tc.A+" AS a, "+tc.B+" AS b", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "WITH 0/0.0 AS hidden, $values AS values CALL (hidden, values) { UNWIND values AS v WITH DISTINCT v RETURN count(*) AS n } RETURN n", map[string]interface{}{"values": values.Rows[0]})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(tc.DistinctRows)}}, result.Rows)
			if tc.Equality != true || tc.Equivalence {
				return
			}
			for _, contract := range []cypherComparisonContract{cypherValueEqualityContract, cypherEquivalenceContract} {
				left, right := values.Rows[0][0], values.Rows[0][1]
				var index cypherValueIndex
				index.addByContract(left, contract)
				_, duplicate := index.addByContract(right, contract)
				require.Equal(t, contract == cypherValueEqualityContract, duplicate)
				var collisions cypherValueIndex
				collisions.addTupleValuesByContract(42, []interface{}{left}, contract)
				_, duplicate = collisions.addTupleValuesByContract(42, []interface{}{right}, contract)
				require.Equal(t, contract == cypherValueEqualityContract, duplicate, "forced collision")
				if contract == cypherValueEqualityContract {
					require.Equal(t, cypherValueHash(left, contract), cypherValueHash(right, contract))
				}
			}
		})
	}
}
