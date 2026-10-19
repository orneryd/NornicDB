package cypher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Measure the actual warmed production index, excluding result/map setup.
func TestTemporalContractIndexAllocations(t *testing.T) {
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	values := []interface{}{CypherDate{Time: stamp}, &CypherDate{Time: stamp}, CypherLocalTime{Time: stamp}, &CypherLocalTime{Time: stamp}, CypherTime{Time: stamp}, &CypherTime{Time: stamp}, CypherLocalDateTime{Time: stamp}, &CypherLocalDateTime{Time: stamp}, CypherDateTime{Time: stamp, ZoneID: "UTC"}, &CypherDateTime{Time: stamp, ZoneID: "UTC"}, stamp, &stamp, CypherDuration{Nanos: -500000000}, &CypherDuration{Seconds: -1, Nanos: 500000000}}
	for _, contract := range []cypherComparisonContract{cypherValueEqualityContract, cypherEquivalenceContract} {
		index := cypherValueSet{}
		for _, value := range values {
			index.addPositionByContract(value, contract)
		}
		allocations := testing.AllocsPerRun(100, func() {
			for _, value := range values {
				index.addPositionByContract(value, contract)
				found := index.hasByContract(value, contract)
				if !found {
					panic("missing temporal value")
				}
			}
		})
		require.Zero(t, allocations, "production index/all fourteen carriers, contract %v", contract)
	}
}
