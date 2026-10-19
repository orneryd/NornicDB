package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Canonicalization and lookup keys allocate nothing per item. Container/result
// setup is excluded, just as in the reviewer's reusable-map layout probe.
func TestTemporalCanonicalKeyAllocations(t *testing.T) {
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	values := []interface{}{CypherDateTime{Time: stamp}, &CypherDateTime{Time: stamp}, stamp, &stamp}
	typed := make(map[temporalValueKey]int, 4)
	allocations := testing.AllocsPerRun(100, func() {
		clear(typed)
		for _, value := range values {
			key, ok := temporalIdentityParts(value, cypherValueEqualityContract)
			if !ok {
				panic("missing temporal key")
			}
			temporalCanonicalKeySink = key
			typed[key]++
		}
	})
	require.Len(t, typed, 1)
	require.Equal(t, 4, typed[temporalCanonicalKeySink])
	require.Zero(t, allocations, "typed canonical keys/four datetime carriers")
	index := cypherValueSet{}
	index.addByContract(values[0], cypherEquivalenceContract)
	allocations = testing.AllocsPerRun(100, func() {
		clear(index.temporal)
		index.count = 0
		for _, value := range values {
			index.addPositionByContract(value, cypherEquivalenceContract)
			if !index.hasByContract(value, cypherEquivalenceContract) {
				panic("missing value")
			}
			if !cypherValueEquals(value, values[0]) {
				panic("unequal value")
			}
		}
	})
	require.Zero(t, allocations, "production lookup/four datetime carriers")
	require.Equal(t, 1, index.count)
	for _, value := range []interface{}{CypherDate{Time: stamp}, &CypherDate{Time: stamp}, CypherLocalTime{Time: stamp}, &CypherLocalTime{Time: stamp}, CypherTime{Time: stamp}, &CypherTime{Time: stamp}, CypherLocalDateTime{Time: stamp}, &CypherLocalDateTime{Time: stamp}, CypherDuration{Nanos: -500000000}, &CypherDuration{Seconds: -1, Nanos: 500000000}} {
		require.Zero(t, testing.AllocsPerRun(100, func() { temporalCanonicalKeySink, _ = temporalIdentityParts(value, cypherValueEqualityContract) }), "%T", value)
	}
}

var temporalCanonicalKeySink temporalValueKey

// Both contracts keep all fourteen temporal carriers allocation-free.
func TestTemporalContractKeyAllocations(t *testing.T) {
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	values := []interface{}{CypherDate{Time: stamp}, &CypherDate{Time: stamp}, CypherLocalTime{Time: stamp}, &CypherLocalTime{Time: stamp}, CypherTime{Time: stamp}, &CypherTime{Time: stamp}, CypherLocalDateTime{Time: stamp}, &CypherLocalDateTime{Time: stamp}, CypherDateTime{Time: stamp, ZoneID: "UTC"}, &CypherDateTime{Time: stamp, ZoneID: "UTC"}, stamp, &stamp, CypherDuration{Nanos: -500000000}, &CypherDuration{Seconds: -1, Nanos: 500000000}}
	for _, contract := range []cypherComparisonContract{cypherValueEqualityContract, cypherEquivalenceContract} {
		for _, value := range values {
			if contract == cypherEquivalenceContract {
				require.Zero(t, testing.AllocsPerRun(100, func() {
					if !apocSameValue(value, value) {
						panic("map.clean lost its own temporal occurrence")
					}
				}), "map.clean temporal key %T", value)
			}
			allocations := testing.AllocsPerRun(100, func() {
				temporalCanonicalKeySink, _ = temporalIdentityParts(value, contract)
				if equal, temporal := compareTemporalByContract(value, value, contract); !equal || !temporal {
					panic("not equal")
				}
			})
			require.Zero(t, allocations, "contract %v, %T", contract, value)
		}
	}
	london, err := time.LoadLocation("Europe/London")
	require.NoError(t, err)
	early := CypherDateTime{Time: time.Unix(1729989000, 0).In(london), ZoneID: "Europe/London"}
	late := CypherDateTime{Time: time.Unix(1729992600, 0).In(london), ZoneID: "Europe/London"}
	for _, contract := range []cypherComparisonContract{cypherValueEqualityContract, cypherEquivalenceContract} {
		keys := make(map[temporalValueKey]int)
		left, _ := temporalIdentityParts(early, contract)
		right, _ := temporalIdentityParts(late, contract)
		keys[left]++
		keys[right]++
		require.Len(t, keys, map[cypherComparisonContract]int{cypherValueEqualityContract: 1, cypherEquivalenceContract: 2}[contract])
		overlapValues := []interface{}{early, late}
		allocations := testing.AllocsPerRun(100, func() {
			clear(keys)
			for _, value := range overlapValues {
				key, _ := temporalIdentityParts(value, contract)
				keys[key]++
			}
		})
		require.Zero(t, allocations, "overlap keys, contract %v", contract)
	}
}
