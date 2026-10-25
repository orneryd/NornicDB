package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise the embedded Go API: literal and WITH expressions can normalize
// temporal representations before APOC sees them and concealed this defect.
func TestApocCollectionsTemporalParameters(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	otherZone := time.FixedZone("other", 0)
	otherDate := time.Date(1999, 7, 8, 3, 4, 5, 6, otherZone)
	otherTime := base.In(otherZone)
	for _, tc := range []struct {
		name    string
		a, b, x interface{}
	}{
		{"date", CypherDate{Time: base}, &CypherDate{Time: base.Add(time.Hour)}, CypherDate{Time: base.AddDate(0, 0, 1)}},
		{"localtime", CypherLocalTime{Time: base}, &CypherLocalTime{Time: otherDate}, CypherLocalTime{Time: base.Add(time.Second)}},
		{"time", CypherTime{Time: base}, &CypherTime{Time: otherDate}, CypherTime{Time: base.Add(time.Second)}},
		{"localdatetime", CypherLocalDateTime{Time: base}, &CypherLocalDateTime{Time: otherTime}, CypherLocalDateTime{Time: base.Add(time.Second)}},
		{"datetime", CypherDateTime{Time: base}, &CypherDateTime{Time: otherTime}, CypherDateTime{Time: base.Add(time.Second)}},
		{"go-time", base, &otherTime, base.Add(time.Second)},
		{"duration", &CypherDuration{Hours: 1}, &CypherDuration{Minutes: 60}, &CypherDuration{Minutes: 61}},
		{"duration-value", CypherDuration{Years: 1}, &CypherDuration{Months: 12}, CypherDuration{Months: 13}},
		{"datetime-carriers", &CypherDateTime{Time: base}, otherTime, base.Add(time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, shape := range []string{"scalar", "list", "map"} {
				t.Run(shape, func(t *testing.T) {
					wrap := func(value interface{}) interface{} {
						switch shape {
						case "list":
							return []interface{}{value}
						case "map":
							return map[string]interface{}{"nested": []interface{}{value}}
						default:
							return value
						}
					}
					a, b, x := wrap(tc.a), wrap(tc.b), wrap(tc.x)
					require.Equal(t, true, cypherEquality(a, b))
					require.Equal(t, false, cypherEquality(a, x))
					for _, programs := range []bool{true, false} {
						t.Run(fmt.Sprintf("programs=%v", programs), func(t *testing.T) {
							rowValueProgramsDisabled.Store(!programs)
							defer rowValueProgramsDisabled.Store(false)
							// One public query covers every equality/set consumer found
							// by codeintel and the source inventory, including split.
							exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal-apoc"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
							result, err := exec.Execute(context.Background(), `RETURN
								$a = $b AS equal,
								apoc.coll.contains([$a], $b) AS contained,
								apoc.coll.containsAll([$a], [$b]) AS allContained,
								apoc.coll.containsAny([$a], [$x, $b]) AS anyContained,
								apoc.coll.indexOf([$x, $a], $b) AS position,
								apoc.coll.occurrences([$a, $x, $b], $b) AS occurrences,
								size(apoc.coll.split([0, $a, 1, $a, 2], $b)) AS partitions,
								size(apoc.coll.toSet([$a, $b, $x])) AS distinctCount,
								size(apoc.coll.union([$a], [$b, $x])) AS unionCount,
								size(apoc.coll.intersection([$a, $x], [$b])) AS intersectionCount,
								size(apoc.coll.subtract([$a, $x], [$b])) AS subtractionCount,
								size(apoc.coll.frequencies([$a, $b, $x])) AS frequencyCount,
								apoc.coll.contains([$a], $x) AS different`, map[string]interface{}{"a": a, "b": b, "x": x})
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{true, true, true, true, int64(1), int64(2), int64(3), int64(2), int64(2), int64(1), int64(1), int64(2), false}}, result.Rows)
						})
					}
				})
			}
		})
	}
}

func TestTemporalEquivalenceCanonicalOwner(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	for _, tc := range []struct {
		name  string
		a, b  interface{}
		equal bool
	}{
		{"hours-minutes", &CypherDuration{Hours: 1}, &CypherDuration{Minutes: 60}, true},
		{"years-months", CypherDuration{Years: 1}, &CypherDuration{Months: 12}, true},
		{"days-hours", &CypherDuration{Days: 1}, &CypherDuration{Hours: 24}, false},
		{"temporal-kinds", CypherDate{Time: base}, CypherLocalDateTime{Time: base}, false},
		{"zone-identity", CypherDateTime{Time: base, ZoneID: "Europe/London"}, CypherDateTime{Time: base}, false},
		{"offset-identity", CypherTime{Time: base}, CypherTime{Time: time.Date(2024, 1, 2, 3, 4, 5, 6, time.FixedZone("offset", 3600))}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.equal, cypherEquality(tc.a, tc.b))
			require.Equal(t, tc.equal, cypherValueKey(tc.a, cypherEquivalenceContract) == cypherValueKey(tc.b, cypherEquivalenceContract))
			require.Equal(t, tc.equal, cypherValueEquals(tc.a, tc.b))
		})
	}
}
