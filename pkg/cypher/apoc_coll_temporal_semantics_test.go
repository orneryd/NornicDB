package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Independent public-path probes. Expected values come from Neo4j's temporal
// contract, rather than agreement with NornicDB's equality implementation.
func TestApocCollectionsTemporalSemantics(t *testing.T) {
	london, err := time.LoadLocation("Europe/London")
	require.NoError(t, err)
	named := time.Date(2024, 7, 1, 12, 34, 56, 7, london)
	fixed := time.Date(1999, 1, 2, 12, 34, 56, 7, time.FixedZone("", 3600))
	cases := []struct {
		name, a, b string
		params     map[string]interface{}
		equal      bool
	}{
		{"date", "date('2024-01-02')", "date({year:2024,month:1,day:2})", nil, true},
		{"localtime", "localtime('12:34:56.000000007')", "localtime({hour:12,minute:34,second:56,nanosecond:7})", nil, true},
		{"time-offset", "time('12:34:56+01:00')", "time('11:34:56Z')", nil, false},
		{"localdatetime", "localdatetime('2024-01-02T12:34:56')", "localdatetime({year:2024,month:1,day:2,hour:12,minute:34,second:56})", nil, true},
		{"datetime-offset", "datetime('2024-01-02T12:00Z')", "datetime('2024-01-02T13:00+01:00')", nil, false},
		{"datetime-name-offset", "datetime('2024-07-01T12:00+01:00[Europe/London]')", "datetime('2024-07-01T12:00+01:00')", nil, false},
		{"datetime-overlap", "datetime('2024-10-27T01:30+01:00[Europe/London]')", "datetime('2024-10-27T01:30Z[Europe/London]')", nil, true},
		{"datetime-utc-name-offset", "datetime('2024-01-02T12:00[UTC]')", "datetime('2024-01-02T12:00Z')", nil, false},
		{"datetime-cet-name-offset", "datetime('2024-01-02T12:00[CET]')", "datetime('2024-01-02T12:00+01:00')", nil, false},
		{"datetime-renamed", "datetime('2024-01-02T12:00[Europe/Kiev]')", "datetime('2024-01-02T12:00[Europe/Kyiv]')", nil, false},
		{"time-projected", "time(datetime('2024-07-01T12:34:56.000000007+01:00[Europe/London]'))", "time('12:34:56.000000007+01:00')", nil, true},
		{"duration-components", "duration('P1D')", "duration('PT24H')", nil, false},
		{"duration-seconds", "duration('PT60S')", "duration('PT1M')", nil, true},
		{"duration-months", "duration('P1Y')", "duration('P12M')", nil, true},
		{"duration-negative-arithmetic", "duration('PT-0.5S')", "duration('PT-0.5S') + duration('PT0S')", nil, true},
		{"typed-time-name-offset", "$a", "$b", map[string]interface{}{"a": CypherTime{Time: named}, "b": &CypherTime{Time: fixed}}, true},
		{"typed-duration-negative-bolt", "duration('PT-0.5S')", "$b", map[string]interface{}{"b": &CypherDuration{Seconds: -1, Nanos: 500000000}}, true},
		{"typed-duration-nanos", "$a", "$b", map[string]interface{}{"a": &CypherDuration{Nanos: 1000000000}, "b": CypherDuration{Seconds: 1}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, shape := range []string{"scalar", "list", "map"} {
				t.Run(shape, func(t *testing.T) {
					a, b := tc.a, tc.b
					if shape == "list" {
						a, b = "["+a+"]", "["+b+"]"
					}
					if shape == "map" {
						a, b = "{v:["+a+"]}", "{v:["+b+"]}"
					}
					for _, programs := range []bool{true, false} {
						t.Run(fmt.Sprintf("programs=%v", programs), func(t *testing.T) {
							rowValueProgramsDisabled.Store(!programs)
							defer rowValueProgramsDisabled.Store(false)
							exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "sol-temporal"), 0, 0)
							for _, with := range []bool{false, true} {
								t.Run(fmt.Sprintf("with=%v", with), func(t *testing.T) {
									left, right, prefix := a, b, ""
									if with {
										prefix = "WITH " + a + " AS a, " + b + " AS b "
										left, right = "a", "b"
									}
									expr := func(s string) string { return fmt.Sprintf(s, left, right) }
									query := prefix + "RETURN " + expr("(%s) = (%s)") + " AS eq, " +
										expr("apoc.coll.contains([%s], %s)") + " AS contained, " +
										expr("apoc.coll.containsAll([%s], [%s])") + " AS allContained, " +
										expr("apoc.coll.containsAny([%s], [%s])") + " AS anyContained, " +
										expr("apoc.coll.indexOf([%s], %s)") + " AS position, " +
										expr("apoc.coll.occurrences([%s], %s)") + " AS occurrences, " +
										expr("size(apoc.coll.split([0, %s, 1], %s))") + " AS partitions, " +
										expr("size(apoc.coll.toSet([%s, %s]))") + " AS distinctCount, " +
										expr("size(apoc.coll.union([%s], [%s]))") + " AS unionCount, " +
										expr("size(apoc.coll.intersection([%s], [%s]))") + " AS intersectionCount, " +
										expr("size(apoc.coll.subtract([%s], [%s]))") + " AS subtractionCount, " +
										expr("size(apoc.coll.frequencies([%s, %s]))") + " AS frequencyCount"
									result, err := exec.Execute(context.Background(), query, tc.params)
									require.NoError(t, err, query)
									want := []interface{}{false, false, false, false, int64(-1), int64(0), int64(1), int64(2), int64(2), int64(0), int64(1), int64(2)}
									if tc.equal {
										want = []interface{}{true, true, true, true, int64(0), int64(1), int64(2), int64(1), int64(1), int64(1), int64(0), int64(1)}
									}
									require.Equal(t, [][]interface{}{want}, result.Rows, query)
								})
							}
						})
					}
				})
			}
		})
	}
}

// Canonicalization and lookup keys allocate nothing per item. Container/result
// setup is excluded, just as in the reviewer's reusable-map layout probe.
func TestTemporalCanonicalKeyAllocations(t *testing.T) {
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	values := []interface{}{CypherDateTime{Time: stamp}, &CypherDateTime{Time: stamp}, stamp, &stamp}
	typed := make(map[temporalEqualityKey]int, 4)
	allocations := testing.AllocsPerRun(100, func() {
		clear(typed)
		for _, value := range values {
			key, ok := temporalEqualityParts(value)
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
	index.add(values[0])
	allocations = testing.AllocsPerRun(100, func() {
		clear(index.temporal)
		index.count = 0
		for _, value := range values {
			index.addPosition(value)
			if !index.has(value) {
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
		require.Zero(t, testing.AllocsPerRun(100, func() { temporalCanonicalKeySink, _ = temporalEqualityParts(value) }), "%T", value)
	}
}

var temporalCanonicalKeySink temporalEqualityKey
