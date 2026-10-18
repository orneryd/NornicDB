package cypher

import (
	"testing"
	"time"
)

func BenchmarkApocTemporalEquivalence(b *testing.B) {
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	for _, tc := range []struct {
		name   string
		values []interface{}
	}{
		{"numbers", []interface{}{int64(1), 1.0, int64(2), 2.0}},
		{"strings", []interface{}{"one", "one", "two", "two"}},
		{"duration", []interface{}{&CypherDuration{Hours: 1}, &CypherDuration{Minutes: 60}, &CypherDuration{Hours: 2}, &CypherDuration{Minutes: 120}}},
		{"datetime", []interface{}{CypherDateTime{Time: stamp}, &CypherDateTime{Time: stamp}, stamp, &stamp}},
		{"nested", []interface{}{map[string]interface{}{"time": []interface{}{&CypherDuration{Hours: 1}}}, map[string]interface{}{"time": []interface{}{&CypherDuration{Minutes: 60}}}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				toSet(tc.values)
			}
		})
	}
}
