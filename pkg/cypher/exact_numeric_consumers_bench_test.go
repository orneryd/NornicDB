package cypher

import "testing"

var exactNumericConsumerBenchmarkSink interface{}

func BenchmarkExactNumericConsumers(b *testing.B) {
	values := []interface{}{int64(42), float64(42), int64(9007199254740992), int64(9007199254740993), float64(9007199254740992)}
	for name, key := range map[string]func(interface{}) string{
		"correlated_call": callSubqueryLookupKeyString,
		"path_distinct":   joinedValueKey,
		"batch_match":     propEqKeyBatch,
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				exactNumericConsumerBenchmarkSink = key(values[i%len(values)])
			}
		})
	}
	b.Run("membership", func(b *testing.B) {
		set, rest := buildComparableMembershipIndex(values)
		equals := (&StorageExecutor{}).compareEqual
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			exactNumericConsumerBenchmarkSink = evaluateComparableMembership(values[i%len(values)], set, rest, equals)
		}
	})
	b.Run("path_numeric_order", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			exactNumericConsumerBenchmarkSink = compareForSort(values[i%len(values)], values[(i+1)%len(values)])
		}
	})
}
