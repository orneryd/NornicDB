package fabric

import "testing"

var exactNumericBenchmarkSink interface{}

func BenchmarkFabricExactNumbers(b *testing.B) {
	for _, value := range []struct {
		name  string
		value interface{}
	}{
		{"ordinary", int64(42)},
		{"large", int64(9007199254740993)},
	} {
		b.Run(value.name, func(b *testing.B) {
			b.Run("key", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					exactNumericBenchmarkSink = applyLookupKeyString(value.value)
				}
			})
			b.Run("order", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					exactNumericBenchmarkSink = compareSimpleOrderValues(value.value, float64(9007199254740992))
				}
			})
			b.Run("hash", func(b *testing.B) {
				row := []interface{}{value.value, "x", true}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					exactNumericBenchmarkSink = hashRow(row)
				}
			})
		})
	}
}
