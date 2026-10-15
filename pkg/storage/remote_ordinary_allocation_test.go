package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var remoteOrdinaryNormalizationSink interface{}

func remoteOrdinaryMapFixture() map[string]interface{} {
	return map[string]interface{}{"name": "Remote", "labels": []interface{}{"ordinary"}, "properties": map[string]interface{}{"id": "plain-map", "count": int64(4)}}
}

func TestRemoteOrdinaryNormalizationAllocationBudget(t *testing.T) {
	for name, value := range map[string]interface{}{"map": remoteOrdinaryMapFixture(), "list": []interface{}{remoteOrdinaryMapFixture(), int64(1), "value"}} {
		t.Run(name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(100, func() { remoteOrdinaryNormalizationSink = normalizeBoltValue(value) })
			require.Zero(t, allocations, "ordinary remote values need no graph conversion")
		})
	}
}

func BenchmarkRemoteOrdinaryNormalization(b *testing.B) {
	value := remoteOrdinaryMapFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		remoteOrdinaryNormalizationSink = normalizeBoltValue(value)
	}
}
