package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

var propertyViewDisabledBenchmarkSink interface{}

func BenchmarkQueryPropertyViewDisabled(b *testing.B) {
	engine, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	engine.SetDecayEnabled(false)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "bench"))
	node := &storage.Node{ID: "n", Properties: map[string]interface{}{"body": "raw"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		propertyViewDisabledBenchmarkSink = exec.queryVisibleValue(node)
	}
}
