package storage

import (
	"encoding/json"
	"runtime"
	"testing"
)

// BenchmarkRecoveryCodecDocFixture compares the old JSON projection with the
// compatible lossless sidecar using the regression Doc entity (two 2D vectors,
// nested integer properties, metadata and timestamps). Bytes are wire size;
// B/op and allocs/op cover encoding/decoding, not retained database memory.
func BenchmarkRecoveryCodecDocFixture(b *testing.B) {
	n, _ := recoveryFieldFixture()
	source := WALNodeData{Node: n}
	for name, encode := range map[string]func(any) ([]byte, error){"legacy-json": json.Marshal, "lossless-sidecar": func(v any) ([]byte, error) { return marshalWALRecoveryPayload(nil, v) }} {
		b.Run(name, func(b *testing.B) {
			raw, err := encode(source)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("encode", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(raw)), "wire-bytes")
				for i := 0; i < b.N; i++ {
					if _, err := encode(source); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("decode", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(raw)), "wire-bytes")
				for i := 0; i < b.N; i++ {
					var target WALNodeData
					var err error
					if name == "legacy-json" {
						err = json.Unmarshal(raw, &target)
					} else {
						err = decodeRecoveryPayload(raw, &target)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkRecoveryRetainedDocFixture measures a retained batch separately from
// transient decode allocations. Heap differences are noisy across processes;
// compare only this named fixture under the same toolchain/configuration.
func BenchmarkRecoveryRetainedDocFixture(b *testing.B) {
	n, _ := recoveryFieldFixture()
	for name, encode := range map[string]func(any) ([]byte, error){"legacy-json": json.Marshal, "lossless-sidecar": func(v any) ([]byte, error) { return marshalWALRecoveryPayload(nil, v) }} {
		b.Run(name, func(b *testing.B) {
			raw, err := encode(WALNodeData{Node: n})
			if err != nil {
				b.Fatal(err)
			}
			const count = 2000
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			retained := make([]*WALNodeData, count)
			for i := range retained {
				retained[i] = &WALNodeData{}
				if name == "legacy-json" {
					err = json.Unmarshal(raw, retained[i])
				} else {
					err = decodeRecoveryPayload(raw, retained[i])
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/count, "retained-B/entity")
			runtime.KeepAlive(retained)
		})
	}
}
