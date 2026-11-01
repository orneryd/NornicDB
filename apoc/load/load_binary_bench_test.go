package load

// The registered apoc.load.binary and apoc.load.stream names both pay the cost
// of the canonical byte loader per call, so its cost is measured here against a
// real import-root read: once for a small source and once for a source large
// enough that the payload copy dominates call overhead. The registry-side
// measurement of the same two registered call shapes lives in
// pkg/cypher/apoc_load_bytes_bench_test.go, where the bundled handlers are
// published.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/apocio"
)

// binaryBenchSizes are the source sizes measured.
var binaryBenchSizes = []struct {
	name string
	size int
}{
	{name: "small", size: 1 << 10},
	{name: "large", size: 1 << 20},
}

// binaryBenchContext writes a source of the measured size into its own import
// root and returns a caller context that may read it.
func binaryBenchContext(b *testing.B, size int) context.Context {
	b.Helper()
	root := b.TempDir()
	payload := make([]byte, size)
	if err := os.WriteFile(filepath.Join(root, "payload.bin"), payload, 0o600); err != nil {
		b.Fatalf("write benchmark payload: %v", err)
	}
	policy := apocio.NewPolicy(apocio.Config{AllowImportFileAccess: true, FileAccessRoot: root})
	return callerContext(policy.Capability())
}

// BenchmarkBinaryDirect measures direct Go calls of the canonical byte loader.
func BenchmarkBinaryDirect(b *testing.B) {
	for _, payload := range binaryBenchSizes {
		ctx := binaryBenchContext(b, payload.size)
		b.Run(payload.name, func(b *testing.B) {
			b.SetBytes(int64(payload.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := Binary(ctx, "payload.bin")
				if err != nil {
					b.Fatalf("Binary: %v", err)
				}
				if len(data) != payload.size {
					b.Fatalf("Binary read %d bytes, want %d", len(data), payload.size)
				}
			}
		})
	}
}

// BenchmarkBinaryDirectConfig measures the same direct Go call with the
// optional config map both registered Cypher names accept, so binding the
// variadic argument is measured rather than assumed free.
func BenchmarkBinaryDirectConfig(b *testing.B) {
	for _, payload := range binaryBenchSizes {
		ctx := binaryBenchContext(b, payload.size)
		b.Run(payload.name, func(b *testing.B) {
			b.SetBytes(int64(payload.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := Binary(ctx, "payload.bin", map[string]interface{}{"format": "json"})
				if err != nil {
					b.Fatalf("Binary with config: %v", err)
				}
				if len(data) != payload.size {
					b.Fatalf("Binary read %d bytes, want %d", len(data), payload.size)
				}
			}
		})
	}
}
