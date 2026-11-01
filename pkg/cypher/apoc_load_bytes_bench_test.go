package cypher

// The bundled apoc.load.binary and apoc.load.stream names are published as
// admitted registry handlers, so their cost is measured through the same typed
// registry call shape query execution uses: the statement context is bound as
// the prefix argument and the Cypher arguments follow. These benchmarks use the
// published handlers rather than the Go functions, so they measure the shape
// each name actually registers.

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/apocio"
)

// apocLoadBytesBenchSizes are the source sizes measured.
var apocLoadBytesBenchSizes = []struct {
	name string
	size int
}{
	{name: "small", size: 1 << 10},
	{name: "large", size: 1 << 20},
}

// apocLoadBytesBenchHandler returns the handler published for a bundled plugin
// name.
func apocLoadBytesBenchHandler(b *testing.B, name string) *registry.Callable {
	b.Helper()
	if err := apoc.Initialize(apocstorage.NewInMemoryStorage(), nil); err != nil {
		b.Fatalf("initialize APOC: %v", err)
	}
	info, found := apoc.GetInfo(name)
	if !found {
		b.Fatalf("the bundled APOC plugin must register %s", name)
	}
	if info.Handler == nil {
		b.Fatalf("%s has no admitted handler", name)
	}
	return info.Handler
}

// apocLoadBytesBenchContext writes a source of the measured size into its own
// import root and returns a caller context that may read it.
func apocLoadBytesBenchContext(b *testing.B, size int) context.Context {
	b.Helper()
	root := b.TempDir()
	if err := os.WriteFile(filepath.Join(root, "payload.bin"), make([]byte, size), 0o600); err != nil {
		b.Fatalf("write benchmark payload: %v", err)
	}
	policy := apocio.NewPolicy(apocio.Config{AllowImportFileAccess: true, FileAccessRoot: root})
	return apocio.WithCapability(context.Background(), policy.Capability())
}

// BenchmarkBundledAPOCLoadBytesRegistry measures the typed registry call shape
// of the two bundled byte-loading names: apoc.load.binary with a source alone,
// and apoc.load.stream with a source and its config map.
func BenchmarkBundledAPOCLoadBytesRegistry(b *testing.B) {
	calls := []struct {
		name   string
		target string
		args   []interface{}
	}{
		{name: "binary-one-arg", target: "apoc.load.binary", args: []interface{}{"payload.bin"}},
		{name: "stream-two-arg", target: "apoc.load.stream", args: []interface{}{"payload.bin", map[string]interface{}{}}},
	}
	for _, size := range apocLoadBytesBenchSizes {
		ctx := apocLoadBytesBenchContext(b, size.size)
		prefix := []reflect.Value{reflect.ValueOf(ctx)}
		for _, call := range calls {
			handler := apocLoadBytesBenchHandler(b, call.target)
			b.Run(size.name+"/"+call.name, func(b *testing.B) {
				b.SetBytes(int64(size.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, err := handler.CallWithPolicy(call.args, prefix, registry.CypherArguments)
					if err != nil {
						b.Fatalf("%s: %v", call.target, err)
					}
					data, ok := value.([]byte)
					if !ok {
						b.Fatalf("%s returned %T, want bytes", call.target, value)
					}
					if len(data) != size.size {
						b.Fatalf("%s read %d bytes, want %d", call.target, len(data), size.size)
					}
				}
			})
		}
	}
}

// BenchmarkBundledAPOCLoadBytesRegisteredNames measures both registered names
// with a source alone and with the optional config map, so each name's
// argument shape is measured now that they converge on one handler.
func BenchmarkBundledAPOCLoadBytesRegisteredNames(b *testing.B) {
	shapes := []struct {
		name string
		args []interface{}
	}{
		{name: "one-arg", args: []interface{}{"payload.bin"}},
		{name: "two-arg", args: []interface{}{"payload.bin", map[string]interface{}{}}},
	}
	for _, size := range apocLoadBytesBenchSizes {
		ctx := apocLoadBytesBenchContext(b, size.size)
		prefix := []reflect.Value{reflect.ValueOf(ctx)}
		for _, target := range []string{"apoc.load.binary", "apoc.load.stream"} {
			handler := apocLoadBytesBenchHandler(b, target)
			for _, shape := range shapes {
				b.Run(size.name+"/"+target+"/"+shape.name, func(b *testing.B) {
					b.SetBytes(int64(size.size))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						value, err := handler.CallWithPolicy(shape.args, prefix, registry.CypherArguments)
						if err != nil {
							b.Fatalf("%s: %v", target, err)
						}
						data, ok := value.([]byte)
						if !ok {
							b.Fatalf("%s returned %T, want bytes", target, value)
						}
						if len(data) != size.size {
							b.Fatalf("%s read %d bytes, want %d", target, len(data), size.size)
						}
					}
				})
			}
		}
	}
}
