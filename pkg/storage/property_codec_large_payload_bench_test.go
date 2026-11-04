package storage

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// These benchmarks qualify the cost of the in-memory decoder lifetime at the
// payload sizes a stored node reaches outside the small-property case. Every
// case decodes an immutable body through the same path a scan uses
// (decodeTokenizedPropertiesProjected), so the numbers isolate the reader and
// decoder lifetime - the codec itself is unchanged - across payload sizes:
//
//	1 KiB documentation body        -> small-record control
//	32 KiB string property          -> medium-record control
//	65,535-byte tokenized payload   -> 64 KiB boundary control
//	65,537-byte tokenized payload   -> 64 KiB boundary control
//	32K float32 embedding           -> large-vector control, ~160 KiB payload
//
// The boundary cases are built to an exact payload length rather than to a
// round string length: the rejected pooling experiment compared against the tokenized
// payload (the bytes after the count varint), so those fixtures assert the
// length they were built for instead of approximating it.

// benchmarkTokenizedPropertyBody encodes props once, outside decode timing, and
// returns the engine that owns the dictionary plus the immutable body.
func benchmarkTokenizedPropertyBody(b *testing.B, namespace string, props map[string]any) (*BadgerEngine, []byte) {
	b.Helper()
	eng := benchEngine(b)
	data := benchmarkEncodedFixture(b, eng, false, func(txn *badger.Txn) ([]byte, error) {
		return eng.encodeTokenizedProperties(txn, namespace, props)
	})
	return eng, data
}

// benchmarkTokenizedPayloadLength returns the length of the tokenized payload
// the rejected pooling experiment compared against: the body less its count
// varint.
func benchmarkTokenizedPayloadLength(b *testing.B, data []byte) int {
	b.Helper()
	_, consumed, err := readUvarint(data)
	if err != nil {
		b.Fatalf("read count varint: %v", err)
	}
	return len(data) - consumed
}

// benchmarkExactPayloadBody encodes one string property whose tokenized payload
// is exactly payloadBytes long. The property keeps a stable dictionary id
// across attempts, so the only moving part is the value length, and the loop
// converges after the string header width settles.
func benchmarkExactPayloadBody(b *testing.B, eng *BadgerEngine, namespace, name string, payloadBytes int) ([]byte, string) {
	b.Helper()
	// A first guess of key varint plus MessagePack string header overhead.
	length := payloadBytes - 8
	for attempt := 0; attempt < 8; attempt++ {
		value := strings.Repeat("d", length)
		data := benchmarkEncodedFixture(b, eng, false, func(txn *badger.Txn) ([]byte, error) {
			return eng.encodeTokenizedProperties(txn, namespace, map[string]any{name: value})
		})
		got := benchmarkTokenizedPayloadLength(b, data)
		if got == payloadBytes {
			return data, value
		}
		length += payloadBytes - got
	}
	b.Fatalf("could not build a %d-byte tokenized payload", payloadBytes)
	return nil, ""
}

// runTokenizedPropertyDecode measures the projected property decode over an
// immutable body. check runs on every decoded result, so correctness is
// asserted inside the measured loop for the payload the benchmark names.
func runTokenizedPropertyDecode(b *testing.B, eng *BadgerEngine, namespace string, data []byte, check func(map[string]any)) {
	b.Helper()
	b.Logf("fixture: body=%d bytes, tokenized payload=%d bytes", len(data), benchmarkTokenizedPayloadLength(b, data))
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := eng.decodeTokenizedPropertiesProjected(nil, namespace, data, nil)
		if err != nil {
			b.Fatal(err)
		}
		check(out)
	}
}

// BenchmarkDecodeTokenizedPropertiesLargePayload measures one stored-body
// decode at each payload size that exercises a different decode-state lifetime.
func BenchmarkDecodeTokenizedPropertiesLargePayload(b *testing.B) {
	createdAt := time.Date(2026, time.November, 3, 12, 0, 0, 0, time.UTC)

	b.Run("documentation_1KiB", func(b *testing.B) {
		const namespace = "bench-large-doc"
		body := strings.Repeat("Documentation body line. ", 64)[:1024]
		eng, data := benchmarkTokenizedPropertyBody(b, namespace, map[string]any{
			"title":      "Storage codec documentation",
			"body":       body,
			"updated_at": createdAt,
		})
		runTokenizedPropertyDecode(b, eng, namespace, data, func(out map[string]any) {
			if got, ok := out["body"].(string); !ok || got != body {
				b.Fatalf("body decoded as %T len=%d", out["body"], len(got))
			}
			if got, ok := out["updated_at"].(time.Time); !ok || !got.Equal(createdAt) {
				b.Fatalf("updated_at decoded as %#v", out["updated_at"])
			}
		})
	})

	b.Run("strings_32KiB", func(b *testing.B) {
		const namespace = "bench-large-strings"
		content := strings.Repeat("x", 32<<10)
		summary := strings.Repeat("s", 1<<10)
		eng, data := benchmarkTokenizedPropertyBody(b, namespace, map[string]any{
			"content": content,
			"summary": summary,
		})
		runTokenizedPropertyDecode(b, eng, namespace, data, func(out map[string]any) {
			if got, ok := out["content"].(string); !ok || got != content {
				b.Fatalf("content decoded as %T len=%d", out["content"], len(got))
			}
			if got, ok := out["summary"].(string); !ok || got != summary {
				b.Fatalf("summary decoded as %T len=%d", out["summary"], len(got))
			}
		})
	})

	b.Run("boundary_65535", func(b *testing.B) {
		const namespace = "bench-large-at-limit"
		eng := benchEngine(b)
		data, value := benchmarkExactPayloadBody(b, eng, namespace, "content", 65_535)
		runTokenizedPropertyDecode(b, eng, namespace, data, func(out map[string]any) {
			if got, ok := out["content"].(string); !ok || got != value {
				b.Fatalf("content decoded as %T len=%d", out["content"], len(got))
			}
		})
	})

	b.Run("boundary_65537", func(b *testing.B) {
		const namespace = "bench-large-over-limit"
		eng := benchEngine(b)
		data, value := benchmarkExactPayloadBody(b, eng, namespace, "content", 65_537)
		runTokenizedPropertyDecode(b, eng, namespace, data, func(out map[string]any) {
			if got, ok := out["content"].(string); !ok || got != value {
				b.Fatalf("content decoded as %T len=%d", out["content"], len(got))
			}
		})
	})

	b.Run("embedding_32K_float32", func(b *testing.B) {
		const namespace = "bench-large-vector"
		embedding := make([]float32, 32<<10)
		for i := range embedding {
			embedding[i] = float32(i) * 0.25
		}
		eng, data := benchmarkTokenizedPropertyBody(b, namespace, map[string]any{
			"name":      "documentation chunk",
			"embedding": embedding,
		})
		if propertyLen := benchmarkTokenizedPayloadLength(b, data); propertyLen <= 64<<10 {
			b.Fatalf("vector payload is only %d bytes", propertyLen)
		}
		runTokenizedPropertyDecode(b, eng, namespace, data, func(out map[string]any) {
			got, ok := out["embedding"].([]float32)
			if !ok || len(got) != len(embedding) || !slices.Equal(got, embedding) {
				b.Fatalf("embedding decoded as %T len=%d", out["embedding"], len(got))
			}
		})
	})
}
