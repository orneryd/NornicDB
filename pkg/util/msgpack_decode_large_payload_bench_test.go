package util

import (
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// These benchmarks qualify the cost of the in-memory decoder lifetime at the
// payload sizes a stored record reaches outside the small-metadata case. Each
// case decodes a typed database record through DecodeMsgpackBytes, the path a
// stored-record reader uses, so the numbers isolate the reader and decoder
// lifetime - the MessagePack library and the record shape are unchanged -
// across payload sizes:
//
//	1 KiB documentation record   -> small-record control
//	32 KiB document body         -> medium-record control
//	65,535-byte payload          -> 64 KiB boundary control
//	65,537-byte payload          -> 64 KiB boundary control
//	32K float32 embedding        -> large-vector control, ~160 KiB payload
//
// Every destination is reset to a fresh record per decode, as a caller decoding
// its own local variable does, so the decoded value is charged to the read
// instead of being recycled through a reused slice or string header.

// documentationRecord is the typed metadata shape this decoder serves: identity
// fields beside a stored documentation body.
type documentationRecord struct {
	ID       string
	Title    string
	Body     string
	Revision int64
}

// embeddingRecord is a stored record whose payload is dominated by a vector.
type embeddingRecord struct {
	ID        string
	Embedding []float32
}

// documentationPayloadForLength marshals a documentation record whose encoded
// payload is exactly payloadBytes long. Only the body length moves between
// attempts, and the MessagePack string header width settles after the first
// correction.
func documentationPayloadForLength(tb testing.TB, payloadBytes int) ([]byte, documentationRecord) {
	tb.Helper()
	length := payloadBytes - 64
	for attempt := 0; attempt < 8; attempt++ {
		record := documentationRecord{
			ID:       "bench:documentation-1",
			Title:    "Storage codec documentation",
			Body:     strings.Repeat("d", length),
			Revision: 7,
		}
		payload, err := msgpack.Marshal(record)
		if err != nil {
			tb.Fatalf("marshal failed: %v", err)
		}
		if len(payload) == payloadBytes {
			return payload, record
		}
		length += payloadBytes - len(payload)
	}
	tb.Fatalf("could not build a %d-byte documentation payload", payloadBytes)
	return nil, documentationRecord{}
}

// runDecodeMsgpackBytes measures one typed decode per iteration, resetting the
// destination first so each read owns the value it produces.
func runDecodeMsgpackBytes(b *testing.B, payload []byte, check func(documentationRecord) bool, describe func(documentationRecord) string) {
	b.Helper()
	b.Logf("fixture payload: %d bytes", len(payload))
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out documentationRecord
		if err := DecodeMsgpackBytes(payload, &out); err != nil {
			b.Fatal(err)
		}
		if !check(out) {
			b.Fatal(describe(out))
		}
	}
}

// BenchmarkDecodeMsgpackBytesLargePayload measures one typed record decode at
// each payload size that exercises a different decode-state lifetime.
func BenchmarkDecodeMsgpackBytesLargePayload(b *testing.B) {
	b.Run("documentation_1KiB", func(b *testing.B) {
		record := documentationRecord{
			ID:       "bench:documentation-1",
			Title:    "Storage codec documentation",
			Body:     strings.Repeat("d", 1<<10),
			Revision: 7,
		}
		payload, err := msgpack.Marshal(record)
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		if len(payload) > 64<<10 {
			b.Fatalf("fixture payload is %d bytes, want a pooled-size payload", len(payload))
		}
		runDecodeMsgpackBytes(b, payload, func(out documentationRecord) bool {
			return out == record
		}, func(out documentationRecord) string {
			return "documentation record decoded to id=" + out.ID
		})
	})

	b.Run("string_32KiB", func(b *testing.B) {
		record := documentationRecord{
			ID:       "bench:documentation-2",
			Title:    "Storage codec documentation",
			Body:     strings.Repeat("d", 32<<10),
			Revision: 8,
		}
		payload, err := msgpack.Marshal(record)
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		if len(payload) > 64<<10 {
			b.Fatalf("fixture payload is %d bytes, want a pooled-size payload", len(payload))
		}
		runDecodeMsgpackBytes(b, payload, func(out documentationRecord) bool {
			return out == record
		}, func(out documentationRecord) string {
			return "documentation record decoded to id=" + out.ID
		})
	})

	b.Run("boundary_65535", func(b *testing.B) {
		payload, record := documentationPayloadForLength(b, 65_535)
		runDecodeMsgpackBytes(b, payload, func(out documentationRecord) bool {
			return out == record
		}, func(out documentationRecord) string {
			return "documentation record decoded to id=" + out.ID
		})
	})

	b.Run("boundary_65537", func(b *testing.B) {
		payload, record := documentationPayloadForLength(b, 65_537)
		runDecodeMsgpackBytes(b, payload, func(out documentationRecord) bool {
			return out == record
		}, func(out documentationRecord) string {
			return "documentation record decoded to id=" + out.ID
		})
	})

	b.Run("embedding_32K_float32", func(b *testing.B) {
		embedding := make([]float32, 32<<10)
		for i := range embedding {
			embedding[i] = float32(i) * 0.25
		}
		record := embeddingRecord{ID: "bench:embedding-1", Embedding: embedding}
		payload, err := msgpack.Marshal(record)
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		if len(payload) <= 64<<10 {
			b.Fatalf("fixture payload is %d bytes, want a payload above the admission bound", len(payload))
		}
		b.Logf("fixture payload: %d bytes", len(payload))
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out embeddingRecord
			if err := DecodeMsgpackBytes(payload, &out); err != nil {
				b.Fatal(err)
			}
			if out.ID != record.ID || len(out.Embedding) != len(embedding) {
				b.Fatalf("embedding record decoded as id=%q len=%d", out.ID, len(out.Embedding))
			}
			for j := range embedding {
				if out.Embedding[j] != embedding[j] {
					b.Fatalf("embedding element %d decoded as %v", j, out.Embedding[j])
				}
			}
		}
	})
}
