package util

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestDecodeMsgpackBytesDecodesDatabaseStateWithoutArbitrarySizePolicy(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]string{"content": "database state"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out map[string]string
	if err := DecodeMsgpackBytes(payload, &out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out["content"] != "database state" {
		t.Fatalf("unexpected decode result: %#v", out)
	}
}

// TestDecodeMsgpackBytesReusesDecodeStatePerRecord proves the per-record decode
// path a stored-record scan uses keeps no state between calls: consecutive
// decodes of different payloads, a payload whose decode fails, and the payload
// shape a node body carries must each observe only their own bytes.
func TestDecodeMsgpackBytesReusesDecodeStatePerRecord(t *testing.T) {
	type record struct {
		ID     string
		Labels []string
		Meta   map[string]any
	}
	first := record{ID: "bench:reading-0", Labels: []string{"Reading"}, Meta: map[string]any{"chunk_count": int64(0)}}
	second := record{ID: "bench:reading-961", Labels: []string{"Reading", "Archived"}, Meta: map[string]any{"chunk_count": int64(3)}}
	firstPayload, err := msgpack.Marshal(first)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	secondPayload, err := msgpack.Marshal(second)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	for round := 0; round < 3; round++ {
		var out record
		if err := DecodeMsgpackBytes(firstPayload, &out); err != nil {
			t.Fatalf("round %d: decode first failed: %v", round, err)
		}
		if out.ID != first.ID || len(out.Labels) != 1 || out.Labels[0] != "Reading" || out.Meta["chunk_count"] != int64(0) {
			t.Fatalf("round %d: first payload decoded to %#v", round, out)
		}

		// A failed decode must not leave the pooled state holding this payload.
		var broken record
		if err := DecodeMsgpackBytes([]byte{0xc1}, &broken); err == nil {
			t.Fatalf("round %d: expected malformed payload to fail", round)
		}

		var next record
		if err := DecodeMsgpackBytes(secondPayload, &next); err != nil {
			t.Fatalf("round %d: decode second failed: %v", round, err)
		}
		if next.ID != second.ID || len(next.Labels) != 2 || next.Labels[1] != "Archived" || next.Meta["chunk_count"] != int64(3) {
			t.Fatalf("round %d: second payload decoded to %#v", round, next)
		}
	}
}

// TestDecodeMsgpackBytesConcurrentDecodesStayIsolated exercises the pooled
// decode state the way concurrent readers use it: every goroutine decodes its
// own payload and must observe only that payload.
func TestDecodeMsgpackBytesConcurrentDecodesStayIsolated(t *testing.T) {
	const workers, rounds = 8, 64
	type record struct {
		ID     string
		Labels []string
		Meta   map[string]any
	}
	payloads := make([][]byte, workers)
	records := make([]record, workers)
	for i := range payloads {
		records[i] = record{
			ID:     "bench:reading-" + string(rune('a'+i)),
			Labels: []string{"Reading"},
			Meta:   map[string]any{"chunk_count": int64(i)},
		}
		payload, err := msgpack.Marshal(records[i])
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		payloads[i] = payload
	}

	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				var out record
				if err := DecodeMsgpackBytes(payloads[worker], &out); err != nil {
					errs <- "decode failed: " + err.Error()
					return
				}
				want := records[worker]
				if out.ID != want.ID || len(out.Labels) != 1 || out.Labels[0] != "Reading" || out.Meta["chunk_count"] != want.Meta["chunk_count"] {
					errs <- "payload decoded into another worker's record: " + out.ID
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestDecodeMsgpackBytesAllocatesNoDecodeStatePerCall is the allocation
// contract of the shared in-memory decode owner: a scan that decodes a stored
// record's MessagePack payload once per record must not allocate a decoder and
// a reader per record. Fixed-size fields isolate the decode state from the
// decoded values, and the destination is allocated outside the measured loop.
func TestDecodeMsgpackBytesAllocatesNoDecodeStatePerCall(t *testing.T) {
	type fixed struct{ Count int64 }
	payload, err := msgpack.Marshal(fixed{Count: 7})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var decoded fixed
	if err := DecodeMsgpackBytes(payload, &decoded); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Count != 7 {
		t.Fatalf("decoded count: want 7, got %d", decoded.Count)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := DecodeMsgpackBytes(payload, &decoded); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
	}); allocs != 0 {
		t.Fatalf("per-record decode allocations: want 0, got %v", allocs)
	}
}

func TestDecodeMsgpackBytesRawErrorDoesNotPoisonSmallReads(t *testing.T) {
	type fixed struct{ Count int64 }
	payload, err := msgpack.Marshal(fixed{Count: 7})
	if err != nil {
		t.Fatal(err)
	}
	var decoded fixed
	poison := true
	allocs := testing.AllocsPerRun(100, func() {
		// AllocsPerRun's warmup and measurements use one P. Fail a raw copy
		// in the warmup, then prove later healthy reads do not keep appending
		// bytes to its abandoned recorder or allocate replacement state.
		if poison {
			var raw msgpack.RawMessage
			if err := DecodeMsgpackBytes([]byte{0x92, 0xa1, 'x'}, &raw); err == nil {
				t.Fatal("truncated array must fail raw decoding")
			}
			poison = false
		}
		if err := DecodeMsgpackBytes(payload, &decoded); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 || decoded.Count != 7 {
		t.Fatalf("healthy reads after raw failure: allocations=%v value=%v", allocs, decoded)
	}
}

func TestDecodeMsgpackBytesLargePayloadRemainsSupported(t *testing.T) {
	want := strings.Repeat("database payload ", 1<<18)
	payload, err := msgpack.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		var got string
		if err := DecodeMsgpackBytes(payload, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("large payload changed on round %d", round)
		}
		var small string
		if err := DecodeMsgpackBytes([]byte{0xa1, 'x'}, &small); err != nil || small != "x" {
			t.Fatalf("small read after large payload: value=%q error=%v", small, err)
		}
	}
}

func TestDecodeMsgpackFileDecodesDatabaseStateWithoutArbitrarySizePolicy(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "msgpack-*.bin")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer tmp.Close()

	payload, err := msgpack.Marshal(map[string]string{"content": "persisted search index state"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		t.Fatalf("seek failed: %v", err)
	}

	var out map[string]string
	if err := DecodeMsgpackFile(tmp, &out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out["content"] != "persisted search index state" {
		t.Fatalf("unexpected decode result: %#v", out)
	}
}

func TestDecodeMsgpackFileRejectsNilFile(t *testing.T) {
	var out map[string]string
	if err := DecodeMsgpackFile(nil, &out); err == nil {
		t.Fatal("expected nil file to fail")
	}
}
