package bolt

import "testing"

var packstreamAllocSink []byte

func TestEncodePackStreamListInto_Allocs_RowSmallIsZero(t *testing.T) {
	row := []any{int64(1), "Alice", int64(30)}
	buf := make([]byte, 0, 128)

	allocs := testing.AllocsPerRun(1000, func() {
		buf = buf[:0]
		buf = encodePackStreamListInto(buf, row)
		packstreamAllocSink = buf
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocations, got %f", allocs)
	}
}

func TestEncodePackStreamValueInto_ReducesAllocsVsLegacy_CommonTypes(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		bufCap int
	}{
		{name: "list-small", value: []any{int64(1), "Alice", int64(30)}, bufCap: 128},
		{name: "ints", value: []int64{1, 2, 3, 4, 5}, bufCap: 128},
		{name: "floats32", value: []float32{1.25, 2.5, 3.75}, bufCap: 128},
		{name: "floats64", value: []float64{1.25, 2.5, 3.75}, bufCap: 128},
		{name: "strings", value: []string{"a", "bb", "ccc"}, bufCap: 128},
		{name: "map", value: map[string]any{"k": "v", "n": int64(1)}, bufCap: 256},
		{name: "bytes", value: []byte{0x01, 0x02, 0x03, 0x04}, bufCap: 128},
		{name: "map-string-string", value: map[string]string{"k": "v", "a": "b"}, bufCap: 256},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 0, tt.bufCap)
			intoAllocs := testing.AllocsPerRun(1000, func() {
				buf = buf[:0]
				buf = encodePackStreamValueInto(buf, tt.value)
				packstreamAllocSink = buf
			})

			if intoAllocs != 0 {
				t.Fatalf("expected 0 allocations for into path, got %f", intoAllocs)
			}

			// Some types (e.g. []byte) were previously unsupported in the legacy encoder
			// and would encode as null, so only compare allocs where legacy supports it.
			switch tt.value.(type) {
			case []byte:
				return
			}

			legacyAllocs := testing.AllocsPerRun(1000, func() {
				packstreamAllocSink = encodePackStreamValue(tt.value)
			})
			if legacyAllocs <= intoAllocs {
				t.Fatalf("expected into path allocations to be lower than legacy: legacy=%f into=%f", legacyAllocs, intoAllocs)
			}
		})
	}
}
