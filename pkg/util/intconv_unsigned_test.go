package util

import (
	"strconv"
	"testing"
)

func TestUnsignedIntConversionsPreserveBounds(t *testing.T) {
	// Exhaust the uint16 domain, including the HNSW neighbour-count range.
	for value := 0; value <= 65535; value++ {
		got, ok := SafeIntToUint16(value)
		if !ok || uint64(got) != uint64(value) {
			t.Fatalf("uint16(%d)=(%d,%v)", value, got, ok)
		}
	}
	for _, value := range []int{minIntValue, -65536, -1, 65536, maxIntValue} {
		got, ok := SafeIntToUint16(value)
		if ok || got != 0 {
			t.Fatalf("out-of-range uint16(%d)=(%d,%v)", value, got, ok)
		}
	}
	values := []int{minIntValue, -1, 0, 1, 65535, 65536, maxIntValue}
	if strconv.IntSize == 64 {
		boundary := int64(1) << 32
		values = append(values, int(boundary-1), int(boundary), int(boundary+1))
	}
	for _, value := range values {
		got, ok := SafeIntToUint32(value)
		wantOK := value >= 0 && uint64(value) <= uint64(^uint32(0))
		if ok != wantOK || (ok && uint64(got) != uint64(value)) || (!ok && got != 0) {
			t.Fatalf("uint32(%d)=(%d,%v), in range=%v", value, got, ok, wantOK)
		}
	}
}

func TestUnsignedIntConversionsDoNotAllocate(t *testing.T) {
	for name, convert := range map[string]func(){
		"uint16": func() {
			got, ok := SafeIntToUint16(32)
			if !ok || got != 32 {
				panic("invalid conversion")
			}
		},
		"uint32": func() {
			got, ok := SafeIntToUint32(4096)
			if !ok || got != 4096 {
				panic("invalid conversion")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			if n := testing.AllocsPerRun(100, convert); n != 0 {
				t.Fatalf("numeric conversion allocated %g objects; want zero", n)
			}
		})
	}
}
