package bolt

import (
	"bytes"
	"testing"
)

func TestEncodeDecodePackStreamBytes_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: []byte{}},
		{name: "small", in: []byte{0x01, 0x02, 0x03}},
		{name: "len255", in: bytes.Repeat([]byte{0xAB}, 255)},
		{name: "len256", in: bytes.Repeat([]byte{0xCD}, 256)},
		{name: "len65535", in: bytes.Repeat([]byte{0xEF}, 65535)},
		{name: "len65536", in: bytes.Repeat([]byte{0x01}, 65536)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodePackStreamValueInto(nil, tt.in)
			got, _, err := decodePackStreamValue(encoded, 0)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			gotBytes, ok := got.([]byte)
			if !ok {
				t.Fatalf("expected []byte, got %T", got)
			}
			if !bytes.Equal(gotBytes, tt.in) {
				t.Fatalf("bytes mismatch: got=%dB want=%dB", len(gotBytes), len(tt.in))
			}
		})
	}
}

