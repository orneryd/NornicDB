package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNamedValuesRejectForgedCollectionLengths(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"map", []byte{0xdf, 0xff, 0xff, 0xff, 0xff}},
		{"nested array", []byte{0x81, 0xa1, 'x', 0xdd, 0xff, 0xff, 0xff, 0xff, 1}},
		{"nested map", []byte{0x81, 0xa1, 'x', 0xdf, 0xff, 0xff, 0xff, 0xff}},
		{"truncated binary", []byte{0x81, 0xa1, 'x', 0xc6, 0xff, 0xff, 0xff, 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeNamedProperties(tc.data)
			require.Error(t, err)
			_, err = DecodeNamedMetadata(tc.data)
			require.Error(t, err)
		})
	}
}

func TestStoredPropertiesRejectForgedCollectionLengths(t *testing.T) {
	for _, data := range [][]byte{
		{0xdf, 0xff, 0xff, 0xff, 0xff},
		{0xdd, 0xff, 0xff, 0xff, 0xff, 1},
	} {
		_, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(data)))
		require.Error(t, err)
	}
	var count [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(count[:], ^uint64(0))
	_, err := (&BadgerEngine{}).decodeTokenizedProperties("test", count[:n])
	require.Error(t, err)
	_, err = newProjectedNodeDecoder(&BadgerEngine{}, StreamNodesOptions{}).decodeProperties(count[:n])
	require.Error(t, err)
}

func TestMessagePackValidationPreservesValidValues(t *testing.T) {
	values := []any{
		nil, true, int8(-7), int16(-300), int32(-70000), int64(-1 << 40),
		uint8(200), uint16(300), uint32(70000), uint64(1 << 40), float32(1.5), float64(2.5),
		"text", bytes.Repeat([]byte("x"), 300), time.Unix(1700000000, 123),
		[]any{nil, true, []int64{1, 2}, map[string]any{"x": "y"}},
		map[string]any{"strings": []string{"a", "b"}, "empty": map[string]any{}},
	}
	for _, value := range values {
		encoded, err := msgpack.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, validateMessagePackValue(encoded), "%T", value)
		require.Error(t, validateMessagePackValue(append(encoded, 0)), "trailing value for %T", value)
	}
	// Valid non-minimal length headers still describe the same single value.
	require.NoError(t, validateMessagePackValue([]byte{0xdf, 0, 0, 0, 1, 0xa1, 'k', 0xc0}))
	require.NoError(t, validateMessagePackValue([]byte{0xdd, 0, 0, 0, 1, 0xc0}))
	require.NoError(t, validateMessagePackValue([]byte{0xde, 0, 1, 0xa1, 'k', 0xc0}))
	require.NoError(t, validateMessagePackValue([]byte{0xdc, 0, 1, 0xc0}))
	for _, encoded := range [][]byte{
		{0xd4, 1, 0}, {0xd5, 1, 0, 0}, {0xd6, 1, 0, 0, 0, 0},
		{0xd7, 1, 0, 0, 0, 0, 0, 0, 0, 0},
		{0xd8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0xc7, 1, 1, 0}, {0xc8, 0, 1, 1, 0}, {0xc9, 0, 0, 0, 1, 1, 0},
	} {
		require.NoError(t, validateMessagePackValue(encoded))
	}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if err := validateMessagePackValue([]byte{0x81, 0xa1, 'k', 0x92, 1, 2}); err != nil {
			panic(err)
		}
	}))
	for _, encoded := range [][]byte{
		{0xc1}, {0xda, 0}, {0xc7, 0}, {0xd8, 0}, {0x81, 0xa1, 'k'},
	} {
		require.Error(t, validateMessagePackValue(encoded))
	}
}

// A short truncated value must not allocate in proportion to a forged
// MessagePack collection length. This benchmark uses bounded declarations so
// it can quantify allocation amplification without risking an OOM.
func BenchmarkNamedPropertiesTruncatedCollection(b *testing.B) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "map", data: []byte{0xdf, 0, 1, 0x86, 0xa0}},
		{name: "array", data: []byte{0x81, 0xa1, 'x', 0xdd, 0, 1, 0x86, 0xa0, 1}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := DecodeNamedProperties(tc.data); err == nil {
					b.Fatal("truncated collection was accepted")
				}
			}
		})
	}
}

func BenchmarkStoredPropertiesTruncatedCollection(b *testing.B) {
	var count [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(count[:], 100000)
	for _, tc := range []struct {
		name string
		data []byte
		read func([]byte) error
	}{
		{"tokenized", append([]byte(nil), count[:n]...), func(data []byte) error {
			_, err := (&BadgerEngine{}).decodeTokenizedProperties("test", data)
			return err
		}},
		{"map", []byte{0xdf, 0, 1, 0x86, 0xa0}, func(data []byte) error {
			_, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(data)))
			return err
		}},
		{"array", []byte{0xdd, 0, 1, 0x86, 0xa0, 1}, func(data []byte) error {
			_, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(data)))
			return err
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := tc.read(tc.data); err == nil {
					b.Fatal("truncated properties were accepted")
				}
			}
		})
	}
}
