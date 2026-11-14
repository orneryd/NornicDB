package storage

import (
	"bytes"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"testing"
)

func TestExperimentalSkipUnselectedProperty(t *testing.T) {
	previous := config.IsExperimentalPropertyStorageEnabled()
	t.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
	for _, enabled := range []bool{false, true} {
		config.SetExperimentalPropertyStorageEnabled(enabled)
		var extension bytes.Buffer
		encoder := msgpack.NewEncoder(&extension)
		require.NoError(t, encoder.EncodeExtHeader(experimentalPropertyExtension, 1<<20))
		extension.Write(make([]byte, 1<<20))
		values := []any{"short", string(bytes.Repeat([]byte("x"), 1<<20)), bytes.Repeat([]byte{17}, 1<<20), []any{"nested", int64(3)}, map[string]any{"a": "b"}, int64(7), nil}
		encoded := [][]byte{extension.Bytes()}
		for _, value := range values {
			raw, err := msgpack.Marshal(value)
			require.NoError(t, err)
			encoded = append(encoded, raw)
		}
		for _, raw := range encoded {
			input := append(append([]byte(nil), raw...), 0x2a)
			reader := bytes.NewReader(input)
			dec := msgpack.NewDecoder(reader)
			require.NoError(t, skipUnselectedProperty(dec, reader, input))
			next, err := dec.DecodeInt64()
			require.NoError(t, err)
			require.Equal(t, int64(42), next)
		}
		for _, raw := range [][]byte{{0xd9}, {0xda, 0}, {0xdb, 0, 0, 0}, {0xc4, 2, 1}, {0xc5, 0, 2, 1}, {0xc6, 0xff, 0xff, 0xff, 0xff}, {0xc7, 1, 60}, {0xc8, 0, 1, 60}, {0xc9, 0xff, 0xff, 0xff, 0xff, 60}, {0xa2, 'x'}} {
			reader := bytes.NewReader(raw)
			dec := msgpack.NewDecoder(reader)
			require.Error(t, skipUnselectedProperty(dec, reader, raw))
		}
	}
	config.SetExperimentalPropertyStorageEnabled(true)
	raw := append([]byte{0xc6, 0, 0x10, 0, 0}, make([]byte, 1<<20)...)
	reader := bytes.NewReader(raw)
	dec := msgpack.NewDecoder(reader)
	allocations := testing.AllocsPerRun(100, func() {
		reader.Reset(raw)
		if err := skipUnselectedProperty(dec, reader, raw); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations, "omitted binary payload must not allocate decoder scratch")
	require.Equal(t, 0, reader.Len())
	reader.Reset([]byte{0xc6, 0xff, 0xff, 0xff, 0xff})
	require.ErrorIs(t, skipUnselectedProperty(dec, reader, []byte{0xc6, 0xff, 0xff, 0xff, 0xff}), io.ErrUnexpectedEOF)
}

func BenchmarkExperimentalProjectionSkip(b *testing.B) {
	previous := config.IsExperimentalPropertyStorageEnabled()
	b.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
	var frame bytes.Buffer
	require.NoError(b, msgpack.NewEncoder(&frame).EncodeExtHeader(experimentalPropertyExtension, 20<<10))
	frame.Write(make([]byte, 20<<10))
	binaryValue, err := msgpack.Marshal(bytes.Repeat([]byte{31}, 4<<10))
	require.NoError(b, err)
	for name, raw := range map[string][]byte{"compressed-body": frame.Bytes(), "random-binary": binaryValue} {
		for _, enabled := range []bool{false, true} {
			b.Run(name+map[bool]string{false: "/copy", true: "/seek"}[enabled], func(b *testing.B) {
				config.SetExperimentalPropertyStorageEnabled(enabled)
				reader := bytes.NewReader(raw)
				dec := msgpack.NewDecoder(reader)
				b.ReportAllocs()
				for b.Loop() {
					reader.Reset(raw)
					if err := skipUnselectedProperty(dec, reader, raw); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
