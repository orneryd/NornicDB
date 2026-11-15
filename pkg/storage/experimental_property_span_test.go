package storage

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	forkcodec "github.com/dgraph-io/badger/v4/experimental/nornic"
	"github.com/klauspost/compress/zstd"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestExperimentalStoredPropertySpanParityAndOwnership(t *testing.T) {
	previous := config.IsExperimentalPropertyStorageEnabled()
	t.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
	legacyEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	defer legacyEncoder.Close()
	values := []any{strings.Repeat("documentation body ", 2048), bytes.Repeat([]byte("bytes"), 2048), make([]int64, 1024), map[string]any{"text": strings.Repeat("nested ", 512)}}
	for _, value := range values {
		raw, err := msgpack.Marshal(value)
		require.NoError(t, err)
		payload, err := forkcodec.EncodePayload(raw, 6)
		require.NoError(t, err)
		require.NotNil(t, payload)
		legacy := make([]byte, 5)
		legacy[0] = 1
		binary.LittleEndian.PutUint32(legacy[1:5], uint32(len(raw)))
		legacy = legacyEncoder.EncodeAll(raw, legacy)
		for _, payload := range [][]byte{payload, legacy} {
			var frame bytes.Buffer
			require.NoError(t, msgpack.NewEncoder(&frame).EncodeExtHeader(experimentalPropertyExtension, len(payload)))
			frame.Write(payload)
			var expected any
			for _, enabled := range []bool{false, true} {
				config.SetExperimentalPropertyStorageEnabled(enabled)
				encoded := append(append([]byte(nil), frame.Bytes()...), 42)
				reader := bytes.NewReader(encoded)
				dec := msgpack.NewDecoder(reader)
				got, err := decodeStoredPropertyValue(dec, reader, encoded)
				require.NoError(t, err)
				if !enabled {
					expected = got
				} else {
					require.Equal(t, expected, got)
				}
				next, err := dec.DecodeInt64()
				require.NoError(t, err)
				require.Equal(t, int64(42), next)
				clear(encoded)
				if enabled {
					scratch := experimentalPropertyScratchPool.Get().(*experimentalPropertyScratch)
					clear(scratch.bytes[:cap(scratch.bytes)])
					require.LessOrEqual(t, cap(scratch.bytes), maxExperimentalPropertyScratchBytes)
					experimentalPropertyScratchPool.Put(scratch)
				}
				require.Equal(t, expected, got, "decoded values cannot borrow the stored frame")
			}
		}
	}
}

func TestExperimentalStoredPropertyScratchDoesNotRetainLargeValues(t *testing.T) {
	experimentalStorageFlag(t, true)
	value := strings.Repeat("large document ", maxExperimentalPropertyScratchBytes/4)
	raw, err := msgpack.Marshal(value)
	require.NoError(t, err)
	payload, err := forkcodec.EncodePayload(raw, 6)
	require.NoError(t, err)
	got, err := decodeExperimentalProperty(payload)
	require.NoError(t, err)
	require.Equal(t, value, got)
	scratch := experimentalPropertyScratchPool.Get().(*experimentalPropertyScratch)
	require.LessOrEqual(t, cap(scratch.bytes), maxExperimentalPropertyScratchBytes)
	experimentalPropertyScratchPool.Put(scratch)
}

func TestExperimentalStoredPropertySpanFraming(t *testing.T) {
	experimentalStorageFlag(t, true)
	// Selected frames retain the canonical codec/length failures.
	for _, raw := range [][]byte{{0xc7, 1, 60}, {0xc7, 0, 60}, {0xc7, 1, 60, 99}, {0xc9, 0xff, 0xff, 0xff, 0xff, 60}} {
		reader := bytes.NewReader(raw)
		_, err := decodeStoredPropertyValue(msgpack.NewDecoder(reader), reader, raw)
		require.Error(t, err)
	}
	raw, _ := msgpack.Marshal(strings.Repeat("text", 256))
	raw = append(raw, 42) // a valid compressed frame containing a second typed value
	payload, err := forkcodec.EncodePayload(raw, 6)
	require.NoError(t, err)
	require.NotNil(t, payload)
	var frame bytes.Buffer
	require.NoError(t, msgpack.NewEncoder(&frame).EncodeExtHeader(experimentalPropertyExtension, len(payload)))
	frame.Write(payload)
	reader := bytes.NewReader(frame.Bytes())
	_, err = decodeStoredPropertyValue(msgpack.NewDecoder(reader), reader, frame.Bytes())
	require.ErrorContains(t, err, "trailing value bytes")
	// String framing used by filters and compressed values shares one owner.
	_, _, ok := msgpackStringPayload([]byte{0xdb, 0xff, 0xff, 0xff, 0xff})
	require.False(t, ok)
	bad := bytes.Clone(payload)
	binary.LittleEndian.PutUint32(bad[1:5], forkcodec.MaxPayloadBytes+1)
	_, err = decodeExperimentalProperty(bad)
	require.Error(t, err)
}
