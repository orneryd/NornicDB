package search

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRelationshipSnapshotPackedRoundTrip(t *testing.T) {
	long := make([]float32, 1030)
	for i := range long {
		long[i] = math.Float32frombits(uint32(i + 1))
	}
	want := relationshipVectorsFile{Format: 2, Entries: map[string]*relationshipEntry{
		"nil-entry":     nil,
		"nil-vectors":   {Source: math.MaxUint64},
		"empty-vectors": {Vectors: [][]float32{}},
		"bits":          {Source: 42, Vectors: [][]float32{nil, {}, {math.Float32frombits(0x80000000), math.Float32frombits(1), math.Float32frombits(0x7fc01234), math.Float32frombits(0xff800000)}, long}, Spans: []embeddingutil.ChunkSpan{{Chunk: 2, Property: "reason", Start: 1, End: 4}}},
	}}
	var buf bytes.Buffer
	require.NoError(t, encodeRelationshipSnapshot(&buf, &want))
	var got relationshipVectorsFile
	require.NoError(t, msgpack.Unmarshal(buf.Bytes(), &got))
	require.Equal(t, want.Format, got.Format)
	require.Nil(t, got.Entries["nil-entry"])
	require.Nil(t, got.Entries["nil-vectors"].Vectors)
	require.NotNil(t, got.Entries["empty-vectors"].Vectors)
	require.Empty(t, got.Entries["empty-vectors"].Vectors)
	require.Equal(t, want.Entries["nil-vectors"].Source, got.Entries["nil-vectors"].Source)
	require.Equal(t, want.Entries["bits"].Spans, got.Entries["bits"].Spans)
	require.Nil(t, got.Entries["bits"].Vectors[0])
	require.NotNil(t, got.Entries["bits"].Vectors[1])
	for i, vector := range want.Entries["bits"].Vectors {
		require.Len(t, got.Entries["bits"].Vectors[i], len(vector))
		for j, value := range vector {
			require.Equal(t, math.Float32bits(value), math.Float32bits(got.Entries["bits"].Vectors[i][j]))
		}
	}
	var wire struct {
		Format  int
		Entries map[string]*struct {
			Source  uint64
			Vectors [][]byte
			Spans   []embeddingutil.ChunkSpan
		}
	}
	require.NoError(t, msgpack.Unmarshal(buf.Bytes(), &wire))
	require.Equal(t, uint32(0x80000000), binary.LittleEndian.Uint32(wire.Entries["bits"].Vectors[2][:4]))
}

// Invalid input must fail before it can be treated as a reusable snapshot. In
// particular, a large declared length must not trigger a huge allocation when
// the bytes are missing.
func TestRelationshipSnapshotPackedRejectsMalformedEntry(t *testing.T) {
	for _, kind := range []string{"wrong-vector-type", "odd-vector-length", "odd-length-over-one-buffer", "truncated-large-vector", "truncated-large-array", "duplicate-field", "missing-field", "unknown-field"} {
		t.Run(kind, func(t *testing.T) {
			var buf bytes.Buffer
			enc := msgpack.NewEncoder(&buf)
			fields := 3
			if kind == "missing-field" {
				fields = 2
			}
			if kind == "duplicate-field" || kind == "unknown-field" {
				fields = 4
			}
			require.NoError(t, enc.EncodeMapLen(fields))
			require.NoError(t, enc.EncodeString("Source"))
			require.NoError(t, enc.EncodeUint64(1))
			require.NoError(t, enc.EncodeString("Vectors"))
			if kind == "truncated-large-array" {
				require.NoError(t, enc.EncodeArrayLen(1<<28))
			} else {
				require.NoError(t, enc.EncodeArrayLen(1))
				switch kind {
				case "wrong-vector-type":
					require.NoError(t, enc.EncodeArrayLen(1))
					require.NoError(t, enc.EncodeFloat32(1))
				case "odd-vector-length":
					require.NoError(t, enc.EncodeBytes([]byte{1, 2, 3}))
				case "odd-length-over-one-buffer":
					require.NoError(t, enc.EncodeBytes(make([]byte, 4098)))
				case "truncated-large-vector":
					require.NoError(t, enc.EncodeBytesLen(1<<28))
				default:
					require.NoError(t, enc.EncodeBytes([]byte{0, 0, 0, 0}))
				}
			}
			if fields >= 3 {
				require.NoError(t, enc.EncodeString("Spans"))
				require.NoError(t, enc.EncodeNil())
			}
			if fields == 4 {
				name := "other"
				if kind == "duplicate-field" {
					name = "Source"
				}
				require.NoError(t, enc.EncodeString(name))
				require.NoError(t, enc.EncodeUint64(2))
			}
			var got relationshipEntry
			require.Error(t, msgpack.NewDecoder(&buf).Decode(&got))
		})
	}
}

func TestRelationshipSnapshotRejectsMalformedEnvelope(t *testing.T) {
	for _, kind := range []string{"missing-entries", "duplicate-format", "truncated-large-map"} {
		t.Run(kind, func(t *testing.T) {
			var buf bytes.Buffer
			enc := msgpack.NewEncoder(&buf)
			fields := 2
			if kind == "missing-entries" {
				fields = 1
			}
			if kind == "duplicate-format" {
				fields = 3
			}
			require.NoError(t, enc.EncodeMapLen(fields))
			require.NoError(t, enc.EncodeString("Format"))
			require.NoError(t, enc.EncodeInt(2))
			if kind == "duplicate-format" {
				require.NoError(t, enc.EncodeString("Format"))
				require.NoError(t, enc.EncodeInt(2))
			}
			if kind != "missing-entries" {
				require.NoError(t, enc.EncodeString("Entries"))
				if kind == "truncated-large-map" {
					require.NoError(t, enc.EncodeMapLen(1<<20))
				} else {
					require.NoError(t, enc.EncodeMapLen(0))
				}
			}
			_, _, err := decodeRelationshipSnapshot(bytes.NewReader(buf.Bytes()))
			require.Error(t, err)
		})
	}
}
