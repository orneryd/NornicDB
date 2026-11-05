package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWALBufferedCommitCompressionRoundTrip(t *testing.T) {
	// Build a payload that mirrors a real buffered-commit: repetitive JSON
	// with many keys and a large raw-write map, which is the dominant WAL
	// cost. Generate enough entries to clear the 512-byte compression floor.
	raw := WALBufferedCommitData{
		TxID:      "tx-compression-test",
		Namespace: "bench",
		Ops:       make([]Operation, 0, 200),
		Writes:    make(map[string][]byte, 500),
		Deletes:   make([]string, 0, 200),
		Version: MVCCVersion{
			CommitTimestamp: time.Now().UTC(),
			CommitSequence:  7,
		},
		NodeMax:     100000,
		EdgeMax:     320000,
		HasCounters: true,
		PropKeys: []WALPropKeyEntry{
			{Namespace: "bench", Name: "name", ID: 1},
			{Namespace: "bench", Name: "email", ID: 2},
		},
		PropKeyCounters: map[string]uint64{"name": 1000, "email": 2000},
	}
	for i := 0; i < 200; i++ {
		raw.Ops = append(raw.Ops, Operation{
			Type:   "create_node",
			NodeID: NodeID(fmt.Sprintf("node-%d", i)),
		})
		raw.Deletes = append(raw.Deletes, fmt.Sprintf("node:%d:props", i))
	}
	for i := 0; i < 500; i++ {
		raw.Writes[fmt.Sprintf("edge:%d:rel", i)] = bytes.Repeat([]byte{byte(i % 251)}, 64)
	}

	rawJSON, err := json.Marshal(raw)
	require.NoError(t, err)
	require.Greater(t, len(rawJSON), 512, "payload must exceed the compression floor")

	compressed := compressWALBufferedCommitPayload(rawJSON)
	require.NotEqual(t, rawJSON, compressed, "large payload should be compressed")
	require.True(t, len(compressed) < len(rawJSON), "compression should shrink the payload")

	// zstd frame magic must be present so replay can detect compression.
	require.Equal(t, byte(0x28), compressed[0])
	require.Equal(t, byte(0xB5), compressed[1])
	require.Equal(t, byte(0x2F), compressed[2])
	require.Equal(t, byte(0xFD), compressed[3])

	decompressed, err := decompressWALBufferedCommitPayload(compressed)
	require.NoError(t, err)
	require.Equal(t, rawJSON, decompressed, "round-trip must be byte-for-byte identical")

	// Raw payloads (below the floor, or never compressed) must pass through.
	small := []byte(`{"TxID":1}`)
	require.Equal(t, small, compressWALBufferedCommitPayload(small))
	smallOut, err := decompressWALBufferedCommitPayload(small)
	require.NoError(t, err)
	require.Equal(t, small, smallOut)

	// A raw large payload with no zstd magic must pass through unchanged too.
	rawBig := bytes.Repeat([]byte("x"), 2048)
	rawBigOut, err := decompressWALBufferedCommitPayload(rawBig)
	require.NoError(t, err)
	require.Equal(t, rawBig, rawBigOut)
}

func TestWALBufferedCommitRejectsCompressedExpansionPastContract(t *testing.T) {
	raw := bytes.Repeat([]byte{'x'}, maxBufferedWALPayloadBytes+1)
	compressed := compressWALBufferedCommitPayload(raw)
	require.Less(t, len(compressed), maxBufferedWALPayloadBytes)
	decoded, err := decompressWALBufferedCommitPayload(compressed)
	require.ErrorContains(t, err, "buffered commit")
	require.Nil(t, decoded)
	decoded, err = decompressWALBufferedCommitPayload(raw)
	require.ErrorContains(t, err, "decoded payload limit")
	require.Nil(t, decoded)
}
