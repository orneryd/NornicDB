package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSnapshotValueIdentityReviewedBoundaries(t *testing.T) {
	check := func(t *testing.T, raw []byte, want bool) {
		t.Helper()
		require.Equal(t, want, snapshotValueIdentity(raw))
		if !want {
			return
		}
		reader := bytes.NewReader(raw)
		value, err := decodeStrictTypedValue(msgpack.NewDecoder(reader))
		require.NoError(t, err)
		require.Zero(t, reader.Len())
		var output bytes.Buffer
		encoder := msgpack.NewEncoder(&output).SetSortMapKeys(true)
		require.NoError(t, encoder.Encode(encodeStoredTemporalValue(restoreStoredTemporalValue(value))))
		require.Equal(t, raw, output.Bytes())
	}
	for _, tc := range []struct {
		name, encoded string
		want          bool
	}{
		{"fixext8-timestamp", "d7ff0000000000000000", false},
		{"unknown-ext8", "c70501aabbccddee", false},
		{"canonical-bin8", "c401ff", true},
		{"noncanonical-bin16", "c50001ff", false},
		{"raw-string-byte", "a1ff", true},
		{"fixed-int", "05", false},
		{"explicit-int8", "d005", false},
		{"uint64-normalizes", "cf0000000000000005", false},
		{"uint64-preserved", "cf8000000000000000", true},
		{"narrow-array", "91d001", false},
		{"wide-array", "91d30000000000000001", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := hex.DecodeString(tc.encoded)
			require.NoError(t, err)
			check(t, raw, tc.want)
		})
	}
	for _, size := range []int{1, 15, 16} {
		t.Run(fmt.Sprintf("float64-array-%d", size), func(t *testing.T) {
			values := make([]float64, size)
			for i := range values {
				values[i] = float64(i) + 0.5
			}
			raw, err := msgpack.Marshal(values)
			require.NoError(t, err)
			check(t, raw, true)
		})
	}
}
