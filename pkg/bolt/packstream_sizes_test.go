package bolt

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Lists and maps of every header size (tiny, 8-, 16- and 32-bit) round-trip:
// a list or map of 65,536 entries or more is not cut to a 16-bit size.
func TestPackStreamListAndMapHeaderSizes(t *testing.T) {
	for _, size := range []int{0, 15, 16, 255, 256, 65535, 65536, 70000} {
		list := make([]any, size)
		ints := make([]int64, size)
		strs := make([]string, size)
		bools := make([]bool, size)
		props := make(map[string]any, size)
		for i := range list {
			list[i] = int64(i)
			ints[i] = int64(i)
			strs[i] = strconv.Itoa(i)
			bools[i] = i%2 == 0
			props[strconv.Itoa(i)] = int64(i)
		}
		for _, value := range []any{list, ints, strs, bools, props} {
			encoded := encodePackStreamValueInto(nil, value)
			decoded, consumed, err := decodePackStreamValue(encoded, 0)
			require.NoError(t, err, "size %d %T", size, value)
			require.Equal(t, len(encoded), consumed, "size %d %T", size, value)
			switch typed := decoded.(type) {
			case []any:
				require.Len(t, typed, size, "%T", value)
			case map[string]any:
				require.Len(t, typed, size, "%T", value)
			default:
				t.Fatalf("size %d %T decoded as %T", size, value, decoded)
			}
		}
	}
}

// A stored LIST<BOOLEAN> or LIST<INTEGER> of int32 encodes as a list, not
// null.
func TestPackStreamBoolAndInt32Lists(t *testing.T) {
	decoded, _, err := decodePackStreamValue(encodePackStreamValueInto(nil, []bool{true, false}), 0)
	require.NoError(t, err)
	require.Equal(t, []any{true, false}, decoded)
	decoded, _, err = decodePackStreamValue(encodePackStreamValueInto(nil, []int32{7, -1}), 0)
	require.NoError(t, err)
	require.Equal(t, []any{int64(7), int64(-1)}, decoded)
	_, _, err = packStreamMapHeader(nil)
	require.ErrorContains(t, err, "missing map")
	_, _, err = packStreamMapHeader([]byte{0xDA, 0, 1, 0, 0})
	require.ErrorContains(t, err, "map size exceeds message size")
	_, _, err = packStreamMapHeader([]byte{0xDA, 0})
	require.ErrorContains(t, err, "incomplete MAP32")
}
