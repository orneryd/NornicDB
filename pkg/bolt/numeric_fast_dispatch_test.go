package bolt

import (
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/stretchr/testify/require"
)

func TestNumericWireTypedAndReflectiveDispatch(t *testing.T) {
	type namedSigned int64
	type namedUnsigned uint64
	type namedFloat float64
	values := []any{
		int(-17), int8(-128), int16(-32768), int32(math.MinInt32),
		int64(math.MinInt64), int64(-9007199254740993), int64(-129), int64(-16), int64(127),
		int64(128), int64(32767), int64(32768), int64(math.MaxInt32), int64(9007199254740993), int64(math.MaxInt64),
		uint(1), uint8(255), uint16(65535), uint32(math.MaxUint32),
		uint64(math.MaxInt64), uint64(math.MaxInt64) + 1, uint64(math.MaxUint64),
		float32(2), float64(2), math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64,
		namedSigned(math.MinInt64), namedUnsigned(math.MaxUint64), namedFloat(2),
	}
	for _, value := range values {
		want, ok := convert.AppendBoltNumber(nil, value)
		require.True(t, ok)
		require.Equal(t, want, encodePackStreamValue(value), "%T(%v)", value, value)
		for _, utc := range []bool{false, true} {
			require.Equal(t, append([]byte{0x90}, want...), encodePackStreamValueIntoWithUTC([]byte{0x90}, value, utc), "%T(%v)", value, value)
			nested := map[string]any{"v": []any{value}}
			raw := encodePackStreamValueIntoWithUTC(nil, nested, utc)
			decoded, used, err := decodePackStreamValue(raw, 0, utc)
			require.NoError(t, err)
			require.Equal(t, len(raw), used)
			leaf, _, err := decodePackStreamValue(want, 0, utc)
			require.NoError(t, err)
			require.Equal(t, map[string]any{"v": []any{leaf}}, decoded)
		}
	}
	// A duration is a protocol conversion, not its named int64 nanoseconds.
	duration := 1234*time.Millisecond + 5*time.Microsecond
	want := convert.AppendBoltInteger(nil, 1234)
	require.Equal(t, want, encodePackStreamValue(duration))
	require.Equal(t, want, encodePackStreamValueInto(nil, duration))
}
