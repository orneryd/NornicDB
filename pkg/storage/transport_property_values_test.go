package storage

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// malformedTransportPropertyCases are MessagePack payloads that must be
// rejected deterministically: forged or truncated collection lengths, an
// invalid code, truncated tuples/tags and malformed scalar payloads.
var malformedTransportPropertyCases = []struct {
	name  string
	value []byte
}{
	{"empty input", []byte{}},
	{"invalid code 0xc1", []byte{0xc1}},
	{"array instead of map", []byte{0x90}},
	{"truncated map32 header", []byte{0xdf, 0xff}},
	{"forged map32 length", []byte{0xdf, 0xff, 0xff, 0xff, 0xff}},
	{"forged map16 length", []byte{0xde, 0xff, 0xff}},
	{"truncated map16 header", []byte{0xde, 0x00}},
	{"entry without value", []byte{0x81, 0xa1, 'x'}},
	{"entry with truncated tag", []byte{0x81, 0xa1, 'x', 0x91}},
	{"entry with non-integer tag", []byte{0x81, 0xa1, 'x', 0x92, 0xa3, 'a', 'b', 'c', 0xc0}},
	{"tuple length three", []byte{0x81, 0xa1, 'x', 0x93, 0x00, 0xc0, 0xc0}},
	{"unknown tag", []byte{0x81, 0xa1, 'x', 0x92, 0x7f, 0xc0}},
	{"overflowing tag", []byte{0x81, 0xa1, 'x', 0x92, 0xcd, 0x01, 0x00, 0xc0}},
	{"overflowing int8", []byte{0x81, 0xa1, 'x', 0x92, 0x04, 0xcd, 0x00, 0x80}},
	{"negative uint8", []byte{0x81, 0xa1, 'x', 0x92, 0x09, 0xff}},
	{"nil integer", []byte{0x81, 0xa1, 'x', 0x92, 0x03, 0xc0}},
	{"overflowing int32 slice", []byte{0x81, 0xa1, 'x', 0x92, 0x13, 0x91, 0xce, 0xff, 0xff, 0xff, 0xff}},
	{"tag without value", []byte{0x81, 0xa1, 'x', 0x92, 0x02}},
	{"mistyped bool", []byte{0x81, 0xa1, 'x', 0x92, 0x01, 0xa1, 'y'}},
	{"truncated string value", []byte{0x81, 0xa1, 'x', 0x92, 0x02, 0xa3, 'a'}},
	{"truncated bytes value", []byte{0x81, 0xa1, 'x', 0x92, 0x10, 0xc6, 0xff, 0xff, 0xff, 0xff}},
	{"truncated float64 value", []byte{0x81, 0xa1, 'x', 0x92, 0x0e, 0xcb, 0x40}},
	{"truncated uint64 value", []byte{0x81, 0xa1, 'x', 0x92, 0x0c, 0xcf, 0x00, 0x00}},
	{"malformed time payload", []byte{0x81, 0xa1, 'x', 0x92, 0x0f, 0xa1, 'z'}},
	{"forged nested list32 length", []byte{0x81, 0xa1, 'x', 0x92, 0x18, 0xdd, 0xff, 0xff, 0xff, 0xff}},
	{"forged nested map32 length", []byte{0x81, 0xa1, 'x', 0x92, 0x19, 0xdf, 0xff, 0xff, 0xff, 0xff}},
	{"truncated nested array16", []byte{0x81, 0xa1, 'x', 0x92, 0x18, 0xdc}},
}

// transportDynamicValues are the complete supported dynamic value set
// reachable from a property map, including every numeric width, temporal
// value, typed slice and nested container.
func transportDynamicValues() map[string]any {
	timestamp := time.Date(2024, time.March, 9, 12, 34, 56, 789123456, time.UTC)
	return map[string]any{
		"nil":      nil,
		"bool":     true,
		"string":   "hello",
		"int":      int(-42),
		"int8":     int8(math.MinInt8),
		"int16":    int16(math.MinInt16),
		"int32":    int32(math.MinInt32),
		"int64":    int64(math.MinInt64),
		"uint":     ^uint(0),
		"uint8":    uint8(math.MaxUint8),
		"uint16":   uint16(math.MaxUint16),
		"uint32":   uint32(math.MaxUint32),
		"uint64":   uint64(math.MaxUint64),
		"float32":  float32(1.5),
		"float64":  math.MaxFloat64,
		"time":     timestamp,
		"bytes":    []byte{0x00, 0xff, 0x10},
		"strings":  []string{"a", ""},
		"ints":     []int{math.MinInt, math.MaxInt},
		"int32s":   []int32{math.MinInt32, math.MaxInt32},
		"int64s":   []int64{math.MinInt64, math.MaxInt64},
		"float32s": []float32{-math.MaxFloat32, math.MaxFloat32},
		"float64s": []float64{math.SmallestNonzeroFloat64, math.MaxFloat64},
		"bools":    []bool{true, false},
		"any":      []any{int64(7), "mixed", nil, []int64{1, -1}},
		"map":      map[string]any{"nested": int64(-1)},
	}
}

func requireTransportDynamicValue(t *testing.T, key string, want, got any) {
	t.Helper()
	switch want := want.(type) {
	case nil:
		require.Nil(t, got, "value for key %q", key)
	case time.Time:
		gotTime, ok := got.(time.Time)
		require.True(t, ok, "value for key %q decoded as %T", key, got)
		require.True(t, want.Equal(gotTime), "value for key %q: want %s, got %s", key, want, gotTime)
	case []byte:
		require.IsType(t, []byte(nil), got, "value for key %q", key)
		require.Equal(t, want, got.([]byte), "value for key %q", key)
	case []any:
		gotAny, ok := got.([]any)
		require.True(t, ok, "value for key %q decoded as %T", key, got)
		require.Len(t, gotAny, len(want), "value for key %q", key)
		for i := range want {
			requireTransportDynamicValue(t, key, want[i], gotAny[i])
		}
	case map[string]any:
		gotMap, ok := got.(map[string]any)
		require.True(t, ok, "value for key %q decoded as %T", key, got)
		require.Len(t, gotMap, len(want), "value for key %q", key)
		for nestedKey, nestedWant := range want {
			nestedGot, ok := gotMap[nestedKey]
			require.True(t, ok, "nested key %q of %q is missing", nestedKey, key)
			requireTransportDynamicValue(t, nestedKey, nestedWant, nestedGot)
		}
	default:
		require.IsType(t, want, got, "value for key %q", key)
		require.Equal(t, want, got, "value for key %q", key)
	}
}

func TestTransportPropertyMapRoundTripsSupportedDynamicValues(t *testing.T) {
	values := transportDynamicValues()

	data, err := msgpack.Marshal(transportPropertyMap(values))
	require.NoError(t, err)

	var decoded transportPropertyMap
	require.NoError(t, msgpack.Unmarshal(data, &decoded))
	require.Len(t, decoded, len(values))

	for key, want := range values {
		got, ok := decoded[key]
		require.True(t, ok, "key %q missing after round trip", key)
		requireTransportDynamicValue(t, key, want, got)
	}

	// Exact Go types for the scalar and slice keys, so a widening to a
	// generic interface value cannot pass unnoticed.
	require.IsType(t, true, decoded["bool"])
	require.IsType(t, "", decoded["string"])
	require.IsType(t, int(0), decoded["int"])
	require.IsType(t, int8(0), decoded["int8"])
	require.IsType(t, int16(0), decoded["int16"])
	require.IsType(t, int32(0), decoded["int32"])
	require.IsType(t, int64(0), decoded["int64"])
	require.IsType(t, uint(0), decoded["uint"])
	require.IsType(t, uint8(0), decoded["uint8"])
	require.IsType(t, uint16(0), decoded["uint16"])
	require.IsType(t, uint32(0), decoded["uint32"])
	require.IsType(t, uint64(0), decoded["uint64"])
	require.IsType(t, float32(0), decoded["float32"])
	require.IsType(t, float64(0), decoded["float64"])
	require.IsType(t, time.Time{}, decoded["time"])
	require.IsType(t, []byte(nil), decoded["bytes"])
	require.IsType(t, []string(nil), decoded["strings"])
	require.IsType(t, []int(nil), decoded["ints"])
	require.IsType(t, []int32(nil), decoded["int32s"])
	require.IsType(t, []int64(nil), decoded["int64s"])
	require.IsType(t, []float32(nil), decoded["float32s"])
	require.IsType(t, []float64(nil), decoded["float64s"])
	require.IsType(t, []bool(nil), decoded["bools"])
	require.IsType(t, []any(nil), decoded["any"])
	require.IsType(t, map[string]any(nil), decoded["map"])

	// Spot-check exact values that a lossy narrowing would corrupt.
	require.Equal(t, int64(math.MinInt64), decoded["int64"])
	require.Equal(t, uint64(math.MaxUint64), decoded["uint64"])
	require.Equal(t, ^uint(0), decoded["uint"])
	require.Equal(t, []int{math.MinInt, math.MaxInt}, decoded["ints"])
	require.Equal(t, []int32{math.MinInt32, math.MaxInt32}, decoded["int32s"])
	require.Equal(t, []float32{-math.MaxFloat32, math.MaxFloat32}, decoded["float32s"])
	require.Equal(t, []bool{true, false}, decoded["bools"])
	require.Equal(t, int64(-1), decoded["any"].([]any)[3].([]int64)[1])
}

func TestTransportPropertyMapRoundTripsNumericBoundaries(t *testing.T) {
	values := map[string]any{
		"maxInt8":     int8(math.MaxInt8),
		"maxInt16":    int16(math.MaxInt16),
		"maxInt32":    int32(math.MaxInt32),
		"maxInt64":    int64(math.MaxInt64),
		"signedInt":   int(-1),
		"zeroInt":     int(0),
		"maxUint8":    uint8(math.MaxUint8),
		"maxUint16":   uint16(math.MaxUint16),
		"maxUint32":   uint32(math.MaxUint32),
		"maxUint64":   uint64(math.MaxUint64),
		"maxUint":     ^uint(0),
		"negativeF32": float32(-3.25),
		"negativeF64": -2.718281828459045,
	}

	data, err := msgpack.Marshal(transportPropertyMap(values))
	require.NoError(t, err)

	var decoded transportPropertyMap
	require.NoError(t, msgpack.Unmarshal(data, &decoded))
	require.Len(t, decoded, len(values))

	for key, want := range values {
		got, ok := decoded[key]
		require.True(t, ok, "key %q missing after round trip", key)
		requireTransportDynamicValue(t, key, want, got)
	}
}

func TestTransportPropertyMapPreservesNilAndEmptyCollections(t *testing.T) {
	values := map[string]any{
		"nilInner":     map[string]any(nil),
		"emptyInner":   map[string]any{},
		"nilAnySlice":  []any(nil),
		"emptyAny":     []any{},
		"nilStrings":   []string(nil),
		"emptyStrings": []string{},
		"nilInts":      []int(nil),
		"emptyInts":    []int{},
		"nilBytes":     []byte(nil),
		"nilBools":     []bool(nil),
		"nilFloats":    []float64(nil),
		"nilValue":     nil,
		"nested": map[string]any{
			"nilMap":    map[string]any(nil),
			"emptyMap":  map[string]any{},
			"nilList":   []any(nil),
			"emptyList": []any{},
			"nilTyped":  []string(nil),
		},
	}

	data, err := msgpack.Marshal(transportPropertyMap(values))
	require.NoError(t, err)

	var decoded transportPropertyMap
	require.NoError(t, msgpack.Unmarshal(data, &decoded))
	require.Len(t, decoded, len(values))

	require.Nil(t, decoded["nilInner"], "nil map must stay nil")
	require.Nil(t, decoded["nilAnySlice"], "nil list must stay nil")
	require.Nil(t, decoded["nilStrings"], "nil typed slice must stay nil")
	require.Nil(t, decoded["nilInts"], "nil typed slice must stay nil")
	require.Nil(t, decoded["nilBytes"], "nil byte slice must stay nil")
	require.Nil(t, decoded["nilBools"], "nil typed slice must stay nil")
	require.Nil(t, decoded["nilFloats"], "nil typed slice must stay nil")
	require.Nil(t, decoded["nilValue"])

	for key, want := range map[string]any{
		"emptyInner":   map[string]any{},
		"emptyAny":     []any{},
		"emptyStrings": []string{},
		"emptyInts":    []int{},
	} {
		got, ok := decoded[key]
		require.True(t, ok, "key %q missing", key)
		require.NotNil(t, got, "empty collection %q must decode non-nil", key)
		requireTransportDynamicValue(t, key, want, got)
	}

	nested, ok := decoded["nested"].(map[string]any)
	require.True(t, ok, "nested map decoded as %T", decoded["nested"])
	require.Nil(t, nested["nilMap"])
	require.Nil(t, nested["nilList"])
	require.Nil(t, nested["nilTyped"])
	require.NotNil(t, nested["emptyMap"])
	require.Empty(t, nested["emptyMap"].(map[string]any))
	require.NotNil(t, nested["emptyList"])
	require.Empty(t, nested["emptyList"].([]any))
}

func TestTransportPropertyMapNilVersusEmptyMap(t *testing.T) {
	nilData, err := msgpack.Marshal(transportPropertyMap(nil))
	require.NoError(t, err)
	emptyData, err := msgpack.Marshal(transportPropertyMap{})
	require.NoError(t, err)

	var nilDecoded transportPropertyMap
	require.NoError(t, msgpack.Unmarshal(nilData, &nilDecoded))
	require.Nil(t, nilDecoded, "a nil map must decode as nil, not an empty map")

	var emptyDecoded transportPropertyMap
	require.NoError(t, msgpack.Unmarshal(emptyData, &emptyDecoded))
	require.NotNil(t, emptyDecoded, "an empty map must decode as a non-nil map")
	require.Empty(t, emptyDecoded)
	require.Len(t, emptyDecoded, 0)
}

func TestTransportPropertyMapRoundTripsNestedStructures(t *testing.T) {
	timestamp := time.Date(2024, time.December, 31, 23, 59, 58, 500, time.UTC)
	values := map[string]any{
		"nested": map[string]any{
			"ints":    []int{1, 2, 3},
			"int64":   int64(math.MinInt64),
			"strings": []string{"x", "y"},
			"ulong":   uint64(math.MaxUint64),
			"deeper": map[string]any{
				"time":    timestamp,
				"floats":  []float32{0.25, -0.5},
				"bytes":   []byte{9, 8, 7},
				"mixed":   []any{uint16(65535), int8(-3), map[string]any{"bools": []bool{false, true}}},
				"nilNode": nil,
			},
		},
		"listOfMaps": []any{
			map[string]any{"id": int64(1)},
			map[string]any{"id": int64(2), "tags": []string{"a"}},
		},
	}

	data, err := msgpack.Marshal(transportPropertyMap(values))
	require.NoError(t, err)

	var decoded transportPropertyMap
	require.NoError(t, msgpack.Unmarshal(data, &decoded))
	require.Len(t, decoded, len(values))

	requireTransportDynamicValue(t, "nested", values["nested"], decoded["nested"])
	requireTransportDynamicValue(t, "listOfMaps", values["listOfMaps"], decoded["listOfMaps"])

	nested := decoded["nested"].(map[string]any)
	require.Equal(t, []int{1, 2, 3}, nested["ints"])
	require.Equal(t, int64(math.MinInt64), nested["int64"])
	require.Equal(t, []string{"x", "y"}, nested["strings"])
	require.Equal(t, uint64(math.MaxUint64), nested["ulong"])

	deeper, ok := nested["deeper"].(map[string]any)
	require.True(t, ok, "deep nested map decoded as %T", nested["deeper"])
	requireTransportDynamicValue(t, "time", timestamp, deeper["time"])
	require.Equal(t, []float32{0.25, -0.5}, deeper["floats"])
	require.Equal(t, []byte{9, 8, 7}, deeper["bytes"])
	require.Nil(t, deeper["nilNode"])

	mixed, ok := deeper["mixed"].([]any)
	require.True(t, ok, "mixed list decoded as %T", deeper["mixed"])
	require.Len(t, mixed, 3)
	require.IsType(t, uint16(0), mixed[0])
	require.Equal(t, uint16(65535), mixed[0])
	require.IsType(t, int8(0), mixed[1])
	require.Equal(t, int8(-3), mixed[1])
	requireTransportDynamicValue(t, "mixed", []bool{false, true}, mixed[2].(map[string]any)["bools"])
}

func TestTransportPropertyMapRejectsMalformedEncodings(t *testing.T) {
	for _, tc := range malformedTransportPropertyCases {
		t.Run(tc.name, func(t *testing.T) {
			var decoded transportPropertyMap
			err := msgpack.Unmarshal(tc.value, &decoded)
			require.Error(t, err, "payload %x must be rejected", tc.value)
			require.NotEmpty(t, err.Error(), "error text for payload %x must be populated", tc.value)

			var repeated transportPropertyMap
			repeatedErr := msgpack.Unmarshal(tc.value, &repeated)
			require.Error(t, repeatedErr, "payload %x must be rejected deterministically", tc.value)
			require.Equal(t, err.Error(), repeatedErr.Error(), "identical payload must produce an identical error")
		})
	}
}

type unsupportedTransportPropertyValue struct {
	Reason string
}

func TestTransportPropertyMapRejectsUnsupportedTypes(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{"struct", unsupportedTransportPropertyValue{Reason: "nope"}},
		{"struct pointer", &unsupportedTransportPropertyValue{Reason: "nope"}},
		{"channel", make(chan int)},
		{"function", func() {}},
		{"complex", complex(1, 2)},
		{"nested struct in list", []any{"ok", unsupportedTransportPropertyValue{Reason: "nope"}}},
		{"nested struct in map", map[string]any{"ok": int64(1), "bad": unsupportedTransportPropertyValue{Reason: "nope"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := msgpack.Marshal(transportPropertyMap{"value": tc.value})
			require.Error(t, err, "value %T must be rejected", tc.value)
			require.Contains(t, err.Error(), "unsupported transport property type", "error for value %T", tc.value)
		})
	}
}
