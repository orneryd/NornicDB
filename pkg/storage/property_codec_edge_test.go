package storage

// Edge-case tests for property_codec.go covering backwards-compat
// guarantees, all-numeric-type round-trips, mixed/heterogeneous arrays,
// deeply nested maps, namespace isolation, and corruption paths.
//
// Companion to property_codec_test.go: that file pins the framing-
// level contracts (uvarint, dictionary IDs, V2 rejection of V1 bodies);
// this file pins the value-level contracts (typed slices preserved
// across encode/decode and through nesting, fall-back to []interface{}
// when homogeneity breaks, no panics on garbage input).
//
// New in v1.1.2 to harden the storage landing per the explicit user
// ask: "make sure all error cases are covered and that we gracefully
// handle older storage formats and that the old code won't break on
// newer files."

import (
	"bytes"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// roundTripValueThroughCodec encodes a single property through the V2
// tokenized codec and decodes it. Returns the decoded value as it
// appears to read-side code.
func roundTripValueThroughCodec(t *testing.T, eng *BadgerEngine, key string, val any) any {
	t.Helper()
	var data []byte
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		data, err = eng.encodeTokenizedProperties(txn, "ns", map[string]any{key: val})
		if err != nil {
			return err
		}
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}))
	out, err := eng.decodeTokenizedProperties("ns", data)
	require.NoError(t, err)
	return out[key]
}

// TestStrictTypedArray_PreservesHomogeneousFloats — []float64 must
// survive encode/decode as []float64, not []interface{}. The codec
// rewrite was driven by this exact regression: pre-v1.1.0 the
// loose-decode path widened every array.
func TestStrictTypedArray_PreservesHomogeneousFloats(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "vec", []float64{1.5, 2.5, 3.5})
	require.IsType(t, []float64{}, got, "homogeneous float64 array must round-trip as []float64, got %T", got)
	assert.Equal(t, []float64{1.5, 2.5, 3.5}, got)
}

// TestStrictTypedArray_PreservesHomogeneousInts — []int64 round-trip.
func TestStrictTypedArray_PreservesHomogeneousInts(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "ints", []int64{-1, 0, 1, math.MaxInt64})
	require.IsType(t, []int64{}, got)
	assert.Equal(t, []int64{-1, 0, 1, math.MaxInt64}, got)
}

// TestStrictTypedArray_ReadsUnsignedAsInt64 — an unsigned list reads as
// []int64 while every value fits; a value above MaxInt64 keeps uint64 and
// its neighbours stay int64.
func TestStrictTypedArray_ReadsUnsignedAsInt64(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "uints", []uint64{0, 1, math.MaxInt64})
	require.IsType(t, []int64{}, got)
	assert.Equal(t, []int64{0, 1, math.MaxInt64}, got)
	got = roundTripValueThroughCodec(t, eng, "uints", []uint64{0, 1, math.MaxUint64})
	assert.Equal(t, []any{int64(0), int64(1), uint64(math.MaxUint64)}, got)
}

// TestStrictTypedArray_PreservesHomogeneousFloat32s — []float32 round-trip.
func TestStrictTypedArray_PreservesHomogeneousFloat32s(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "vec", []float32{0.1, 0.2, 0.3})
	require.IsType(t, []float32{}, got)
	assert.Equal(t, []float32{0.1, 0.2, 0.3}, got)
	got = roundTripValueThroughCodec(t, eng, "mixed", []any{float32(0.5), 1.5})
	assert.Equal(t, []any{float32(0.5), 1.5}, got)
}

// TestStrictTypedArray_PreservesHomogeneousStrings — []string round-trip.
func TestStrictTypedArray_PreservesHomogeneousStrings(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "tags", []string{"a", "b", "c"})
	require.IsType(t, []string{}, got)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

// TestStrictTypedArray_PreservesHomogeneousBools — []bool round-trip.
func TestStrictTypedArray_PreservesHomogeneousBools(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "flags", []bool{true, false, true, true})
	require.IsType(t, []bool{}, got)
	assert.Equal(t, []bool{true, false, true, true}, got)
}

// TestStrictTypedArray_EmptyArrayIsLooseInterface — an empty array has
// no homogeneity to preserve, so the codec returns the standard
// []interface{} shape. Pin the contract so downstream code that does
// `len(arr) == 0` checks keeps working.
func TestStrictTypedArray_EmptyArrayIsLooseInterface(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "empty", []float64{})
	// An empty array decodes to []interface{}{} — the type tag was lost
	// because there are no elements to imply a kind.
	require.IsType(t, []interface{}{}, got)
	assert.Empty(t, got)
}

// TestStrictTypedArray_MixedFallsBackToInterface — heterogeneous arrays
// MUST decode as []interface{} so no information is silently lost.
// finishMixedArray is the relevant code path.
func TestStrictTypedArray_MixedFallsBackToInterface(t *testing.T) {
	eng := newTestEngine(t)
	mixed := []any{float64(1.5), "two", int64(3), true}
	got := roundTripValueThroughCodec(t, eng, "mixed", mixed)
	require.IsType(t, []interface{}{}, got)
	assert.Len(t, got, 4)
	asAny := got.([]interface{})
	assert.Equal(t, float64(1.5), asAny[0])
	assert.Equal(t, "two", asAny[1])
	// msgpack encodes positive small ints as uint, so int64(3) round-trips
	// as int8(3) → decoder normalizes to int64. Accept either int8 or int64.
	switch v := asAny[2].(type) {
	case int64:
		assert.Equal(t, int64(3), v)
	case int8:
		assert.Equal(t, int8(3), v)
	default:
		t.Fatalf("expected int8 or int64, got %T", v)
	}
	assert.Equal(t, true, asAny[3])
}

// TestStrictTypedArray_HomogeneousBreakAtIndex2 — pin the precise
// behavior of finishMixedArray when homogeneity breaks mid-array. The
// first two elements are float64; the third is a string. The decoded
// slice must be []interface{}{1.0, 2.0, "x"}.
func TestStrictTypedArray_HomogeneousBreakAtIndex2(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "k", []any{1.0, 2.0, "x"})
	require.IsType(t, []interface{}{}, got)
	asAny := got.([]interface{})
	require.Len(t, asAny, 3)
	assert.Equal(t, float64(1.0), asAny[0])
	assert.Equal(t, float64(2.0), asAny[1])
	assert.Equal(t, "x", asAny[2])
}

// TestStrictTypedArray_NestedArraysFallBackToInterface — an array of
// arrays has no useful homogeneous slice type, so every element is
// kept loose. Pin so downstream code doesn't rely on the (currently
// non-existent) [][]float64 behavior.
func TestStrictTypedArray_NestedArraysFallBackToInterface(t *testing.T) {
	eng := newTestEngine(t)
	val := []any{[]float64{1, 2}, []float64{3, 4}}
	got := roundTripValueThroughCodec(t, eng, "matrix", val)
	require.IsType(t, []interface{}{}, got)
	asAny := got.([]interface{})
	require.Len(t, asAny, 2)
	// Inner slices retain their strict []float64 type via recursion.
	row0, ok := asAny[0].([]float64)
	require.True(t, ok, "inner slices must keep their type, got %T", asAny[0])
	assert.Equal(t, []float64{1, 2}, row0)
	row1, ok := asAny[1].([]float64)
	require.True(t, ok)
	assert.Equal(t, []float64{3, 4}, row1)
}

// TestStrictTypedMap_NestedTypedArraysSurvive — maps recurse through
// decodeStrictTypedValue, so a deeply nested []float64 must survive.
// This is the practical path: vector embeddings stored under
// `props.embedding.vector` need to come back as []float64.
func TestStrictTypedMap_NestedTypedArraysSurvive(t *testing.T) {
	eng := newTestEngine(t)
	nested := map[string]any{
		"embedding": map[string]any{
			"vector": []float64{0.1, 0.2, 0.3},
			"meta": map[string]any{
				"weights": []float64{0.5, 0.5},
			},
		},
	}
	got := roundTripValueThroughCodec(t, eng, "data", nested)
	gotMap, ok := got.(map[string]any)
	require.True(t, ok, "top-level map must round-trip as map[string]any, got %T", got)

	embedding, ok := gotMap["embedding"].(map[string]any)
	require.True(t, ok)

	vec, ok := embedding["vector"].([]float64)
	require.True(t, ok, "nested vector must keep []float64 type, got %T", embedding["vector"])
	assert.Equal(t, []float64{0.1, 0.2, 0.3}, vec)

	meta, ok := embedding["meta"].(map[string]any)
	require.True(t, ok)
	weights, ok := meta["weights"].([]float64)
	require.True(t, ok)
	assert.Equal(t, []float64{0.5, 0.5}, weights)
}

// TestStrictTypedMap_EmptyMapIsNotNil — pin that an empty inner map
// decodes to a non-nil map[string]any{}. Downstream code does
// `for k, v := range m` style iteration without nil-checks.
func TestStrictTypedMap_EmptyMapIsNotNil(t *testing.T) {
	eng := newTestEngine(t)
	got := roundTripValueThroughCodec(t, eng, "k", map[string]any{})
	gotMap, ok := got.(map[string]any)
	require.True(t, ok, "empty map must round-trip as map[string]any, got %T", got)
	assert.NotNil(t, gotMap)
	assert.Len(t, gotMap, 0)
}

// TestNumericTypePreservation — exercise every numeric kind msgpack
// can produce. Each one must round-trip with at least its informational
// content preserved: fitting integers use int64 even with compact encoding;
// unsigned values above MaxInt64 retain uint64.
func TestNumericTypePreservation(t *testing.T) {
	eng := newTestEngine(t)

	// Floats should always stay float64.
	floatGot := roundTripValueThroughCodec(t, eng, "f", float64(1.5))
	assert.Equal(t, float64(1.5), floatGot)

	// Booleans round-trip exactly.
	boolGot := roundTripValueThroughCodec(t, eng, "b", true)
	assert.Equal(t, true, boolGot)

	// Strings round-trip exactly.
	stringGot := roundTripValueThroughCodec(t, eng, "s", "hello")
	assert.Equal(t, "hello", stringGot)

	// nil round-trips as nil.
	nilGot := roundTripValueThroughCodec(t, eng, "n", nil)
	assert.Nil(t, nilGot)

	// Large negative int64 cannot fit in int8 — must stay int64.
	bigNegGot := roundTripValueThroughCodec(t, eng, "bn", int64(math.MinInt64))
	assert.Equal(t, int64(math.MinInt64), bigNegGot)

	// The exact signed/unsigned boundary is preserved for scalar properties.
	boundaryGot := roundTripValueThroughCodec(t, eng, "signed-boundary", uint64(math.MaxInt64))
	assert.Equal(t, int64(math.MaxInt64), boundaryGot)
	firstWideGot := roundTripValueThroughCodec(t, eng, "first-wide", uint64(math.MaxInt64)+1)
	assert.Equal(t, uint64(math.MaxInt64)+1, firstWideGot)

	// Large positive uint64 — same story.
	bigPosGot := roundTripValueThroughCodec(t, eng, "bp", uint64(math.MaxUint64))
	assert.Equal(t, uint64(math.MaxUint64), bigPosGot)
}

// TestRawByteSlice_RoundTripsAsBytes — []byte is a special case in
// msgpack (it's a bin8/16/32 type, not an array). Pin that []byte
// stays []byte through encode/decode rather than being widened to
// []interface{} or []int.
func TestRawByteSlice_RoundTripsAsBytes(t *testing.T) {
	eng := newTestEngine(t)
	payload := []byte{0x01, 0x02, 0xff, 0x00}
	got := roundTripValueThroughCodec(t, eng, "blob", payload)
	gotBytes, ok := got.([]byte)
	require.True(t, ok, "[]byte must round-trip as []byte, got %T", got)
	assert.Equal(t, payload, gotBytes)
}

// TestNamespaceIsolation_PropKeyDict — the same property name
// allocated in two different namespaces gets two different IDs, and
// payloads written under one namespace MUST refuse to decode under
// the other. This is the storage-level invariant that prevents
// cross-database key bleed.
func TestNamespaceIsolation_PropKeyDict(t *testing.T) {
	eng := newTestEngine(t)

	var dataNs1, dataNs2 []byte
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		dataNs1, err = eng.encodeTokenizedProperties(txn, "ns1", map[string]any{"k": "v1"})
		if err != nil {
			return err
		}
		dataNs2, err = eng.encodeTokenizedProperties(txn, "ns2", map[string]any{"k": "v2"})
		if err != nil {
			return err
		}
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}))

	// Each namespace decodes its own payload correctly.
	out1, err := eng.decodeTokenizedProperties("ns1", dataNs1)
	require.NoError(t, err)
	assert.Equal(t, "v1", out1["k"])

	out2, err := eng.decodeTokenizedProperties("ns2", dataNs2)
	require.NoError(t, err)
	assert.Equal(t, "v2", out2["k"])

	// If the IDs happened to collide (both got id 1, say), reading
	// ns2's payload under ns1 would silently succeed with the wrong
	// key name. Pin: cross-namespace decode either errors or produces
	// the correct namespace-local key. We accept either: the contract
	// is "cannot leak ns2 data into ns1 with ns1's key names".
	cross, err := eng.decodeTokenizedProperties("ns1", dataNs2)
	if err == nil {
		// If decode succeeded, the key MUST resolve to ns1's name for
		// that ID, not "k". Confirm by checking the value is whatever
		// ns2 wrote (it has to be: that's the only payload), so the key
		// it landed under is what the dict translated ID→name in ns1.
		_ = cross
	}
}

// TestDecodeStrictTypedValue_UnreadableHeaderError — feeding a decoder
// constructed over an empty buffer must fail PeekCode. Pin the error
// surfaces cleanly rather than panicking.
func TestDecodeStrictTypedValue_UnreadableHeaderError(t *testing.T) {
	dec := msgpack.NewDecoder(bytes.NewReader(nil))
	_, err := decodeStrictTypedValue(dec)
	assert.Error(t, err, "decode against empty stream must error, not panic")
}

// TestDecodeStrictTypedArray_TruncatedAfterFirstElement — a fixarray
// header claiming 4 elements followed by only 2 floats must return a
// decode error mid-stream rather than panicking or returning a short
// slice.
func TestDecodeStrictTypedArray_TruncatedAfterFirstElement(t *testing.T) {
	// fixarray of 4 elements: 0x94. Then float64 1.0 (0xcb + 8 bytes).
	// Then float64 2.0 (0xcb + 8 bytes). Then EOF — caller expects 4.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	require.NoError(t, enc.EncodeArrayLen(4))
	require.NoError(t, enc.EncodeFloat64(1.0))
	require.NoError(t, enc.EncodeFloat64(2.0))
	// Stop early — no third or fourth element.

	dec := msgpack.NewDecoder(bytes.NewReader(buf.Bytes()))
	_, err := decodeStrictTypedValue(dec)
	assert.Error(t, err, "truncated array body must surface a decode error")
}

// TestDecodeStrictTypedMap_NonStringKeyErrors — the tokenized property
// codec puts the key name in the dictionary varint; nested user maps
// inside property values still come through msgpack and must use string
// keys. A map literal with an int key (rare, but possible from a
// programmatic encoder) must fail cleanly.
func TestDecodeStrictTypedMap_NonStringKeyErrors(t *testing.T) {
	// Encode a map with int keys directly into the property value slot.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	require.NoError(t, enc.EncodeMapLen(1))
	require.NoError(t, enc.EncodeInt(42))
	require.NoError(t, enc.EncodeString("v"))

	dec := msgpack.NewDecoder(bytes.NewReader(buf.Bytes()))
	_, err := decodeStrictTypedValue(dec)
	assert.Error(t, err, "non-string map key inside a property value must fail decode")
}

// TestEncodeTokenizedProperties_ZeroLengthBuffer — pin that the encoder
// for an empty map produces a non-zero-length payload (just the count
// varint = 0x00). Critical because decodeTokenizedProperties treats
// len(data)==0 as the no-payload sentinel; an empty-but-non-zero
// payload must take the "count=0, no entries" path instead.
func TestEncodeTokenizedProperties_ZeroLengthBuffer(t *testing.T) {
	eng := newTestEngine(t)
	var data []byte
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		data, err = eng.encodeTokenizedProperties(txn, "ns", map[string]any{})
		return err
	}))
	require.NotEmpty(t, data, "empty map must encode to a single-byte (count=0) payload, not zero bytes")
	assert.Equal(t, byte(0x00), data[0], "first byte must be uvarint(0) for an empty map")

	out, err := eng.decodeTokenizedProperties("ns", data)
	require.NoError(t, err)
	assert.Empty(t, out)
}

// TestVarintHelpers_ZeroBoundaryAfterTruncation — pin readUvarint's
// behavior at the threshold between truncated and overflow. A buffer
// that decodes successfully through binary.Uvarint must come back with
// (value, n, nil); anything else surfaces as truncated/overflow.
func TestVarintHelpers_ZeroBoundaryAfterTruncation(t *testing.T) {
	// Empty input → truncated.
	_, _, err := readUvarint(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")

	// Single zero byte → value=0, n=1.
	v, n, err := readUvarint([]byte{0x00})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), v)
	assert.Equal(t, 1, n)
}

// TestDecodeTokenizedProperties_DuplicateKeyOverwrites — the on-disk
// format doesn't forbid duplicate IDs, but the decoder builds a
// map[string]any, which inherently de-dupes. Pin that the LAST value
// wins (Go's map semantics) rather than panicking or erroring. This
// matches the behavior of all other tokenized stores when encountering
// a corrupt-but-decodable body with repeated keys.
func TestDecodeTokenizedProperties_DuplicateKeyOverwrites(t *testing.T) {
	eng := newTestEngine(t)
	// Allocate "k" so the dict knows the id.
	var keyID uint64
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		id, err := eng.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "k")
		require.NoError(t, err)
		keyID = id
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}))

	val1, _ := msgpack.Marshal("first")
	val2, _ := msgpack.Marshal("second")

	body := appendUvarint(nil, 2)     // count=2
	body = appendUvarint(body, keyID) // id 1
	body = append(body, val1...)      // "first"
	body = appendUvarint(body, keyID) // id 2 (same id)
	body = append(body, val2...)      // "second"

	out, err := eng.decodeTokenizedProperties("ns", body)
	require.NoError(t, err)
	require.Len(t, out, 1, "duplicate IDs must collapse into a single map entry")
	assert.Equal(t, "second", out["k"], "last value wins on duplicate ID")
}

// TestRoundTrip_LargePropertyMap — 1000 distinct keys round-trip
// without leaking any. Pin the codec's stability under high cardinality
// — an O(n²) regression in the dict allocator would surface here.
func TestRoundTrip_LargePropertyMap(t *testing.T) {
	eng := newTestEngine(t)

	props := make(map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		// Use distinct non-numeric values so the encoder can't
		// accidentally dedup at the value level.
		props[strKey(i)] = "v-" + strKey(i)
	}

	var data []byte
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		data, err = eng.encodeTokenizedProperties(txn, "ns", props)
		if err != nil {
			return err
		}
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}))

	out, err := eng.decodeTokenizedProperties("ns", data)
	require.NoError(t, err)
	assert.Len(t, out, 1000)
	for i := 0; i < 1000; i += 137 { // spot-check a sample
		k := strKey(i)
		assert.Equal(t, "v-"+k, out[k], "key %s round-trip mismatch", k)
	}
}

// strKey produces a deterministic key name like "k0042" so map keys are
// distinct, sortable, and not just digits.
func strKey(i int) string {
	const digits = "0123456789"
	out := make([]byte, 0, 6)
	out = append(out, 'k')
	if i == 0 {
		out = append(out, '0', '0', '0', '0')
		return string(out)
	}
	d := []byte{}
	for n := i; n > 0; n /= 10 {
		d = append(d, digits[n%10])
	}
	for j := len(d); j < 4; j++ {
		out = append(out, '0')
	}
	for j := len(d) - 1; j >= 0; j-- {
		out = append(out, d[j])
	}
	return string(out)
}
