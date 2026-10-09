package cypher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// VECTOR and UUID values on their own: text, parsing, order, storage and the
// Bolt 5 placeholder, as Neo4j 2026.09 writes them (#907).
func TestVectorAndUUIDValues(t *testing.T) {
	ints := CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}
	floats := CypherVector{Type: VectorFloat32, Floats: []float64{float64(float32(1.5)), 2}}
	require.Equal(t, "vector([1, 2], 2, INTEGER64)", ints.String())
	require.Equal(t, "vector([1.5, 2.0], 2, FLOAT32)", floats.String())
	require.Equal(t, "vector([1.0E20], 1, FLOAT64)", CypherVector{Type: VectorFloat64, Floats: []float64{1e20}}.String())
	require.Equal(t, int64(2), ints.CypherSize())
	require.Equal(t, []interface{}{int64(1), int64(2)}, ints.coordinates())
	require.Equal(t, []interface{}{1.5, 2.0}, floats.coordinates())
	require.True(t, ints.Equal(CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}))
	require.False(t, ints.Equal(CypherVector{Type: VectorInteger8, Ints: []int64{1, 2}}))
	require.False(t, ints.Equal(CypherVector{Type: VectorInteger64, Ints: []int64{1}}))
	require.False(t, ints.Equal(CypherVector{Type: VectorInteger64, Ints: []int64{1, 3}}))
	require.False(t, floats.Equal(CypherVector{Type: VectorFloat32, Floats: []float64{1.5, 3}}))
	require.True(t, floats.Equal(floats))

	for name, want := range map[string]VectorCoordinateType{
		"integer": VectorInteger64, "INT": VectorInteger64, "INT64": VectorInteger64, "SIGNED INTEGER": VectorInteger64,
		"int32": VectorInteger32, "INTEGER16": VectorInteger16, "INT8": VectorInteger8, "FLOAT": VectorFloat64,
		"FLOAT64": VectorFloat64, "float32": VectorFloat32,
	} {
		got, ok := parseVectorCoordinateType(name)
		require.True(t, ok, name)
		require.Equal(t, want, got, name)
	}
	_, ok := parseVectorCoordinateType("BOOLEAN")
	require.False(t, ok)

	order := []CypherVector{
		{Type: VectorInteger8, Ints: []int64{2}},
		{Type: VectorInteger16, Ints: []int64{1}},
		{Type: VectorInteger64, Ints: []int64{2}},
		{Type: VectorInteger64, Ints: []int64{1, 2}},
		{Type: VectorInteger64, Ints: []int64{1, 3}},
		{Type: VectorFloat32, Floats: []float64{3}},
		{Type: VectorFloat64, Floats: []float64{0}},
		{Type: VectorFloat64, Floats: []float64{1}},
	}
	for i := 1; i < len(order); i++ {
		require.Equal(t, -1, compareVectors(order[i-1], order[i]), i)
		require.Equal(t, 1, compareVectors(order[i], order[i-1]), i)
	}
	require.Equal(t, 0, compareVectors(order[3], order[3]))

	u, ok := parseCypherUUID("550E8400-E29B-41D4-A716-446655440000")
	require.True(t, ok)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", u.String())
	require.Equal(t, int64(6128981282234515924), u.MostSignificantBits())
	require.Equal(t, int64(-6406858213580079104), u.LeastSignificantBits())
	require.Equal(t, "ffffffff-ffff-ffff-ffff-ffffffffffff", uuidFromHalves(-1, -1).String())
	for _, text := range []string{"550e8400e29b41d4a716446655440000", "{550e8400-e29b-41d4-a716-446655440000}", "nope", "550e8400-e29b-41d4-a716-44665544000g", "550e8400-e29b-41d4a-716-446655440000"} {
		_, ok := parseCypherUUID(text)
		require.False(t, ok, text)
	}
	require.Equal(t, -1, compareUUIDs(uuidFromHalves(0, 1), uuidFromHalves(-1, 0)), "unsigned")
	require.Equal(t, 0, compareUUIDs(u, u))
	require.Equal(t, 1, compareUUIDs(uuidFromHalves(1, 0), uuidFromHalves(0, -1)))
	v7 := newVersion7UUID(time.UnixMilli(0x0123456789ab))
	require.Equal(t, "01234567-89ab-7", v7.String()[:15])
	require.Contains(t, "89ab", string(v7.String()[19]))

	for _, value := range []interface{}{ints, floats, u} {
		data, err := msgpack.Marshal(value)
		require.NoError(t, err)
		switch value.(type) {
		case CypherVector:
			var decoded CypherVector
			require.NoError(t, msgpack.Unmarshal(data, &decoded))
			require.True(t, value.(CypherVector).Equal(decoded))
		case CypherUUID:
			var decoded CypherUUID
			require.NoError(t, msgpack.Unmarshal(data, &decoded))
			require.Equal(t, value, decoded)
		}
	}
	var vector CypherVector
	require.Error(t, msgpack.Unmarshal([]byte{0xd4, 48, 9}, &vector), "unknown coordinate type")
	var id CypherUUID
	require.Error(t, msgpack.Unmarshal([]byte{0xd4, 49, 1}, &id), "short UUID")

	placeholder, ok := UnsupportedTypePlaceholder(ints)
	require.True(t, ok)
	require.Equal(t, map[string]interface{}{"reason": "UNKNOWN_TYPE", "originalType": "VECTOR(2, INTEGER64)"}, placeholder)
	placeholder, _ = UnsupportedTypePlaceholder(u)
	require.Equal(t, "UUID", placeholder["originalType"])
	_, ok = UnsupportedTypePlaceholder(1)
	require.False(t, ok)

	require.Equal(t, "VECTOR<INTEGER NOT NULL>(2) NOT NULL", valueTypeOf(ints).render(true))
	require.Equal(t, "VECTOR<FLOAT32 NOT NULL>(2) NOT NULL", valueTypeOf(floats).render(true))
	require.Equal(t, "UUID NOT NULL", valueTypeOf(u).render(true))
	require.Equal(t, "Vector", cypherTypeName(ints))
	require.Equal(t, "UUID", cypherTypeName(&u))
}
