package storage

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstraintCreationValuesPreserveUnsupportedAndNumericForms(t *testing.T) {
	seen := make(constraintCreationValues)
	for _, value := range []interface{}{[]byte{1, 2}, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)} {
		_, duplicate := seen.add("first", []interface{}{value})
		require.False(t, duplicate)
		id, duplicate := seen.add("second", []interface{}{value})
		require.True(t, duplicate)
		require.Equal(t, "first", id)
	}
	for _, value := range []interface{}{[]byte{1, 3}, time.Date(2020, 1, 3, 3, 4, 5, 0, time.UTC)} {
		_, duplicate := seen.add("distinct", []interface{}{value})
		require.False(t, duplicate)
	}
	for _, pair := range [][2]interface{}{{int64(math.MaxInt64), float64(math.MaxInt64)}, {uint64(math.MaxUint64), float64(math.MaxUint64)}} {
		require.False(t, constraintCreationValueEqual(pair[0], pair[1]))
	}
	require.True(t, constraintCreationValueEqual(nil, nil))
}

func TestUniqueConstraintCachePrecisionAndPendingCandidates(t *testing.T) {
	values := []interface{}{int64(9007199254740992), int64(9007199254740993), uint64(math.MaxUint64), int64(-9007199254740993), uint(9007199254740995)}
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("exact_unique", "N", "v"))
	buffer := newCommitBuffer()
	index := newPendingNodeIndex()
	index.tracked["N"] = map[string]struct{}{"v": {}}
	for i, value := range values {
		id := NodeID([]string{"even", "odd", "uintmax", "negative", "platformuint"}[i])
		schema.RegisterUniqueValue("N", "v", value, id)
		node := &Node{ID: id, Labels: []string{"N"}, Properties: map[string]interface{}{"v": value}}
		buffer.nodes[id] = node
		index.add(node)
	}
	view := pendingWriteView{index: index, owner: &WriteBehindBuffer{active: buffer}}
	for i, value := range values {
		want := NodeID([]string{"even", "odd", "uintmax", "negative", "platformuint"}[i])
		got, found, _ := schema.LookupUniqueConstraintValue("N", "v", value)
		require.True(t, found)
		require.Equal(t, want, got)
		key, ok := uniqueConstraintValueKey(value)
		require.True(t, ok)
		require.Equal(t, []NodeID{want}, view.uniqueValueMatches(nil, "N", "v", value, key))
	}
	// A user string matching the printed exceptional-key tag is still an
	// ordinary string key, not the private comparable integer-key type.
	numericKey, ok := uniqueConstraintValueKey(values[1])
	require.True(t, ok)
	lookalike := fmt.Sprint(numericKey)
	schema.RegisterUniqueValue("N", "v", lookalike, "string-lookalike")
	stringHolder, stringFound, _ := schema.LookupUniqueConstraintValue("N", "v", lookalike)
	require.True(t, stringFound)
	require.Equal(t, NodeID("string-lookalike"), stringHolder)
	numericHolder, numericFound, _ := schema.LookupUniqueConstraintValue("N", "v", values[1])
	require.True(t, numericFound)
	require.Equal(t, NodeID("odd"), numericHolder)
	// The old rounded property-index key remains byte-for-byte/type compatible;
	// only the UNIQUE cache distinguishes the otherwise colliding exact integers.
	a, _ := indexValueKey(values[0])
	b, _ := indexValueKey(values[1])
	require.Equal(t, a, b)
	a, _ = uniqueConstraintValueKey(values[0])
	b, _ = uniqueConstraintValueKey(values[1])
	require.NotEqual(t, a, b)
	schema.UnregisterUniqueValue("N", "v", values[1])
	_, found, _ := schema.LookupUniqueConstraintValue("N", "v", values[1])
	require.False(t, found)
	got, found, _ := schema.LookupUniqueConstraintValue("N", "v", float64(9007199254740992))
	require.True(t, found)
	require.Equal(t, NodeID("even"), got)
}

var uniqueCacheBenchmarkNode NodeID

func BenchmarkUniqueCachePrecision(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value interface{}
	}{{"string", "value"}, {"smallInteger", int64(42)}, {"largeInteger", int64(9007199254740993)}} {
		b.Run(tc.name, func(b *testing.B) {
			schema := NewSchemaManager()
			if err := schema.AddUniqueConstraint("bench", "N", "v"); err != nil {
				b.Fatal(err)
			}
			schema.RegisterUniqueValue("N", "v", tc.value, "node")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				node, found, _ := schema.LookupUniqueConstraintValue("N", "v", tc.value)
				if !found {
					b.Fatal("missing registered value")
				}
				uniqueCacheBenchmarkNode = node
			}
		})
	}
}
