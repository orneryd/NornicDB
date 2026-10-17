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

// A namespace view filters the buffer's shared rounded property-index bucket,
// while unattached schemas and ordinary keys keep their existing behavior.
func TestUniqueConstraintPendingNamespaceAndOrdinaryKeys(t *testing.T) {
	buffer := newCommitBuffer()
	index := newPendingNodeIndex()
	index.tracked["N"] = map[string]struct{}{"v": {}}
	entries := []struct {
		id    NodeID
		value interface{}
	}{
		{"tenant:odd", int64(9007199254740993)},
		{"tenant:even", int64(9007199254740992)},
		{"other:odd", int64(9007199254740993)},
		{"other:even", int64(9007199254740992)},
		{"tenant:small", int(42)},
		{"tenant:text", "ordinary"},
		{"other:small", int(42)},
		{"other:text", "ordinary"},
	}
	for _, entry := range entries {
		node := &Node{ID: entry.id, Labels: []string{"N"}, Properties: map[string]interface{}{"v": entry.value}}
		buffer.nodes[entry.id] = node
		index.add(node)
	}
	view := pendingWriteView{index: index, owner: &WriteBehindBuffer{active: buffer}, namespace: "tenant"}
	for _, tc := range []struct {
		value interface{}
		want  NodeID
	}{
		{int64(9007199254740993), "tenant:odd"},
		{int64(9007199254740992), "tenant:even"},
		{int(42), "tenant:small"},
		{float64(42), "tenant:small"},
		{"ordinary", "tenant:text"},
	} {
		key, ok := uniqueConstraintValueKey(tc.value)
		require.True(t, ok)
		require.Equal(t, []NodeID{tc.want}, view.uniqueValueMatches(nil, "N", "v", tc.value, key))
		// The same real value on a schema with no write-behind attachment must
		// neither fabricate a pending holder nor allocate an index dependency.
		require.Empty(t, (pendingWriteView{}).uniqueValueMatches(nil, "N", "v", tc.value, key))
	}
	// The platform int path must preserve equality with an integral float.
	integerKey, ok := uniqueConstraintValueKey(int(42))
	require.True(t, ok)
	floatKey, ok := uniqueConstraintValueKey(float64(42))
	require.True(t, ok)
	require.Equal(t, integerKey, floatKey)
}

func TestConstraintCreationByteListEquivalenceBothOrders(t *testing.T) {
	cases := []struct {
		name  string
		a, b  interface{}
		equal bool
	}{
		{"bytes and integers", []byte{1, 2}, []int64{1, 2}, true},
		{"empty bytes and integers", []byte{}, []int64{}, true},
		{"nil bytes and integers", []byte(nil), []int64(nil), true},
		{"nested bytes and integers", []interface{}{[]byte{1, 2}}, [][]int64{{1, 2}}, true},
		{"nested empty bytes and integers", []interface{}{[]byte{}}, [][]int64{{}}, true},
		{"nested prefix and tail preserved", []interface{}{int64(7), []byte{1, 2}, "tail"}, []interface{}{int64(7), []int64{1, 2}, "tail"}, true},
		{"same nested byte arrays", []interface{}{[2]byte{1, 2}}, []interface{}{[2]byte{1, 2}}, true},
		{"nested byte array versus list stays distinct", []interface{}{[2]byte{1, 2}}, []interface{}{[]int64{1, 2}}, false},
		{"nested byte slice versus array stays distinct", []interface{}{[]byte{1, 2}}, []interface{}{[2]byte{1, 2}}, false},
		{"empty versus nil stays distinct", []byte{}, []int64(nil), false},
	}
	for _, tc := range cases {
		for direction, pair := range [][2]interface{}{{tc.a, tc.b}, {tc.b, tc.a}} {
			t.Run(fmt.Sprintf("%s/order%d", tc.name, direction), func(t *testing.T) {
				require.Equal(t, tc.equal, constraintCreationValueEqual(pair[0], pair[1]), "pin existing shared equality semantics")
				seen := make(constraintCreationValues)
				_, duplicate := seen.add("first", []interface{}{pair[0]})
				require.False(t, duplicate)
				id, duplicate := seen.add("second", []interface{}{pair[1]})
				require.Equal(t, tc.equal, duplicate)
				if tc.equal {
					require.Equal(t, "first", id)
				}
				// A comparable temporal second property takes the existing
				// fallback key path; it must use the same normalized bucket.
				stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
				composite := make(constraintCreationValues)
				_, duplicate = composite.add("first", []interface{}{pair[0], stamp})
				require.False(t, duplicate)
				_, duplicate = composite.add("second", []interface{}{pair[1], stamp})
				require.Equal(t, tc.equal, duplicate)

			})
		}
	}
}
