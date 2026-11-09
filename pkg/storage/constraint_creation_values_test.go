package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstraintCreationValuesPreserveUnsupportedAndNumericForms(t *testing.T) {
	seen := make(constraintCreationValues)
	for _, value := range []interface{}{[]byte{1, 2}, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)} {
		_, duplicate, keyErr := seen.add(t.Context(), "first", []interface{}{value})
		require.NoError(t, keyErr)
		require.False(t, duplicate)
		id, duplicate, keyErr := seen.add(t.Context(), "second", []interface{}{value})
		require.NoError(t, keyErr)
		require.True(t, duplicate)
		require.Equal(t, "first", id)
	}
	for _, value := range []interface{}{[]byte{1, 3}, time.Date(2020, 1, 3, 3, 4, 5, 0, time.UTC)} {
		_, duplicate, keyErr := seen.add(t.Context(), "distinct", []interface{}{value})
		require.NoError(t, keyErr)
		require.False(t, duplicate)
	}
	for _, pair := range [][2]interface{}{{int64(math.MaxInt64), float64(math.MaxInt64)}, {uint64(math.MaxUint64), float64(math.MaxUint64)}} {
		require.False(t, compareValues(pair[0], pair[1]))
	}
	require.True(t, compareValues(nil, nil))
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
		schema.ApplyUniqueValue(context.Background(), "N", "v", value, id, ValidatedUniqueClaim)
		node := &Node{ID: id, Labels: []string{"N"}, Properties: map[string]interface{}{"v": value}}
		buffer.nodes[id] = node
		index.add(node)
	}
	view := pendingWriteView{index: index, owner: &WriteBehindBuffer{active: buffer}}
	for i, value := range values {
		want := NodeID([]string{"even", "odd", "uintmax", "negative", "platformuint"}[i])
		got, found, _, _, uniqueLookupErr1 := schema.LookupUniqueConstraintValue(t.Context(), "N", "v", value)
		if uniqueLookupErr1 != nil {
			t.Errorf("unique index read failed: %v", uniqueLookupErr1)
		}
		require.True(t, found)
		require.Equal(t, want, got)
		key, ok, keyErr := indexValueKey(t.Context(), value)
		require.NoError(t, keyErr)
		require.True(t, ok)
		require.Equal(t, []NodeID{want}, checkedIndexRead[[]NodeID](t)(view.valueMatches(t.Context(), nil, "N", "v", key)))
	}
	// A user string matching the printed exceptional-key tag is still an
	// ordinary string key, not the private comparable integer-key type.
	numericKey, ok, keyErr := indexValueKey(t.Context(), values[1])
	require.NoError(t, keyErr)
	require.True(t, ok)
	lookalike := fmt.Sprint(numericKey)
	schema.ApplyUniqueValue(context.Background(), "N", "v", lookalike, "string-lookalike", ValidatedUniqueClaim)
	stringHolder, stringFound, _, _, uniqueLookupErr2 := schema.LookupUniqueConstraintValue(t.Context(), "N", "v", lookalike)
	if uniqueLookupErr2 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr2)
	}
	require.True(t, stringFound)
	require.Equal(t, NodeID("string-lookalike"), stringHolder)
	numericHolder, numericFound, _, _, uniqueLookupErr3 := schema.LookupUniqueConstraintValue(t.Context(), "N", "v", values[1])
	if uniqueLookupErr3 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr3)
	}
	require.True(t, numericFound)
	require.Equal(t, NodeID("odd"), numericHolder)
	// One key for every index: a float64 holds 2^53 exactly, so its key is
	// unchanged; 2^53+1 keeps its exact integer instead of colliding with it.
	a, _, keyErr := indexValueKey(t.Context(), values[0])
	require.NoError(t, keyErr)
	b, _, keyErr := indexValueKey(t.Context(), values[1])
	require.NoError(t, keyErr)
	require.Equal(t, float64(9007199254740992), a)
	require.Equal(t, uint64(9007199254740993), b)
	schema.ApplyUniqueValue(context.Background(), "N", "v", values[1], "odd", RetiredUniqueClaim)
	_, found, _, _, uniqueLookupErr4 := schema.LookupUniqueConstraintValue(t.Context(), "N", "v", values[1])
	if uniqueLookupErr4 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr4)
	}
	require.False(t, found)
	got, found, _, _, uniqueLookupErr5 := schema.LookupUniqueConstraintValue(t.Context(), "N", "v", float64(9007199254740992))
	if uniqueLookupErr5 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr5)
	}
	require.True(t, found)
	require.Equal(t, NodeID("even"), got)
}

// ApplyUniqueValue owns every canonical claim transition: a validated claim
// takes the value unconditionally, a recovered claim keeps a different current
// holder, a retirement leaves another owner intact, and an unknown action is a
// programmer error rather than a publication mode.
func TestApplyUniqueValueActionsPreserveOwnershipContract(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("apply_unique", "N", "v"))

	schema.ApplyUniqueValue(context.Background(), "N", "v", "recovered", "node-1", RecoveredUniqueClaim)
	holder, found, _, _, uniqueLookupErr6 := schema.LookupUniqueConstraintValue(t.Context(), "N", "v", "recovered")
	if uniqueLookupErr6 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr6)
	}
	require.True(t, found)
	require.Equal(t, NodeID("node-1"), holder)

	// A recovered claim must not overwrite a different current holder.
	schema.ApplyUniqueValue(context.Background(), "N", "v", "recovered", "node-2", RecoveredUniqueClaim)
	var uniqueLookupErr7 error
	holder, found, _, _, uniqueLookupErr7 = schema.LookupUniqueConstraintValue(t.Context(), "N", "v", "recovered")
	if uniqueLookupErr7 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr7)
	}
	require.True(t, found)
	require.Equal(t, NodeID("node-1"), holder)

	// A validated claim is the authoritative owner.
	schema.ApplyUniqueValue(context.Background(), "N", "v", "recovered", "node-2", ValidatedUniqueClaim)
	var uniqueLookupErr8 error
	holder, found, _, _, uniqueLookupErr8 = schema.LookupUniqueConstraintValue(t.Context(), "N", "v", "recovered")
	if uniqueLookupErr8 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr8)
	}
	require.True(t, found)
	require.Equal(t, NodeID("node-2"), holder)

	// A retirement from a stale owner cannot release the current holder.
	schema.ApplyUniqueValue(context.Background(), "N", "v", "recovered", "node-1", RetiredUniqueClaim)
	var uniqueLookupErr9 error
	holder, found, _, _, uniqueLookupErr9 = schema.LookupUniqueConstraintValue(t.Context(), "N", "v", "recovered")
	if uniqueLookupErr9 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr9)
	}
	require.True(t, found)
	require.Equal(t, NodeID("node-2"), holder)

	schema.ApplyUniqueValue(context.Background(), "N", "v", "recovered", "node-2", RetiredUniqueClaim)
	var uniqueLookupErr10 error
	_, found, _, _, uniqueLookupErr10 = schema.LookupUniqueConstraintValue(t.Context(), "N", "v", "recovered")
	if uniqueLookupErr10 != nil {
		t.Errorf("unique index read failed: %v", uniqueLookupErr10)
	}
	require.False(t, found)

	// Constraint and value-key no-ops stay silent for every action.
	require.NotPanics(t, func() {
		schema.ApplyUniqueValue(context.Background(), "N", "absent", "recovered", "node-3", ValidatedUniqueClaim)
		schema.ApplyUniqueValue(context.Background(), "N", "v", []string{"unindexable"}, "node-3", RetiredUniqueClaim)
	})

	require.PanicsWithValue(t, "storage: ApplyUniqueValue: unknown UniqueValueAction 3", func() {
		schema.ApplyUniqueValue(context.Background(), "N", "v", "recovered", "node-3", UniqueValueAction(3))
	})
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
			schema.ApplyUniqueValue(context.Background(), "N", "v", tc.value, "node", ValidatedUniqueClaim)
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				node, found, _, _, uniqueLookupErr11 := schema.LookupUniqueConstraintValue(ctx, "N", "v", tc.value)
				if uniqueLookupErr11 != nil {
					b.Fatalf("unique index read failed: %v", uniqueLookupErr11)
				}
				if !found {
					b.Fatal("missing registered value")
				}
				uniqueCacheBenchmarkNode = node
			}
		})
	}
}

// A namespace view filters the buffer's shared property-index bucket,
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
		key, ok, keyErr := indexValueKey(t.Context(), tc.value)
		require.NoError(t, keyErr)
		require.True(t, ok)
		require.Equal(t, []NodeID{tc.want}, checkedIndexRead[[]NodeID](t)(view.valueMatches(t.Context(), nil, "N", "v", key)))
		// The same real value on a schema with no write-behind attachment must
		// neither fabricate a pending holder nor allocate an index dependency.
		require.Empty(t, checkedIndexRead[[]NodeID](t)((pendingWriteView{}).valueMatches(t.Context(), nil, "N", "v", key)))
	}
	// The platform int path must preserve equality with an integral float.
	integerKey, ok, keyErr := indexValueKey(t.Context(), int(42))
	require.NoError(t, keyErr)
	require.True(t, ok)
	floatKey, ok, keyErr := indexValueKey(t.Context(), float64(42))
	require.NoError(t, keyErr)
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
				require.Equal(t, tc.equal, compareValues(pair[0], pair[1]), "pin existing shared equality semantics")
				seen := make(constraintCreationValues)
				_, duplicate, keyErr := seen.add(t.Context(), "first", []interface{}{pair[0]})
				require.NoError(t, keyErr)
				require.False(t, duplicate)
				id, duplicate, keyErr := seen.add(t.Context(), "second", []interface{}{pair[1]})
				require.NoError(t, keyErr)
				require.Equal(t, tc.equal, duplicate)
				if tc.equal {
					require.Equal(t, "first", id)
				}
				// A comparable temporal second property takes the existing
				// fallback key path; it must use the same normalized bucket.
				stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
				composite := make(constraintCreationValues)
				_, duplicate, keyErr = composite.add(t.Context(), "first", []interface{}{pair[0], stamp})
				require.NoError(t, keyErr)
				require.False(t, duplicate)
				_, duplicate, keyErr = composite.add(t.Context(), "second", []interface{}{pair[1], stamp})
				require.NoError(t, keyErr)
				require.Equal(t, tc.equal, duplicate)

			})
		}
	}
}
