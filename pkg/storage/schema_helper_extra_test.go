package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllowedValuesEqualOrderInsensitiveWithDuplicates(t *testing.T) {
	require.True(t, allowedValuesEqual([]interface{}{"a", "b", "a", 3}, []interface{}{3, "a", "a", "b"}))
	require.False(t, allowedValuesEqual([]interface{}{"a", "a"}, []interface{}{"a"}))
	require.False(t, allowedValuesEqual([]interface{}{"a", "a"}, []interface{}{"a", "b"}))
}

func TestCompareSchemaIndexValues(t *testing.T) {
	cases := []struct {
		name string
		a    interface{}
		b    interface{}
		want int
	}{
		{name: "both nil", a: nil, b: nil, want: 0},
		{name: "nil sorts first", a: nil, b: 1, want: -1},
		{name: "nil sorts last", a: 1, b: nil, want: 1},
		{name: "numeric less", a: 1, b: 2.5, want: -1},
		{name: "numeric greater", a: 3.5, b: 2, want: 1},
		{name: "numeric equal", a: int64(4), b: float64(4), want: 0},
		{name: "string less", a: "alpha", b: "beta", want: -1},
		{name: "string greater", a: "zeta", b: "beta", want: 1},
		{name: "string equal", a: "same", b: "same", want: 0},
		{name: "fallback less", a: true, b: false, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, compareSchemaIndexValues(tc.a, tc.b))
		})
	}
}

func TestSchemaManagerPropertyIndexHelpers(t *testing.T) {
	sm := NewSchemaManager()
	require.False(t, sm.HasPropertyIndex("Doc", "rank"))
	require.False(t, sm.HasAnyPropertyIndexForLabel("Doc"))
	require.Nil(t, sm.PropertyIndexAllNonNil("Doc", "rank", false))

	require.NoError(t, sm.AddPropertyIndex("doc_rank", "Doc", []string{"rank"}))
	require.True(t, sm.HasPropertyIndex("Doc", "rank"))
	require.True(t, sm.HasAnyPropertyIndexForLabel("Doc"))
	require.False(t, sm.HasAnyPropertyIndexForLabel("Other"))

	idx, _ := sm.GetPropertyIndex("Doc", "rank")
	idx.mu.Lock()
	idx.values[2] = []NodeID{"n2b", "n2a"}
	idx.values[1] = []NodeID{"n1"}
	idx.values[nil] = []NodeID{"nil"}
	idx.values[3] = nil
	idx.keysDirty = true
	idx.mu.Unlock()

	require.Equal(t, []NodeID{"n1", "n2a", "n2b"}, sm.PropertyIndexTopK("Doc", "rank", 3, false))
	require.Equal(t, []NodeID{"n2a", "n2b"}, sm.PropertyIndexTopK("Doc", "rank", 2, true))
	require.Equal(t, []NodeID{"n1", "n2a", "n2b"}, sm.PropertyIndexAllNonNil("Doc", "rank", false))
	require.Equal(t, []NodeID{"n2a", "n2b", "n1"}, sm.PropertyIndexAllNonNil("Doc", "rank", true))
	require.Nil(t, sm.PropertyIndexTopK("Doc", "rank", 0, false))
	require.Nil(t, sm.PropertyIndexTopK("Missing", "rank", 1, false))
}

func TestSchemaManager_RemoveFromPropertyIndex_PartialRemovalRetainsBucket(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("doc_rank", "Doc", []string{"rank"}))
	require.NoError(t, sm.PropertyIndexInsert("Doc", "rank", "n1", int64(5)))
	require.NoError(t, sm.PropertyIndexInsert("Doc", "rank", "n2", int64(5)))

	require.NoError(t, sm.PropertyIndexDelete("Doc", "rank", "n1", int64(5)))

	idx, _ := sm.GetPropertyIndex("Doc", "rank")
	require.NotNil(t, idx)
	require.Equal(t, []NodeID{"n2"}, sm.PropertyIndexLookup("Doc", "rank", int64(5)))
}

func TestSchemaManager_AddRangeIndexForEntity_RejectsEmptyProperties(t *testing.T) {
	sm := NewSchemaManager()
	err := sm.AddRangeIndexForEntity("bad_range", "Person", nil, ConstraintEntityNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "range index requires at least one property")
}

func TestConstraintViolationErrorUnwrap(t *testing.T) {
	cause := errors.New("peer committed first")
	err := &ConstraintViolationError{
		Type:       ConstraintUnique,
		Label:      "User",
		Properties: []string{"email"},
		Message:    "duplicate email",
		Cause:      cause,
	}

	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), "Constraint violation")
	require.Contains(t, err.Error(), "duplicate email")
}
