package callerowned

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type record struct {
	Name  string
	Tags  []string
	Attrs map[string]any
	inner map[string]any
}

type copier struct{ Values []any }

func (c *copier) CallerCopy() any { return &copier{Values: List(c.Values)} }

// Value preserves concrete types and nil spellings and detaches every
// container a caller could reach.
func TestValuePreservesTypesAndDetaches(t *testing.T) {
	var nilMap map[string]any
	var nilList []any
	var nilCopier *copier
	require.Nil(t, Value(nil))
	require.IsType(t, nilMap, Value(nilMap))
	require.Nil(t, Value(nilMap).(map[string]any))
	require.Nil(t, Value(nilList).([]any))
	require.Nil(t, Value(nilCopier).(*copier))

	source := map[string]any{
		"list":   []any{"a", []any{"b"}, map[string]any{"c": []int64{1}}},
		"ints":   []int64{1, 2},
		"bytes":  []byte{1},
		"record": &record{Name: "r", Tags: []string{"t"}, Attrs: map[string]any{"k": []any{"v"}}},
		"copier": &copier{Values: []any{map[string]any{"x": "y"}}},
	}
	copied := Map(source)
	require.Equal(t, source, copied)
	require.IsType(t, []int64(nil), copied["ints"])

	copied["list"].([]any)[1].([]any)[0] = "changed"
	copied["list"].([]any)[2].(map[string]any)["c"].([]int64)[0] = 9
	copied["ints"].([]int64)[0] = 9
	copied["bytes"].([]byte)[0] = 9
	copied["record"].(*record).Tags[0] = "changed"
	copied["record"].(*record).Attrs["k"].([]any)[0] = "changed"
	copied["copier"].(*copier).Values[0].(map[string]any)["x"] = "changed"

	require.Equal(t, "b", source["list"].([]any)[1].([]any)[0])
	require.Equal(t, int64(1), source["list"].([]any)[2].(map[string]any)["c"].([]int64)[0])
	require.Equal(t, int64(1), source["ints"].([]int64)[0])
	require.Equal(t, byte(1), source["bytes"].([]byte)[0])
	require.Equal(t, "t", source["record"].(*record).Tags[0])
	require.Equal(t, "v", source["record"].(*record).Attrs["k"].([]any)[0])
	require.Equal(t, "y", source["copier"].(*copier).Values[0].(map[string]any)["x"])
}

// Rows gives a fresh window with no spare capacity into the source rows, and
// reports every copied map so identity-keyed metadata can follow it.
func TestRowsObservingMaps(t *testing.T) {
	inner := map[string]any{"k": "v"}
	source := make([][]any, 1, 4)
	source[0] = []any{map[string]any{"inner": inner}}
	seen := map[uintptr]uintptr{}
	copied := RowsObservingMaps(source, func(original, copy map[string]any) {
		seen[reflect.ValueOf(original).Pointer()] = reflect.ValueOf(copy).Pointer()
	})
	require.Equal(t, source, copied)
	require.Equal(t, 1, cap(copied))
	require.Len(t, seen, 2)
	copiedInner := copied[0][0].(map[string]any)["inner"].(map[string]any)
	require.Equal(t, reflect.ValueOf(copiedInner).Pointer(), seen[reflect.ValueOf(inner).Pointer()])
	require.Nil(t, Rows(nil))
}

type valueCopier struct{ Values []any }

func (c valueCopier) CallerCopy() any { return valueCopier{Values: List(c.Values)} }

// One Copier dispatch rule keeps every input's type: a pointer to a
// value-receiver Copier stays a pointer (to a detached copy), wherever it sits.
func TestCopierDispatchPreservesType(t *testing.T) {
	hit := &valueCopier{Values: []any{map[string]any{"k": "original"}}}
	for name, copied := range map[string]any{
		"Value": Value(hit),
		"Of":    Of(hit),
		"row":   Rows([][]any{{hit}})[0][0],
		"typed": Value([]*valueCopier{hit}).([]*valueCopier)[0],
	} {
		got, ok := copied.(*valueCopier)
		require.True(t, ok, "%s changed the type to %T", name, copied)
		require.NotSame(t, hit, got, name)
		got.Values[0].(map[string]any)["k"] = "caller"
		require.Equal(t, "original", hit.Values[0].(map[string]any)["k"], name)
	}
	byValue, ok := Value(*hit).(valueCopier)
	require.True(t, ok)
	require.Equal(t, hit.Values, byValue.Values)
}

// countingCopier counts its CallerCopy calls (a value receiver, as
// search.SearchResult has).
type countingCopier struct{ calls *int }

func (c countingCopier) CallerCopy() any {
	*c.calls++
	return countingCopier{calls: c.calls}
}

// The dispatch rule makes one CallerCopy call per value, the pointer to a
// value-receiver Copier included, wherever the value sits.
func TestCopierDispatchCallsCopierOnce(t *testing.T) {
	for name, copy := range map[string]func(*countingCopier) any{
		"Value": func(c *countingCopier) any { return Value(c) },
		"Of":    func(c *countingCopier) any { return Of(c) },
		"row":   func(c *countingCopier) any { return Rows([][]any{{c}})[0][0] },
		"typed": func(c *countingCopier) any { return Value([]*countingCopier{c}).([]*countingCopier)[0] },
		"value": func(c *countingCopier) any { return Value(*c) },
	} {
		calls := 0
		copy(&countingCopier{calls: &calls})
		require.Equal(t, 1, calls, name)
	}
}

// Each is the one slice owner: nil stays nil, every element is copied by the
// element owner, and the result has no spare capacity into the source.
func TestEachCopiesEveryElement(t *testing.T) {
	require.Nil(t, Each([]*valueCopier(nil), Of[*valueCopier]))
	source := make([]*valueCopier, 1, 4)
	source[0] = &valueCopier{Values: []any{"a"}}
	copied := Each(source, Of[*valueCopier])
	require.Len(t, copied, 1)
	require.Equal(t, 1, cap(copied))
	require.NotSame(t, source[0], copied[0])
}
