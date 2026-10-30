// Package callerowned is the single structural ownership implementation for
// values the product retains and later hands to a caller (NOR-160).
//
// A retention point (cache, buffer, replay window, stored record, materialized
// hit) hands out exactly one caller-owned copy, made here, at its exit. Types
// that own private state implement Copier and copy only their own fields,
// passing every contained value back to Value, so one traversal detaches a
// whole row, result, page or entity.
package callerowned

import (
	"reflect"
	"slices"
	"time"
)

// Copier is implemented by entity and record types that hand out caller-owned
// copies of themselves (for example storage nodes and edges). CallerCopy must
// detach every reference it returns, using Value for contained values.
type Copier interface {
	CallerCopy() any
}

// Value returns a caller-owned copy of value. Concrete types, nil containers
// and nil pointers are preserved; immutable scalars are returned as is.
func Value(value any) any {
	return walker{}.value(value)
}

// walker is the one traversal; onMap, when set, observes each copied map so a
// holder can carry identity-keyed metadata (map key orders) to the copy.
type walker struct {
	onMap func(original, copied map[string]any)
}

func (w walker) value(value any) any {
	switch value := value.(type) {
	case nil, string, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, time.Time, time.Duration:
		return value
	case Copier:
		// The one Copier dispatch rule (copierCopy) keeps the input's type.
		return w.reference(reflect.ValueOf(value)).Interface()
	case []byte:
		return slices.Clone(value)
	case []string:
		return slices.Clone(value)
	case []int:
		return slices.Clone(value)
	case []int32:
		return slices.Clone(value)
	case []int64:
		return slices.Clone(value)
	case []float32:
		return slices.Clone(value)
	case []float64:
		return slices.Clone(value)
	case []bool:
		return slices.Clone(value)
	case []any:
		return w.list(value)
	case map[string]any:
		return w.mapping(value)
	default:
		// Registered typed scalars, metadata and records can contain pointers,
		// arrays or named containers. Their exported fields become caller-owned;
		// unexported codec state is preserved without being interpreted.
		return w.reference(reflect.ValueOf(value)).Interface()
	}
}

// Map returns a caller-owned copy of a property or metadata map.
func Map(m map[string]any) map[string]any {
	return walker{}.mapping(m)
}

func (w walker) mapping(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for key, item := range m {
		out[key] = w.value(item)
	}
	if w.onMap != nil {
		w.onMap(m, out)
	}
	return out
}

// List returns a caller-owned copy of a list.
func List(l []any) []any {
	return walker{}.list(l)
}

func (w walker) list(l []any) []any {
	if l == nil {
		return nil
	}
	out := make([]any, len(l))
	for i, item := range l {
		out[i] = w.value(item)
	}
	return out
}

// Rows returns a caller-owned copy of a result window: a fresh outer slice
// with no spare capacity into retained rows, and caller-owned cells.
func Rows(rows [][]any) [][]any {
	return RowsObservingMaps(rows, nil)
}

// RowsObservingMaps is Rows for holders that key metadata by map identity:
// onMap sees every map copied anywhere in the rows, with its copy.
func RowsObservingMaps(rows [][]any, onMap func(original, copied map[string]any)) [][]any {
	if rows == nil {
		return nil
	}
	w := walker{onMap: onMap}
	out := make([][]any, len(rows))
	for i, row := range rows {
		out[i] = w.list(row)
	}
	return out
}

// Strings returns a caller-owned copy of a string list (labels, columns).
func Strings(s []string) []string {
	return slices.Clone(s)
}

// Of returns a caller-owned copy of a typed value, preserving its type.
func Of[T any](value T) T {
	copied, _ := Value(value).(T)
	return copied
}

// Each is the one slice owner: a fresh slice (nil stays nil, no spare
// capacity) whose elements are copyElement of each element. Callers pass the
// element's owner: Of, a type's own Copier, or a retention point's shaping
// copy.
func Each[T any](items []T, copyElement func(T) T) []T {
	if items == nil {
		return nil
	}
	out := make([]T, len(items))
	for i, item := range items {
		out[i] = copyElement(item)
	}
	return out
}

var copierType = reflect.TypeFor[Copier]()

// copierCopy is the one Copier dispatch rule. A Copier whose CallerCopy
// returns its own type is copied by it; a non-nil pointer to a value-receiver
// Copier is copied through its element and pointed at the copy. Either way the
// result has the input's type. Anything else is walked structurally.
func copierCopy(value reflect.Value) (reflect.Value, bool) {
	if !value.CanInterface() || value.Kind() == reflect.Interface || (value.Kind() == reflect.Pointer && value.IsNil()) {
		return value, false
	}
	if value.Type().Implements(copierType) {
		if copied := reflect.ValueOf(value.Interface().(Copier).CallerCopy()); copied.IsValid() && copied.Type() == value.Type() {
			return copied, true
		}
	}
	if value.Kind() == reflect.Pointer && value.Elem().Type().Implements(copierType) && value.Elem().CanInterface() {
		if copied := reflect.ValueOf(value.Elem().Interface().(Copier).CallerCopy()); copied.IsValid() && copied.Type() == value.Elem().Type() {
			out := reflect.New(copied.Type())
			out.Elem().Set(copied)
			return out, true
		}
	}
	return value, false
}

func (w walker) reference(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	if copied, ok := copierCopy(value); ok {
		return copied
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return value
		}
		out := reflect.New(value.Type()).Elem()
		out.Set(reflect.ValueOf(w.value(value.Elem().Interface())))
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return value
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(w.reference(value.Elem()))
		return out
	case reflect.Map:
		if value.IsNil() {
			return value
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		for it := value.MapRange(); it.Next(); {
			out.SetMapIndex(w.reference(it.Key()), w.reference(it.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return value
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(w.reference(value.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(w.reference(value.Index(i)))
		}
		return out
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		out.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				out.Field(i).Set(w.reference(value.Field(i)))
			}
		}
		return out
	default:
		return value
	}
}
