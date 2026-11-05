// Bounded storage-delivery correction: Codex, NOR-160, 1 November 2026.
// Restores the caller-copy protocol of the approved original head
// (refs/codex/reference/pr-1688-20261104) as this release's structural owner.
package storage

import (
	"fmt"
	"reflect"
	"slices"
	"time"
)

// callerCopier is the caller-copy protocol: a storage value that hands out an
// independent copy of itself. A type implements it when a structural copy is not
// the right owner for its contents, as storage nodes and edges are: their
// embedding vectors and runtime record have their own copy rules. CallerCopy
// must return the receiver's own type; it is asked at most once per value.
type callerCopier interface {
	CallerCopy() any
}

// ownedValueCopy returns a caller-owned copy of one storage value. It is the
// generic copy owner: durable property normalization is a different contract and
// belongs to canonicalPropertyValue (property_ownership.go). Concrete types,
// pointer, map, slice and array shapes and Go scalar widths are preserved, nil
// containers stay nil, and a caller-declared type that owns a copy protocol is
// asked once, after which its own copy is used as is.
func ownedValueCopy(value any) any {
	walker := callerCopyWalker{}
	return walker.value(value)
}

// seen is allocated only for reference-bearing values. It preserves cycles and
// shared references in caller-declared records without affecting scalar copies.
type callerCopyWalker struct {
	seen map[callerCopyReference]reflect.Value
}
type callerCopyReference struct {
	typ     reflect.Type
	pointer uintptr
	length  int
}

func (w *callerCopyWalker) referenceKey(value reflect.Value) callerCopyReference {
	key := callerCopyReference{typ: value.Type(), pointer: uintptr(value.UnsafePointer())}
	if value.Kind() == reflect.Slice {
		key.length = value.Len()
	}
	return key
}

func (w *callerCopyWalker) remember(key callerCopyReference, value reflect.Value) {
	if w.seen == nil {
		w.seen = make(map[callerCopyReference]reflect.Value)
	}
	w.seen[key] = value
}

func (w *callerCopyWalker) value(value any) any {
	switch value := value.(type) {
	case nil, string, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, time.Time, time.Duration:
		// Immutable scalars are their own copies.
		return value
	case *Node:
		return copyNode(value)
	case *Edge:
		return copyEdge(value)
	case callerCopier:
		// The one protocol dispatch (copierCopy) keeps the input's type.
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
		// Caller-declared maps, registered scalars, records and other named
		// containers keep their concrete type: their exported fields become
		// caller-owned and unexported state is copied without being interpreted.
		return w.reference(reflect.ValueOf(value)).Interface()
	}
}

func (w *callerCopyWalker) mapping(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	key := w.referenceKey(reflect.ValueOf(m))
	if previous, ok := w.seen[key]; ok {
		return previous.Interface().(map[string]any)
	}
	out := make(map[string]any, len(m))
	w.remember(key, reflect.ValueOf(out))
	for key, item := range m {
		out[key] = w.value(item)
	}

	return out
}

func (w *callerCopyWalker) list(l []any) []any {
	if l == nil {
		return nil
	}
	key := w.referenceKey(reflect.ValueOf(l))
	if previous, ok := w.seen[key]; ok {
		return previous.Interface().([]any)
	}
	out := make([]any, len(l))
	w.remember(key, reflect.ValueOf(out))
	for i, item := range l {
		out[i] = w.value(item)
	}
	return out
}

var callerCopierType = reflect.TypeFor[callerCopier]()

// copierCopy is the one protocol dispatch, and it invokes at most one
// CallerCopy. A non-nil pointer whose element implements the protocol (a value
// receiver) is copied through its element and pointed at the copy; any other
// implementation is copied by its own CallerCopy. A result of another type is
// never produced: the receiver is chosen before the call, so no copy is made and
// discarded. Anything else is walked structurally.
func copierCopy(value reflect.Value) (reflect.Value, bool) {
	if !value.CanInterface() || value.Kind() == reflect.Interface || (value.Kind() == reflect.Pointer && value.IsNil()) {
		return value, false
	}
	if value.Kind() == reflect.Pointer && value.Elem().Type().Implements(callerCopierType) && value.Elem().CanInterface() {
		result := value.Elem().Interface().(callerCopier).CallerCopy()
		copied := reflect.ValueOf(result)
		if !copied.IsValid() || copied.Type() != value.Elem().Type() {
			panic(fmt.Sprintf("storage: CallerCopy of %s returned %T", value.Elem().Type(), result))
		}
		out := reflect.New(copied.Type())
		out.Elem().Set(copied)
		return out, true
	}
	if value.Type().Implements(callerCopierType) {
		result := value.Interface().(callerCopier).CallerCopy()
		copied := reflect.ValueOf(result)
		if !copied.IsValid() || copied.Type() != value.Type() {
			panic(fmt.Sprintf("storage: CallerCopy of %s returned %T", value.Type(), result))
		}
		return copied, true
	}
	return value, false
}

func (w *callerCopyWalker) reference(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	if value.CanInterface() {
		switch entity := value.Interface().(type) {
		case *Node:
			return reflect.ValueOf(copyNode(entity))
		case *Edge:
			return reflect.ValueOf(copyEdge(entity))
		}
	}
	if copied, ok := copierCopy(value); ok {
		return copied
	}
	var key callerCopyReference
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if value.IsNil() {
			return value
		}
		key = w.referenceKey(value)
		if previous, ok := w.seen[key]; ok {
			return previous
		}
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
		w.remember(key, out)
		out.Elem().Set(w.reference(value.Elem()))
		return out
	case reflect.Map:
		if value.IsNil() {
			return value
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		w.remember(key, out)
		for it := value.MapRange(); it.Next(); {
			out.SetMapIndex(w.reference(it.Key()), w.reference(it.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return value
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		w.remember(key, out)
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
