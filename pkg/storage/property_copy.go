// Bounded storage-delivery correction: Codex, NOR-160, 1 November 2026.
// Restores the caller-copy protocol of the approved original head
// (refs/codex/reference/pr-1688-20261104) as this release's structural owner.
package storage

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
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

// CopyValue returns an independent caller-owned copy of a value. It is the
// generic copy owner: durable property normalization is a different contract and
// belongs to ownRetainedValue (property_ownership.go). Concrete types,
// pointer, map, slice and array shapes and Go scalar widths are preserved, nil
// containers stay nil, and a caller-declared type that owns a copy protocol is
// asked once, after which its own copy is used as is.
//
// Result holders can use this same owner at their retention and delivery
// boundaries without durable serialization or type normalization. For example:
//
//	owned := storage.CopyValue(map[string]any{"tags": []string{"graph"}}, nil).(map[string]any)
//
// mapCopied, when non-nil, observes plain string-keyed maps copied by the
// structural walk, allowing a result owner to rebind map-identity metadata.
// Domain-owned Node, Edge and CallerCopy values retain their own copy contract.
// Callers must synchronize access to a value while it is being copied.
func CopyValue(value any, mapCopied func(source, copied map[string]any)) any {
	if callerCopyScalar(value) {
		return value
	}
	walker := callerCopyWalker{mapCopied: mapCopied}
	// A panicking copy discards its local scratch instead of reusing it.
	copied := walker.value(value)
	walker.release()
	return copied
}

// Only a cleared overflow index is reused. Large copy graphs are discarded
// rather than retaining their high-water index in an otherwise small request.
const callerCopyPooledReferences = 1024

var callerCopyOverflowPool sync.Pool

func (w *callerCopyWalker) release() {
	if w.seen != nil && len(w.seen) <= callerCopyPooledReferences {
		clear(w.seen)
		callerCopyOverflowPool.Put(w.seen)
	}
}

// callerCopyInlineReferences is how many reference-bearing values one copy
// tracks without allocating. An ordinary property container - a map holding a
// list or a record, a list holding a map - stays within it; only deeper or more
// widely shared graphs spill into a map. Scalar values allocate no visited state
// at all, because the walker never records them.
const callerCopyInlineReferences = 4

// callerCopyTracked pairs a reference-bearing source with the copy made for it,
// so a cycle or a repeated reference resolves to that same copy.
type callerCopyTracked struct {
	key  callerCopyReference
	copy reflect.Value
}

// callerCopyWalker owns the visited state of a single copy call. The inline
// entries cover the common shallow container without allocating; the map is
// created only when a graph outgrows them. The state never outlives the call, so
// no copy shares visited state with another.
type callerCopyWalker struct {
	mapCopied func(source, copied map[string]any)
	inline    [callerCopyInlineReferences]callerCopyTracked
	count     int
	seen      map[callerCopyReference]reflect.Value
}

// callerCopyReference identifies one reference-bearing source. The length keeps
// distinct slices over the same array apart, since only their headers differ.
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

// copied reports the copy already made for this reference, if any.
func (w *callerCopyWalker) copied(key callerCopyReference) (reflect.Value, bool) {
	for i := 0; i < w.count; i++ {
		if w.inline[i].key == key {
			return w.inline[i].copy, true
		}
	}
	if w.seen == nil {
		return reflect.Value{}, false
	}
	copied, ok := w.seen[key]
	return copied, ok
}

// remember records the copy made for a reference before its contents are walked,
// so a reference back to it is answered by this copy instead of recursing.
func (w *callerCopyWalker) remember(key callerCopyReference, copied reflect.Value) {
	if w.count < len(w.inline) {
		w.inline[w.count] = callerCopyTracked{key: key, copy: copied}
		w.count++
		return
	}
	if w.seen == nil {
		if reused, ok := callerCopyOverflowPool.Get().(map[callerCopyReference]reflect.Value); ok {
			w.seen = reused
		} else {
			w.seen = make(map[callerCopyReference]reflect.Value, callerCopyInlineReferences)
		}
	}
	w.seen[key] = copied
}

// callerCopyScalar is the immutable runtime-value policy shared by root
// copies and recursive entries. It does not perform durable normalization.
func callerCopyScalar(value any) bool {
	switch value.(type) {
	case nil, string, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, time.Time, time.Duration:
		return true
	}
	return false
}

func (w *callerCopyWalker) value(value any) any {
	if callerCopyScalar(value) {
		return value
	}
	switch value := value.(type) {
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
		_, boxed := callerCopySlice(w, value, w.value)
		return boxed
	case [][]any:
		_, boxed := callerCopySlice(w, value, func(row []any) []any {
			copied, _ := callerCopySlice(w, row, w.value)
			return copied
		})
		return boxed
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
	if previous, ok := w.copied(key); ok {
		return previous.Interface().(map[string]any)
	}
	out := make(map[string]any, len(m))
	w.remember(key, reflect.ValueOf(out))
	if w.mapCopied != nil {
		w.mapCopied(m, out)
	}
	for key, item := range m {
		out[key] = w.value(item)
	}

	return out
}

// callerCopySlice is the typed slice owner for mutable value graphs. It
// preserves aliases/cycles before copying elements; a matrix uses the same
// owner recursively. The boxed header used by reference tracking is also the
// interface result, so delivery does not allocate a second identical header.
func callerCopySlice[S ~[]E, E any](w *callerCopyWalker, source S, copyItem func(E) E) (S, any) {
	if source == nil {
		return nil, S(nil)
	}
	key := w.referenceKey(reflect.ValueOf(source))
	if previous, ok := w.copied(key); ok {
		boxed := previous.Interface()
		return boxed.(S), boxed
	}
	out := make(S, len(source))
	boxed := any(out)
	w.remember(key, reflect.ValueOf(boxed))
	for i, item := range source {
		out[i] = copyItem(item)
	}
	return out, boxed
}

// copierCopy is the one protocol dispatch, and it invokes at most one
// CallerCopy. A non-nil pointer whose element implements the protocol (a value
// receiver) is copied through its element and pointed at the copy; any other
// implementation is copied by its own CallerCopy. A result of another type is
// never produced: the receiver is chosen before the call, so no copy is made and
// discarded. Anything else is walked structurally. The protocol's runtime type
// descriptor is bound here, at the dispatch, rather than in a package-level
// initializer: TypeFor yields the same descriptor on every call and needs no
// initialization order of its own.
func copierCopy(value reflect.Value) (reflect.Value, bool) {
	if !value.CanInterface() || value.Kind() == reflect.Interface || (value.Kind() == reflect.Pointer && value.IsNil()) {
		return value, false
	}
	copierType := reflect.TypeFor[callerCopier]()
	if value.Kind() == reflect.Pointer && value.Elem().Type().Implements(copierType) && value.Elem().CanInterface() {
		result := value.Elem().Interface().(callerCopier).CallerCopy()
		copied := reflect.ValueOf(result)
		if !copied.IsValid() || copied.Type() != value.Elem().Type() {
			panic(fmt.Sprintf("storage: CallerCopy of %s returned %T", value.Elem().Type(), result))
		}
		out := reflect.New(copied.Type())
		out.Elem().Set(copied)
		return out, true
	}
	if value.Type().Implements(copierType) {
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
		boxed := value.Interface()
		if callerCopyScalar(boxed) {
			return value
		}
		switch entity := boxed.(type) {
		case *Node:
			return reflect.ValueOf(copyNode(entity))
		case *Edge:
			return reflect.ValueOf(copyEdge(entity))
		case map[string]any:
			return reflect.ValueOf(w.mapping(entity))
		case []any, [][]any:
			return reflect.ValueOf(w.value(boxed))
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
		if previous, ok := w.copied(key); ok {
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
		if w.mapCopied != nil {
			if source, ok := value.Interface().(map[string]any); ok {
				w.mapCopied(source, out.Interface().(map[string]any))
			}
		}
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
