package fn

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)

// The value walker is the one owner of a recursive pass over a Cypher value:
// its maps and lists, at any depth, and the typed Go containers a handler or
// a driver produces (reflected maps, slices and arrays, which become Cypher
// maps and lists). Each value it reaches is one check of the statement
// (walk), so a large value observes the statement however it nests and
// whatever a step decides about it.

// ValueStep decides one value of a transform: its replacement, whether that
// differs from the value, and whether the walk descends into the
// replacement's members.
type ValueStep func(value interface{}) (replacement interface{}, changed, descend bool)

// TransformValue rewrites value with step, copying a container only when one
// of its members changed (copy on change), so an unchanged value is returned
// as it is. Reflected typed containers are always converted.
func TransformValue(walk *bulk.Checkpoint, value interface{}, step ValueStep) (interface{}, bool) {
	walk.Check()
	replacement, changed, descend := step(value)
	if !descend {
		return replacement, changed
	}
	switch typed := replacement.(type) {
	case map[string]interface{}:
		out, membersChanged := transformMap(walk, typed, step)
		return out, changed || membersChanged
	case map[interface{}]interface{}:
		out, membersChanged := transformMap(walk, typed, step)
		return out, changed || membersChanged
	case []interface{}:
		out, membersChanged := transformList(walk, typed, step)
		if !membersChanged {
			// The value as it came: boxing the list again would allocate.
			return replacement, changed
		}
		return out, true
	}
	reflected := reflect.ValueOf(replacement)
	switch reflected.Kind() {
	case reflect.Map:
		if reflected.Type().Key().Kind() == reflect.String {
			out := make(map[string]interface{}, reflected.Len())
			for iterator := reflected.MapRange(); iterator.Next(); {
				walk.Check()
				out[iterator.Key().String()], _ = TransformValue(walk, iterator.Value().Interface(), step)
			}
			return out, true
		}
		out := make(map[interface{}]interface{}, reflected.Len())
		for iterator := reflected.MapRange(); iterator.Next(); {
			walk.Check()
			out[iterator.Key().Interface()], _ = TransformValue(walk, iterator.Value().Interface(), step)
		}
		return out, true
	case reflect.Slice, reflect.Array:
		out := make([]interface{}, reflected.Len())
		for index := range out {
			walk.Check()
			out[index], _ = TransformValue(walk, reflected.Index(index).Interface(), step)
		}
		return out, true
	}
	return replacement, changed
}

// transformMap is TransformValue over a map's members, copied on change.
func transformMap[K comparable](walk *bulk.Checkpoint, members map[K]interface{}, step ValueStep) (map[K]interface{}, bool) {
	var changes map[K]interface{}
	for key, member := range members {
		walk.Check()
		if normalized, changed := TransformValue(walk, member, step); changed {
			if changes == nil {
				changes = make(map[K]interface{})
			}
			changes[key] = normalized
		}
	}
	if changes == nil {
		return members, false
	}
	out := make(map[K]interface{}, len(members))
	for key, member := range members {
		walk.Check()
		out[key] = member
	}
	for key, normalized := range changes {
		walk.Check()
		out[key] = normalized
	}
	return out, true
}

// transformList is TransformValue over a list's members, copied on change.
func transformList(walk *bulk.Checkpoint, members []interface{}, step ValueStep) ([]interface{}, bool) {
	var out []interface{}
	for index, member := range members {
		walk.Check()
		normalized, changed := TransformValue(walk, member, step)
		if !changed {
			if out != nil {
				out[index] = member
			}
			continue
		}
		if out == nil {
			out = make([]interface{}, len(members))
			for earlier := 0; earlier < index; earlier++ {
				walk.Check()
				out[earlier] = members[earlier]
			}
		}
		out[index] = normalized
	}
	if out == nil {
		return members, false
	}
	return out, true
}

// VisitValue walks value depth first, calling visit on each value: descend
// says whether to walk its members, stop ends the walk. It reports whether
// a visit stopped it.
func VisitValue(walk *bulk.Checkpoint, value interface{}, visit func(value interface{}) (descend, stop bool)) bool {
	return VisitValueDepth(walk, value, func(value interface{}, _ int) (bool, bool) { return visit(value) })
}

// VisitValueDepth is VisitValue with each value's depth (the value itself is
// depth 0, its members 1, ...).
func VisitValueDepth(walk *bulk.Checkpoint, value interface{}, visit func(value interface{}, depth int) (descend, stop bool)) bool {
	return visitValueAt(walk, value, 0, visit)
}

func visitValueAt(walk *bulk.Checkpoint, value interface{}, depth int, visit func(value interface{}, depth int) (descend, stop bool)) bool {
	walk.Check()
	descend, stop := visit(value, depth)
	if stop {
		return true
	}
	if !descend {
		return false
	}
	switch typed := value.(type) {
	case map[string]interface{}:
		for _, member := range typed {
			walk.Check()
			if visitValueAt(walk, member, depth+1, visit) {
				return true
			}
		}
		return false
	case map[interface{}]interface{}:
		for _, member := range typed {
			walk.Check()
			if visitValueAt(walk, member, depth+1, visit) {
				return true
			}
		}
		return false
	case []interface{}:
		for _, member := range typed {
			walk.Check()
			if visitValueAt(walk, member, depth+1, visit) {
				return true
			}
		}
		return false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Map:
		for iterator := reflected.MapRange(); iterator.Next(); {
			walk.Check()
			if visitValueAt(walk, iterator.Value().Interface(), depth+1, visit) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < reflected.Len(); index++ {
			walk.Check()
			if visitValueAt(walk, reflected.Index(index).Interface(), depth+1, visit) {
				return true
			}
		}
	}
	return false
}

// ProjectValue rebuilds value with project applied to each value it handles,
// keeping every container's carrier type (a typed list, array or map stays
// that type) and copying every container, so the result shares no container
// with value. A nil container is returned as it is.
func ProjectValue(walk *bulk.Checkpoint, value interface{}, project func(value interface{}) (projected interface{}, handled bool)) interface{} {
	walk.Check()
	if projected, handled := project(value); handled {
		return projected
	}
	switch typed := value.(type) {
	case []interface{}:
		if typed == nil {
			return value
		}
		out := make([]interface{}, len(typed))
		for index, member := range typed {
			walk.Check()
			out[index] = ProjectValue(walk, member, project)
		}
		return out
	case map[string]interface{}:
		if typed == nil {
			return value
		}
		out := make(map[string]interface{}, len(typed))
		for key, member := range typed {
			walk.Check()
			out[key] = ProjectValue(walk, member, project)
		}
		return out
	}
	if value == nil {
		return value
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		if reflected.Kind() == reflect.Slice && reflected.IsNil() {
			return value
		}
		var out reflect.Value
		if reflected.Kind() == reflect.Slice {
			out = reflect.MakeSlice(reflected.Type(), reflected.Len(), reflected.Len())
		} else {
			out = reflect.New(reflected.Type()).Elem()
		}
		for index := 0; index < reflected.Len(); index++ {
			walk.Check()
			if projected := ProjectValue(walk, reflected.Index(index).Interface(), project); projected != nil {
				out.Index(index).Set(reflect.ValueOf(projected))
			}
		}
		return out.Interface()
	case reflect.Map:
		if reflected.IsNil() || reflected.Type().Key().Kind() != reflect.String {
			return value
		}
		out := reflect.MakeMapWithSize(reflected.Type(), reflected.Len())
		for entries := reflected.MapRange(); entries.Next(); {
			walk.Check()
			entry := reflect.Zero(reflected.Type().Elem())
			if projected := ProjectValue(walk, entries.Value().Interface(), project); projected != nil {
				entry = reflect.ValueOf(projected)
			}
			out.SetMapIndex(entries.Key(), entry)
		}
		return out.Interface()
	}
	return value
}

// IdenticalValues is reflect.DeepEqual for values: the same Go types and
// contents at every depth. It walks maps, lists, slices and arrays itself,
// each member through the owner's check (walk); other values compare as
// reflect.DeepEqual compares them.
func IdenticalValues(walk *bulk.Checkpoint, left, right interface{}) bool {
	walk.Check()
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	l, r := reflect.ValueOf(left), reflect.ValueOf(right)
	if l.Type() != r.Type() {
		return false
	}
	switch l.Kind() {
	case reflect.Map:
		if l.IsNil() != r.IsNil() || l.Len() != r.Len() {
			return false
		}
		if l.UnsafePointer() == r.UnsafePointer() {
			return true
		}
		for entries := l.MapRange(); entries.Next(); {
			walk.Check()
			other := r.MapIndex(entries.Key())
			if !other.IsValid() || !identicalMembers(walk, entries.Value(), other) {
				return false
			}
		}
		return true
	case reflect.Slice:
		if l.IsNil() != r.IsNil() || l.Len() != r.Len() {
			return false
		}
		if l.Len() > 0 && l.UnsafePointer() == r.UnsafePointer() {
			return true
		}
		fallthrough
	case reflect.Array:
		for index := 0; index < l.Len(); index++ {
			walk.Check()
			if !identicalMembers(walk, l.Index(index), r.Index(index)) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(left, right)
}

// identicalMembers is IdenticalValues of two members as reflect.DeepEqual
// compares them: by their dynamic values when they are interfaces.
func identicalMembers(walk *bulk.Checkpoint, left, right reflect.Value) bool {
	if left.Kind() == reflect.Interface && right.Kind() == reflect.Interface {
		if left.IsNil() || right.IsNil() {
			return left.IsNil() && right.IsNil()
		}
		left, right = left.Elem(), right.Elem()
	}
	if !left.CanInterface() || !right.CanInterface() {
		return reflect.DeepEqual(left, right)
	}
	return IdenticalValues(walk, left.Interface(), right.Interface())
}
