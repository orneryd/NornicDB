// NOR-160 lane (Claude), caller-copy completeness oracle, 30 October 2026.
package readownership

import (
	"fmt"
	"reflect"
)

// SharedReferences lists every exported path at which copied still shares a
// mutable map, slice backing array or pointer with original. A complete caller
// copy returns none. Unexported state is not a caller's to reach and is not
// inspected.
func SharedReferences(original, copied any) []string {
	var out []string
	shared("", reflect.ValueOf(original), reflect.ValueOf(copied), &out, 0)
	return out
}

func shared(path string, a, b reflect.Value, out *[]string, depth int) {
	if !a.IsValid() || !b.IsValid() || depth > 64 {
		return
	}
	if a.Kind() == reflect.Interface {
		if a.IsNil() || b.Kind() != reflect.Interface || b.IsNil() {
			return
		}
		shared(path, a.Elem(), b.Elem(), out, depth+1)
		return
	}
	if a.Type() != b.Type() {
		return
	}
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return
		}
		if a.Pointer() == b.Pointer() {
			*out = append(*out, path)
			return
		}
		shared(path, a.Elem(), b.Elem(), out, depth+1)
	case reflect.Map:
		if a.IsNil() || b.IsNil() {
			return
		}
		if a.Pointer() == b.Pointer() {
			*out = append(*out, path)
			return
		}
		for it := a.MapRange(); it.Next(); {
			if item := b.MapIndex(it.Key()); item.IsValid() {
				shared(fmt.Sprintf("%s[%v]", path, it.Key().Interface()), it.Value(), item, out, depth+1)
			}
		}
	case reflect.Slice:
		if a.Len() == 0 || b.Len() == 0 {
			return
		}
		if a.Pointer() == b.Pointer() {
			*out = append(*out, path)
			return
		}
		for i := 0; i < a.Len() && i < b.Len(); i++ {
			shared(fmt.Sprintf("%s[%d]", path, i), a.Index(i), b.Index(i), out, depth+1)
		}
	case reflect.Array:
		for i := 0; i < a.Len(); i++ {
			shared(fmt.Sprintf("%s[%d]", path, i), a.Index(i), b.Index(i), out, depth+1)
		}
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if a.Type().Field(i).IsExported() {
				shared(path+"."+a.Type().Field(i).Name, a.Field(i), b.Field(i), out, depth+1)
			}
		}
	}
}

// Unpopulated lists every exported reference-bearing path of sample that is
// nil or empty, so a completeness check cannot pass on a sample that leaves a
// field out. A type nested in itself (a passage of a hit) is checked once;
// values held in interfaces (row cells) need only be present.
func Unpopulated(sample any) []string {
	var out []string
	unpopulated("", reflect.ValueOf(sample), &out, map[reflect.Type]bool{})
	return out
}

func unpopulated(path string, v reflect.Value, out *[]string, onPath map[reflect.Type]bool) {
	if !v.IsValid() {
		*out = append(*out, path)
		return
	}
	switch v.Kind() {
	case reflect.Interface:
		// A dynamic cell's own type is checked by its own copier's test.
		if v.IsNil() {
			*out = append(*out, path)
		}
	case reflect.Pointer:
		if v.IsNil() {
			*out = append(*out, path)
			return
		}
		unpopulated(path, v.Elem(), out, onPath)
	case reflect.Map:
		if v.Len() == 0 {
			*out = append(*out, path)
		}
	case reflect.Slice:
		if v.Len() == 0 {
			*out = append(*out, path)
			return
		}
		for i := 0; i < v.Len(); i++ {
			unpopulated(fmt.Sprintf("%s[%d]", path, i), v.Index(i), out, onPath)
		}
	case reflect.Struct:
		if onPath[v.Type()] {
			return
		}
		onPath[v.Type()] = true
		defer delete(onPath, v.Type())
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if field.IsExported() && referenceKind(field.Type) {
				unpopulated(path+"."+field.Name, v.Field(i), out, onPath)
			}
		}
	}
}

func referenceKind(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice:
		return true
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).IsExported() && referenceKind(t.Field(i).Type) {
				return true
			}
		}
	}
	return false
}
