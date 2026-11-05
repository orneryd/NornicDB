// Author: Codex, NOR-180 terminal diagnostic ownership, 2026-11-01.
package errors

import (
	"fmt"
	"reflect"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// cloneOutcomeMessage owns both descriptor inputs and every outward diagnostic
// copy. Message data is display data; error identity remains in the cause tree.
func cloneOutcomeMessage(message localization.Message) localization.Message {
	if message.Data != nil {
		message.Data = cloneOutcomeValue(reflect.ValueOf(message.Data), make(map[outcomeValueVisit]reflect.Value)).Interface().(map[string]any)
	}
	if message.PluralCount != nil {
		message.PluralCount = cloneOutcomeValue(reflect.ValueOf(message.PluralCount), make(map[outcomeValueVisit]reflect.Value)).Interface()
	}
	return message
}

type outcomeValueVisit struct {
	typ     reflect.Type
	address uintptr
	length  int
}
type frozenOutcomeError string

func (e frozenOutcomeError) Error() string { return string(e) }

// cloneOutcomeValue retains scalar and container types while detaching mutable
// storage. Display errors are frozen as errors, rather than retaining mutable
// producer objects. Opaque values are frozen to their display text.
func cloneOutcomeValue(value reflect.Value, seen map[outcomeValueVisit]reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	if value.CanInterface() {
		// time.Time is an immutable scalar; preserve its formatter and concrete type.
		if _, ok := value.Interface().(time.Time); ok {
			return value
		}
		if err, ok := value.Interface().(error); ok {
			if value.Kind() == reflect.Pointer && value.IsNil() {
				return value
			}
			return reflect.ValueOf(frozenOutcomeError(err.Error()))
		}
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return value
		}
		result := reflect.New(value.Type()).Elem()
		item := cloneOutcomeValue(value.Elem(), seen)
		if !item.Type().AssignableTo(value.Type()) {
			return item
		}
		result.Set(item)
		return result
	case reflect.Map, reflect.Slice, reflect.Pointer:
		if value.IsNil() {
			return value
		}
		visit := outcomeValueVisit{typ: value.Type(), address: uintptr(value.UnsafePointer())}
		if value.Kind() == reflect.Slice {
			visit.length = value.Len()
		}
		if copy, ok := seen[visit]; ok {
			return copy
		}
		switch value.Kind() {
		case reflect.Map:
			result := reflect.MakeMapWithSize(value.Type(), value.Len())
			seen[visit] = result
			for it := value.MapRange(); it.Next(); {
				key, item := cloneOutcomeValue(it.Key(), seen), cloneOutcomeValue(it.Value(), seen)
				if !key.Type().AssignableTo(value.Type().Key()) || !item.Type().AssignableTo(value.Type().Elem()) {
					return reflect.ValueOf(fmt.Sprint(value.Interface()))
				}
				result.SetMapIndex(key, item)
			}
			return result
		case reflect.Slice:
			result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
			seen[visit] = result
			for i := 0; i < value.Len(); i++ {
				item := cloneOutcomeValue(value.Index(i), seen)
				if !item.Type().AssignableTo(value.Type().Elem()) {
					return reflect.ValueOf(fmt.Sprint(value.Interface()))
				}
				result.Index(i).Set(item)
			}
			return result
		default:
			result := reflect.New(value.Type().Elem())
			seen[visit] = result
			item := cloneOutcomeValue(value.Elem(), seen)
			if !item.Type().AssignableTo(value.Type().Elem()) {
				return reflect.ValueOf(fmt.Sprint(value.Interface()))
			}
			result.Elem().Set(item)
			return result
		}
	case reflect.Array, reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		if value.Kind() == reflect.Array {
			for i := 0; i < value.Len(); i++ {
				item := cloneOutcomeValue(value.Index(i), seen)
				if !item.Type().AssignableTo(value.Type().Elem()) {
					return reflect.ValueOf(fmt.Sprint(value.Interface()))
				}
				result.Index(i).Set(item)
			}
		} else {
			for i := 0; i < value.NumField(); i++ {
				if !result.Field(i).CanSet() {
					if outcomeValueHasReferences(value.Type().Field(i).Type) {
						return reflect.ValueOf(fmt.Sprint(value.Interface()))
					}
					continue
				}
				item := cloneOutcomeValue(value.Field(i), seen)
				if !item.Type().AssignableTo(result.Field(i).Type()) {
					return reflect.ValueOf(fmt.Sprint(value.Interface()))
				}
				result.Field(i).Set(item)
			}
		}
		return result
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.ValueOf(fmt.Sprint(value.Interface()))
	default:
		return value
	}
}
func outcomeValueHasReferences(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return true
	case reflect.Array:
		return outcomeValueHasReferences(typ.Elem())
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			if outcomeValueHasReferences(typ.Field(i).Type) {
				return true
			}
		}
	}
	return false
}
