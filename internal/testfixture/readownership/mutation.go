// Author: Codex, NOR-160 public read ownership tests, 29 October 2026.
// Package readownership supplies independent mutation/oracle helpers for public
// storage and namespace read tests. It never calls a production copy helper.
package readownership

import "reflect"

// Capture freezes every exported field, including fields excluded from JSON.
// Maps and slices are captured by value, never by an address or a shared copy.
func Capture(value any) any { return capture(reflect.ValueOf(value)) }

func capture(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return capture(value.Elem())
	case reflect.Map:
		out := make(map[any]any, value.Len())
		for it := value.MapRange(); it.Next(); {
			out[it.Key().Interface()] = capture(it.Value())
		}
		return out
	case reflect.Slice, reflect.Array:
		out := make([]any, value.Len())
		for i := range out {
			out[i] = capture(value.Index(i))
		}
		return out
	case reflect.Struct:
		out := make(map[string]any)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				out[value.Type().Field(i).Name] = capture(value.Field(i))
			}
		}
		return out
	default:
		return value.Interface()
	}
}

// Mutate changes every reachable exported value, all map containers, every
// list element, and the vector/map containers themselves. Return values may
// safely be kept and mutated while a later read is in progress.
func Mutate(value any) { mutate(reflect.ValueOf(value)) }

func mutate(value reflect.Value) {
	if !value.IsValid() {
		return
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !value.IsNil() {
			mutate(value.Elem())
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				mutate(value.Field(i))
			}
		}
	case reflect.Map:
		keys := value.MapKeys()
		for _, key := range keys {
			item := reflect.New(value.Type().Elem()).Elem()
			item.Set(value.MapIndex(key))
			mutate(item)
			value.SetMapIndex(key, item)
		}
		if len(keys) > 0 {
			value.SetMapIndex(keys[0], reflect.Value{})
		}
		if !value.IsNil() && value.Type().Key().Kind() == reflect.String {
			key := reflect.New(value.Type().Key()).Elem()
			key.SetString("caller-added")
			value.SetMapIndex(key, reflect.Zero(value.Type().Elem()))
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			mutate(value.Index(i))
		}
		if value.Len() > 0 && value.Index(0).CanSet() {
			value.Index(0).Set(reflect.Zero(value.Type().Elem()))
		}
	case reflect.String:
		if value.CanSet() {
			value.SetString("caller")
		}
	case reflect.Bool:
		if value.CanSet() {
			value.SetBool(!value.Bool())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.CanSet() {
			value.SetInt(value.Int() + 1)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.CanSet() {
			value.SetUint(value.Uint() + 1)
		}
	case reflect.Float32, reflect.Float64:
		if value.CanSet() {
			value.SetFloat(value.Float() + 1)
		}
	}
}
