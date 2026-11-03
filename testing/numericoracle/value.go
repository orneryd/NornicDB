// Package numericoracle compares public graph values independently of production
// numeric readers. INTEGERs use arbitrary precision; FLOATs use exact IEEE bits.
// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Test support only.
package numericoracle

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
)

// Equal checks numeric kind/value and graph container shape. Go integer widths
// and signedness are carriers of the same INTEGER meaning; typed and boxed
// lists are likewise equivalent carriers. No INTEGER is widened to FLOAT.
func Equal(t testing.TB, want, got interface{}) {
	t.Helper()
	if err := Compare(want, got); err != nil {
		t.Fatal(err)
	}
}

// Compare supports the primitive, list and string-keyed map shapes in graph
// fixtures. Expected values are explicit fixture literals, never codec output.
func Compare(want, got interface{}) error { return compare("$", want, got) }

func integer(value reflect.Value) (*big.Int, bool) {
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return big.NewInt(value.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return new(big.Int).SetUint64(value.Uint()), true
	}
	return nil, false
}

func compare(path string, want, got interface{}) error {
	if want == nil || got == nil {
		if want == nil && got == nil {
			return nil
		}
		return fmt.Errorf("%s: expected %T(%v), got %T(%v)", path, want, want, got, got)
	}
	w, g := reflect.ValueOf(want), reflect.ValueOf(got)
	if expected, isInteger := integer(w); isInteger {
		actual, isInteger := integer(g)
		if isInteger && expected.Cmp(actual) == 0 {
			return nil
		}
		return fmt.Errorf("%s: expected INTEGER %s, got %T(%v)", path, expected, got, got)
	}
	switch w.Kind() {
	case reflect.Float32, reflect.Float64:
		if (g.Kind() == reflect.Float32 || g.Kind() == reflect.Float64) && math.Float64bits(w.Float()) == math.Float64bits(g.Float()) {
			return nil
		}
	case reflect.Slice, reflect.Array:
		if (g.Kind() == reflect.Slice || g.Kind() == reflect.Array) && w.Len() == g.Len() {
			for i := 0; i < w.Len(); i++ {
				if err := compare(fmt.Sprintf("%s[%d]", path, i), w.Index(i).Interface(), g.Index(i).Interface()); err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.Map:
		if w.Type().Key().Kind() == reflect.String && g.Kind() == reflect.Map && g.Type().Key().Kind() == reflect.String && w.Len() == g.Len() {
			for _, key := range w.MapKeys() {
				actual := g.MapIndex(key.Convert(g.Type().Key()))
				if !actual.IsValid() {
					return fmt.Errorf("%s: missing map key %s", path, key.String())
				}
				if err := compare(path+"."+key.String(), w.MapIndex(key).Interface(), actual.Interface()); err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.String, reflect.Bool:
		if reflect.DeepEqual(want, got) {
			return nil
		}
	}
	return fmt.Errorf("%s: expected %T(%v), got %T(%v)", path, want, want, got, got)
}
