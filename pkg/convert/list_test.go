package convert

import (
	"reflect"
	"testing"
)

type opaqueListFixture []int64

func (opaqueListFixture) CypherOpaqueSlice() {}

func TestListValue(t *testing.T) {
	for _, test := range []struct {
		name  string
		input interface{}
		want  []interface{}
		ok    bool
	}{
		{"boxed", []interface{}{int64(1)}, []interface{}{int64(1)}, true},
		{"strings", []string{"a"}, []interface{}{"a"}, true},
		{"ints", []int{1}, []interface{}{int64(1)}, true},
		{"int64", []int64{1}, []interface{}{int64(1)}, true},
		{"float32", []float32{1.5}, []interface{}{float64(1.5)}, true},
		{"float64", []float64{1.5}, []interface{}{float64(1.5)}, true},
		{"booleans", []bool{true}, []interface{}{true}, true},
		{"maps", []map[string]interface{}{{"k": "v"}}, []interface{}{map[string]interface{}{"k": "v"}}, true},
		{"array", [1]int16{1}, []interface{}{int16(1)}, true},
		{"empty", []string(nil), []interface{}{}, true},
		{"null", nil, nil, false},
		{"scalar", "a", nil, false},
		{"opaque", opaqueListFixture{1}, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ListValue(test.input)
			if ok != test.ok || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ListValue(%#v) = %#v, %t; want %#v, %t", test.input, got, ok, test.want, test.ok)
			}
		})
	}
}
