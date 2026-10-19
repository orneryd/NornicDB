package convert

import (
	"reflect"
	"testing"
)

func TestListInputConversions(t *testing.T) {
	if got := ToList([]string{"a", "b"}); !reflect.DeepEqual(got, []interface{}{"a", "b"}) {
		t.Fatalf("typed list converted to %#v", got)
	}
	for _, value := range []interface{}{int8(3), int16(3), int32(3), int64(3), uint8(3), uint16(3), uint32(3), uint64(3), float32(3), float64(3)} {
		if ToFloat(value) != 3 || ToInteger(value) != 3 || !ToBoolean(value) {
			t.Errorf("numeric conversion lost %T(%v)", value, value)
		}
	}
	for _, zero := range []interface{}{int8(0), int16(0), int32(0), int64(0), uint8(0), uint16(0), uint32(0), uint64(0), float32(0), float64(0)} {
		if ToBoolean(zero) {
			t.Errorf("zero %T converted to true", zero)
		}
	}
	if ToInteger("3.5") != 0 || !ToBoolean("yes") || !ToBoolean("1") || ToBoolean("1.0") {
		t.Fatal("existing APOC string conversion policy changed")
	}
}
