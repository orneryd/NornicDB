package convert

import "testing"

// Storage returns an integer in the width its codec keeps (a small Go int
// reads back as int8), so every integer width converts.
func TestNumericConversionsAcceptEveryIntegerWidth(t *testing.T) {
	for _, value := range []interface{}{int(7), int8(7), int16(7), int32(7), int64(7), uint(7), uint8(7), uint16(7), uint32(7), uint64(7)} {
		if f, ok := ToFloat64(value); !ok || f != 7 {
			t.Errorf("ToFloat64(%T) = %v, %v", value, f, ok)
		}
		if i, ok := ToInt64(value); !ok || i != 7 {
			t.Errorf("ToInt64(%T) = %v, %v", value, i, ok)
		}
	}
}
