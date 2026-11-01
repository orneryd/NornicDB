package convert

// NativeScalar reports whether value is nil or one of the native Go string,
// boolean or numeric property types. It neither coerces values nor admits named
// types, pointers, containers, uintptr or complex numbers. Callers whose policy
// excludes null must test that separately; nil remains a safe equality value.
//
// For example, NativeScalar(int64(7)) is true, while
// NativeScalar([]int64{7}) is false.
func NativeScalar(value interface{}) bool {
	switch value.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	}
	return false
}
