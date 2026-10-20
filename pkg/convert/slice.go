package convert

// ToFloat64Slice converts various slice types to []float64.
// Returns (slice, true) on success, (nil, false) on failure.
//
// Supported types:
//   - []float64 (returned as-is)
//   - []float32 (each element converted)
//   - []interface{} (each element converted via ToFloat64)
//
// Example:
//
//	s, ok := ToFloat64Slice([]interface{}{1, 2.5, "3"})  // Returns ([1.0, 2.5, 3.0], true)
//	s, ok := ToFloat64Slice([]float32{1.0, 2.0})        // Returns ([1.0, 2.0], true)
//
// ELI12:
//
// This takes a list of numbers (in any format) and converts them all
// to decimal numbers. If any number in the list can't be converted,
// it returns nil to tell you something went wrong.
func ToFloat64Slice(v interface{}) ([]float64, bool) {
	return float64Slice(v, true)
}

// NumericFloat64Slice uses the same list and numeric readers but rejects text.
// Vector/math callers retain their numeric-only argument contract.
func NumericFloat64Slice(value interface{}) ([]float64, bool) {
	return float64Slice(value, false)
}

func float64Slice(value interface{}, textAllowed bool) ([]float64, bool) {
	list, ok := ReadList(value)
	if !ok {
		return nil, false
	}
	return list.float64Values(textAllowed)
}

// Numbers reads numeric coordinates from this view without a boxed-list copy.
func (list List) Numbers() ([]float64, bool) {
	return list.float64Values(false)
}

func (list List) float64Values(textAllowed bool) ([]float64, bool) {
	if list.kind == float64List {
		return list.value.([]float64), true
	}
	result := make([]float64, list.Len())
	for index := range result {
		if number, ok := list.numberAt(index); ok {
			result[index] = number.Float64()
		} else if textAllowed {
			number, ok := ToFloat64(list.At(index))
			if !ok {
				return nil, false
			}
			result[index] = number
		} else {
			return nil, false
		}
	}
	return result, true
}

// ToFloat32Slice converts various slice types to []float32.
// Returns slice on success, nil on failure.
//
// This is commonly used for converting data to embedding vector format,
// since most ML models use float32 vectors.
//
// Supported types:
//   - []float32 (returned as-is)
//   - []float64 (each element converted)
//   - []interface{} (each element converted)
//
// Example:
//
//	s := ToFloat32Slice([]interface{}{1, 2.5, 3})  // Returns [1.0, 2.5, 3.0]
//	s := ToFloat32Slice([]float64{1.0, 2.0})      // Returns [1.0, 2.0]
//
// ELI12:
//
// This converts a list of numbers to the format used by AI/ML models
// for vector embeddings. The result is more memory efficient than float64.
func ToFloat32Slice(v interface{}) []float32 {
	list, ok := ReadList(v)
	if !ok {
		return nil
	}
	result, issue := list.float32Values(true, true)
	if issue != nil {
		return nil
	}
	return result
}

// NumberListIssue lets a public adapter retain its existing localized invalid-
// type or out-of-range policy without reimplementing coordinate conversion.
type NumberListIssue struct {
	Index      int
	Value      interface{}
	OutOfRange bool
}

// Numbers32 reads every coordinate or reports the first unconvertible value.
func (list List) Numbers32() ([]float32, *NumberListIssue) {
	return list.float32Values(false, false)
}

func (list List) float32Values(textAllowed, skipNonNumeric bool) ([]float32, *NumberListIssue) {
	if list.kind == float32List {
		return list.value.([]float32), nil
	}
	result := make([]float32, 0, list.Len())
	for index := 0; index < list.Len(); index++ {
		number, ok := list.numberAt(index)
		if !ok {
			if textAllowed {
				floating, converted := ToFloat64(list.At(index))
				if converted {
					number, ok = ReadNumber(floating)
				}
			}
			if !ok {
				if skipNonNumeric {
					continue
				}
				return nil, &NumberListIssue{Index: index, Value: list.At(index)}
			}
		}
		floating, ok := number.Float32()
		if !ok {
			return nil, &NumberListIssue{Index: index, Value: list.At(index), OutOfRange: true}
		}
		result = append(result, floating)
	}
	return result, nil
}

// ToStringSlice converts various slice types to []string.
// Returns slice on success, nil on failure.
//
// Supported types:
//   - []string (returned as-is)
//   - []interface{} (each element converted via fmt.Sprint)
//
// Example:
//
//	s := ToStringSlice([]interface{}{"a", "b", "c"})  // Returns ["a", "b", "c"]
//	s := ToStringSlice([]interface{}{1, 2, 3})        // Returns ["1", "2", "3"]
func ToStringSlice(v interface{}) []string {
	list, ok := ReadList(v)
	if !ok {
		return nil
	}
	if list.kind == stringList {
		return list.value.([]string)
	}
	result := make([]string, list.Len())
	for index := range result {
		text, ok := list.At(index).(string)
		if !ok {
			return nil
		}
		result[index] = text
	}
	return result
}
