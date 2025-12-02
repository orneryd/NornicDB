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
	switch val := v.(type) {
	case []float64:
		return val, true
	case []float32:
		result := make([]float64, len(val))
		for i, f := range val {
			result[i] = float64(f)
		}
		return result, true
	case []interface{}:
		result := make([]float64, len(val))
		for i, item := range val {
			if f, ok := ToFloat64(item); ok {
				result[i] = f
			} else {
				return nil, false
			}
		}
		return result, true
	}
	return nil, false
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
	switch val := v.(type) {
	case []float32:
		return val
	case []float64:
		result := make([]float32, len(val))
		for i, f := range val {
			result[i] = float32(f)
		}
		return result
	case []interface{}:
		result := make([]float32, 0, len(val))
		for _, item := range val {
			if f, ok := ToFloat64(item); ok {
				result = append(result, float32(f))
			}
		}
		return result
	}
	return nil
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
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		result := make([]string, len(val))
		for i, item := range val {
			if s, ok := item.(string); ok {
				result[i] = s
			} else {
				return nil
			}
		}
		return result
	}
	return nil
}
