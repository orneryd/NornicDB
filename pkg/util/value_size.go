package util

// EstimateValueBytes returns an approximate in-memory size of a decoded
// property value (string, []byte, []string, []any, map[string]any; any other
// type counts as one 16-byte word pair). It is used for byte-bounded caches,
// where a cheap, deterministic estimate is preferable to reflection.
func EstimateValueBytes(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return int64(16 + len(typed))
	case []byte:
		return int64(24 + len(typed))
	case []string:
		bytes := int64(24 + 16*len(typed))
		for _, item := range typed {
			bytes += int64(len(item))
		}
		return bytes
	case []any:
		bytes := int64(24 + 16*len(typed))
		for _, item := range typed {
			bytes += EstimateValueBytes(item)
		}
		return bytes
	case map[string]any:
		bytes := int64(48 + 32*len(typed))
		for key, item := range typed {
			bytes += int64(len(key)) + EstimateValueBytes(item)
		}
		return bytes
	default:
		return 16
	}
}
