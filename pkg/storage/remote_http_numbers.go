package storage

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// Keep integer tokens intact until the canonical numeric owner decides whether
// float64 represents them exactly. This also preserves the existing float64
// representation for ordinary HTTP numbers. Rows, graph properties and REST
// envelopes must all use this reader so entity matching sees identical values.
func decodeRemoteHTTPJSON(raw []byte, target interface{}) error {
	// Match json.Unmarshal's rejection of trailing values and malformed input.
	if !json.Valid(raw) {
		return json.Unmarshal(raw, target)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func decodeRemoteHTTPJSONValue(raw []byte) (interface{}, error) {
	var value interface{}
	if err := decodeRemoteHTTPJSON(raw, &value); err != nil {
		return nil, err
	}
	return normalizeRemoteHTTPNumbers(value)
}

func normalizeRemoteHTTPNumbers(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(v), ".eE") {
			if n, err := v.Int64(); err == nil {
				key, _ := NumericValueKey(n)
				return key, nil
			}
			if n, err := strconv.ParseUint(string(v), 10, 64); err == nil {
				key, _ := NumericValueKey(n)
				return key, nil
			}
		}
		n, err := v.Float64()
		if err != nil {
			return nil, err
		}
		key, _ := NumericValueKey(n)
		return key, nil
	case []interface{}:
		for i, item := range v {
			normalized, err := normalizeRemoteHTTPNumbers(item)
			if err != nil {
				return nil, err
			}
			v[i] = normalized
		}
	case map[string]interface{}:
		for name, item := range v {
			normalized, err := normalizeRemoteHTTPNumbers(item)
			if err != nil {
				return nil, err
			}
			v[name] = normalized
		}
	}
	return value, nil
}
