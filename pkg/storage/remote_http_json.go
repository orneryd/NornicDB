package storage

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

// Neo4j's HTTP API writes an Integer as a JSON integer (-42) and a Float with
// a fraction or an exponent (-42.0, 1.0E20), and reads parameters the same
// way. unmarshalRemoteHTTPJSON and remoteStatement.MarshalJSON keep that distinction
// in both directions, so an integer stays an exact int64 and a float a
// float64 at any nesting depth.

// unmarshalRemoteHTTPJSON decodes HTTP API JSON into out with every number
// read by remoteHTTPNumber.
func unmarshalRemoteHTTPJSON(data []byte, out interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		// Same rejection as json.Unmarshal for text after the value.
		var probe interface{}
		return json.Unmarshal(data, &probe)
	}
	return nil
}

// remoteHTTPNumbers replaces every json.Number in a decoded value, at any
// depth, with remoteHTTPNumber's value.
func remoteHTTPNumbers(value interface{}) interface{} {
	switch v := value.(type) {
	case json.Number:
		return remoteHTTPNumber(v)
	case map[string]interface{}:
		for key, item := range v {
			v[key] = remoteHTTPNumbers(item)
		}
	case []interface{}:
		for i, item := range v {
			v[i] = remoteHTTPNumbers(item)
		}
	}
	return value
}

// remoteHTTPNumber is an int64 for a JSON integer and a float64 for a number
// with a fraction or an exponent.
func remoteHTTPNumber(number json.Number) interface{} {
	text := number.String()
	if !strings.ContainsAny(text, ".eE") {
		if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
			return integer
		}
	}
	float, _ := strconv.ParseFloat(text, 64)
	return float
}

// remoteHTTPFloat64 reads a JSON number decoded by remoteHTTPNumber as a
// float64.
func remoteHTTPFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	}
	return 0, false
}
