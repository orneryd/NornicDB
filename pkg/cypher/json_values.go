package cypher

import (
	"bytes"
	"encoding/json"
	"strings"
)

// JSONNumbers converts the json.Number values of a value decoded with
// UseNumber to Cypher numbers, as Neo4j's HTTP API and APOC read JSON: a
// number without '.', 'e' or 'E' that fits int64 is an INTEGER, any other a
// FLOAT. Lists and maps are converted in place.
func JSONNumbers(value interface{}) interface{} {
	switch v := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(v), ".eE") {
			if i, err := v.Int64(); err == nil {
				return i
			}
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return string(v)
	case []interface{}:
		for i, item := range v {
			v[i] = JSONNumbers(item)
		}
		return v
	case map[string]interface{}:
		for key, item := range v {
			v[key] = JSONNumbers(item)
		}
		return v
	default:
		return value
	}
}

// apocConvertFromJSON is apoc.convert.fromJsonMap / fromJsonList(text): the
// JSON text's map or list with JSONNumbers' numbers; ok is false for a
// value that isn't text, text that isn't JSON, or JSON of the other kind.
func apocConvertFromJSON(wantMap bool, value interface{}) (interface{}, bool) {
	text, isText := value.(string)
	if !isText {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	decoder.UseNumber()
	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return nil, false
	}
	decoded = JSONNumbers(decoded)
	if wantMap {
		result, isMap := decoded.(map[string]interface{})
		return result, isMap
	}
	result, isList := decoded.([]interface{})
	return result, isList
}
