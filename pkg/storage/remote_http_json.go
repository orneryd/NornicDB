package storage

import (
	"encoding/json"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// unmarshalRemoteHTTPJSON selects property admission over the canonical
// lossless string/framing owner; it implements no independent JSON parser.
func unmarshalRemoteHTTPJSON(data []byte, out interface{}) error {
	return valueconvert.PropertyJSON.Unmarshal(data, out)
}

// remoteHTTPNumbers interprets legacy Neo4j metadata number tokens. Property
// images are already normalized by PropertyJSON before this transport adapter.
func remoteHTTPNumbers(value interface{}) (interface{}, error) {
	return valueconvert.CypherInputJSON.Numbers(value)
}

func remoteHTTPNumber(number json.Number) (interface{}, error) {
	return valueconvert.CypherInputJSON.Numbers(number)
}

// Point coordinates intentionally widen numeric components to FLOAT; they do
// not change the kind of stored properties. One numeric conversion owner.
func remoteHTTPFloat64(value interface{}) (float64, bool) {
	return valueconvert.NumericFloat64(value)
}
