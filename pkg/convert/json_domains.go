package convert

import (
	"encoding/json"
	"io"
	"math"
	"strconv"
)

// PropertyJSON owns existing graph data: exports, imports, backups, document
// transforms, result properties, recovery and replication fallbacks. INTEGER
// retains the complete int64/uint64 domain. It never falls back to FLOAT.
// Every production entry point is bound to its domain in json_data_paths.json.
var PropertyJSON propertyJSONDomain

// CypherInputJSON owns external Cypher values: parameters, request bodies,
// explicit fromJson conversions and load.json. Its INTEGER/FLOAT distinction
// follows Neo4j. It is a different type from PropertyJSON; no policy argument,
// generic decoder constructor or mutable policy is exposed to callers.
var CypherInputJSON cypherInputJSONDomain

type propertyJSONDomain struct{}
type cypherInputJSONDomain struct{}

func (propertyJSONDomain) NewDecoder(reader io.Reader) *jsonDecoder[jsonPropertyPolicy] {
	return newJSONDecoder[jsonPropertyPolicy](reader)
}
func (propertyJSONDomain) Unmarshal(data []byte, target interface{}) error {
	return unmarshalJSON[jsonPropertyPolicy](data, target)
}
func (propertyJSONDomain) Numbers(value interface{}) (interface{}, error) {
	return jsonNumbers(value, jsonNumberPolicy{exact: true})
}
func (propertyJSONDomain) Marshal(value interface{}) ([]byte, error) { return marshalJSON(value) }
func (propertyJSONDomain) MarshalIndent(value interface{}, prefix, indent string) ([]byte, error) {
	return marshalJSONIndent(value, prefix, indent)
}
func (propertyJSONDomain) NewEncoder(writer io.Writer) *JSONEncoder  { return newJSONEncoder(writer) }
func (propertyJSONDomain) Prepare(value interface{}) interface{}     { return prepareJSON(value) }
func (propertyJSONDomain) Float(value float64, bits int) interface{} { return jsonFloat(value, bits) }

func (cypherInputJSONDomain) NewDecoder(reader io.Reader) *jsonDecoder[jsonInputPolicy] {
	return newJSONDecoder[jsonInputPolicy](reader)
}
func (cypherInputJSONDomain) Unmarshal(data []byte, target interface{}) error {
	return unmarshalJSON[jsonInputPolicy](data, target)
}

// NewCompatibilityDecoder retains the established overflow-to-text behavior
// of HTTP transactions and explicit APOC fromJson conversion only. The path
// registry pins these consumers; persisted data cannot select this entry point.
func (cypherInputJSONDomain) NewCompatibilityDecoder(reader io.Reader) *jsonDecoder[jsonCompatibilityPolicy] {
	return newJSONDecoder[jsonCompatibilityPolicy](reader)
}
func (cypherInputJSONDomain) Numbers(value interface{}) interface{} {
	normalized, _ := jsonNumbers(value, jsonNumberPolicy{textFallback: true})
	return normalized
}
func (cypherInputJSONDomain) Marshal(value interface{}) ([]byte, error) { return marshalJSON(value) }
func (cypherInputJSONDomain) MarshalIndent(value interface{}, prefix, indent string) ([]byte, error) {
	return marshalJSONIndent(value, prefix, indent)
}
func (cypherInputJSONDomain) NewEncoder(writer io.Writer) *JSONEncoder { return newJSONEncoder(writer) }

// Typed numeric leaves let transport adapters rebuild temporal/spatial values
// while retaining the same JSON numeric spelling and errors as domain writers.
func (cypherInputJSONDomain) AppendInteger(dst []byte, value int64) []byte {
	return appendJSONInteger(dst, value)
}
func (cypherInputJSONDomain) AppendUnsigned(dst []byte, value uint64) []byte {
	return appendJSONUnsigned(dst, value)
}
func (cypherInputJSONDomain) AppendFloat(dst []byte, value float64, bits int) ([]byte, error) {
	return appendJSONFloat(dst, value, bits)
}

func appendJSONInteger(dst []byte, value int64) []byte   { return strconv.AppendInt(dst, value, 10) }
func appendJSONUnsigned(dst []byte, value uint64) []byte { return strconv.AppendUint(dst, value, 10) }
func appendJSONFloat(dst []byte, value float64, bits int) ([]byte, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		var err error
		if bits == 32 {
			_, err = json.Marshal(float32(value))
		} else {
			_, err = json.Marshal(value)
		}
		return dst, err
	}
	return appendFloatText(dst, value, bits), nil
}
