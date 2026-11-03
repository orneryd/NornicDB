// Author: #1665 implementation lane (Codex). Cross-contract public codec regressions.
package convert_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/convert"
)

type numericJSONDomain interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

func TestNumericDomainsPreserveLosslessStringIdentity(t *testing.T) {
	unit := string([]byte{0xed, 0xa0, 0x80})
	raw := []byte(`{"n":9007199254740993,"f":1.0,"nested":{"\ud800":"\ud800","valid":"<>&"}}`)
	for name, domain := range map[string]numericJSONDomain{"property": convert.PropertyJSON, "input": convert.CypherInputJSON} {
		t.Run(name, func(t *testing.T) {
			var value map[string]interface{}
			if err := domain.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			if value["n"] != int64(9007199254740993) || value["f"] != float64(1) {
				t.Fatalf("number kind/value changed: %#v", value)
			}
			nested := value["nested"].(map[string]interface{})
			if nested[unit] != unit {
				t.Fatal("escaped UTF16 key/value identity changed at numeric admission")
			}
			encoded, err := domain.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			generic, err := decodeNumericJSONFixture(encoded)
			if err != nil {
				t.Fatal(err)
			}
			numbers := generic.(map[string]any)
			if numbers["n"] != json.Number("9007199254740993") || numbers["f"] != json.Number("1.0") {
				t.Fatalf("numeric wire identity changed: %s", encoded)
			}
			var restored map[string]interface{}
			if err := domain.Unmarshal(encoded, &restored); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(value, restored) {
				t.Fatal("combined numeric/string round trip changed values")
			}
		})
	}
}

type losslessCustomValue struct{}

func (losslessCustomValue) MarshalJSON() ([]byte, error) {
	return []byte(`{"\ud800":"\ud800","n":9007199254740993,"f":1.0}`), nil
}

func TestNumericDomainCustomEnvelopeUsesLosslessStringOwner(t *testing.T) {
	encoded, err := convert.PropertyJSON.Marshal(map[string]any{"custom": losslessCustomValue{}, "f": float64(1)})
	if err != nil {
		t.Fatal(err)
	}
	value, err := decodeNumericJSONFixture(encoded)
	if err != nil {
		t.Fatal(err)
	}
	root := value.(map[string]any)
	custom := root["custom"].(map[string]any)
	if !bytes.Contains(encoded, []byte(`"\ud800":"\ud800"`)) || custom["n"] != json.Number("9007199254740993") || root["f"] != json.Number("1.0") {
		t.Fatal("custom and numeric writers did not preserve the same JSON identities")
	}
}

// This fixture reader checks number lexemes only. String identity is checked
// through the public domain and escaped wire bytes above, not this stdlib reader.
func decodeNumericJSONFixture(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	err := decoder.Decode(&value)
	return value, err
}
