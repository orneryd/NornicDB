package cypher

import (
	"bytes"
	"fmt"
	"strings"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// apocConvertToJSON is apoc.convert.toJson(value): value as JSON text, as
// APOC (Jackson) writes it: <, > and & as themselves rather than the \u
// escapes encoding/json writes for HTML by default,
// and U+2028 and U+2029 as themselves too, which encoding/json always
// escapes as JavaScript line terminators. Control characters stay
// escaped. ok is false for a value JSON can't hold.
func apocConvertToJSON(value interface{}) (string, bool) {
	var buffer bytes.Buffer
	encoder := valueconvert.PropertyJSON.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", false
	}
	return unescapeLineSeparators(strings.TrimSuffix(buffer.String(), "\n")), true
}

// unescapeLineSeparators writes encoding/json's \u2028 and \u2029 escapes
// as the characters. An escape is read pairwise, so an escaped backslash
// followed by the text u2028 stays as it is.
func unescapeLineSeparators(encoded string) string {
	if !strings.Contains(encoded, `\u202`) {
		return encoded
	}
	var out strings.Builder
	out.Grow(len(encoded))
	for index := 0; index < len(encoded); index++ {
		if encoded[index] != '\\' || index+1 >= len(encoded) {
			out.WriteByte(encoded[index])
			continue
		}
		if rest := encoded[index:]; strings.HasPrefix(rest, `\u2028`) || strings.HasPrefix(rest, `\u2029`) {
			out.WriteRune(rune(0x2028 + int(rest[5]-'8')))
			index += 5
			continue
		}
		out.WriteString(encoded[index : index+2])
		index++
	}
	return out.String()
}

// JSONNumbers converts the json.Number values of a value decoded with
// UseNumber to Cypher numbers, as Neo4j's HTTP API reads JSON: a number
// without '.', 'e' or 'E' that fits int64 is an INTEGER, a larger one is
// rejected, any other is a FLOAT (+/-Infinity when out of range). Lists and
// maps are converted in place.
func JSONNumbers(value interface{}) (interface{}, error) {
	return valueconvert.CypherInputJSON.Numbers(value)
}

// apocConvertFromJSON is apoc.convert.fromJsonMap / fromJsonList(text): the
// JSON text's map or list with JSONNumbers' numbers; ok is false for a
// value that isn't text, text that isn't JSON, or JSON of the other kind.
func apocConvertFromJSON(wantMap bool, value interface{}) (interface{}, error) {
	// APOC 5.26.30 JsonUtil.parse: null/empty input and decoded JSON null
	// succeed with NULL; malformed JSON and the wrong root type throw.
	if cypherfn.RuntimeValue(value) == nil {
		return nil, nil
	}
	text, isText := value.(string)
	kind := "List"
	if wantMap {
		kind = "Map"
	}
	invalid := func(cause error) (interface{}, error) {
		return nil, newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "InvalidArgumentValue", fmt.Sprintf("Can't convert %v to %s with path : %v", value, kind, cause))
	}
	if !isText {
		return invalid(fmt.Errorf("expected String"))
	}
	if text == "" {
		return nil, nil
	}
	decoder := valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(text))
	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return invalid(err)
	}
	if decoded == nil {
		return nil, nil
	}
	if wantMap {
		if result, ok := decoded.(map[string]interface{}); ok {
			return result, nil
		}
	} else {
		if result, ok := decoded.([]interface{}); ok {
			return result, nil
		}
	}
	return invalid(fmt.Errorf("unexpected JSON root type"))
}
