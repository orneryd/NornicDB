package cypher

import (
	"reflect"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// conversionFunctionNames and conversionFunctionInputs word a conversion
// function's run-time TypeError as Neo4j does: "Invalid input for function
// 'toInteger()': Expected a String, Float, Integer or Boolean, got: …".
var conversionFunctionNames = map[string]string{
	"tointeger": "toInteger", "toint": "toInteger", "tofloat": "toFloat", "toboolean": "toBoolean", "tostring": "toString",
}

var conversionFunctionInputs = map[string]string{
	"tointeger": "a String, Float, Integer or Boolean",
	"toint":     "a String, Float, Integer or Boolean",
	"tofloat":   "a String, Float or Integer",
	"toboolean": "a Boolean, Integer or String",
	"tostring":  "a String, Float, Integer, Boolean, Temporal or Duration",
}

func validConversionArgument(function string, value interface{}, cypher25 bool) bool {
	switch function {
	case "toboolean":
		switch value.(type) {
		case bool, string:
			return true
		}
		number, numeric := valueconvert.ReadNumber(value)
		return numeric && number.IsInteger()
	case "tostring":
		if _, numeric := valueconvert.ReadNumber(value); numeric {
			return true
		}
		// A Cypher 25 statement writes lists, maps and graph entities too
		// (cypher25ValueText).
		if cypher25 {
			_, ok := cypher25ValueText(value)
			return ok
		}
		switch value.(type) {
		case string, bool:
			return true
		case *storage.Node, *storage.Edge:
			return false
		}
		kind := reflect.TypeOf(value).Kind()
		return kind != reflect.Slice && kind != reflect.Array && kind != reflect.Map
	case "tointeger", "toint", "tofloat":
		if _, boolean := value.(bool); boolean {
			return function != "tofloat"
		}
		switch value.(type) {
		case string:
			return true
		}
		_, numeric := valueconvert.ReadNumber(value)
		return numeric
	default:
		return true
	}
}
