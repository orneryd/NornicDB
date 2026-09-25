package cypher

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validatePipelineConversionArguments enforces the runtime type contract for
// conversion functions before a projection is materialized. Validation walks
// list comprehensions with their typed loop bindings, so the streaming row
// executor does not lose type errors inside nested expressions.
func (e *StorageExecutor) validatePipelineConversionArguments(rows []pipelineRow, clause, keyword string) error {
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	if body == "" || body == "*" {
		return nil
	}
	for _, item := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		for _, row := range rows {
			if err := e.validateRowConversionArguments(expression, row); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateRowConversionArguments(expression string, row pipelineRow) error {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		return e.validateRowConversionArguments(inner, row)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expression, '[', ']'); enclosed {
		variable, listExpression, predicate, projection, comprehension := parseListComprehension(inner)
		if comprehension {
			if err := e.validateRowConversionArguments(listExpression, row); err != nil {
				return err
			}
			listValue, evaluated := e.evaluateRowExpression(listExpression, row)
			if !evaluated || listValue == nil {
				return nil
			}
			valueType := reflect.TypeOf(listValue)
			if valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array {
				return nil
			}
			for _, item := range toAnySlice(listValue) {
				scope := make(pipelineRow, len(row)+1)
				for name, value := range row {
					scope[name] = value
				}
				scope[variable] = item
				if predicate != "" {
					if err := e.validateRowConversionArguments(predicate, scope); err != nil {
						return err
					}
				}
				if projection != "" {
					if err := e.validateRowConversionArguments(projection, scope); err != nil {
						return err
					}
				}
			}
			return nil
		}
		for _, item := range splitTopLevelComma(inner) {
			if err := e.validateRowConversionArguments(item, row); err != nil {
				return err
			}
		}
		return nil
	}
	if function, argument, ok := parseFunctionCallWS(expression); ok {
		for _, item := range splitTopLevelComma(argument) {
			if err := e.validateRowConversionArguments(item, row); err != nil {
				return err
			}
		}
		name := strings.ToLower(function)
		if name != "toboolean" && name != "tointeger" && name != "toint" && name != "tofloat" && name != "tostring" {
			return nil
		}
		value, evaluated := e.evaluateRowExpression(argument, row)
		if !evaluated || value == nil || validConversionArgument(name, value) {
			return nil
		}
		return newSemanticError(
			"Neo.ClientError.Statement.TypeError",
			"InvalidArgumentValue",
			fmt.Sprintf("Invalid input for function '%s()': Expected %s, got: %s", conversionFunctionNames[name], conversionFunctionInputs[name], neo4jValueRepr(value)),
		)
	}
	return nil
}

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

func validConversionArgument(function string, value interface{}) bool {
	switch function {
	case "toboolean":
		switch value.(type) {
		case bool, string:
			return true
		}
		return false
	case "tostring":
		switch value.(type) {
		case string, bool,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return true
		case *storage.Node, *storage.Edge:
			return false
		}
		kind := reflect.TypeOf(value).Kind()
		return kind != reflect.Slice && kind != reflect.Array && kind != reflect.Map
	case "tointeger", "toint", "tofloat":
		switch value.(type) {
		case string,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return true
		}
		return false
	default:
		return true
	}
}
