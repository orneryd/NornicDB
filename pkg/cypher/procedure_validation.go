package cypher

import (
	"context"
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"maps"
	"strconv"
	"strings"
)

func explicitProcedureArgumentTexts(ctx context.Context, source string) []string {
	clause, ok := canonicalQueryClause(ctx, source)
	if !ok {
		return nil
	}
	return procedureArgumentTextsFromSyntax(clause)
}

func procedureArgumentTextsFromSyntax(clause cypherantlr.SyntaxNode) []string {
	arguments := clause.ChildRule(cypherantlr.CypherParserRULE_parenExpressionChain)
	if !arguments.Valid() {
		return nil
	}
	expressions := arguments.ChildRule(cypherantlr.CypherParserRULE_expressionChain)
	texts := []string{}
	if expressions.Valid() {
		for i := 0; i < expressions.ChildCount(); i++ {
			expression := expressions.Child(i)
			if expression.Rule() == cypherantlr.CypherParserRULE_expression {
				texts = append(texts, expression.Code())
			}
		}
	}
	return texts
}
func validateAndCoerceProcedureArguments(spec ProcedureSpec, args []interface{}, explicitTexts []string) ([]interface{}, error) {
	coerced := append([]interface{}(nil), args...)
	for index := range coerced {
		if index >= len(spec.Params) {
			break
		}
		if explicitTexts != nil && index < len(explicitTexts) && !isStaticallyTypedProcedureArgument(explicitTexts[index]) {
			continue
		}

		parameter := spec.Params[index]
		value, ok := coerceProcedureArgument(parameter, coerced[index])
		if !ok {
			return nil, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("procedure %s argument %s requires %s but received %s", spec.Name, parameter.Name, parameter.Type, cypherTypeSystemName(coerced[index])),
			)
		}
		coerced[index] = value
	}
	return coerced, nil
}

// staticProcedureArgumentTypeError is Neo4j's compile-time error for a
// procedure argument whose static type its parameter can't take: a literal, a
// variable bound to one by WITH, or a pattern variable (CALL
// db.resampleIndex(1.5), WITH 1.5 AS v CALL db.resampleIndex(v), MATCH (n)
// CALL db.resampleIndex(n)). Neo4j types a value bound by UNWIND only when the
// call runs, so those are left to the call. Only the parameter types the call
// itself checks (coerceProcedureArgument) are checked here: a NODE, MAP or
// LIST parameter also takes NornicDB's other forms (a node id, a label, a
// request's short form).
func staticProcedureArgumentTypeError(ctx context.Context, clause string, scope staticTypeScope, unwound map[string]struct{}, spec ProcedureSpec) error {
	texts := explicitProcedureArgumentTexts(ctx, clause)
	if len(texts) == 0 {
		return nil
	}
	if len(unwound) > 0 {
		// The whole binding is left to the call: its type name and its
		// derived operand alike.
		scope.values = maps.Clone(scope.values)
		scope.derived = maps.Clone(scope.derived)
		for name := range unwound {
			delete(scope.values, name)
			delete(scope.derived, name)
		}
	}
	for index, text := range texts {
		if index >= len(spec.Params) {
			break
		}
		typeName := scope.staticExpressionType(ctx, text)
		if typeName == "" {
			continue
		}
		if expected, accepted := procedureParameterAcceptsStaticType(spec.Params[index].Type, typeName); !accepted {
			return typeNameMismatchError(expected, procedureArgumentTypeName(typeName))
		}
	}
	return nil
}

// projectUnwoundValues returns which of the variables a WITH clause binds
// carry a type that came from UNWIND: an alias of such a variable, or the
// variable itself under WITH *.
func projectUnwoundValues(ctx context.Context, unwound map[string]struct{}, clause string) map[string]struct{} {
	if len(unwound) == 0 {
		return nil
	}
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")
	var projected map[string]struct{}
	keep := func(name string) {
		if projected == nil {
			projected = make(map[string]struct{})
		}
		projected[name] = struct{}{}
	}
	for _, raw := range splitTopLevelComma(body) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(raw), projectionAdmitted)
		expression, alias := itemSource.expression, itemSource.alias
		if expression == "*" {
			for name := range unwound {
				keep(name)
			}
			continue
		}
		variable := simpleSemanticIdentifier(expression)
		if _, fromUnwind := unwound[variable]; !fromUnwind || variable == "" {
			continue
		}
		if alias == "" {
			alias = variable
		}
		keep(alias)
	}
	return projected
}

// procedureParameterAcceptsStaticType reports whether a parameter of
// parameterType takes an argument of static type typeName (any one of its
// choices, for a type such as "Float, Integer or Number"), and the type
// Neo4j's error names as expected. An INTEGER is a FLOAT argument, as when the
// call runs.
func procedureParameterAcceptsStaticType(parameterType, typeName string) (string, bool) {
	var expected string
	var accepted []string
	switch upperASCII(strings.TrimSpace(parameterType)) {
	case "STRING":
		expected, accepted = "String", []string{"String"}
	case "BOOLEAN", "BOOL":
		expected, accepted = "Boolean", []string{"Boolean"}
	case "INTEGER":
		expected, accepted = "Integer", []string{"Integer", "Number"}
	case "FLOAT":
		expected, accepted = "Float", []string{"Float", "Integer", "Number"}
	case "NUMBER":
		expected, accepted = "Number", []string{"Number", "Float", "Integer"}
	default:
		return "", true
	}
	// The typing owner decides: one fitting alternative is enough.
	return expected, typeName != "" && !typedOperand(typeName).proved().excludes(typeNameIn(append(accepted, "Any")...))
}

func isStaticallyTypedProcedureArgument(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	lower := lowerASCII(text)
	if lower == "null" || lower == "true" || lower == "false" {
		return true
	}
	if (strings.HasPrefix(text, "'") && strings.HasSuffix(text, "'")) ||
		(strings.HasPrefix(text, "\"") && strings.HasSuffix(text, "\"")) {
		return true
	}
	if _, err := strconv.ParseInt(text, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(text, 64); err == nil {
		return true
	}
	return false
}

// coerceProcedureArgument checks and converts one argument against its
// parameter's type. null is a value of every type, as in Neo4j: the call
// goes ahead and the procedure decides what null means (its own failure is
// ProcedureCallFailed).
func coerceProcedureArgument(parameter ProcedureParam, value interface{}) (interface{}, bool) {
	if value == nil {
		return nil, true
	}

	switch upperASCII(strings.TrimSpace(parameter.Type)) {
	case "", "ANY":
		return value, true
	case "STRING":
		_, ok := value.(string)
		return value, ok
	case "BOOLEAN", "BOOL":
		_, ok := value.(bool)
		return value, ok
	case "INTEGER":
		return value, isIntegerProcedureValue(value)
	case "FLOAT":
		if converted, ok := procedureFloat64(value); ok {
			return converted, true
		}
		return value, false
	case "NUMBER":
		return value, isIntegerProcedureValue(value) || isFloatProcedureValue(value)
	default:
		return value, true
	}
}

func procedureFloat64(value interface{}) (float64, bool) {
	return valueconvert.NumericFloat64(value)
}

func isIntegerProcedureValue(value interface{}) bool {
	return valueconvert.IsInteger(value)
}

func isFloatProcedureValue(value interface{}) bool {
	_, ok := valueconvert.FloatValue(value)
	return ok
}

// unknownProcedureError is the shared planning/execution terminal for a missing procedure.
func unknownProcedureError(name string) error {
	return newSemanticError("Neo.ClientError.Procedure.ProcedureNotFound", "ProcedureNotFound", fmt.Sprintf("There is no procedure with the name `%s` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.", name))
}
