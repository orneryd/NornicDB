package cypher

import (
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// propertyAccessExpectedTypes is what Neo4j names as the types a property
// access (x.key) accepts, in "Type mismatch: expected … but was …".
const propertyAccessExpectedTypes = "Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime"

// propertyAccessStaticTypes are the static types a property access accepts.
var propertyAccessStaticTypes = map[string]bool{
	"Map": true, "Node": true, "Relationship": true, "Point": true, "Duration": true,
	"Date": true, "Time": true, "LocalTime": true, "LocalDateTime": true, "DateTime": true,
	"Map, Node or Relationship": true,
}

// validateStaticPropertyAccessTypes rejects, when the statement compiles, a
// property access whose base has a static type without properties: a string,
// number, boolean or list literal ('x'.y), or a variable a WITH or UNWIND
// bound to one (WITH 5 AS s RETURN s.x), wherever the access is (projections,
// WHERE, ORDER BY, pattern properties, function arguments, CASE, list and
// pattern comprehensions, subquery bodies). Neo4j's SyntaxError is "Type
// mismatch: expected Map, Node, Relationship, … but was Integer"; a map
// projection of such a base (s {.a}) is "expected Map, Node or Relationship". A base whose type depends on the data (a
// property, a function result) is checked when it is evaluated, with Neo4j's
// TypeError "Type mismatch: expected a map but was Long(5)"
// (propertyAccessTypeError). Parameters are typed by
// validateStaticPropertyAccessParameters, per execution.
func validateStaticPropertyAccessTypes(cypher string, inputs ...*semanticStatementInput) error {
	return validatePropertyAccessTypes(cypher, nil, inputs...)
}

// validateStaticPropertyAccessParameters is validateStaticPropertyAccessTypes
// with the statement's parameter values as static types, as Neo4j compiles a
// statement with its parameters: RETURN $m.a for m = 5 is "Type mismatch for
// parameter 'm': … but was Integer", and WITH $m AS m RETURN m.a names the
// variable's type without the parameter. Neo4j checks a Float parameter only
// at run time ("Type mismatch: expected a map but was Double(1.5)"), and so
// does this. It parses the statement only when a parameter that a property
// access would reject is accessed ($m.a) or projected ($m AS m), so
// parameterized statements on hot paths cost one scan.
func validateStaticPropertyAccessParameters(cypher string, params map[string]interface{}) error {
	return (&StorageExecutor{}).validateInvocationParameterTypes(cypher, params, false)
}

// validatePropertyAccessTypes walks the clauses, tracking the static types of
// the variables WITH and UNWIND bind, and checks every property access.
func validatePropertyAccessTypes(cypher string, params map[string]interface{}, inputs ...*semanticStatementInput) error {
	return validatePropertyAccessClauses(cypher, nil, params, inputs...)
}

// validatePropertyAccessClauses is validatePropertyAccessTypes for a
// statement or a subquery body that sees the outer variables' types. The
// outer map is never modified. Declaration validity belongs to the shared
// semantic scope walk; this visitor only tracks property receiver types.
func validatePropertyAccessClauses(cypher string, outer map[string]string, params map[string]interface{}, inputs ...*semanticStatementInput) error {
	return propertyStatementAdmission(cypher, outer, params, inputs...)
}

// projectionSemanticBody is the projection items of a WITH / RETURN clause.
func projectionSemanticBody(clause, keyword string, syntax ...*fabric.QueryClause) string {
	body, _ := projectionSemanticBodyAndTail(clause, keyword, syntax...)
	return body
}

// projectionSemanticBodyAndTail splits a WITH / RETURN clause into its
// projection items (after DISTINCT) and the WHERE / ORDER BY / SKIP / LIMIT
// that follow. The clause is scanned with its keyword, which tells a
// keyword-named first item from a clause (WITH where WHERE where = 3, #894).
func projectionSemanticBodyAndTail(clause, keyword string, syntax ...*fabric.QueryClause) (string, string) {
	if len(syntax) > 0 && syntax[0] != nil && syntax[0].Projection != nil {
		return syntax[0].Projection.Text, syntax[0].Projection.Tail
	}
	return physicalProjectionBodyAndTail(clause, keyword)
}

// propertyAccessExpressionType is the static type of a projected or unwound
// expression: a literal's, a parameter's (when checking with parameter
// values), or the type of the variable it copies. A list comprehension is a
// list of a type this check doesn't infer (List<T>). "" is unknown.
func propertyAccessExpressionType(expression string, types map[string]string, params map[string]interface{}) string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return ""
	}
	if typeName := staticLiteralTypeName(expression); typeName != "" {
		return typeName
	}
	if _, list := stripEnclosingRowDelimiter(expression, '[', ']'); list {
		return "List<T>"
	}
	if mayContainArithmetic(expression) {
		if typeName := (staticTypeScope{values: types}).staticExpressionType(expression); typeName != "" {
			return typeName
		}
	}
	if expression[0] == '$' {
		if params == nil {
			return ""
		}
		value, bound := params[strings.TrimSpace(expression[1:])]
		if !bound {
			return ""
		}
		return parameterPropertyAccessType(value)
	}
	if variable := simpleSemanticIdentifier(expression); variable != "" {
		return types[normalizeProjectionColumnName(variable)]
	}
	return ""
}

// parameterPropertyAccessType is the static type Neo4j gives a parameter
// value for a property access; "" for null, a Float (checked at run
// time) and values whose type isn't a Cypher literal type.
func parameterPropertyAccessType(value interface{}) string {
	if _, isMap := toStringAnyMap(value); isMap {
		return "Map"
	}
	switch value.(type) {
	case float32, float64:
		return ""
	}
	return staticParameterOperand(value).display
}

// rejectsPropertyAccess reports whether a known static type has no
// properties.
func rejectsPropertyAccess(typeName string) bool {
	if typeName == "" {
		return false
	}
	for _, choice := range strings.Split(strings.ReplaceAll(typeName, " or ", ", "), ", ") {
		if propertyAccessStaticTypes[choice] {
			return false
		}
	}
	return true
}

// checkExpressionPropertyAccesses is a compatibility entrance for expression
// tests and adapters. It uses the same admitted frame as public statements;
// there is no second scanner or local-scope map to reconstruct here.
func checkExpressionPropertyAccesses(text string, types map[string]string, params map[string]interface{}) error {
	return propertyStatementAdmission("RETURN "+text, types, params)
}

// propertyAccessMismatch is the SyntaxError of a property access on a base of
// static type typeName, or nil when the type is unknown or has properties.
func propertyAccessMismatch(typeName, parameter string) error {
	if !rejectsPropertyAccess(typeName) {
		return nil
	}
	return operandMismatch(staticOperand{kind: typeName, display: typeName, parameter: parameter}, propertyAccessExpectedTypes)
}

// propertyAccessFollows reports whether text[index:] starts with a property
// access (.key), not a range or a number.
func propertyAccessFollows(text string, index int) bool {
	index = queryGapEnd(text, index)
	if index >= len(text) || text[index] != '.' {
		return false
	}
	next := queryGapEnd(text, index+1)
	return next < len(text) && (isIdentStartByte(text[next]) || isCypherBacktickByte(text[next]))
}

// namespacedCallFollows reports whether text[index:] continues a namespaced
// function or procedure name (apoc.coll.sum(…), db.labels()).
func namespacedCallFollows(text string, index int) bool {
	for {
		index = skipSpaces(text, index)
		if index >= len(text) || text[index] != '.' {
			return index < len(text) && text[index] == '('
		}
		_, next, ok := scanIdentifierToken(text, skipSpaces(text, index+1))
		if !ok {
			return false
		}
		index = next
	}
}

// isSubqueryBrace reports whether the { at text[index] opens a subquery body
// (EXISTS { … }, COUNT { … }, COLLECT { … }, CALL { … }) rather than a map.
func isSubqueryBrace(text string, index int) bool {
	return precedingSubqueryExpressionKeyword(text, index)
}

// parameterMayRejectPropertyAccess reports whether a parameter whose value a
// property access rejects is accessed ($m.a) or projected ($m AS m) in
// cypher.
func parameterMayRejectPropertyAccess(cypher string, params map[string]interface{}) bool {
	for index := 0; index < len(cypher); index++ {
		switch cypherStringLexicalByte(cypher[index]) {
		case cypherStringQuoteClass:
			index = skipQuotedSemanticText(cypher, index) - 1
			continue
		case '/':
			if end := queryCommentEnd(cypher, index); end >= 0 {
				index = end - 1
				continue
			}
		case '$':
		default:
			continue
		}
		dollar := index
		name, next, ok := scanIdentifierToken(cypher, index+1)
		if !ok {
			continue
		}
		index = next - 1
		after := queryGapEnd(cypher, next)
		accessed := propertyAccessFollows(cypher, next) || after < len(cypher) && cypher[after] == '['
		projected := after+2 <= len(cypher) && strings.EqualFold(cypher[after:after+2], "AS") && (after+2 == len(cypher) || !isIdentByte(cypher[after+2]))
		if !accessed && !projected {
			continue
		}
		value, bound := params[name]
		if !bound {
			continue
		}
		typeName := parameterPropertyAccessType(value)
		// UNWIND $m AS x binds the list's elements: only a list of strings
		// gives them a static type (String) that a property access rejects.
		if projected && !accessed && strings.EqualFold(previousWord(cypher, dollar), "UNWIND") {
			if typeName == "List<String>" {
				return true
			}
			continue
		}
		if rejectsPropertyAccess(typeName) {
			return true
		}
	}
	return false
}

// isAllStringList reports whether value is a non-empty list of strings,
// which Neo4j types as List<String>.
func isAllStringList(value interface{}) bool {
	switch list := value.(type) {
	case []string:
		return len(list) > 0
	case []interface{}:
		if len(list) == 0 {
			return false
		}
		for _, item := range list {
			if _, text := item.(string); !text {
				return false
			}
		}
		return true
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice || reflected.Len() == 0 {
		return false
	}
	return reflected.Type().Elem().Kind() == reflect.String
}
