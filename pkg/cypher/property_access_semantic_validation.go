package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"reflect"
	"strings"
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

// validateStaticPropertyAccessParameters is validateStaticPropertyAccessTypes
// with the statement's parameter values as static types, as Neo4j compiles a
// statement with its parameters: RETURN $m.a for m = 5 is "Type mismatch for
// parameter 'm': … but was Integer", and WITH $m AS m RETURN m.a names the
// variable's type without the parameter. Neo4j checks a Float parameter only
// at run time ("Type mismatch: expected a map but was Double(1.5)"), and so
// does this. It parses the statement only when a parameter that a property
// access would reject is accessed ($m.a) or projected ($m AS m), so
// parameterized statements on hot paths cost one scan.
func validateStaticPropertyAccessParameters(ctx context.Context, cypher string, params map[string]interface{}) error {
	if len(params) == 0 || !strings.ContainsAny(cypher, ".{[") || !parameterMayRejectPropertyAccess(cypher, params) {
		return nil
	}
	return validatePropertyAccessTypes(ctx, cypher, params)
}

// validatePropertyAccessTypes walks the clauses, tracking the static types of
// the variables WITH and UNWIND bind, and checks every property access.
func validatePropertyAccessTypes(ctx context.Context, cypher string, params map[string]interface{}) error {
	return validatePropertyAccessClauses(ctx, cypher, nil, params)
}

// validatePropertyAccessClauses is validatePropertyAccessTypes for a
// statement or a subquery body that sees the outer variables' types. The
// outer map is never modified. Declaration validity belongs to the shared
// semantic scope walk; this visitor only tracks property receiver types.
func validatePropertyAccessClauses(ctx context.Context, cypher string, outer map[string]string, params map[string]interface{}) error {
	input := make(typeScope, len(outer))
	for name, kind := range outer {
		input[name] = typedOperand(kind).proved()
	}
	_, _, err := (&StorageExecutor{}).enrichBoundTypes(withParams(ctx, params), cypher, input, semanticBodyFromContext(ctx), "", cypherVersionFromContext(ctx) == "25")
	return err
}

// projectionSemanticBody is the projection items of a WITH / RETURN clause.
func projectionSemanticBody(clause, keyword string) string {
	body, _ := projectionSemanticBodyAndTail(clause, keyword)
	return body
}

// projectionSemanticBodyAndTail splits a WITH / RETURN clause into its
// projection items (after DISTINCT) and the WHERE / ORDER BY / SKIP / LIMIT
// that follow. The clause is scanned with its keyword, which tells a
// keyword-named first item from a clause (WITH where WHERE where = 3, #894).
func projectionSemanticBodyAndTail(clause, keyword string) (string, string) {
	end := len(clause)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(clause, suffix); index >= len(keyword) && index < end {
			end = index
		}
	}
	body, _ := cutDistinct(clause[len(keyword):end])
	return body, clause[end:]
}

// propertyAccessExpressionType is the static type of a projected or unwound
// expression: a parameter's (when checking with parameter values), the type
// of the variable it copies, or the operator check's. A list comprehension is a
// list of a type this check doesn't infer (List<T>). "" is unknown.
func propertyAccessExpressionType(ctx context.Context, expression string, types map[string]string, params map[string]interface{}) string {
	return (staticTypeScope{values: types, params: params}).parameterExpressionType(ctx, expression)
}

// parameterExpressionType is the one reader of request-parameter values. It
// keeps the caller's whole typed environment: a typed program's atom, alone
// or inside a larger expression, is never read as a request parameter.
func (scope staticTypeScope) parameterExpressionType(ctx context.Context, expression string) string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return ""
	}
	if operand, known := scope.typeParameters[expression]; known {
		return operand.typeName()
	}
	if expression[0] == '$' && simpleSemanticIdentifier(strings.TrimSpace(expression[1:])) != "" {
		if scope.params == nil {
			return ""
		}
		value, bound := scope.params[strings.TrimSpace(expression[1:])]
		if !bound {
			return ""
		}
		return parameterPropertyAccessType(value)
	}
	if variable := simpleSemanticIdentifier(expression); variable != "" {
		return scope.values[normalizeProjectionColumnName(variable)]
	}
	// Every other expression (CASE, a computed list, a call, arithmetic) is
	// typed by the static operand owner, which keeps a union's alternatives.
	// Only the parameter values are left out; the typed environment stays.
	rest := scope
	rest.params = nil
	if typeName := rest.staticExpressionType(ctx, expression); typeName != "" {
		return typeName
	}
	if _, list := stripEnclosingRowDelimiter(expression, '[', ']'); list {
		return "List<T>"
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

// hasPropertiesTypeName reports one static type a property access reads.
func hasPropertiesTypeName(typeName string) bool { return propertyAccessStaticTypes[typeName] }

// rejectsPropertyAccess reports whether a known static type has no
// properties: the operand owner decides, so a union is rejected only when
// none of its alternatives has properties.
func rejectsPropertyAccess(typeName string) bool {
	return typeName != "" && typedOperand(typeName).excludes(hasPropertiesTypeName)
}

// checkPropertyAccesses checks each property access in text whose base is a
// string literal, a parameter or a variable of a known static type. A list
// comprehension, quantifier (any / all / none / single) or reduce is checked
// with its own variables shadowing the scope; a pattern comprehension and a
// subquery body (EXISTS / COUNT / COLLECT / CALL { … }) see the outer
// variables' types.
func checkPropertyAccesses(ctx context.Context, text string, types map[string]string, params map[string]interface{}) error {
	return scanPropertyAccesses(ctx, text, types, params, false)
}

// checkExpressionPropertyAccesses is checkPropertyAccesses for text that
// holds only expressions (projection items, WHERE, ORDER BY, an UNWIND list),
// where name { … } is a map projection: Neo4j's "Type mismatch: expected
// Map, Node or Relationship but was Integer" for a base without properties.
func checkExpressionPropertyAccesses(ctx context.Context, text string, types map[string]string, params map[string]interface{}) error {
	if params != nil {
		if err := validateStaticPropertySubscripts(ctx, text, staticTypeScope{values: types, params: params}); err != nil {
			return err
		}
	}
	return scanPropertyAccesses(ctx, text, types, params, true)
}

// scanPropertyAccesses checks the property accesses (and, in expression
// text, the map projections) of text; see checkPropertyAccesses.
func scanPropertyAccesses(ctx context.Context, text string, types map[string]string, params map[string]interface{}, expressions bool) error {
	braces := newSubqueryBraceScanner(text, 0)
	if strings.IndexByte(text, '.') < 0 && strings.IndexByte(text, '{') < 0 {
		return nil
	}
	// A comprehension, quantifier or reduce is scanned in place: an access
	// whose base reads a local of that name (localVariableShadowsAt) isn't
	// the clause's variable, and a pattern comprehension or subquery body
	// sees the outer types without the names bound around it.
	var locals *localReader
	local := func() *localReader {
		if locals == nil {
			reader := newLocalReader(text)
			locals = &reader
		}
		return locals
	}
	for index := 0; index < len(text); index++ {
		switch c := text[index]; {
		case isCypherStringQuoteByte(c):
			end := skipQuotedSemanticText(text, index)
			if propertyAccessFollows(text, end) {
				if err := propertyAccessMismatch("String", ""); err != nil {
					return err
				}
			}
			index = end - 1
		case isCypherBacktickByte(c):
			if _, next, ok := scanSymbolicName(text, index); ok {
				index = next - 1
			}
		case c == '[':
			end := cyphertext.FindMatchingDelimiter(text, index, '[', ']', cyphertext.OpeningDelimiter)
			if end < 0 {
				return nil
			}
			if pattern, projection, isPattern := splitPatternComprehension(text[index : end+1]); isPattern {
				// The pattern's own variables are nodes, relationships or
				// paths; outer variables keep their types inside, but for
				// the names bound around it.
				inside := local().withoutTypes(types, index)
				if err := checkPropertyAccesses(ctx, pattern, inside, params); err != nil {
					return err
				}
				if err := checkExpressionPropertyAccesses(ctx, projection, inside, params); err != nil {
					return err
				}
				index = end
			}
		case c == '{':
			if index > 0 && braces.at(index) {
				end := cyphertext.FindMatchingDelimiter(text, index, '{', '}', cyphertext.OpeningDelimiter)
				if end < 0 {
					return nil
				}
				// A subquery body sees the outer variables.
				if err := validatePropertyAccessClauses(ctx, text[index+1:end], local().withoutTypes(types, index), params); err != nil {
					return err
				}
				index = end
			}
		case c == '$':
			name, ok := cyphertext.ScanIdentifier(text, index+1)
			if !ok {
				continue
			}
			if params != nil && propertyAccessFollows(text, name.End) && !namespacedCallFollows(text, name.End) {
				if value, bound := params[name.Value]; bound {
					if err := propertyAccessMismatch(parameterPropertyAccessType(value), name.Value); err != nil {
						return err
					}
				}
			}
			index = name.End - 1
		case cyphertext.NameStartAt(text, index):
			if index > 0 && (cyphertext.NamePartAt(text, index-1) || text[index-1] == '.' || text[index-1] == '$') {
				continue
			}
			name, _ := cyphertext.ScanIdentifier(text, index)
			typeName := types[name.Value]
			if typeName != "" && localVariableShadowsAt(text, index, name.Value) {
				typeName = "" // a local of that name, not the clause's
			}
			if propertyAccessFollows(text, name.End) && !namespacedCallFollows(text, name.End) {
				if err := propertyAccessMismatch(typeName, ""); err != nil {
					return err
				}
			}
			if after := skipSpaces(text, name.End); expressions && after < len(text) && text[after] == '{' && rejectsPropertyAccess(typeName) && !braces.at(after) {
				return typeNameMismatchError("Map, Node or Relationship", typeName)
			}
			index = name.End - 1
		}
	}
	return nil
}

// propertyAccessMismatch is the SyntaxError of a property access on a base of
// static type typeName, or nil when the type is unknown or has properties.
func propertyAccessMismatch(typeName, parameter string) error {
	if !rejectsPropertyAccess(typeName) {
		return nil
	}
	return operandMismatch(displayedOperand(typeName, typeName, parameter), propertyAccessExpectedTypes)
}

// propertyAccessFollows reports whether text[index:] starts with a property
// access (.key), not a range or a number.
func propertyAccessFollows(text string, index int) bool {
	index = queryGapEnd(text, index)
	if index >= len(text) || text[index] != '.' {
		return false
	}
	next := queryGapEnd(text, index+1)
	return next < len(text) && (cyphertext.NameStartAt(text, next) || isCypherBacktickByte(text[next]))
}

// namespacedCallFollows reports whether text[index:] continues a namespaced
// function or procedure name (apoc.coll.sum(…), db.labels()).
func namespacedCallFollows(text string, index int) bool {
	for {
		index = skipSpaces(text, index)
		if index >= len(text) || text[index] != '.' {
			return index < len(text) && text[index] == '('
		}
		next, ok := cyphertext.ScanIdentifier(text, skipSpaces(text, index+1))
		if !ok {
			return false
		}
		index = next.End
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
		name, ok := cyphertext.ScanIdentifier(cypher, index+1)
		if !ok {
			continue
		}
		index = name.End - 1
		after := queryGapEnd(cypher, name.End)
		accessed := propertyAccessFollows(cypher, name.End) || after < len(cypher) && cypher[after] == '['
		projected := after+2 <= len(cypher) && strings.EqualFold(cypher[after:after+2], "AS") && (after+2 == len(cypher) || !cyphertext.NamePartAt(cypher, after+2))
		if !accessed && !projected {
			continue
		}
		value, bound := params[name.Value]
		if !bound {
			continue
		}
		typeName := parameterPropertyAccessType(value)
		// UNWIND $m AS x binds the list's elements: only a list of strings
		// gives them a static type (String) that a property access rejects.
		if projected && !accessed && strings.EqualFold(rawWordBefore(cypher, dollar), "UNWIND") {
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
