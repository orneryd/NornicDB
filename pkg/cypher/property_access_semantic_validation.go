package cypher

import (
	"fmt"
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
func validateStaticPropertyAccessTypes(cypher string) error {
	return validatePropertyAccessTypes(cypher, nil)
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
	if len(params) == 0 || (strings.IndexByte(cypher, '.') < 0 && strings.IndexByte(cypher, '{') < 0) || !parameterMayRejectPropertyAccess(cypher, params) {
		return nil
	}
	return validatePropertyAccessTypes(cypher, params)
}

// validatePropertyAccessTypes walks the clauses, tracking the static types of
// the variables WITH and UNWIND bind, and checks every property access.
func validatePropertyAccessTypes(cypher string, params map[string]interface{}) error {
	return validatePropertyAccessClauses(cypher, nil, params)
}

// validatePropertyAccessClauses is validatePropertyAccessTypes for a
// statement or a subquery body that sees the outer variables' types. The
// outer map is never modified.
func validatePropertyAccessClauses(cypher string, outer map[string]string, params map[string]interface{}) error {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	types := outer
	for _, clause := range clauses {
		text := clause.text
		switch clause.kind {
		case pipelineClauseWith, pipelineClauseReturn:
			keyword := "WITH"
			if clause.kind == pipelineClauseReturn {
				keyword = "RETURN"
			}
			body, tail := projectionSemanticBodyAndTail(text, keyword)
			if err := checkExpressionPropertyAccesses(body, types, params); err != nil {
				return err
			}
			next := types
			if clause.kind == pipelineClauseWith {
				next = map[string]string{}
				for _, raw := range splitTopLevelComma(body) {
					expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
					if expression == "*" {
						for variable, typeName := range types {
							next[variable] = typeName
						}
						continue
					}
					name := alias
					if name == "" {
						name = simpleSemanticIdentifier(expression)
					}
					if name == "" {
						continue
					}
					name = normalizeProjectionColumnName(name)
					if _, imported := outer[name]; imported && simpleSemanticIdentifier(expression) != name {
						return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							fmt.Sprintf("Variable `%s` already declared in outer scope", name))
					}
					next[name] = propertyAccessExpressionType(expression, types, params)
				}
			}
			// WHERE and ORDER BY see the projection's names; ORDER BY also
			// sees the names it replaced.
			if tail != "" {
				visible := make(map[string]string, len(types)+len(next))
				for variable, typeName := range types {
					visible[variable] = typeName
				}
				for variable, typeName := range next {
					visible[variable] = typeName
				}
				if err := checkExpressionPropertyAccesses(tail, visible, params); err != nil {
					return err
				}
			}
			types = next
		case pipelineClauseUnwind:
			body := strings.TrimSpace(text[len("UNWIND"):])
			as := findKeywordIndexInContext(body, "AS")
			if as < 0 {
				continue
			}
			if err := checkExpressionPropertyAccesses(body[:as], types, params); err != nil {
				return err
			}
			variable := normalizeProjectionColumnName(strings.TrimSpace(body[as+len("AS"):]))
			if _, imported := outer[variable]; imported {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
					fmt.Sprintf("Variable `%s` already declared in outer scope", variable))
			}
			element := ""
			listType := propertyAccessExpressionType(body[:as], types, params)
			if strings.HasPrefix(listType, "List<") && strings.HasSuffix(listType, ">") && !strings.Contains(listType, ",") {
				if inner := listType[len("List<") : len(listType)-1]; inner != "T" {
					element = inner
				}
			}
			if _, known := types[variable]; known || element != "" {
				next := make(map[string]string, len(types)+1)
				for name, typeName := range types {
					next[name] = typeName
				}
				delete(next, variable)
				if element != "" {
					next[variable] = element
				}
				types = next
			}
		default:
			if err := checkPropertyAccesses(text, types, params); err != nil {
				return err
			}
		}
	}
	return nil
}

// projectionSemanticBody is the projection items of a WITH / RETURN clause.
func projectionSemanticBody(clause, keyword string) string {
	body, _ := projectionSemanticBodyAndTail(clause, keyword)
	return body
}

// projectionSemanticBodyAndTail splits a WITH / RETURN clause into its
// projection items and the WHERE / ORDER BY / SKIP / LIMIT that follow.
func projectionSemanticBodyAndTail(clause, keyword string) (string, string) {
	body := strings.TrimSpace(clause[len(keyword):])
	body, _ = cutDistinct(body)
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	return strings.TrimSpace(body[:end]), body[end:]
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
// value for a property access; "" for null, a map, a Float (checked at run
// time) and values whose type isn't a Cypher literal type.
func parameterPropertyAccessType(value interface{}) string {
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

// checkPropertyAccesses checks each property access in text whose base is a
// string literal, a parameter or a variable of a known static type. A list
// comprehension, quantifier (any / all / none / single) or reduce is checked
// with its own variables shadowing the scope; a pattern comprehension and a
// subquery body (EXISTS / COUNT / COLLECT / CALL { … }) see the outer
// variables' types.
func checkPropertyAccesses(text string, types map[string]string, params map[string]interface{}) error {
	return scanPropertyAccesses(text, types, params, false)
}

// checkExpressionPropertyAccesses is checkPropertyAccesses for text that
// holds only expressions (projection items, WHERE, ORDER BY, an UNWIND list),
// where name { … } is a map projection: Neo4j's "Type mismatch: expected
// Map, Node or Relationship but was Integer" for a base without properties.
func checkExpressionPropertyAccesses(text string, types map[string]string, params map[string]interface{}) error {
	return scanPropertyAccesses(text, types, params, true)
}

// scanPropertyAccesses checks the property accesses (and, in expression
// text, the map projections) of text; see checkPropertyAccesses.
func scanPropertyAccesses(text string, types map[string]string, params map[string]interface{}, expressions bool) error {
	if strings.IndexByte(text, '.') < 0 && strings.IndexByte(text, '{') < 0 {
		return nil
	}
	for index := 0; index < len(text); index++ {
		switch c := text[index]; {
		case c == '\'' || c == '"':
			end := skipQuotedSemanticText(text, index)
			if propertyAccessFollows(text, end) {
				if err := propertyAccessMismatch("String", ""); err != nil {
					return err
				}
			}
			index = end - 1
		case c == '`':
			if _, next, ok := scanSymbolicName(text, index); ok {
				index = next - 1
			}
		case c == '[':
			end := findMatchingDelimiter(text, index, '[', ']')
			if end < 0 {
				return nil
			}
			inner := text[index+1 : end]
			if pattern, projection, isPattern := splitPatternComprehension(text[index : end+1]); isPattern {
				// The pattern's own variables are nodes, relationships or
				// paths; outer variables keep their types inside.
				if err := checkPropertyAccesses(pattern, types, params); err != nil {
					return err
				}
				if err := checkExpressionPropertyAccesses(projection, types, params); err != nil {
					return err
				}
				index = end
				continue
			}
			if variable, list, predicate, projection, comprehension := parseListComprehension(inner); comprehension {
				if err := checkExpressionPropertyAccesses(list, types, params); err != nil {
					return err
				}
				inside := shadowPropertyAccessTypes(types, variable)
				if err := checkExpressionPropertyAccesses(predicate, inside, params); err != nil {
					return err
				}
				if err := checkExpressionPropertyAccesses(projection, inside, params); err != nil {
					return err
				}
				index = end
			}
		case c == '{':
			if index > 0 && isSubqueryBrace(text, index) {
				end := findMatchingDelimiter(text, index, '{', '}')
				if end < 0 {
					return nil
				}
				// A subquery body sees the outer variables.
				if err := validatePropertyAccessClauses(text[index+1:end], types, params); err != nil {
					return err
				}
				index = end
			}
		case c == '$':
			name, next, ok := scanIdentifierToken(text, index+1)
			if !ok {
				continue
			}
			if params != nil && propertyAccessFollows(text, next) && !namespacedCallFollows(text, next) {
				if value, bound := params[name]; bound {
					if err := propertyAccessMismatch(parameterPropertyAccessType(value), name); err != nil {
						return err
					}
				}
			}
			index = next - 1
		case isIdentifierStart(c):
			if index > 0 && (isIdentifierPart(text[index-1]) || text[index-1] == '.' || text[index-1] == '$') {
				continue
			}
			name, next, _ := scanIdentifierToken(text, index)
			if after := skipSpaces(text, next); after < len(text) && text[after] == '(' && isQuantifierOrReduceFunction(name) {
				end := findMatchingDelimiter(text, after, '(', ')')
				if end < 0 {
					return nil
				}
				if err := checkBindingFunctionPropertyAccesses(name, text[after+1:end], types, params); err != nil {
					return err
				}
				index = end
				continue
			}
			if propertyAccessFollows(text, next) && !namespacedCallFollows(text, next) {
				if err := propertyAccessMismatch(types[name], ""); err != nil {
					return err
				}
			}
			if after := skipSpaces(text, next); expressions && after < len(text) && text[after] == '{' && rejectsPropertyAccess(types[name]) && !isSubqueryBrace(text, after) {
				return typeNameMismatchError("Map, Node or Relationship", types[name])
			}
			index = next - 1
		}
	}
	return nil
}

// checkBindingFunctionPropertyAccesses checks any / all / none / single
// (x IN list WHERE predicate) and reduce(acc = init, x IN list | expression),
// whose variables shadow the scope.
func checkBindingFunctionPropertyAccesses(function, arguments string, types map[string]string, params map[string]interface{}) error {
	if strings.EqualFold(function, "reduce") {
		parts := splitTopLevelComma(arguments)
		if len(parts) != 2 {
			return nil
		}
		accumulator, initial, found := strings.Cut(parts[0], "=")
		if !found {
			return nil
		}
		if err := checkExpressionPropertyAccesses(initial, types, params); err != nil {
			return err
		}
		variable, list, _, projection, ok := parseListComprehension(parts[1])
		if !ok {
			return nil
		}
		if err := checkExpressionPropertyAccesses(list, types, params); err != nil {
			return err
		}
		return checkExpressionPropertyAccesses(projection, shadowPropertyAccessTypes(types, variable, strings.TrimSpace(accumulator)), params)
	}
	variable, list, predicate, _, ok := parseListComprehension(arguments)
	if !ok {
		return nil
	}
	if err := checkExpressionPropertyAccesses(list, types, params); err != nil {
		return err
	}
	return checkExpressionPropertyAccesses(predicate, shadowPropertyAccessTypes(types, variable), params)
}

// shadowPropertyAccessTypes is types without the variables an inner scope
// binds.
func shadowPropertyAccessTypes(types map[string]string, variables ...string) map[string]string {
	shadowed := false
	for _, variable := range variables {
		if _, known := types[normalizeProjectionColumnName(variable)]; known {
			shadowed = true
		}
	}
	if !shadowed {
		return types
	}
	inner := make(map[string]string, len(types))
	for name, typeName := range types {
		inner[name] = typeName
	}
	for _, variable := range variables {
		delete(inner, normalizeProjectionColumnName(variable))
	}
	return inner
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
	return next < len(text) && (isIdentifierStart(text[next]) || text[next] == '`')
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
	before := index - 1
	for before >= 0 && isWhitespace(text[before]) {
		before--
	}
	start := before
	for start >= 0 && isIdentifierPart(text[start]) {
		start--
	}
	switch upperASCII(text[start+1 : before+1]) {
	case "EXISTS", "COUNT", "COLLECT", "CALL":
		return true
	}
	return false
}

// parameterMayRejectPropertyAccess reports whether a parameter whose value a
// property access rejects is accessed ($m.a) or projected ($m AS m) in
// cypher.
func parameterMayRejectPropertyAccess(cypher string, params map[string]interface{}) bool {
	for index := 0; index < len(cypher); index++ {
		switch cypher[index] {
		case '\'', '"':
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
		accessed := propertyAccessFollows(cypher, next)
		projected := after+2 <= len(cypher) && strings.EqualFold(cypher[after:after+2], "AS") && (after+2 == len(cypher) || !isIdentifierPart(cypher[after+2]))
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
