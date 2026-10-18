// Typed parameter bindings for NornicDB Cypher.
//
// This file contains functions for handling Cypher query parameters.
// Parameters allow passing values into queries without string interpolation,
// providing both security (SQL injection prevention) and performance benefits.
//
// # Parameter Syntax
//
// Parameters are specified with $ prefix:
//
//	$name        - Simple parameter
//	$userId      - Camel case parameter
//	$count123    - Parameter with numbers
//
// # Usage Example
//
//	params := map[string]interface{}{
//	    "name": "Alice",
//	    "age":  30,
//	}
//	result, err := exec.Execute(ctx, "MATCH (p:Person {name: $name, age: $age}) RETURN p", params)
//
// # Supported Types
//
//	- string: Escaped and quoted ('value')
//	- int, int64, etc.: Numeric literals (42)
//	- float32, float64: Decimal literals (3.14)
//	- bool: true/false
//	- []interface{}: List literals ([1, 2, 3])
//	- map[string]interface{}: Map literals ({key: value})
//
// # ELI12
//
// Parameters are like fill-in-the-blanks in a story:
//
//	"Hello, my name is _____" + {name: "Alice"} = "Hello, my name is Alice"
//
// Instead of building the query string yourself (which can cause problems),
// you leave blanks ($name) and provide the values separately. The database
// fills in the blanks safely!
//
// # Neo4j Compatibility
//
// Parameter handling matches Neo4j behavior exactly:
//   - $param syntax
//   - Type conversion rules
//   - Escaping behavior

package cypher

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// getParamKeys returns the keys from a parameter map as a slice.
//
// # Parameters
//
//   - params: Map of parameter names to values
//
// # Returns
//
//   - A slice of parameter names
//
// # Example
//
//	params := map[string]interface{}{"name": "Alice", "age": 30}
//	keys := getParamKeys(params)  // ["name", "age"] (order may vary)
func getParamKeys(params map[string]interface{}) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// paramsKeyType is the key type for storing params in context.
// Using a custom type prevents collisions with other context values.
type paramsKeyType struct{}

// paramsKey is the context key for query parameters.
var paramsKey = paramsKeyType{}

// getParamsFromContext extracts parameters from context if present.
//
// Parameters are stored in context using paramsKey to avoid collisions.
// This is used internally by the query executor.
//
// # Parameters
//
//   - ctx: Context that may contain parameters
//
// # Returns
//
//   - Parameter map from context, or nil if not present
//
// # Example
//
//	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"id": 123})
//	params := getParamsFromContext(ctx)  // {"id": 123}
func getParamsFromContext(ctx context.Context) map[string]interface{} {
	if params, ok := ctx.Value(paramsKey).(map[string]interface{}); ok {
		return params
	}
	return nil
}

// withParams returns a child ctx with the given parameter map attached
// under paramsKey. Used by internal call sites that receive an explicit
// params map (binding-where eval, parameter substitution helpers) but
// need to make those params reachable from the recursive expression
// evaluator's getParamsFromContext lookup.
func withParams(ctx context.Context, params map[string]interface{}) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if params == nil {
		return ctx
	}
	// Fast path: if the same params are already on ctx, don't allocate.
	if existing, ok := ctx.Value(paramsKey).(map[string]interface{}); ok {
		if mapsEqualShallow(existing, params) {
			return ctx
		}
	}
	return withQueryParams(ctx, params)
}

// mapsEqualShallow does pointer-identity comparison on the same map header.
// This is intentional — we only want to skip allocation when the caller
// passed the exact same map that's already attached.
func mapsEqualShallow(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	// Compare addresses by writing both into reflect.Value or using a
	// no-op approach. Go doesn't expose map header equality, so a shallow
	// length check suffices for the fast-path optimisation; correctness
	// is unaffected if we always allocate.
	return false
}

// substituteParams keeps parameter references as typed bindings. Query entry
// normalizes values once; every clause resolves the original references.
func (e *StorageExecutor) substituteParams(cypher string, params map[string]interface{}) string {
	return cypher
}

// scanParameterReferences is the one scanner for $name parameter references
// in query text: it calls visit with the offsets of each reference's '$' and
// of its name. Text inside string literals ('…' and "…", with backslash and
// doubled-quote escapes) and backtick-quoted names is not a parameter
// reference (#701). A quoted parameter name ($`a b`) is made plain by
// canonicalizeQuotedVariables before any scan.
func scanParameterReferences(query string, visit func(dollar, start, end int)) {
	scanParameterReferencesWithCancellation(query, visit, nil)
}
func scanParameterReferencesWithCancellation(query string, visit func(dollar, start, end int), probe *pipelineCancellation) {
	if indexByteWithCancellation(query, '$', probe) < 0 {
		return
	}
	quoted := containsQuoteCharacterWithCancellation(query, probe)
	for i := 0; i < len(query); {
		if probe != nil && !probe.check() {
			return
		}
		next := nextUnquotedDollar(query, i, quoted)
		if next < 0 {
			return
		}
		if !isParameterStart(query, next) {
			i = next + 1
			continue
		}
		start := next + 1
		if start >= len(query) {
			return
		}
		first := query[start]
		if !isIdentStartByte(first) {
			i = start
			continue
		}
		end := start + 1
		for end < len(query) && isIdentByte(query[end]) {
			if probe != nil && !probe.check() {
				return
			}
			end++
		}
		if dynamicLabelStartsAt(query, next) {
			// $all(e), $any(e): a dynamic label, not a parameter.
			i = end
			continue
		}
		visit(next, start, end)
		if probe != nil && probe.err != nil {
			return
		}
		i = end
	}
}

// containsQuoteCharacter reports whether query has a quote character, so
// nextUnquotedDollar has quoted text to skip.
func containsQuoteCharacter(query string) bool {
	return strings.IndexByte(query, '\'') >= 0 || strings.IndexByte(query, '"') >= 0 || strings.IndexByte(query, '`') >= 0
}

func containsQuoteCharacterWithCancellation(query string, probe *pipelineCancellation) bool {
	if probe == nil {
		return containsQuoteCharacter(query)
	}
	return indexAnyWithCancellation(query, "'\"`", probe) >= 0
}

// nextUnquotedDollar is the offset of the first '$' at or after from that
// isn't inside a string literal or a backtick-quoted name, or -1. Without
// quote characters (quoted false) the single-byte search is much faster
// than IndexAny.
func nextUnquotedDollar(query string, from int, quoted bool) int {
	return nextUnquotedDollarWithCancellation(query, from, quoted, nil)
}
func nextUnquotedDollarWithCancellation(query string, from int, quoted bool, probe *pipelineCancellation) int {
	for from < len(query) {
		if probe != nil && !probe.check() {
			return -1
		}
		if !quoted {
			if next := indexByteWithCancellation(query[from:], '$', probe); next >= 0 {
				return from + next
			}
			return -1
		}
		next := indexAnyWithCancellation(query[from:], "$'\"`", probe)
		if next < 0 {
			return -1
		}
		next += from
		if query[next] == '$' {
			return next
		}
		from = skipCypherQuotedText(query, next, query[next])
	}
	return -1
}

// separateCypher25Parameters returns a Cypher 25 statement with a space
// before each '$' that follows a name's character (L$props, IN$list,
// LIMIT$n), and the rewrite that maps columns and messages back to the
// client's text; query and nil when there is none. In Cypher 25 a '$' always
// starts a parameter, so the text means L $props, as Neo4j 2026.09 reads it;
// the scanners, written for Cypher 5, where '$' goes on a name (a$b,
// isParameterStart), then read a parameter there too.
func separateCypher25Parameters(query string) (string, *queryRewrite) {
	return separateCypher25ParametersWithCancellation(query, nil)
}
func separateCypher25ParametersWithCancellation(query string, probe *pipelineCancellation) (string, *queryRewrite) {
	if indexByteWithCancellation(query, '$', probe) < 0 {
		return query, nil
	}
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	quoted := containsQuoteCharacterWithCancellation(query, probe)
	for i := nextUnquotedDollarWithCancellation(query, 0, quoted, probe); i >= 0; i = nextUnquotedDollarWithCancellation(query, i+1, quoted, probe) {
		if probe != nil && !probe.check() {
			return query, nil
		}
		if isParameterStart(query, i) {
			continue
		}
		if rewrite == nil {
			rewrite = &queryRewrite{original: query}
			if !reservePipelineString(&out, len(query)+4, probe) {
				return query, nil
			}
		}
		if !appendPipelineString(&out, query[last:i], probe) {
			return query, nil
		}
		canonStart := out.Len()
		if !appendPipelineString(&out, " $", probe) {
			return query, nil
		}
		rewrite.edits = appendPipelineItem(rewrite.edits, queryTextEdit{origStart: i, origEnd: i + 1, canonStart: canonStart, canonEnd: out.Len()}, probe)
		if probe != nil && probe.err != nil {
			return query, nil
		}
		last = i + 1
	}
	if rewrite == nil {
		return query, nil
	}
	if !appendPipelineString(&out, query[last:], probe) {
		return query, nil
	}
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// replaceParameterReferences calls replace for each parameter reference
// (scanParameterReferences) with its name and the byte after it (0 at the
// end), and writes the returned text in its place when ok is true, or keeps
// the reference.
func replaceParameterReferences(query string, replace func(name string, next byte) (string, bool)) string {
	return replaceParameterReferencesWithCancellation(query, replace, nil)
}
func replaceParameterReferencesWithCancellation(query string, replace func(name string, next byte) (string, bool), probe *pipelineCancellation) string {
	var result strings.Builder
	last := 0
	scanParameterReferencesWithCancellation(query, func(dollar, start, end int) {
		var following byte
		if end < len(query) {
			following = query[end]
		}
		replacement, ok := replace(query[start:end], following)
		if !ok {
			return
		}
		if last == 0 {
			if probe == nil {
				result.Grow(len(query))
			} else if !reservePipelineString(&result, min(len(query), 128), probe) {
				return
			}
		}
		if !appendPipelineString(&result, query[last:dollar], probe) || !appendPipelineString(&result, replacement, probe) {
			return
		}
		last = end
	}, probe)
	if probe != nil && probe.err != nil {
		return ""
	}
	if last == 0 {
		return query
	}
	if !appendPipelineString(&result, query[last:], probe) {
		return ""
	}
	return result.String()
}

// skipQuotedText returns the index just past the quoted text that starts at
// start (a ', " or ` character), or len(query) when it isn't closed. A
// backslash escapes the next character in a string literal; a doubled quote
// continues the quoted text.
func skipQuotedText(query string, start int) int {
	return skipCypherQuotedText(query, start, query[start])
}

// validateStatementParameters is Neo4j's ParameterMissing check: every
// parameter the statement references ($name, $0, $`quoted name`) outside
// string literals and comments must be supplied, and the error lists the
// missing names in order of first appearance ("Expected parameter(s): p, q").
// EXPLAIN doesn't run the statement and needs none, and the $names of a
// CREATE PROCEDURE (a NornicDB extension) declare the procedure's arguments.
// A statement without "$" costs one byte scan.
func validateStatementParameters(cypher string, params map[string]interface{}) error {
	if strings.IndexByte(cypher, '$') < 0 || startsWithKeywordFold(strings.TrimSpace(cypher), "EXPLAIN") || isCreateProcedureCommand(cypher) {
		return nil
	}
	missing := statementMissingParameters(cypher, func(name string) bool {
		_, supplied := params[name]
		return supplied
	})
	if len(missing) == 0 {
		return nil
	}
	return parameterMissingError(missing)
}

// statementMissingParameters lists, in order of first appearance, the
// parameters the statement references ($name, $0, $`quoted name`) outside
// string literals and comments that bound doesn't have. It is the one
// ParameterMissing scan (#657, #712): Execute calls it with the statement's,
// the shell's and the enclosing query's parameters.
func statementMissingParameters(cypher string, bound func(name string) bool) []string {
	var missing []string
	for index := 0; index < len(cypher); index++ {
		switch cypher[index] {
		case '\'', '"', '`':
			index = skipQuotedSemanticText(cypher, index) - 1
		case '/':
			if end := queryCommentEnd(cypher, index); end >= 0 {
				index = end - 1
			}
		case '$':
			if !isParameterStart(cypher, index) {
				continue
			}
			start := index + 1
			var name string
			if start < len(cypher) && cypher[start] == '`' {
				end := skipQuotedSemanticText(cypher, start)
				name = normalizePropertyKey(cypher[start:end])
				index = end - 1
			} else {
				end := start
				for end < len(cypher) && isIdentByte(cypher[end]) {
					end++
				}
				name = cypher[start:end]
				index = end - 1
			}
			if name == "" || dynamicLabelStartsAt(cypher, start-1) {
				// $(e), $all(e), $any(e): a dynamic label, not a parameter.
				continue
			}
			if bound(name) {
				continue
			}
			known := false
			for _, previous := range missing {
				known = known || previous == name
			}
			if !known {
				missing = append(missing, name)
			}
		}
	}
	return missing
}

// parameterMissingError is Neo4j's error for the parameters a statement
// references but wasn't given.
func parameterMissingError(names []string) error {
	return newSemanticError("Neo.ClientError.Statement.ParameterMissing", "MissingParameter", "Expected parameter(s): "+strings.Join(names, ", "))
}

// resolveDirectParamRef returns the typed parameter value for a literal
// "$name" reference, paired with true. Returns (nil, false) when the
// expression isn't a bare $param reference or the parameter isn't
// present in the request context. Used by SET / SET +=  / map-merge
// dispatchers to bypass the literal-stringification path that
// substituteParams uses for scalars; preserving the typed value here is
// what keeps []string, []float64, and map[string]any from widening to
// []interface{} / map[string]interface{} on the read-back path.
func resolveDirectParamRef(ctx context.Context, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "$") {
		return nil, false
	}
	name := strings.TrimSpace(expr[1:])
	if name == "" {
		return nil, false
	}
	// Reject anything beyond a single identifier — e.g. "$name + 1" or
	// "$name.foo" must keep going through the expression evaluator.
	for _, r := range name {
		if !isIdentRune(r) {
			return nil, false
		}
	}
	params := getParamsFromContext(ctx)
	if params == nil {
		return nil, false
	}
	v, ok := params[name]
	if !ok {
		return nil, false
	}
	return v, true
}

// resolveParamPathRef returns the typed parameter value for a literal
// "$name.path" reference, paired with true. Bare "$name" references are also
// supported, but callers that only want single identifiers should continue to
// use resolveDirectParamRef.
func resolveParamPathRef(ctx context.Context, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "$") {
		return nil, false
	}
	params := getParamsFromContext(ctx)
	if params == nil {
		return nil, false
	}
	return resolveSetMergeSourceFromParams(params, strings.TrimSpace(expr[1:]))
}

// resolveContextPathRef resolves a variable path in the row's value scope.
// Parameters have a separate namespace, reached only through $name by
// resolveParamPathRef; an unrelated parameter must never shadow a row's
// MERGE identity or SET source.
func resolveContextPathRef(ctx context.Context, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	return resolveSetMergeSourceFromParams(valueBindingsFromContext(ctx), expr)
}

// normalizeQueryParameters converts parameter values whose Go types aren't the
// executor's value shapes into those shapes, once, where parameters enter the
// executor (#712): a typed map (map[string]string, map[string]int64, …) is a
// Cypher map (map[string]interface{}), and a slice or array of maps or lists
// is a Cypher list ([]interface{}), recursively. Typed lists of scalars
// ([]string, []int64, []float64, …) stay typed: storage keeps their element
// type. The map is copied only when a value changes.
func normalizeQueryParameters(params map[string]interface{}) map[string]interface{} {
	return normalizeQueryParametersWithCancellation(params, nil)
}
func normalizeQueryParametersWithCancellation(params map[string]interface{}, probe *pipelineCancellation) map[string]interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	var normalized map[string]interface{}
	for name, value := range params {
		if probe != nil && !probe.check() {
			return nil
		}
		converted, changed := normalizeParameterValueWithCancellation(value, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		if !changed {
			continue
		}
		if normalized == nil {
			normalized = make(map[string]interface{}, pipelineInitialCapacity(len(params), probe))
			for key, original := range params {
				if probe != nil && !probe.check() {
					return nil
				}
				normalized[key] = original
			}
		}
		normalized[name] = converted
	}
	if normalized == nil {
		return params
	}
	return normalized
}

// parameterRowValue is a parameter's value as a row holds it ($name): a map's
// or list's typed lists (map[string][]string) are Cypher lists, as when the
// value is read through a WITH, while a typed list parameter itself keeps
// its type (normalizeParameterValue).
func parameterRowValue(value interface{}) interface{} {
	return parameterRowValueWithCancellation(value, nil)
}
func parameterRowValueWithCancellation(value interface{}, probe *pipelineCancellation) interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	switch typed := value.(type) {
	case map[string]interface{}:
		var converted map[string]interface{}
		for key, element := range typed {
			if probe != nil && !probe.check() {
				return nil
			}
			nested := nestedParameterValueWithCancellation(element, probe)
			if probe != nil && probe.err != nil {
				return nil
			}
			if !sameParameterValue(nested, element) {
				if converted == nil {
					converted = make(map[string]interface{}, pipelineInitialCapacity(len(typed), probe))
					for k, v := range typed {
						if probe != nil && !probe.check() {
							return nil
						}
						converted[k] = v
					}
				}
				converted[key] = nested
			}
		}
		if converted != nil {
			return converted
		}
	case []interface{}:
		var converted []interface{}
		for index, element := range typed {
			if probe != nil && !probe.check() {
				return nil
			}
			nested := nestedParameterValueWithCancellation(element, probe)
			if probe != nil && probe.err != nil {
				return nil
			}
			if !sameParameterValue(nested, element) {
				if converted == nil {
					converted = copyParameterListWithCancellation(typed, probe)
					if probe != nil && probe.err != nil {
						return nil
					}
				}
				converted[index] = nested
			}
		}
		if converted != nil {
			return converted
		}
	}
	return value
}

// nestedParameterValue is a value inside a parameter's map or list: a
// typed list is a Cypher list of its elements.
func nestedParameterValue(value interface{}) interface{} {
	return nestedParameterValueWithCancellation(value, nil)
}
func nestedParameterValueWithCancellation(value interface{}, probe *pipelineCancellation) interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	switch value.(type) {
	case []string, []int, []int64, []float32, []float64, []bool:
		return toAnySliceWithCancellation(value, probe)
	case map[string]interface{}, []interface{}:
		return parameterRowValueWithCancellation(value, probe)
	}
	return value
}

// sameParameterValue reports whether nestedParameterValue left v unchanged.
func sameParameterValue(converted, original interface{}) bool {
	switch original.(type) {
	case []string, []int, []int64, []float32, []float64, []bool:
		return false
	case map[string]interface{}, []interface{}:
		return reflect.ValueOf(converted).Pointer() == reflect.ValueOf(original).Pointer()
	}
	return true
}

// normalizeParameterValue converts one parameter value (normalizeQueryParameters).
func normalizeParameterValue(value interface{}) (interface{}, bool) {
	return normalizeParameterValueWithCancellation(value, nil)
}
func normalizeParameterValueWithCancellation(value interface{}, probe *pipelineCancellation) (interface{}, bool) {
	if probe != nil && !probe.check() {
		return nil, false
	}
	switch typed := value.(type) {
	case storage.RemotePathValue:
		if typed == nil {
			return nil, true
		}
		return value, false
	case storage.RemoteEntityValue:
		// Fabric graph values retain their transport provenance across parameters.
		return value, false
	case nil, string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, []byte, time.Time,
		[]string, []int, []int64, []float32, []float64, []bool:
		return value, false
	case *time.Time, storage.TypedPropertyValue:
		// Typed bindings replace literal reconstruction, which represented a
		// nil temporal/point pointer as null. Keep that at the query-entry owner
		// for scalar parameters and members of maps/lists alike.
		reflected := reflect.ValueOf(value)
		if reflected.Kind() == reflect.Ptr && reflected.IsNil() {
			return nil, true
		}
		return value, false
	case map[string]interface{}:
		public := storage.PublicRemoteMap(typed)
		if len(public) != len(typed) {
			normalized, _ := normalizeParameterValueWithCancellation(public, probe)
			if probe != nil && probe.err != nil {
				return nil, false
			}
			return normalized, true
		}

		var converted map[string]interface{}
		for key, element := range typed {
			if probe != nil && !probe.check() {
				return nil, false
			}
			normalized, changed := normalizeParameterValueWithCancellation(element, probe)
			if probe != nil && probe.err != nil {
				return nil, false
			}
			if !changed {
				continue
			}
			if converted == nil {
				converted = make(map[string]interface{}, pipelineInitialCapacity(len(typed), probe))
				for k, v := range typed {
					if probe != nil && !probe.check() {
						return nil, false
					}
					converted[k] = v
				}
			}
			converted[key] = normalized
		}
		if converted == nil {
			return value, false
		}
		return converted, true
	case []interface{}:
		var converted []interface{}
		for index, element := range typed {
			if probe != nil && !probe.check() {
				return nil, false
			}
			normalized, changed := normalizeParameterValueWithCancellation(element, probe)
			if probe != nil && probe.err != nil {
				return nil, false
			}
			if !changed {
				continue
			}
			if converted == nil {
				converted = copyParameterListWithCancellation(typed, probe)
				if probe != nil && probe.err != nil {
					return nil, false
				}
			}
			converted[index] = normalized
		}
		if converted == nil {
			return value, false
		}
		return converted, true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Ptr:
		if reflected.IsNil() {
			return nil, true
		}
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			converted := make(map[interface{}]interface{}, pipelineInitialCapacity(reflected.Len(), probe))
			iterator := reflected.MapRange()
			for iterator.Next() {
				if probe != nil && !probe.check() {
					return nil, false
				}
				element, _ := normalizeParameterValueWithCancellation(iterator.Value().Interface(), probe)
				if probe != nil && probe.err != nil {
					return nil, false
				}
				converted[iterator.Key().Interface()] = element
			}
			return converted, true
		}
		converted := make(map[string]interface{}, pipelineInitialCapacity(reflected.Len(), probe))
		iterator := reflected.MapRange()
		for iterator.Next() {
			if probe != nil && !probe.check() {
				return nil, false
			}
			element, _ := normalizeParameterValueWithCancellation(iterator.Value().Interface(), probe)
			if probe != nil && probe.err != nil {
				return nil, false
			}
			converted[iterator.Key().String()] = element
		}
		return converted, true
	case reflect.Slice, reflect.Array:
		if reflected.Len() > pipelineCancellationInterval && probe != nil && !probe.poll() {
			return nil, false
		}
		converted := make([]interface{}, reflected.Len())
		for index := range converted {
			if probe != nil && !probe.check() {
				return nil, false
			}
			element, _ := normalizeParameterValueWithCancellation(reflected.Index(index).Interface(), probe)
			if probe != nil && probe.err != nil {
				return nil, false
			}
			converted[index] = element
		}
		return converted, true
	}
	return value, false
}

func escapeCypherStringLiteral(raw string) string {
	escaped := strings.ReplaceAll(raw, "'", "''")
	escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
	return escaped
}

func quoteCypherStringLiteral(raw string) string {
	return "'" + escapeCypherStringLiteral(raw) + "'"
}

// paramRowsKey is the context key of a query's parameters as row values;
// see withQueryParams.
type paramRowsKeyType struct{}

var paramRowsKey = paramRowsKeyType{}

// queryParamsContext carries a query's parameters (paramsKey) and the
// cache of their row values (paramRowsKey), computed on first use. It is
// one context layer, so attaching parameters costs the single allocation
// context.WithValue did.
type queryParamsContext struct {
	context.Context
	params map[string]interface{}
	once   sync.Once
	rows   map[string]interface{}
}

// Value answers paramsKey and paramRowsKey and defers every other key to
// the parent context.
func (c *queryParamsContext) Value(key any) any {
	switch key {
	case paramsKey:
		return c.params
	case paramRowsKey:
		return c
	}
	return c.Context.Value(key)
}

func mergeInheritedQueryParams(ctx context.Context, params map[string]interface{}) map[string]interface{} {
	inherited := getParamsFromContext(ctx)
	if len(inherited) == 0 {
		return params
	}
	if params == nil {
		return inherited
	}
	merged := make(map[string]interface{}, len(inherited)+len(params))
	probe := parameterCancellationForContext(ctx)
	for name, value := range inherited {
		if probe != nil && !probe.check() {
			return nil
		}
		merged[name] = value
	}
	for name, value := range params {
		if probe != nil && !probe.check() {
			return nil
		}
		merged[name] = value
	}
	return merged
}

// withQueryParams attaches a query's parameters to ctx, together with a
// cache of their row values (parameterRowValues). Every row and every
// expression evaluation of the query binds the same "$name" values, and
// converting a parameter walks all of it: converted per evaluation, a
// 500-row $rows list made UNWIND $rows CREATE quadratic (#823).
func withQueryParams(ctx context.Context, params map[string]interface{}) context.Context {
	return &queryParamsContext{Context: ctx, params: params}
}

// parameterRowValues returns the context's parameters as row values,
// keyed "$name" (parameterRowValue of each), with a Cypher 25 statement's
// mark (cypher25RowKey): every row that gets the parameters gets the
// statement's language version with them. Each parameter map attached with
// withQueryParams is converted once; the result is shared and must not be
// modified.
func parameterRowValues(ctx context.Context) map[string]interface{} {
	params := getParamsFromContext(ctx)
	cypher25 := cypherVersionFromContext(ctx) == "25"
	if len(params) == 0 {
		if cypher25 {
			return cypher25RowValues
		}
		return nil
	}
	if cache, ok := ctx.Value(paramRowsKey).(*queryParamsContext); ok && sameParameterMap(cache.params, params) {
		cache.once.Do(func() {
			cache.rows = parameterRowsOfWithCancellation(params, cypher25, parameterCancellationForContext(ctx))
		})
		return cache.rows
	}
	return parameterRowsOfWithCancellation(params, cypher25, parameterCancellationForContext(ctx))
}

// cypher25RowValues are the row values of a Cypher 25 statement without
// parameters (shared; never modified).
var cypher25RowValues = map[string]interface{}{cypher25RowKey: true}

func parameterRowsOf(params map[string]interface{}, cypher25 bool) map[string]interface{} {
	return parameterRowsOfWithCancellation(params, cypher25, nil)
}
func parameterRowsOfWithCancellation(params map[string]interface{}, cypher25 bool, probe *pipelineCancellation) map[string]interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	rows := make(map[string]interface{}, len(params)+1)
	for name, value := range params {
		if probe != nil && !probe.check() {
			return nil
		}
		key := concatPipelineStrings("$", name, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		rows[key] = parameterRowValueWithCancellation(value, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
	}
	if cypher25 {
		rows[cypher25RowKey] = true
	}
	return rows
}

// sameParameterMap reports whether a and b are the same map.
func sameParameterMap(a, b map[string]interface{}) bool {
	return reflect.ValueOf(a).UnsafePointer() == reflect.ValueOf(b).UnsafePointer()
}

// bindParameterRow adds the context's parameters to row as "$name" values
// (with a Cypher 25 statement's mark, parameterRowValues).
func bindParameterRow(ctx context.Context, row pipelineRow) {
	probe := parameterCancellationForContext(ctx)
	for name, value := range parameterRowValues(ctx) {
		if probe != nil && !probe.check() {
			return
		}
		row[name] = value
	}
}

// cypher25RowKey marks the rows of a Cypher 25 statement. Like a parameter
// it is a "$" key: projections carry it across WITH and UNWIND and none
// returns it, so the row evaluator, which has no context, reads the
// statement's language version from the row (rowIsCypher25).
const cypher25RowKey = "$__nornic_cypher25"

// rowIsCypher25 reports whether values are a Cypher 25 statement's row.
func rowIsCypher25(values map[string]interface{}) bool {
	marked, _ := values[cypher25RowKey].(bool)
	return marked
}
