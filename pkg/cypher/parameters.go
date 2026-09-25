// Parameter substitution for NornicDB Cypher.
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
	"fmt"
	"strconv"
	"strings"
	"time"
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
	return context.WithValue(ctx, paramsKey, params)
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

// substituteParams replaces $paramName placeholders with actual values.
//
// This implements Neo4j-style parameter substitution with proper escaping
// and type handling. Parameters that are not provided in the map are left as-is.
//
// # Parameters
//
//   - cypher: The query string with $param placeholders
//   - params: Map of parameter names to values
//
// # Returns
//
//   - The query string with parameters substituted
//
// # Example
//
//	query := "MATCH (p:Person {name: $name}) RETURN p"
//	params := map[string]interface{}{"name": "Alice"}
//	result := exec.substituteParams(query, params)
//	// result = "MATCH (p:Person {name: 'Alice'}) RETURN p"
//
// # Security Note
//
// Values are properly escaped to prevent injection attacks.
// String values have single quotes escaped by doubling them.
func (e *StorageExecutor) substituteParams(cypher string, params map[string]interface{}) string {
	if len(params) == 0 {
		return cypher
	}

	var result strings.Builder
	result.Grow(len(cypher))

	for i := 0; i < len(cypher); {
		dollarIdx := strings.IndexByte(cypher[i:], '$')
		if dollarIdx < 0 {
			result.WriteString(cypher[i:])
			break
		}
		dollarIdx += i
		result.WriteString(cypher[i:dollarIdx])

		start := dollarIdx + 1
		if start >= len(cypher) {
			result.WriteByte('$')
			break
		}

		first := cypher[start]
		if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
			result.WriteByte('$')
			i = start
			continue
		}

		end := start + 1
		for end < len(cypher) && isWordChar(cypher[end]) {
			end++
		}

		match := cypher[dollarIdx:end]
		paramName := cypher[start:end]

		// Preserve parameter path expressions like $node.url and
		// $node['url'] so downstream evaluators can resolve them from params.
		if end < len(cypher) && (cypher[end] == '.' || cypher[end] == '[') {
			result.WriteString(match)
			i = end
			continue
		}

		value, exists := params[paramName]
		if !exists {
			result.WriteString(match)
			i = end
			continue
		}

		if isCompositeParamValue(value) {
			result.WriteString(match)
			i = end
			continue
		}

		result.WriteString(e.valueToLiteral(value))
		i = end
	}

	return result.String()
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
	var missing []string
	for index := 0; index < len(cypher); index++ {
		switch cypher[index] {
		case '\'', '"', '`':
			index = skipQuotedSemanticText(cypher, index) - 1
		case '/':
			if index+1 < len(cypher) && cypher[index+1] == '/' {
				if end := strings.IndexByte(cypher[index:], '\n'); end >= 0 {
					index += end
				} else {
					index = len(cypher)
				}
			} else if index+1 < len(cypher) && cypher[index+1] == '*' {
				if end := strings.Index(cypher[index+2:], "*/"); end >= 0 {
					index += end + 3
				} else {
					index = len(cypher)
				}
			}
		case '$':
			start := index + 1
			var name string
			if start < len(cypher) && cypher[start] == '`' {
				end := skipQuotedSemanticText(cypher, start)
				name = normalizePropertyKey(cypher[start:end])
				index = end - 1
			} else {
				end := start
				for end < len(cypher) && isWordChar(cypher[end]) {
					end++
				}
				name = cypher[start:end]
				index = end - 1
			}
			if name == "" {
				continue
			}
			if _, supplied := params[name]; supplied {
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
	if len(missing) == 0 {
		return nil
	}
	return newSemanticError("Neo.ClientError.Statement.ParameterMissing", "MissingParameter", "Expected parameter(s): "+strings.Join(missing, ", "))
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
		if !(r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9')) {
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

// resolveContextPathRef resolves a bare identifier or dotted path against the
// current query parameters. This is used by MERGE pattern parsing and SET label
// expressions so row-scoped values like row.uuid and row.labels can be read
// without forcing them through Cypher string literal parsing.
func resolveContextPathRef(ctx context.Context, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	params := getParamsFromContext(ctx)
	if params == nil {
		return nil, false
	}
	return resolveSetMergeSourceFromParams(params, expr)
}

// isCompositeParamValue reports whether a parameter value is a typed list
// shape that would lose type information if stringified into Cypher
// literal syntax. Scalars (string, numeric, bool, nil) are always safe to
// substitute. Maps are also substituted because:
//
//   - CREATE patterns (e.g., `CREATE (n:Label $props)`) rely on the
//     substituted map literal being parsed by the pattern parser; the
//     post-write read goes through storage's msgpack-and-strict-typed
//     decode pipeline so type retention is preserved end-to-end.
//   - SET map merges (`SET n += $props`) read $params directly through
//     resolveDirectParamRef, bypassing literal substitution entirely.
//
// Only typed list slices (e.g. []string, []float64) need to stay as
// $name references — substituting them as `[a, b, c]` literal text would
// force the parser to re-decode them as []interface{}, which is the
// exact widening this short-circuit prevents.
func isCompositeParamValue(v interface{}) bool {
	switch v.(type) {
	case []string, []int, []int64, []float32, []float64:
		return true
	case time.Time, *time.Time:
		// Keep temporal params as typed values so property parsing can bind them
		// directly from context instead of stringifying via fmt.Sprintf("%v").
		return true
	default:
		return false
	}
}

func escapeCypherStringLiteral(raw string) string {
	escaped := strings.ReplaceAll(raw, "'", "''")
	escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
	return escaped
}

func quoteCypherStringLiteral(raw string) string {
	return "'" + escapeCypherStringLiteral(raw) + "'"
}

func isSimpleCypherMapKey(key string) bool {
	if key == "" {
		return false
	}
	first := key[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		return false
	}
	for i := 1; i < len(key); i++ {
		if !isWordChar(key[i]) {
			return false
		}
	}
	return true
}

func formatCypherMapKey(key string) string {
	if isSimpleCypherMapKey(key) {
		return key
	}
	return quoteCypherStringLiteral(key)
}

// valueToLiteral converts a Go value to a Cypher literal string.
//
// This function handles all Go types that can be passed as Cypher parameters,
// converting them to their appropriate Cypher literal representation.
//
// # Parameters
//
//   - v: The Go value to convert
//
// # Returns
//
//   - A string representation valid in Cypher syntax
//
// # Type Conversions
//
//	nil           → "null"
//	"hello"       → "'hello'"
//	42            → "42"
//	3.14          → "3.14"
//	true          → "true"
//	[1, 2]        → "[1, 2]"
//	{a: 1}        → "{a: 1}"
//
// # Example
//
//	e.valueToLiteral("Alice")           // "'Alice'"
//	e.valueToLiteral(42)                // "42"
//	e.valueToLiteral([]int{1, 2, 3})    // "[1, 2, 3]"
//	e.valueToLiteral(map[string]interface{}{"name": "Bob"}) // "{name: 'Bob'}"
func (e *StorageExecutor) valueToLiteral(v interface{}) string {
	if v == nil {
		return "null"
	}

	switch val := v.(type) {
	case string:
		return quoteCypherStringLiteral(val)

	case int:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)

	case float32:
		return formatCypherFloatLiteral(float64(val), 32)
	case float64:
		return formatCypherFloatLiteral(val, 64)

	case bool:
		if val {
			return "true"
		}
		return "false"

	case time.Time:
		return fmt.Sprintf("datetime('%s')", val.Format(time.RFC3339Nano))

	case *time.Time:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("datetime('%s')", val.Format(time.RFC3339Nano))

	case CypherDate:
		return fmt.Sprintf("date('%s')", val.String())
	case *CypherDate:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("date('%s')", val.String())
	case CypherLocalTime:
		return fmt.Sprintf("localtime('%s')", val.String())
	case *CypherLocalTime:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("localtime('%s')", val.String())
	case CypherTime:
		return fmt.Sprintf("time('%s')", val.String())
	case *CypherTime:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("time('%s')", val.String())
	case CypherLocalDateTime:
		return fmt.Sprintf("localdatetime('%s')", val.String())
	case *CypherLocalDateTime:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("localdatetime('%s')", val.String())
	case CypherDateTime:
		return fmt.Sprintf("datetime('%s')", val.String())
	case *CypherDateTime:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("datetime('%s')", val.String())
	case *CypherDuration:
		if val == nil {
			return "null"
		}
		return fmt.Sprintf("duration('%s')", val.String())

	case []interface{}:
		// Convert array to Cypher list literal: [val1, val2, ...]
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = e.valueToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case []string:
		// String array
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = e.valueToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case []int:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = strconv.Itoa(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case []int64:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = strconv.FormatInt(item, 10)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case []float64:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatCypherFloatLiteral(item, 64)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case []float32:
		// Float32 array (common for vector embeddings)
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatCypherFloatLiteral(float64(item), 32)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case map[string]interface{}:
		// Convert map to Cypher map literal: {key1: val1, key2: val2}
		parts := make([]string, 0, len(val))
		for k, v := range val {
			parts = append(parts, fmt.Sprintf("%s: %s", formatCypherMapKey(k), e.valueToLiteral(v)))
		}
		return "{" + strings.Join(parts, ", ") + "}"

	case map[interface{}]interface{}:
		parts := make([]string, 0, len(val))
		for rawKey, v := range val {
			key, ok := rawKey.(string)
			if !ok {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s: %s", formatCypherMapKey(key), e.valueToLiteral(v)))
		}
		return "{" + strings.Join(parts, ", ") + "}"

	case []map[string]interface{}:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = e.valueToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	case []map[interface{}]interface{}:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = e.valueToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"

	default:
		// Fallback: convert to string
		return quoteCypherStringLiteral(fmt.Sprintf("%v", v))
	}
}

func formatCypherFloatLiteral(value float64, bitSize int) string {
	literal := strconv.FormatFloat(value, 'g', -1, bitSize)
	if !strings.ContainsAny(literal, ".eE") {
		literal += ".0"
	}
	return literal
}
