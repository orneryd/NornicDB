// Type conversion utilities for NornicDB Cypher.
//
// This file contains helper functions for converting between Go types
// and Cypher types. These are used throughout the Cypher executor for
// handling query parameters, property values, and function arguments.
//
// # Type Coercion Rules
//
// Cypher has specific type coercion rules that match Neo4j:
//
//   - Numeric types: int, int64, float64 are interchangeable where sensible
//   - Strings: Can be parsed as numbers when needed
//   - Booleans: true/false only, no numeric coercion
//   - Null: Propagates through most operations
//
// # ELI12
//
// Think of type conversion like a universal translator:
//
//   - toInt64: "Turn any number into a whole number" (42.7 → 42, "42" → 42)
//   - toFloat64: "Turn any number into a decimal number" (42 → 42.0)
//   - getCypherType: "Tell me what kind of thing this is" (42 → "INTEGER")
//
// It's like asking "What language is this?" and then translating it
// into the language you need!
//
// # Refactoring Note
//
// This file is part of the ongoing refactoring of pkg/cypher/functions.go.
// The following functions currently remain in functions.go and will be
// moved here in a future refactoring step:
//   - toFloat64 (helpers.go)
//   - getCypherType
//   - mergeMaps
//   - fromPairs
//   - fromLists

package cypher

import (
	"strconv"
)

// toInt64 converts various numeric types to int64.
//
// This function handles type coercion for numeric values in Cypher queries.
// It's used extensively for array indexing, property values, and function arguments.
//
// # Supported Types
//
//   - int: Direct conversion
//   - int64: Returned as-is
//   - float64: Truncated (not rounded)
//   - string: Parsed as decimal integer
//
// # Parameters
//
//   - v: The value to convert
//
// # Returns
//
//   - The int64 value, or 0 if conversion fails
//
// # Example
//
//	toInt64(42)       // 42
//	toInt64(42.7)     // 42 (truncated)
//	toInt64("42")     // 42
//	toInt64("hello")  // 0 (parse fails)
//	toInt64(nil)      // 0
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}
