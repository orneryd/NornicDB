// Package json provides APOC JSON functions.
//
// This package implements all apoc.json.* functions for working with
// JSON data in Cypher queries.
package json

import (
	"encoding/json"
	"strings"
)

// Path extracts a value from JSON using a path expression.
//
// Example:
//   apoc.json.path('{"user":{"name":"Alice"}}', '$.user.name') => 'Alice'
func Path(jsonStr, path string) interface{} {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil
	}
	
	return extractPath(data, parsePath(path))
}

// Validate validates a JSON string.
//
// Example:
//   apoc.json.validate('{"name":"Alice"}') => true
//   apoc.json.validate('{invalid}') => false
func Validate(jsonStr string) bool {
	var data interface{}
	return json.Unmarshal([]byte(jsonStr), &data) == nil
}

// Parse parses a JSON string into a value.
//
// Example:
//   apoc.json.parse('{"name":"Alice"}') => {name: 'Alice'}
func Parse(jsonStr string) interface{} {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil
	}
	return data
}

// Stringify converts a value to JSON string.
//
// Example:
//   apoc.json.stringify({name: 'Alice'}) => '{"name":"Alice"}'
func Stringify(value interface{}) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(bytes)
}

// Pretty formats JSON with indentation.
//
// Example:
//   apoc.json.pretty('{"name":"Alice"}') => formatted JSON
func Pretty(jsonStr string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return jsonStr
	}
	return string(bytes)
}

// Compact removes whitespace from JSON.
//
// Example:
//   apoc.json.compact('{\n  "name": "Alice"\n}') => '{"name":"Alice"}'
func Compact(jsonStr string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	bytes, err := json.Marshal(data)
	if err != nil {
		return jsonStr
	}
	return string(bytes)
}

// Keys returns all keys from a JSON object.
//
// Example:
//   apoc.json.keys('{"name":"Alice","age":30}') => ['name', 'age']
func Keys(jsonStr string) []string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return []string{}
	}
	
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	return keys
}

// Values returns all values from a JSON object.
//
// Example:
//   apoc.json.values('{"name":"Alice","age":30}') => ['Alice', 30]
func Values(jsonStr string) []interface{} {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return []interface{}{}
	}
	
	values := make([]interface{}, 0, len(data))
	for _, v := range data {
		values = append(values, v)
	}
	return values
}

// Type returns the type of a JSON value.
//
// Example:
//   apoc.json.type('{"name":"Alice"}') => 'object'
//   apoc.json.type('[1,2,3]') => 'array'
func Type(jsonStr string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "invalid"
	}
	
	switch data.(type) {
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}

// Size returns the size of a JSON structure.
//
// Example:
//   apoc.json.size('{"name":"Alice","age":30}') => 2
//   apoc.json.size('[1,2,3,4,5]') => 5
func Size(jsonStr string) int {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return 0
	}
	
	switch v := data.(type) {
	case map[string]interface{}:
		return len(v)
	case []interface{}:
		return len(v)
	default:
		return 0
	}
}

// Merge merges multiple JSON objects.
//
// Example:
//   apoc.json.merge('{"a":1}', '{"b":2}') => '{"a":1,"b":2}'
func Merge(jsonStrs ...string) string {
	result := make(map[string]interface{})
	
	for _, jsonStr := range jsonStrs {
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			continue
		}
		
		for k, v := range data {
			result[k] = v
		}
	}
	
	bytes, _ := json.Marshal(result)
	return string(bytes)
}

// Set sets a value in JSON at a path.
//
// Example:
//   apoc.json.set('{"user":{}}', '$.user.name', 'Alice')
//   => '{"user":{"name":"Alice"}}'
func Set(jsonStr, path string, value interface{}) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	setPath(data, parsePath(path), value)
	
	bytes, _ := json.Marshal(data)
	return string(bytes)
}

// Delete removes a value from JSON at a path.
//
// Example:
//   apoc.json.delete('{"name":"Alice","age":30}', '$.age')
//   => '{"name":"Alice"}'
func Delete(jsonStr, path string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	deletePath(data, parsePath(path))
	
	bytes, _ := json.Marshal(data)
	return string(bytes)
}

// Flatten flattens a nested JSON structure.
//
// Example:
//   apoc.json.flatten('{"user":{"name":"Alice"}}')
//   => '{"user.name":"Alice"}'
func Flatten(jsonStr string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	flattened := make(map[string]interface{})
	flattenHelper(data, "", flattened)
	
	bytes, _ := json.Marshal(flattened)
	return string(bytes)
}

// Unflatten unflattens a flat JSON structure.
//
// Example:
//   apoc.json.unflatten('{"user.name":"Alice"}')
//   => '{"user":{"name":"Alice"}}'
func Unflatten(jsonStr string) string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	unflattened := make(map[string]interface{})
	for key, value := range data {
		parts := strings.Split(key, ".")
		current := unflattened
		
		for i := 0; i < len(parts)-1; i++ {
			if _, ok := current[parts[i]]; !ok {
				current[parts[i]] = make(map[string]interface{})
			}
			current = current[parts[i]].(map[string]interface{})
		}
		
		current[parts[len(parts)-1]] = value
	}
	
	bytes, _ := json.Marshal(unflattened)
	return string(bytes)
}

// Filter filters a JSON array based on a predicate.
//
// Example:
//   apoc.json.filter('[1,2,3,4,5]', 'x > 3') => '[4,5]'
func Filter(jsonStr string, predicate func(interface{}) bool) string {
	var data []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	filtered := make([]interface{}, 0)
	for _, item := range data {
		if predicate(item) {
			filtered = append(filtered, item)
		}
	}
	
	bytes, _ := json.Marshal(filtered)
	return string(bytes)
}

// Map transforms a JSON array.
//
// Example:
//   apoc.json.map('[1,2,3]', 'x * 2') => '[2,4,6]'
func Map(jsonStr string, transform func(interface{}) interface{}) string {
	var data []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return jsonStr
	}
	
	mapped := make([]interface{}, len(data))
	for i, item := range data {
		mapped[i] = transform(item)
	}
	
	bytes, _ := json.Marshal(mapped)
	return string(bytes)
}

// Reduce reduces a JSON array to a single value.
//
// Example:
//   apoc.json.reduce('[1,2,3,4,5]', 0, 'acc + x') => 15
func Reduce(jsonStr string, initial interface{}, reducer func(interface{}, interface{}) interface{}) interface{} {
	var data []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return initial
	}
	
	result := initial
	for _, item := range data {
		result = reducer(result, item)
	}
	
	return result
}

// Helper functions

func parsePath(path string) []string {
	// Simple path parser for $.a.b.c format
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, ".")
}

func extractPath(data interface{}, path []string) interface{} {
	if len(path) == 0 {
		return data
	}
	
	switch v := data.(type) {
	case map[string]interface{}:
		if val, ok := v[path[0]]; ok {
			return extractPath(val, path[1:])
		}
	case []interface{}:
		// Handle array index
		// Simplified - in production, parse index from path
		if len(v) > 0 {
			return extractPath(v[0], path[1:])
		}
	}
	
	return nil
}

func setPath(data interface{}, path []string, value interface{}) {
	if len(path) == 0 {
		return
	}
	
	if m, ok := data.(map[string]interface{}); ok {
		if len(path) == 1 {
			m[path[0]] = value
		} else {
			if _, ok := m[path[0]]; !ok {
				m[path[0]] = make(map[string]interface{})
			}
			setPath(m[path[0]], path[1:], value)
		}
	}
}

func deletePath(data interface{}, path []string) {
	if len(path) == 0 {
		return
	}
	
	if m, ok := data.(map[string]interface{}); ok {
		if len(path) == 1 {
			delete(m, path[0])
		} else {
			if nested, ok := m[path[0]]; ok {
				deletePath(nested, path[1:])
			}
		}
	}
}

func flattenHelper(data interface{}, prefix string, result map[string]interface{}) {
	switch v := data.(type) {
	case map[string]interface{}:
		for key, value := range v {
			newKey := key
			if prefix != "" {
				newKey = prefix + "." + key
			}
			
			if nested, ok := value.(map[string]interface{}); ok {
				flattenHelper(nested, newKey, result)
			} else {
				result[newKey] = value
			}
		}
	default:
		result[prefix] = data
	}
}
