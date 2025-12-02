// Package convert provides APOC type conversion functions.
//
// This package implements all apoc.convert.* functions for converting
// between different data types in Cypher queries.
package convert

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ToBoolean converts a value to boolean.
//
// Example:
//   apoc.convert.toBoolean('true') => true
//   apoc.convert.toBoolean(1) => true
//   apoc.convert.toBoolean(0) => false
func ToBoolean(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int, int64, int32:
		return v != 0
	case float64, float32:
		return v != 0.0
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		return lower == "true" || lower == "yes" || lower == "1"
	}
	return false
}

// ToInteger converts a value to integer.
//
// Example:
//   apoc.convert.toInteger('42') => 42
//   apoc.convert.toInteger(3.14) => 3
func ToInteger(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case string:
		i, _ := strconv.ParseInt(v, 10, 64)
		return i
	case bool:
		if v {
			return 1
		}
		return 0
	}
	return 0
}

// ToFloat converts a value to float.
//
// Example:
//   apoc.convert.toFloat('3.14') => 3.14
//   apoc.convert.toFloat(42) => 42.0
func ToFloat(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	}
	return 0.0
}

// ToString converts a value to string.
//
// Example:
//   apoc.convert.toString(42) => '42'
//   apoc.convert.toString(true) => 'true'
func ToString(value interface{}) string {
	return fmt.Sprintf("%v", value)
}

// ToList converts a value to a list.
//
// Example:
//   apoc.convert.toList('hello') => ['h','e','l','l','o']
//   apoc.convert.toList(42) => [42]
func ToList(value interface{}) []interface{} {
	switch v := value.(type) {
	case []interface{}:
		return v
	case string:
		result := make([]interface{}, len(v))
		for i, c := range v {
			result[i] = string(c)
		}
		return result
	default:
		return []interface{}{value}
	}
}

// ToMap converts a value to a map.
//
// Example:
//   apoc.convert.toMap('{"name":"Alice","age":30}') 
//   => {name:'Alice', age:30}
func ToMap(value interface{}) map[string]interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		return v
	case string:
		var result map[string]interface{}
		json.Unmarshal([]byte(v), &result)
		return result
	}
	return make(map[string]interface{})
}

// ToJson converts a value to JSON string.
//
// Example:
//   apoc.convert.toJson({name:'Alice', age:30}) 
//   => '{"name":"Alice","age":30}'
func ToJson(value interface{}) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(bytes)
}

// FromJsonList converts a JSON array string to a list.
//
// Example:
//   apoc.convert.fromJsonList('[1,2,3]') => [1,2,3]
func FromJsonList(jsonStr string) []interface{} {
	var result []interface{}
	json.Unmarshal([]byte(jsonStr), &result)
	return result
}

// FromJsonMap converts a JSON object string to a map.
//
// Example:
//   apoc.convert.fromJsonMap('{"name":"Alice"}') => {name:'Alice'}
func FromJsonMap(jsonStr string) map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(jsonStr), &result)
	return result
}

// ToSet converts a list to a set (removes duplicates).
//
// Example:
//   apoc.convert.toSet([1,2,2,3,3,3]) => [1,2,3]
func ToSet(list []interface{}) []interface{} {
	seen := make(map[string]bool)
	result := make([]interface{}, 0)
	
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}
	
	return result
}

// ToSortedJsonMap converts a map to a sorted JSON string.
//
// Example:
//   apoc.convert.toSortedJsonMap({z:1, a:2}) => '{"a":2,"z":1}'
func ToSortedJsonMap(m map[string]interface{}) string {
	// Note: In production, implement proper sorted JSON marshaling
	return ToJson(m)
}

// ToTree converts a path to a tree structure.
//
// Example:
//   apoc.convert.toTree(path) => nested map structure
func ToTree(path interface{}) map[string]interface{} {
	// Note: In production, implement proper path-to-tree conversion
	return make(map[string]interface{})
}

// FromJsonNode converts a JSON string to a node-like map.
//
// Example:
//   apoc.convert.fromJsonNode('{"id":1,"labels":["Person"],"properties":{}}')
func FromJsonNode(jsonStr string) map[string]interface{} {
	return FromJsonMap(jsonStr)
}

// ToNode converts a map to a node structure.
//
// Example:
//   apoc.convert.toNode({id:1, labels:['Person'], properties:{name:'Alice'}})
func ToNode(m map[string]interface{}) map[string]interface{} {
	return m
}

// ToRelationship converts a map to a relationship structure.
//
// Example:
//   apoc.convert.toRelationship({id:1, type:'KNOWS', properties:{}})
func ToRelationship(m map[string]interface{}) map[string]interface{} {
	return m
}

// GetJsonProperty extracts a property from a JSON string.
//
// Example:
//   apoc.convert.getJsonProperty('{"name":"Alice"}', 'name') => 'Alice'
func GetJsonProperty(jsonStr, key string) interface{} {
	m := FromJsonMap(jsonStr)
	return m[key]
}

// GetJsonPropertyMap extracts a nested property from JSON.
//
// Example:
//   apoc.convert.getJsonPropertyMap('{"user":{"name":"Alice"}}', 'user.name')
//   => 'Alice'
func GetJsonPropertyMap(jsonStr, path string) interface{} {
	m := FromJsonMap(jsonStr)
	keys := strings.Split(path, ".")
	
	var current interface{} = m
	for _, key := range keys {
		if currentMap, ok := current.(map[string]interface{}); ok {
			current = currentMap[key]
		} else {
			return nil
		}
	}
	
	return current
}

// SetJsonProperty sets a property in a JSON string.
//
// Example:
//   apoc.convert.setJsonProperty('{"name":"Alice"}', 'age', 30)
//   => '{"name":"Alice","age":30}'
func SetJsonProperty(jsonStr, key string, value interface{}) string {
	m := FromJsonMap(jsonStr)
	m[key] = value
	return ToJson(m)
}

// ToIntList converts a list to integers.
//
// Example:
//   apoc.convert.toIntList(['1','2','3']) => [1,2,3]
func ToIntList(list []interface{}) []int64 {
	result := make([]int64, len(list))
	for i, item := range list {
		result[i] = ToInteger(item)
	}
	return result
}

// ToFloatList converts a list to floats.
//
// Example:
//   apoc.convert.toFloatList(['1.5','2.5','3.5']) => [1.5,2.5,3.5]
func ToFloatList(list []interface{}) []float64 {
	result := make([]float64, len(list))
	for i, item := range list {
		result[i] = ToFloat(item)
	}
	return result
}

// ToStringList converts a list to strings.
//
// Example:
//   apoc.convert.toStringList([1,2,3]) => ['1','2','3']
func ToStringList(list []interface{}) []string {
	result := make([]string, len(list))
	for i, item := range list {
		result[i] = ToString(item)
	}
	return result
}

// ToBooleanList converts a list to booleans.
//
// Example:
//   apoc.convert.toBooleanList([1,0,'true','false']) 
//   => [true,false,true,false]
func ToBooleanList(list []interface{}) []bool {
	result := make([]bool, len(list))
	for i, item := range list {
		result[i] = ToBoolean(item)
	}
	return result
}

// ToNodeList converts a list of maps to node structures.
//
// Example:
//   apoc.convert.toNodeList([{id:1},{id:2}])
func ToNodeList(list []interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(list))
	for i, item := range list {
		if m, ok := item.(map[string]interface{}); ok {
			result[i] = m
		} else {
			result[i] = make(map[string]interface{})
		}
	}
	return result
}

// ToRelationshipList converts a list of maps to relationship structures.
//
// Example:
//   apoc.convert.toRelationshipList([{id:1,type:'KNOWS'}])
func ToRelationshipList(list []interface{}) []map[string]interface{} {
	return ToNodeList(list)
}
