// Package map provides APOC map manipulation functions.
//
// This package implements all apoc.map.* functions for working with
// maps/dictionaries in Cypher queries.
package maputil

import (
	"fmt"
	"sort"
	"strings"
)

// FromPairs creates a map from a list of key-value pairs.
//
// Example:
//   apoc.map.fromPairs([['name','Alice'],['age',30]]) 
//   => {name:'Alice', age:30}
func FromPairs(pairs [][]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, pair := range pairs {
		if len(pair) >= 2 {
			key := fmt.Sprintf("%v", pair[0])
			result[key] = pair[1]
		}
	}
	return result
}

// FromLists creates a map from separate key and value lists.
//
// Example:
//   apoc.map.fromLists(['name','age'], ['Alice',30]) 
//   => {name:'Alice', age:30}
func FromLists(keys []interface{}, values []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	minLen := len(keys)
	if len(values) < minLen {
		minLen = len(values)
	}
	
	for i := 0; i < minLen; i++ {
		key := fmt.Sprintf("%v", keys[i])
		result[key] = values[i]
	}
	
	return result
}

// FromValues creates a map from alternating keys and values.
//
// Example:
//   apoc.map.fromValues(['name','Alice','age',30]) 
//   => {name:'Alice', age:30}
func FromValues(values []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for i := 0; i < len(values)-1; i += 2 {
		key := fmt.Sprintf("%v", values[i])
		result[key] = values[i+1]
	}
	return result
}

// Merge merges multiple maps into one.
//
// Example:
//   apoc.map.merge({a:1}, {b:2}, {c:3}) => {a:1, b:2, c:3}
func Merge(maps ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// MergeList merges a list of maps.
//
// Example:
//   apoc.map.mergeList([{a:1},{b:2},{c:3}]) => {a:1, b:2, c:3}
func MergeList(maps []map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// SetKey sets a key in a map.
//
// Example:
//   apoc.map.setKey({name:'Alice'}, 'age', 30) 
//   => {name:'Alice', age:30}
func SetKey(m map[string]interface{}, key string, value interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = v
	}
	result[key] = value
	return result
}

// SetEntry sets a key-value pair in a map.
//
// Example:
//   apoc.map.setEntry({name:'Alice'}, 'age', 30)
func SetEntry(m map[string]interface{}, key string, value interface{}) map[string]interface{} {
	return SetKey(m, key, value)
}

// SetPairs sets multiple key-value pairs in a map.
//
// Example:
//   apoc.map.setPairs({}, [['name','Alice'],['age',30]])
func SetPairs(m map[string]interface{}, pairs [][]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = v
	}
	
	for _, pair := range pairs {
		if len(pair) >= 2 {
			key := fmt.Sprintf("%v", pair[0])
			result[key] = pair[1]
		}
	}
	
	return result
}

// SetLists sets keys from two lists.
//
// Example:
//   apoc.map.setLists({}, ['name','age'], ['Alice',30])
func SetLists(m map[string]interface{}, keys []interface{}, values []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = v
	}
	
	minLen := len(keys)
	if len(values) < minLen {
		minLen = len(values)
	}
	
	for i := 0; i < minLen; i++ {
		key := fmt.Sprintf("%v", keys[i])
		result[key] = values[i]
	}
	
	return result
}

// SetValues sets keys from alternating key-value list.
//
// Example:
//   apoc.map.setValues({}, ['name','Alice','age',30])
func SetValues(m map[string]interface{}, values []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = v
	}
	
	for i := 0; i < len(values)-1; i += 2 {
		key := fmt.Sprintf("%v", values[i])
		result[key] = values[i+1]
	}
	
	return result
}

// RemoveKey removes a key from a map.
//
// Example:
//   apoc.map.removeKey({name:'Alice',age:30}, 'age') 
//   => {name:'Alice'}
func RemoveKey(m map[string]interface{}, key string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		if k != key {
			result[k] = v
		}
	}
	return result
}

// RemoveKeys removes multiple keys from a map.
//
// Example:
//   apoc.map.removeKeys({a:1,b:2,c:3}, ['b','c']) => {a:1}
func RemoveKeys(m map[string]interface{}, keys []string) map[string]interface{} {
	keySet := make(map[string]bool)
	for _, key := range keys {
		keySet[key] = true
	}
	
	result := make(map[string]interface{})
	for k, v := range m {
		if !keySet[k] {
			result[k] = v
		}
	}
	
	return result
}

// Clean removes null values from a map.
//
// Example:
//   apoc.map.clean({a:1,b:null,c:3}, [], []) => {a:1,c:3}
func Clean(m map[string]interface{}, keys []string, values []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	
	for k, v := range m {
		if v == nil {
			continue
		}
		
		// Check if key should be excluded
		skip := false
		for _, excludeKey := range keys {
			if k == excludeKey {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		
		// Check if value should be excluded
		for _, excludeValue := range values {
			if v == excludeValue {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		
		result[k] = v
	}
	
	return result
}

// Get gets a value from a map with a default.
//
// Example:
//   apoc.map.get({name:'Alice'}, 'age', 0) => 0
func Get(m map[string]interface{}, key string, defaultValue interface{}) interface{} {
	if value, ok := m[key]; ok {
		return value
	}
	return defaultValue
}

// MGet gets multiple values from a map.
//
// Example:
//   apoc.map.mget({name:'Alice',age:30}, ['name','age']) 
//   => ['Alice', 30]
func MGet(m map[string]interface{}, keys []string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, key := range keys {
		result[i] = m[key]
	}
	return result
}

// SubMap extracts a subset of a map by keys.
//
// Example:
//   apoc.map.submap({a:1,b:2,c:3}, ['a','c']) => {a:1,c:3}
func SubMap(m map[string]interface{}, keys []string) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range keys {
		if value, ok := m[key]; ok {
			result[key] = value
		}
	}
	return result
}

// Keys returns all keys from a map.
//
// Example:
//   apoc.map.keys({name:'Alice',age:30}) => ['name','age']
func Keys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Values returns all values from a map.
//
// Example:
//   apoc.map.values({name:'Alice',age:30}) => ['Alice',30]
func Values(m map[string]interface{}) []interface{} {
	values := make([]interface{}, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

// SortedProperties returns key-value pairs sorted by key.
//
// Example:
//   apoc.map.sortedProperties({z:1,a:2}) => [['a',2],['z',1]]
func SortedProperties(m map[string]interface{}) [][]interface{} {
	keys := Keys(m)
	result := make([][]interface{}, len(keys))
	
	for i, key := range keys {
		result[i] = []interface{}{key, m[key]}
	}
	
	return result
}

// Flatten flattens a nested map structure.
//
// Example:
//   apoc.map.flatten({a:{b:{c:1}}}) => {'a.b.c':1}
func Flatten(m map[string]interface{}, delimiter string) map[string]interface{} {
	result := make(map[string]interface{})
	flattenHelper(m, "", delimiter, result)
	return result
}

func flattenHelper(m map[string]interface{}, prefix, delimiter string, result map[string]interface{}) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + delimiter + k
		}
		
		if nested, ok := v.(map[string]interface{}); ok {
			flattenHelper(nested, key, delimiter, result)
		} else {
			result[key] = v
		}
	}
}

// Unflatten unflattens a flat map into nested structure.
//
// Example:
//   apoc.map.unflatten({'a.b.c':1}) => {a:{b:{c:1}}}
func Unflatten(m map[string]interface{}, delimiter string) map[string]interface{} {
	result := make(map[string]interface{})
	
	for key, value := range m {
		parts := strings.Split(key, delimiter)
		current := result
		
		for i := 0; i < len(parts)-1; i++ {
			if _, ok := current[parts[i]]; !ok {
				current[parts[i]] = make(map[string]interface{})
			}
			current = current[parts[i]].(map[string]interface{})
		}
		
		current[parts[len(parts)-1]] = value
	}
	
	return result
}

// GroupBy groups a list of maps by a key.
//
// Example:
//   apoc.map.groupBy([{type:'A',val:1},{type:'A',val:2},{type:'B',val:3}], 'type')
//   => {A:[{type:'A',val:1},{type:'A',val:2}], B:[{type:'B',val:3}]}
func GroupBy(list []map[string]interface{}, key string) map[string][]map[string]interface{} {
	result := make(map[string][]map[string]interface{})
	
	for _, item := range list {
		groupKey := fmt.Sprintf("%v", item[key])
		result[groupKey] = append(result[groupKey], item)
	}
	
	return result
}

// GroupByMulti groups by multiple keys.
//
// Example:
//   apoc.map.groupByMulti([...], ['type','category'])
func GroupByMulti(list []map[string]interface{}, keys []string) map[string][]map[string]interface{} {
	result := make(map[string][]map[string]interface{})
	
	for _, item := range list {
		var groupKeyParts []string
		for _, key := range keys {
			groupKeyParts = append(groupKeyParts, fmt.Sprintf("%v", item[key]))
		}
		groupKey := strings.Join(groupKeyParts, "|")
		result[groupKey] = append(result[groupKey], item)
	}
	
	return result
}

// UpdateTree updates a nested map structure.
//
// Example:
//   apoc.map.updateTree({a:{b:1}}, 'a.b', 2) => {a:{b:2}}
func UpdateTree(m map[string]interface{}, path string, value interface{}) map[string]interface{} {
	result := copyMap(m)
	parts := strings.Split(path, ".")
	
	current := result
	for i := 0; i < len(parts)-1; i++ {
		if _, ok := current[parts[i]]; !ok {
			current[parts[i]] = make(map[string]interface{})
		}
		current = current[parts[i]].(map[string]interface{})
	}
	
	current[parts[len(parts)-1]] = value
	return result
}

// DropNullValues removes all null values from a map.
//
// Example:
//   apoc.map.dropNullValues({a:1,b:null,c:3}) => {a:1,c:3}
func DropNullValues(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		if v != nil {
			result[k] = v
		}
	}
	return result
}

// Helper functions

func copyMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		if nested, ok := v.(map[string]interface{}); ok {
			result[k] = copyMap(nested)
		} else {
			result[k] = v
		}
	}
	return result
}
