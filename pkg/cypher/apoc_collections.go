// APOC collection functions for NornicDB Cypher.
//
// This file contains implementations of APOC (Awesome Procedures on Cypher)
// collection manipulation functions. These provide enhanced list processing
// capabilities beyond standard Cypher.
//
// # Available Functions
//
// List Manipulation:
//   - apoc.coll.flatten - Flatten nested lists
//   - apoc.coll.toSet - Remove duplicates preserving order
//   - apoc.coll.sort - Sort list ascending
//   - apoc.coll.sortNodes - Sort nodes by property
//   - apoc.coll.reverse - Reverse list order
//
// Set Operations:
//   - apoc.coll.union - Union of two lists (unique)
//   - apoc.coll.unionAll - Union of two lists (with duplicates)
//   - apoc.coll.intersection - Common elements
//   - apoc.coll.subtract - Difference of two lists
//
// Aggregations:
//   - apoc.coll.sum - Sum numeric values
//   - apoc.coll.avg - Average of numeric values
//   - apoc.coll.min - Minimum value
//   - apoc.coll.max - Maximum value
//
// Search & Analysis:
//   - apoc.coll.contains - Check if list contains value
//   - apoc.coll.containsAll - Check if list contains all values
//   - apoc.coll.containsAny - Check if list contains any value
//   - apoc.coll.indexOf - Find index of value
//   - apoc.coll.frequencies - Count occurrences of each value
//   - apoc.coll.occurrences - Count occurrences of specific value
//
// Transformation:
//   - apoc.coll.split - Split list by separator
//   - apoc.coll.partition - Split into fixed-size chunks
//   - apoc.coll.pairs - Create [value, nextValue] pairs
//   - apoc.coll.zip - Combine two lists into pairs
//
// Map Functions:
//   - apoc.map.merge - Merge two maps
//   - apoc.map.fromPairs - Create map from [key, value] pairs
//   - apoc.map.fromLists - Create map from parallel key/value lists
//
// # ELI12
//
// APOC collection functions are like special tools for working with lists:
//
//   - flatten: Like unstacking all the boxes-within-boxes to see everything
//   - toSet: Like removing duplicate trading cards from your collection
//   - union: Like combining two friend groups (no duplicates)
//   - intersection: Like finding friends you have in common
//   - frequencies: Like counting how many of each candy type you have
//
// They make it easy to work with groups of things in your database!
//
// # Neo4j Compatibility
//
// These functions match Neo4j APOC library behavior for compatibility
// with existing queries and applications.

package cypher

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// List Utility Functions
// ========================================

// flattenList recursively flattens nested lists into a single list.
//
// This function recursively processes nested lists to produce a single-level list.
//
// # Parameters
//
//   - val: A list that may contain nested lists
//
// # Returns
//
//   - A single-level list with all elements
//
// # Example
//
//	flattenList([[1, 2], [3, [4, 5]]])  // [1, 2, 3, 4, 5]
//	flattenList([1, 2, 3])              // [1, 2, 3]
func flattenList(val interface{}) []interface{} {
	var result []interface{}
	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			// Check if item is also a list
			switch inner := item.(type) {
			case []interface{}:
				result = append(result, flattenList(inner)...)
			default:
				result = append(result, item)
			}
		}
	case []string:
		for _, s := range v {
			result = append(result, s)
		}
	default:
		result = append(result, val)
	}
	return result
}

// toSet removes duplicates from a list while preserving order.
//
// Uses a map for O(1) lookup to track seen elements.
//
// # Parameters
//
//   - val: A list that may contain duplicates
//
// # Returns
//
//   - A list with duplicates removed, preserving first occurrence order
//
// # Example
//
//	toSet([1, 2, 2, 3, 1])  // [1, 2, 3]
//	toSet(["a", "b", "a"]) // ["a", "b"]
func toSet(val interface{}) []interface{} {
	var result []interface{}
	seen := make(map[string]bool)

	addUnique := func(item interface{}) {
		key := fmt.Sprintf("%T:%v", item, item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}

	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			addUnique(item)
		}
	case []string:
		for _, s := range v {
			addUnique(s)
		}
	}
	return result
}

// ========================================
// APOC Collection Aggregations
// ========================================

// apocCollSum sums numeric values in a list.
//
// Non-numeric values are ignored.
//
// # Parameters
//
//   - val: A list of values (numeric values summed)
//
// # Returns
//
//   - The sum as float64
//
// # Example
//
//	apocCollSum([1, 2, 3, 4])  // 10.0
//	apocCollSum([1.5, 2.5])   // 4.0
func apocCollSum(val interface{}) float64 {
	var sum float64
	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			if f, ok := toFloat64(item); ok {
				sum += f
			}
		}
	}
	return sum
}

// apocCollAvg calculates average of numeric values in a list.
//
// # Parameters
//
//   - val: A list of numeric values
//
// # Returns
//
//   - The average as float64, or nil if empty
//
// # Example
//
//	apocCollAvg([1, 2, 3, 4])  // 2.5
//	apocCollAvg([])           // nil
func apocCollAvg(val interface{}) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		var sum float64
		for _, item := range v {
			if f, ok := toFloat64(item); ok {
				sum += f
			}
		}
		return sum / float64(len(v))
	}
	return nil
}

// apocCollMin finds minimum value in a list.
//
// # Parameters
//
//   - val: A list of comparable values
//
// # Returns
//
//   - The minimum value, or nil if empty
//
// # Example
//
//	apocCollMin([3, 1, 4, 1, 5])  // 1
//	apocCollMin([])              // nil
func apocCollMin(val interface{}) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		min := v[0]
		minFloat, _ := toFloat64(min)
		for _, item := range v[1:] {
			if f, ok := toFloat64(item); ok && f < minFloat {
				min = item
				minFloat = f
			}
		}
		return min
	}
	return nil
}

// apocCollMax finds maximum value in a list.
//
// # Parameters
//
//   - val: A list of comparable values
//
// # Returns
//
//   - The maximum value, or nil if empty
//
// # Example
//
//	apocCollMax([3, 1, 4, 1, 5])  // 5
//	apocCollMax([])              // nil
func apocCollMax(val interface{}) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		max := v[0]
		maxFloat, _ := toFloat64(max)
		for _, item := range v[1:] {
			if f, ok := toFloat64(item); ok && f > maxFloat {
				max = item
				maxFloat = f
			}
		}
		return max
	}
	return nil
}

// ========================================
// APOC Collection Sorting
// ========================================

// apocCollSort sorts a list in ascending order.
//
// Uses bubble sort for simplicity; values are compared as float64.
//
// # Parameters
//
//   - val: A list to sort
//
// # Returns
//
//   - A new sorted list (does not modify original)
//
// # Example
//
//	apocCollSort([3, 1, 4, 1, 5])  // [1, 1, 3, 4, 5]
func apocCollSort(val interface{}) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		copy(result, v)
		// Sort by converting to float64 for comparison
		for i := 0; i < len(result)-1; i++ {
			for j := i + 1; j < len(result); j++ {
				fj, _ := toFloat64(result[j])
				fi, _ := toFloat64(result[i])
				if fj < fi {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
		return result
	}
	return nil
}

// apocCollSortNodes sorts nodes by a property value.
//
// # Parameters
//
//   - val: A list of nodes
//   - propName: The property to sort by
//
// # Returns
//
//   - A new list of nodes sorted by the property
//
// # Example
//
//	apocCollSortNodes(nodes, "age")  // Nodes sorted by age property
func apocCollSortNodes(val interface{}, propName string) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		copy(result, v)
		// Sort by property value
		for i := 0; i < len(result)-1; i++ {
			for j := i + 1; j < len(result); j++ {
				vi := getNodeProperty(result[i], propName)
				vj := getNodeProperty(result[j], propName)
				fj, _ := toFloat64(vj)
				fi, _ := toFloat64(vi)
				if fj < fi {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
		return result
	}
	return nil
}

// getNodeProperty extracts a property from a node (map or *storage.Node).
//
// # Parameters
//
//   - node: A node (map or *storage.Node)
//   - propName: The property name to extract
//
// # Returns
//
//   - The property value, or nil if not found
func getNodeProperty(node interface{}, propName string) interface{} {
	switch n := node.(type) {
	case map[string]interface{}:
		if props, ok := n["properties"].(map[string]interface{}); ok {
			return props[propName]
		}
		return n[propName]
	case *storage.Node:
		return n.Properties[propName]
	}
	return nil
}

// apocCollReverse reverses a list.
//
// # Parameters
//
//   - val: A list to reverse
//
// # Returns
//
//   - A new list in reverse order
//
// # Example
//
//	apocCollReverse([1, 2, 3])  // [3, 2, 1]
func apocCollReverse(val interface{}) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[len(v)-1-i] = item
		}
		return result
	}
	return nil
}

// ========================================
// APOC Set Operations
// ========================================

// apocCollUnion returns union of two lists (unique elements).
//
// # Parameters
//
//   - list1: First list
//   - list2: Second list
//
// # Returns
//
//   - A list with all unique elements from both lists
//
// # Example
//
//	apocCollUnion([1, 2], [2, 3])  // [1, 2, 3]
func apocCollUnion(list1, list2 interface{}) []interface{} {
	combined := apocCollUnionAll(list1, list2)
	return toSet(combined)
}

// apocCollUnionAll returns union of two lists (including duplicates).
//
// # Parameters
//
//   - list1: First list
//   - list2: Second list
//
// # Returns
//
//   - A list with all elements from both lists
//
// # Example
//
//	apocCollUnionAll([1, 2], [2, 3])  // [1, 2, 2, 3]
func apocCollUnionAll(list1, list2 interface{}) []interface{} {
	var result []interface{}
	if l1, ok := list1.([]interface{}); ok {
		result = append(result, l1...)
	}
	if l2, ok := list2.([]interface{}); ok {
		result = append(result, l2...)
	}
	return result
}

// apocCollIntersection returns common elements of two lists.
//
// # Parameters
//
//   - list1: First list
//   - list2: Second list
//
// # Returns
//
//   - A list with elements present in both lists
//
// # Example
//
//	apocCollIntersection([1, 2, 3], [2, 3, 4])  // [2, 3]
func apocCollIntersection(list1, list2 interface{}) []interface{} {
	var result []interface{}
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	// Create lookup set from list2
	set2 := make(map[string]bool)
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		set2[key] = true
	}

	// Find elements in list1 that are also in list2
	seen := make(map[string]bool)
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		if set2[key] && !seen[key] {
			result = append(result, item)
			seen[key] = true
		}
	}
	return result
}

// apocCollSubtract returns list1 minus elements in list2.
//
// # Parameters
//
//   - list1: List to subtract from
//   - list2: List of elements to remove
//
// # Returns
//
//   - Elements in list1 that are not in list2
//
// # Example
//
//	apocCollSubtract([1, 2, 3], [2])  // [1, 3]
func apocCollSubtract(list1, list2 interface{}) []interface{} {
	var result []interface{}
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	// Create lookup set from list2
	set2 := make(map[string]bool)
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		set2[key] = true
	}

	// Find elements in list1 that are NOT in list2
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		if !set2[key] {
			result = append(result, item)
		}
	}
	return result
}

// ========================================
// APOC Search Functions
// ========================================

// apocCollContains checks if list contains a value.
//
// # Parameters
//
//   - listVal: The list to search
//   - value: The value to find
//
// # Returns
//
//   - true if value is in list
//
// # Example
//
//	apocCollContains([1, 2, 3], 2)  // true
//	apocCollContains([1, 2, 3], 5)  // false
func apocCollContains(listVal, value interface{}) bool {
	list, ok := listVal.([]interface{})
	if !ok {
		return false
	}
	valKey := fmt.Sprintf("%T:%v", value, value)
	for _, item := range list {
		itemKey := fmt.Sprintf("%T:%v", item, item)
		if itemKey == valKey {
			return true
		}
	}
	return false
}

// apocCollContainsAll checks if list contains all values from another list.
//
// # Parameters
//
//   - list1: The list to search in
//   - list2: The list of values to find
//
// # Returns
//
//   - true if all values from list2 are in list1
//
// # Example
//
//	apocCollContainsAll([1, 2, 3, 4], [2, 3])  // true
//	apocCollContainsAll([1, 2, 3], [2, 5])    // false
func apocCollContainsAll(list1, list2 interface{}) bool {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return false
	}

	// Create lookup set from list1
	set1 := make(map[string]bool)
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		set1[key] = true
	}

	// Check all items in list2 are in set1
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		if !set1[key] {
			return false
		}
	}
	return true
}

// apocCollContainsAny checks if list contains any value from another list.
//
// # Parameters
//
//   - list1: The list to search in
//   - list2: The list of values to find
//
// # Returns
//
//   - true if any value from list2 is in list1
//
// # Example
//
//	apocCollContainsAny([1, 2, 3], [5, 2])  // true
//	apocCollContainsAny([1, 2, 3], [5, 6]) // false
func apocCollContainsAny(list1, list2 interface{}) bool {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return false
	}

	// Create lookup set from list1
	set1 := make(map[string]bool)
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		set1[key] = true
	}

	// Check if any item in list2 is in set1
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		if set1[key] {
			return true
		}
	}
	return false
}

// apocCollIndexOf finds the index of a value in a list.
//
// # Parameters
//
//   - listVal: The list to search
//   - value: The value to find
//
// # Returns
//
//   - The zero-based index, or -1 if not found
//
// # Example
//
//	apocCollIndexOf([1, 2, 3], 2)  // 1
//	apocCollIndexOf([1, 2, 3], 5)  // -1
func apocCollIndexOf(listVal, value interface{}) int64 {
	list, ok := listVal.([]interface{})
	if !ok {
		return -1
	}
	valKey := fmt.Sprintf("%T:%v", value, value)
	for i, item := range list {
		itemKey := fmt.Sprintf("%T:%v", item, item)
		if itemKey == valKey {
			return int64(i)
		}
	}
	return -1
}

// ========================================
// APOC Transformation Functions
// ========================================

// apocCollSplit splits a list by a separator value.
//
// # Parameters
//
//   - listVal: The list to split
//   - value: The separator value
//
// # Returns
//
//   - A list of sublists split by the separator
//
// # Example
//
//	apocCollSplit([1, 0, 2, 0, 3], 0)  // [[1], [2], [3]]
func apocCollSplit(listVal, value interface{}) []interface{} {
	list, ok := listVal.([]interface{})
	if !ok {
		return nil
	}

	var result []interface{}
	var current []interface{}
	valKey := fmt.Sprintf("%T:%v", value, value)

	for _, item := range list {
		itemKey := fmt.Sprintf("%T:%v", item, item)
		if itemKey == valKey {
			if len(current) > 0 {
				result = append(result, current)
				current = []interface{}{}
			}
		} else {
			current = append(current, item)
		}
	}
	if len(current) > 0 {
		result = append(result, current)
	}
	return result
}

// apocCollPartition splits a list into fixed-size chunks.
//
// # Parameters
//
//   - listVal: The list to partition
//   - sizeVal: The size of each partition
//
// # Returns
//
//   - A list of sublists of the specified size
//
// # Example
//
//	apocCollPartition([1, 2, 3, 4, 5], 2)  // [[1, 2], [3, 4], [5]]
func apocCollPartition(listVal, sizeVal interface{}) []interface{} {
	list, ok := listVal.([]interface{})
	if !ok {
		return nil
	}
	size := toInt64(sizeVal)
	if size <= 0 {
		return nil
	}

	var result []interface{}
	for i := int64(0); i < int64(len(list)); i += size {
		end := i + size
		if end > int64(len(list)) {
			end = int64(len(list))
		}
		result = append(result, list[i:end])
	}
	return result
}

// apocCollPairs creates [value, nextValue] pairs from a list.
//
// # Parameters
//
//   - val: The list to process
//
// # Returns
//
//   - A list of [current, next] pairs, last element paired with nil
//
// # Example
//
//	apocCollPairs([1, 2, 3])  // [[1, 2], [2, 3], [3, nil]]
func apocCollPairs(val interface{}) []interface{} {
	list, ok := val.([]interface{})
	if !ok {
		return nil
	}

	var result []interface{}
	for i := 0; i < len(list); i++ {
		var next interface{}
		if i+1 < len(list) {
			next = list[i+1]
		}
		result = append(result, []interface{}{list[i], next})
	}
	return result
}

// apocCollZip combines two lists into pairs.
//
// # Parameters
//
//   - list1: First list
//   - list2: Second list
//
// # Returns
//
//   - A list of [list1[i], list2[i]] pairs
//
// # Example
//
//	apocCollZip([1, 2], ["a", "b"])  // [[1, "a"], [2, "b"]]
func apocCollZip(list1, list2 interface{}) []interface{} {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return nil
	}

	// Use length of shorter list
	length := len(l1)
	if len(l2) < length {
		length = len(l2)
	}

	result := make([]interface{}, length)
	for i := 0; i < length; i++ {
		result[i] = []interface{}{l1[i], l2[i]}
	}
	return result
}

// ========================================
// APOC Analysis Functions
// ========================================

// apocCollFrequencies counts occurrences of each value.
//
// # Parameters
//
//   - val: The list to analyze
//
// # Returns
//
//   - A map of value -> count
//
// # Example
//
//	apocCollFrequencies([1, 2, 2, 3, 3, 3])  // {1: 1, 2: 2, 3: 3}
func apocCollFrequencies(val interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	list, ok := val.([]interface{})
	if !ok {
		return result
	}

	counts := make(map[string]int64)
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		counts[key]++
	}

	for k, v := range counts {
		result[k] = v
	}
	return result
}

// apocCollOccurrences counts occurrences of a specific value.
//
// # Parameters
//
//   - listVal: The list to search
//   - value: The value to count
//
// # Returns
//
//   - The count of occurrences
//
// # Example
//
//	apocCollOccurrences([1, 2, 2, 3], 2)  // 2
//	apocCollOccurrences([1, 2, 3], 5)    // 0
func apocCollOccurrences(listVal, value interface{}) int64 {
	list, ok := listVal.([]interface{})
	if !ok {
		return 0
	}
	valKey := fmt.Sprintf("%T:%v", value, value)
	var count int64
	for _, item := range list {
		itemKey := fmt.Sprintf("%T:%v", item, item)
		if itemKey == valKey {
			count++
		}
	}
	return count
}

// ========================================
// Type and Map Functions
// ========================================

// getCypherType returns the Cypher type name for a value.
//
// # Parameters
//
//   - val: The value to check
//
// # Returns
//
//   - The Cypher type name as a string
//
// # Example
//
//	getCypherType(42)         // "INTEGER"
//	getCypherType(3.14)       // "FLOAT"
//	getCypherType("hello")    // "STRING"
//	getCypherType(nil)        // "NULL"
func getCypherType(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case bool:
		return "BOOLEAN"
	case int, int32, int64:
		return "INTEGER"
	case float32, float64:
		return "FLOAT"
	case string:
		return "STRING"
	case []interface{}, []string:
		return "LIST"
	case map[string]interface{}:
		return "MAP"
	case *storage.Node:
		return "NODE"
	case *storage.Edge:
		return "RELATIONSHIP"
	case *CypherDuration:
		return "DURATION"
	default:
		_ = v
		return "ANY"
	}
}

// mergeMaps merges two maps, with map2 values overriding map1.
//
// # Parameters
//
//   - map1: The base map
//   - map2: The map to merge (overrides map1)
//
// # Returns
//
//   - A new map with merged values
//
// # Example
//
//	mergeMaps({a: 1, b: 2}, {b: 3, c: 4})  // {a: 1, b: 3, c: 4}
func mergeMaps(map1, map2 interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy map1
	if m1, ok := map1.(map[string]interface{}); ok {
		for k, v := range m1 {
			result[k] = v
		}
	}

	// Override with map2
	if m2, ok := map2.(map[string]interface{}); ok {
		for k, v := range m2 {
			result[k] = v
		}
	}

	return result
}

// fromPairs creates a map from a list of [key, value] pairs.
//
// # Parameters
//
//   - val: A list of [key, value] pairs
//
// # Returns
//
//   - A map constructed from the pairs
//
// # Example
//
//	fromPairs([["name", "Alice"], ["age", 30]])  // {name: "Alice", age: 30}
func fromPairs(val interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	list, ok := val.([]interface{})
	if !ok {
		return result
	}

	for _, item := range list {
		pair, ok := item.([]interface{})
		if !ok || len(pair) < 2 {
			continue
		}
		key, ok := pair[0].(string)
		if !ok {
			key = fmt.Sprintf("%v", pair[0])
		}
		result[key] = pair[1]
	}

	return result
}

// fromLists creates a map from parallel lists of keys and values.
//
// # Parameters
//
//   - keys: List of keys (strings)
//   - values: List of corresponding values
//
// # Returns
//
//   - A map where keys[i] maps to values[i]
//
// # Example
//
//	fromLists(["name", "age"], ["Alice", 30])  // {name: "Alice", age: 30}
func fromLists(keys, values interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	keyList, ok1 := keys.([]interface{})
	valList, ok2 := values.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	for i := 0; i < len(keyList) && i < len(valList); i++ {
		key, ok := keyList[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyList[i])
		}
		result[key] = valList[i]
	}

	return result
}
