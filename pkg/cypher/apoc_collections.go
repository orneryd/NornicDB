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
	"math"
)

// ========================================
// List Utility Functions
// ========================================

// flattenList is apoc.coll.flatten(list, recursive): the list with its
// nested lists spliced in one level, or every level when recursive, as in
// APOC (#1164). A null list is empty.
//
// # Example
//
//	flattenList([[1, [2]], [3]], false)  // [1, [2], 3]
//	flattenList([[1, [2]], [3]], true)   // [1, 2, 3]
func flattenList(val interface{}, recursive bool) []interface{} {
	depth := int64(1)
	if recursive {
		depth = math.MaxInt64
	}
	var result []interface{}
	switch v := val.(type) {
	case nil:
		return []interface{}{}
	case []interface{}:
		return collFlatten(make([]interface{}, 0, len(v)), v, depth)
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
//
// A list of any element type gives a list, an empty one too; anything else
// gives an empty list. Two items are duplicates when they are equal as
// APOC's set of AnyValues compares them (cypherValueIndex): 1 and 1.0 are
// one item, lists and maps compare element by element, a null is one item,
// and every NaN is its own.
func toSet(val interface{}) []interface{} {
	items, _ := cypherListValue(val)
	result := make([]interface{}, 0, len(items))
	var seen cypherValueIndex
	for _, item := range items {
		if _, duplicate := seen.addByContract(item, cypherEquivalenceContract); !duplicate {
			result = append(result, item)
		}
	}
	return result
}

// apocCollToSet is apoc.coll.toSet(list): toSet, and null for null, as
// APOC gives it.
func apocCollToSet(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	return toSet(val)
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
		// In Cypher's order, as coll.sort and ORDER BY sort (strings too;
		// null last).
		return sortedCypherValues(v)
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
	return apocCollSortNodesByProperty(val, propName, getNodeProperty)
}

// The query route supplies its visible property reader while sorting the raw
// entity bindings, so later mutations cannot persist a filtered property map.
func apocCollSortNodesByProperty(val interface{}, propName string, property func(interface{}, string) interface{}) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		copy(result, v)
		// Sort by property value
		for i := 0; i < len(result)-1; i++ {
			for j := i + 1; j < len(result); j++ {
				vi := property(result[i], propName)
				vj := property(result[j], propName)
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
	property, _ := rowPropertyValue(node, propName)
	return property
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
//
// Both lists are sets of AnyValues, as in APOC (cypherValueIndex; toSet):
// each common item once, in list1's order.
func apocCollIntersection(list1, list2 interface{}) []interface{} {
	var result []interface{}
	l1, ok1 := cypherListValue(list1)
	l2, ok2 := cypherListValue(list2)
	if !ok1 || !ok2 {
		return result
	}
	var in2, seen cypherValueIndex
	for _, item := range l2 {
		in2.addByContract(item, cypherEquivalenceContract)
	}
	for _, item := range l1 {
		if _, common := in2.findByContract(item, cypherEquivalenceContract); common {
			if _, duplicate := seen.addByContract(item, cypherEquivalenceContract); !duplicate {
				result = append(result, item)
			}
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
//
// Both lists are sets of AnyValues, as in APOC (cypherValueIndex; toSet):
// each of list1's items that list2 doesn't hold, once, in list1's order.
func apocCollSubtract(list1, list2 interface{}) []interface{} {
	var result []interface{}
	l1, ok1 := cypherListValue(list1)
	l2, ok2 := cypherListValue(list2)
	if !ok1 || !ok2 {
		return result
	}
	var in2, seen cypherValueIndex
	for _, item := range l2 {
		in2.addByContract(item, cypherEquivalenceContract)
	}
	for _, item := range l1 {
		if _, removed := in2.findByContract(item, cypherEquivalenceContract); removed {
			continue
		}
		if _, duplicate := seen.addByContract(item, cypherEquivalenceContract); !duplicate {
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
//
// Items compare by APOC's value equality (cypherValueEquals): a null is in
// no list.
func apocCollContains(listVal, value interface{}) bool {
	list, _ := cypherListValue(listVal)
	for _, item := range list {
		if cypherValueEquals(item, value) {
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
//
// Items compare by APOC's value equality (cypherValueEquals): a null is in
// no list. As in APOC, an empty list1 contains nothing, not even [].
func apocCollContainsAll(list1, list2 interface{}) bool {
	l1, ok1 := cypherListValue(list1)
	l2, ok2 := cypherListValue(list2)
	if !ok1 || !ok2 || len(l1) == 0 {
		return false
	}
	in1 := apocValueSet(l1)
	for _, item := range l2 {
		if _, found := in1.findByContract(item, cypherValueEqualityContract); item == nil || !found {
			return false
		}
	}
	return true
}

// apocValueSet is a list's non-null items as a set, for lookups by APOC's
// value equality (cypherValueEquals): a null finds nothing.
func apocValueSet(list []interface{}) *cypherValueIndex {
	set := cypherValueIndex{}
	for _, item := range list {
		if item != nil {
			set.addByContract(item, cypherValueEqualityContract)
		}
	}
	return &set
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
//
// A NornicDB function (APOC 5.26 has no containsAny); items compare as in
// apoc.coll.containsAll (cypherValueEquals).
func apocCollContainsAny(list1, list2 interface{}) bool {
	l1, ok1 := cypherListValue(list1)
	l2, ok2 := cypherListValue(list2)
	if !ok1 || !ok2 {
		return false
	}
	in1 := apocValueSet(l1)
	for _, item := range l2 {
		if _, found := in1.findByContract(item, cypherValueEqualityContract); item != nil && found {
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
//
// Items compare by APOC's value equality (cypherValueEquals): a null is
// never found.
func apocCollIndexOf(listVal, value interface{}) int64 {
	list, _ := cypherListValue(listVal)
	for i, item := range list {
		if cypherValueEquals(item, value) {
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
//
// Items compare by APOC's value equality (cypherValueEquals): a null
// separates nothing.
func apocCollSplit(listVal, value interface{}) []interface{} {
	list, ok := cypherListValue(listVal)
	if !ok {
		return nil
	}

	var result []interface{}
	var current []interface{}

	for _, item := range list {
		if cypherValueEquals(item, value) {
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

// apocSameValue uses Java-object removal-set equivalence: overlap occurrences
// and numeric representations stay distinct, including nested values (#1164).
func apocSameValue(value, listed interface{}) bool {
	if cypherValueKindOf(value) == valueKindNull || cypherValueKindOf(listed) == valueKindNull {
		return false
	}
	_, valueFloat := value.(float64)
	_, listedFloat := listed.(float64)
	if valueFloat != listedFloat {
		return false
	}
	if equal, numeric := cypherNumericEquality(value, listed); numeric {
		return equal
	}
	switch typed := value.(type) {
	case string:
		other, ok := listed.(string)
		return ok && typed == other
	case bool:
		other, ok := listed.(bool)
		return ok && typed == other
	}
	if equal, temporal := compareTemporalByContract(value, listed, cypherEquivalenceContract); temporal {
		return equal
	}
	return cypherValueKeyWithNumericPolicy(value, cypherNumericKinds, cypherEquivalenceContract) ==
		cypherValueKeyWithNumericPolicy(listed, cypherNumericKinds, cypherEquivalenceContract)
}

// apocCollFrequencies is apoc.coll.frequencies(list): each distinct item
// with its count, {item, count}, in the order the items first appear, as in
// APOC (#1164). Equivalent items count together under the first one;
// named-zone overlap occurrences remain separate even when equal with =.
//
// # Example
//
//	apocCollFrequencies([3, 1, 3])  // [{item: 3, count: 2}, {item: 1, count: 1}]
func apocCollFrequencies(val interface{}) []interface{} {
	list, _ := val.([]interface{})
	result := make([]interface{}, 0, len(list))
	var positions cypherValueIndex
	for _, item := range list {
		if position, seen := positions.addByContract(item, cypherEquivalenceContract); seen {
			entry := result[position].(map[string]interface{})
			entry["count"] = entry["count"].(int64) + 1
			continue
		}
		result = append(result, map[string]interface{}{"item": item, "count": int64(1)})
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
//
// Items compare by APOC's value equality (cypherValueEquals): a null is
// counted nowhere.
func apocCollOccurrences(listVal, value interface{}) int64 {
	list, _ := cypherListValue(listVal)
	var count int64
	for _, item := range list {
		if cypherValueEquals(item, value) {
			count++
		}
	}
	return count
}

// ========================================
// Type and Map Functions
// ========================================

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
