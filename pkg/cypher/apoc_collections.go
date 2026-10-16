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
	return flattenListWithCancellation(val, recursive, nil)
}

func flattenListWithCancellation(val interface{}, recursive bool, probe *pipelineCancellation) []interface{} {
	depth := int64(1)
	if recursive {
		depth = math.MaxInt64
	}
	var result []interface{}
	switch v := val.(type) {
	case nil:
		return []interface{}{}
	case []interface{}:
		return collFlattenWithCancellation(make([]interface{}, 0, pipelineInitialCapacity(len(v), probe)), v, depth, probe)
	case []string:
		for _, s := range v {
			if probe != nil && !probe.check() {
				return nil
			}
			result = appendPipelineItem[interface{}](result, s, probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
	default:
		result = appendPipelineItem[interface{}](result, val, probe)
		if probe != nil && probe.err != nil {
			return nil
		}

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
	return toSetWithCancellation(val, nil)
}

func toSetWithCancellation(val interface{}, probe *pipelineCancellation) []interface{} {
	var result []interface{}
	seen := pipelineStringSet{probe: probe}

	addUnique := func(item interface{}) {
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return
		}
		if !seen.add(key) {
			if probe != nil && probe.err != nil {
				return
			}
			result = appendPipelineItem[interface{}](result, item, probe)
			if probe != nil && probe.err != nil {
				return
			}

		}
	}

	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			if probe != nil && !probe.check() {
				return nil
			}
			addUnique(item)
			if probe != nil && probe.err != nil {
				return nil
			}
		}
	case []string:
		for _, s := range v {
			if probe != nil && !probe.check() {
				return nil
			}
			addUnique(s)
			if probe != nil && probe.err != nil {
				return nil
			}
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
	return apocCollSumWithCancellation(val, nil)
}

func apocCollSumWithCancellation(val interface{}, probe *pipelineCancellation) float64 {
	var sum float64
	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			if probe != nil && !probe.check() {
				return 0
			}
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
	return apocCollAvgWithCancellation(val, nil)
}

func apocCollAvgWithCancellation(val interface{}, probe *pipelineCancellation) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		var sum float64
		for _, item := range v {
			if probe != nil && !probe.check() {
				return nil
			}
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
	return apocCollMinWithCancellation(val, nil)
}

func apocCollMinWithCancellation(val interface{}, probe *pipelineCancellation) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		min := v[0]
		minFloat, _ := toFloat64(min)
		for _, item := range v[1:] {
			if probe != nil && !probe.check() {
				return nil
			}
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
	return apocCollMaxWithCancellation(val, nil)
}

func apocCollMaxWithCancellation(val interface{}, probe *pipelineCancellation) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		max := v[0]
		maxFloat, _ := toFloat64(max)
		for _, item := range v[1:] {
			if probe != nil && !probe.check() {
				return nil
			}
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
	return apocCollSortWithCancellation(val, nil)
}

func apocCollSortWithCancellation(val interface{}, probe *pipelineCancellation) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		// In Cypher's order, as coll.sort and ORDER BY sort (strings too;
		// null last).
		return sortedCypherValuesWithCancellation(v, probe)
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
	return apocCollSortNodesWithCancellation(val, propName, nil)
}

func apocCollSortNodesWithCancellation(val interface{}, propName string, probe *pipelineCancellation) []interface{} {
	return apocCollSortNodesByPropertyWithCancellation(val, propName, getNodeProperty, probe)
}

// The query route supplies its visible property reader while sorting the raw
// entity bindings, so later mutations cannot persist a filtered property map.
func apocCollSortNodesByProperty(val interface{}, propName string, property func(interface{}, string) interface{}) []interface{} {
	return apocCollSortNodesByPropertyWithCancellation(val, propName, property, nil)
}

func apocCollSortNodesByPropertyWithCancellation(val interface{}, propName string, property func(interface{}, string) interface{}, probe *pipelineCancellation) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, 0, pipelineInitialCapacity(len(v), probe))
		result = appendPipelineValues(result, v, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		// Sort by property value
		for i := 0; i < len(result)-1; i++ {
			if probe != nil && !probe.check() {
				return nil
			}
			for j := i + 1; j < len(result); j++ {
				if probe != nil && !probe.check() {
					return nil
				}
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
	return apocCollReverseWithCancellation(val, nil)
}

func apocCollReverseWithCancellation(val interface{}, probe *pipelineCancellation) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, 0, pipelineInitialCapacity(len(v), probe))
		for i := range v {
			if probe != nil && !probe.check() {
				return nil
			}
			result = appendPipelineItem[interface{}](result, v[len(v)-1-i], probe)
			if probe != nil && probe.err != nil {
				return nil
			}

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
	return apocCollUnionWithCancellation(list1, list2, nil)
}

func apocCollUnionWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) []interface{} {
	combined := apocCollUnionAllWithCancellation(list1, list2, probe)
	if probe != nil && probe.err != nil {
		return nil
	}
	return toSetWithCancellation(combined, probe)
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
	return apocCollUnionAllWithCancellation(list1, list2, nil)
}

func apocCollUnionAllWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) []interface{} {
	var result []interface{}
	if l1, ok := list1.([]interface{}); ok {
		result = appendPipelineValues(result, l1, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
	}
	if l2, ok := list2.([]interface{}); ok {
		result = appendPipelineValues(result, l2, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
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
	return apocCollIntersectionWithCancellation(list1, list2, nil)
}

func apocCollIntersectionWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) []interface{} {
	var result []interface{}
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	// Create lookup set from list2
	set2 := pipelineStringSet{probe: probe}
	for _, item := range l2 {
		if probe != nil && !probe.check() {
			return nil
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		set2.add(key)
		if probe != nil && probe.err != nil {
			return nil
		}
	}

	// Find elements in list1 that are also in list2
	seen := pipelineStringSet{probe: probe}
	for _, item := range l1 {
		if probe != nil && !probe.check() {
			return nil
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		present := set2.contains(key)
		if probe != nil && probe.err != nil {
			return nil
		}
		if !present {
			continue
		}
		duplicate := seen.add(key)
		if probe != nil && probe.err != nil {
			return nil
		}
		if !duplicate {
			result = appendPipelineItem[interface{}](result, item, probe)
			if probe != nil && probe.err != nil {
				return nil
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
func apocCollSubtract(list1, list2 interface{}) []interface{} {
	return apocCollSubtractWithCancellation(list1, list2, nil)
}

func apocCollSubtractWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) []interface{} {
	var result []interface{}
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	// Create lookup set from list2
	set2 := pipelineStringSet{probe: probe}
	for _, item := range l2 {
		if probe != nil && !probe.check() {
			return nil
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		set2.add(key)
		if probe != nil && probe.err != nil {
			return nil
		}
	}

	// Find elements in list1 that are NOT in list2
	for _, item := range l1 {
		if probe != nil && !probe.check() {
			return nil
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		present := set2.contains(key)
		if probe != nil && probe.err != nil {
			return nil
		}
		if !present {
			result = appendPipelineItem[interface{}](result, item, probe)
			if probe != nil && probe.err != nil {
				return nil
			}

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
	return apocCollContainsWithCancellation(listVal, value, nil)
}

func apocCollContainsWithCancellation(listVal, value interface{}, probe *pipelineCancellation) bool {
	list, ok := listVal.([]interface{})
	if !ok {
		return false
	}
	valKey := pipelineDisplayKey(value, probe)
	if probe != nil && probe.err != nil {
		return false
	}
	for _, item := range list {
		if probe != nil && !probe.check() {
			return false
		}
		itemKey := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return false
		}
		if pipelineStringsEqual(itemKey, valKey, probe) {
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
	return apocCollContainsAllWithCancellation(list1, list2, nil)
}

func apocCollContainsAllWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) bool {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return false
	}

	// Create lookup set from list1
	set1 := pipelineStringSet{probe: probe}
	for _, item := range l1 {
		if probe != nil && !probe.check() {
			return false
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return false
		}
		set1.add(key)
		if probe != nil && probe.err != nil {
			return false
		}
	}

	// Check all items in list2 are in set1
	for _, item := range l2 {
		if probe != nil && !probe.check() {
			return false
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return false
		}
		present := set1.contains(key)
		if probe != nil && probe.err != nil {
			return false
		}
		if !present {
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
	return apocCollContainsAnyWithCancellation(list1, list2, nil)
}

func apocCollContainsAnyWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) bool {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return false
	}

	// Create lookup set from list1
	set1 := pipelineStringSet{probe: probe}
	for _, item := range l1 {
		if probe != nil && !probe.check() {
			return false
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return false
		}
		set1.add(key)
		if probe != nil && probe.err != nil {
			return false
		}
	}

	// Check if any item in list2 is in set1
	for _, item := range l2 {
		if probe != nil && !probe.check() {
			return false
		}
		key := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return false
		}
		present := set1.contains(key)
		if probe != nil && probe.err != nil {
			return false
		}
		if present {
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
	return apocCollIndexOfWithCancellation(listVal, value, nil)
}

func apocCollIndexOfWithCancellation(listVal, value interface{}, probe *pipelineCancellation) int64 {
	list, ok := listVal.([]interface{})
	if !ok {
		return -1
	}
	valKey := pipelineDisplayKey(value, probe)
	if probe != nil && probe.err != nil {
		return 0
	}
	for i, item := range list {
		if probe != nil && !probe.check() {
			return 0
		}
		itemKey := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return 0
		}
		if pipelineStringsEqual(itemKey, valKey, probe) {
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
	return apocCollSplitWithCancellation(listVal, value, nil)
}

func apocCollSplitWithCancellation(listVal, value interface{}, probe *pipelineCancellation) []interface{} {
	list, ok := listVal.([]interface{})
	if !ok {
		return nil
	}

	var result []interface{}
	var current []interface{}
	valKey := pipelineDisplayKey(value, probe)
	if probe != nil && probe.err != nil {
		return nil
	}

	for _, item := range list {
		if probe != nil && !probe.check() {
			return nil
		}
		itemKey := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		if pipelineStringsEqual(itemKey, valKey, probe) {
			if len(current) > 0 {
				result = appendPipelineItem[interface{}](result, current, probe)
				if probe != nil && probe.err != nil {
					return nil
				}

				current = []interface{}{}
			}
		} else {
			current = appendPipelineItem[interface{}](current, item, probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
	}
	if len(current) > 0 {
		result = appendPipelineItem[interface{}](result, current, probe)
		if probe != nil && probe.err != nil {
			return nil
		}

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
	return apocCollPartitionWithCancellation(listVal, sizeVal, nil)
}

func apocCollPartitionWithCancellation(listVal, sizeVal interface{}, probe *pipelineCancellation) []interface{} {
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
		if probe != nil && !probe.check() {
			return nil
		}
		end := i + size
		if end > int64(len(list)) {
			end = int64(len(list))
		}
		result = appendPipelineItem[interface{}](result, list[i:end], probe)
		if probe != nil && probe.err != nil {
			return nil
		}

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
	return apocCollPairsWithCancellation(val, nil)
}

func apocCollPairsWithCancellation(val interface{}, probe *pipelineCancellation) []interface{} {
	list, ok := val.([]interface{})
	if !ok {
		return nil
	}

	var result []interface{}
	for i := 0; i < len(list); i++ {
		if probe != nil && !probe.check() {
			return nil
		}
		var next interface{}
		if i+1 < len(list) {
			next = list[i+1]
		}
		result = appendPipelineItem[interface{}](result, []interface{}{list[i], next}, probe)
		if probe != nil && probe.err != nil {
			return nil
		}

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
	return apocCollZipWithCancellation(list1, list2, nil)
}

func apocCollZipWithCancellation(list1, list2 interface{}, probe *pipelineCancellation) []interface{} {
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
		if probe != nil && !probe.check() {
			return nil
		}
		result[i] = []interface{}{l1[i], l2[i]}
	}
	return result
}

// ========================================
// APOC Analysis Functions
// ========================================

// apocSameValue reports whether apoc.map.clean's value list holds value: an
// equal value of the same kind, as APOC compares (2 is not 2.0) (#1164).
func apocSameValue(value, listed interface{}) bool {
	return apocSameValueWithCancellation(value, listed, nil)
}

func apocSameValueWithCancellation(value, listed interface{}, probe *pipelineCancellation) bool {
	if equal, _ := cypherEqualityWithCancellation(value, listed, probe).(bool); !equal || probe != nil && probe.err != nil {
		return false
	}
	_, valueFloat := value.(float64)
	_, listedFloat := listed.(float64)
	return valueFloat == listedFloat
}

// apocCollFrequencies is apoc.coll.frequencies(list): each distinct item
// with its count, {item, count}, in the order the items first appear, as in
// APOC (#1164). Items equal in Cypher (1 and 1.0, [1] and [1]) count
// together, under the first one.
//
// # Example
//
//	apocCollFrequencies([3, 1, 3])  // [{item: 3, count: 2}, {item: 1, count: 1}]
func apocCollFrequencies(val interface{}) []interface{} {
	return apocCollFrequenciesWithCancellation(val, nil)
}

func apocCollFrequenciesWithCancellation(val interface{}, probe *pipelineCancellation) []interface{} {
	list, _ := val.([]interface{})
	result := make([]interface{}, 0, pipelineInitialCapacity(len(list), probe))
	positions := cypherValueIndex{probe: probe}
	for _, item := range list {
		if probe != nil && !probe.check() {
			return nil
		}
		if position, seen := positions.add(item); seen {
			entry := result[position].(map[string]interface{})
			entry["count"] = entry["count"].(int64) + 1
			continue
		}
		result = appendPipelineItem[interface{}](result, map[string]interface{}{"item": item, "count": int64(1)}, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
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
	return apocCollOccurrencesWithCancellation(listVal, value, nil)
}

func apocCollOccurrencesWithCancellation(listVal, value interface{}, probe *pipelineCancellation) int64 {
	list, ok := listVal.([]interface{})
	if !ok {
		return 0
	}
	valKey := pipelineDisplayKey(value, probe)
	if probe != nil && probe.err != nil {
		return 0
	}
	var count int64
	for _, item := range list {
		if probe != nil && !probe.check() {
			return 0
		}
		itemKey := pipelineDisplayKey(item, probe)
		if probe != nil && probe.err != nil {
			return 0
		}
		if pipelineStringsEqual(itemKey, valKey, probe) {
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
	return mergeMapsWithCancellation(map1, map2, nil)
}

func mergeMapsWithCancellation(map1, map2 interface{}, probe *pipelineCancellation) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy map1
	if m1, ok := map1.(map[string]interface{}); ok {
		for k, v := range m1 {
			if probe != nil && !probe.check() {
				return nil
			}
			result[k] = v
		}
	}

	// Override with map2
	if m2, ok := map2.(map[string]interface{}); ok {
		for k, v := range m2 {
			if probe != nil && !probe.check() {
				return nil
			}
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
	return fromPairsWithCancellation(val, nil)
}

func fromPairsWithCancellation(val interface{}, probe *pipelineCancellation) map[string]interface{} {
	return fromPairsWithKeyPolicy(val, true, probe)
}

func fromPairsWithKeyPolicy(val interface{}, formatKeys bool, probe *pipelineCancellation) map[string]interface{} {
	result := make(map[string]interface{})

	list, ok := val.([]interface{})
	if !ok {
		return result
	}

	for _, item := range list {
		if probe != nil && !probe.check() {
			return nil
		}
		pair, ok := item.([]interface{})
		if !ok || len(pair) < 2 {
			continue
		}
		key, ok := pair[0].(string)
		if !ok {
			if !formatKeys {
				continue
			}
			key = pipelineDisplayString(pair[0], probe)
		}
		if probe != nil && probe.err != nil {
			return nil
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
	return fromListsWithCancellation(keys, values, nil)
}

func fromListsWithCancellation(keys, values interface{}, probe *pipelineCancellation) map[string]interface{} {
	result := make(map[string]interface{})

	keyList, ok1 := keys.([]interface{})
	valList, ok2 := values.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	for i := 0; i < len(keyList) && i < len(valList); i++ {
		if probe != nil && !probe.check() {
			return nil
		}
		key, ok := keyList[i].(string)
		if !ok {
			key = pipelineDisplayString(keyList[i], probe)
		}
		if probe != nil && probe.err != nil {
			return nil
		}
		result[key] = valList[i]
	}

	return result
}
