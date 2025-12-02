// Package coll provides APOC collection manipulation functions.
//
// This package implements all apoc.coll.* functions for list/collection
// processing in Cypher queries.
package coll

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

// Sum returns the sum of all numeric values in a list.
// Non-numeric values are ignored.
//
// Example:
//   apoc.coll.sum([1, 2, 3, 4, 5]) => 15
//   apoc.coll.sum([1.5, 2.5, 3.0]) => 7.0
func Sum(list []interface{}) float64 {
	var sum float64
	for _, item := range list {
		if num, ok := toFloat64(item); ok {
			sum += num
		}
	}
	return sum
}

// Avg returns the average of all numeric values in a list.
// Non-numeric values are ignored.
//
// Example:
//   apoc.coll.avg([1, 2, 3, 4, 5]) => 3.0
//   apoc.coll.avg([10, 20, 30]) => 20.0
func Avg(list []interface{}) float64 {
	var sum float64
	var count int
	for _, item := range list {
		if num, ok := toFloat64(item); ok {
			sum += num
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// Min returns the minimum value in a list.
// Works with numbers and strings.
//
// Example:
//   apoc.coll.min([5, 2, 8, 1, 9]) => 1
//   apoc.coll.min(['zebra', 'apple', 'banana']) => 'apple'
func Min(list []interface{}) interface{} {
	if len(list) == 0 {
		return nil
	}
	
	min := list[0]
	for i := 1; i < len(list); i++ {
		if compare(list[i], min) < 0 {
			min = list[i]
		}
	}
	return min
}

// Max returns the maximum value in a list.
// Works with numbers and strings.
//
// Example:
//   apoc.coll.max([5, 2, 8, 1, 9]) => 9
//   apoc.coll.max(['zebra', 'apple', 'banana']) => 'zebra'
func Max(list []interface{}) interface{} {
	if len(list) == 0 {
		return nil
	}
	
	max := list[0]
	for i := 1; i < len(list); i++ {
		if compare(list[i], max) > 0 {
			max = list[i]
		}
	}
	return max
}

// Partition divides a list into two lists based on a predicate.
// Returns [matching, notMatching].
//
// Example:
//   apoc.coll.partition([1,2,3,4,5], 'x', 'x > 3') => [[4,5], [1,2,3]]
func Partition(list []interface{}, predicate func(interface{}) bool) [][]interface{} {
	matching := make([]interface{}, 0)
	notMatching := make([]interface{}, 0)
	
	for _, item := range list {
		if predicate(item) {
			matching = append(matching, item)
		} else {
			notMatching = append(notMatching, item)
		}
	}
	
	return [][]interface{}{matching, notMatching}
}

// Zip combines multiple lists into a list of lists.
//
// Example:
//   apoc.coll.zip([1,2,3], ['a','b','c']) => [[1,'a'], [2,'b'], [3,'c']]
func Zip(lists ...[]interface{}) [][]interface{} {
	if len(lists) == 0 {
		return [][]interface{}{}
	}
	
	minLen := len(lists[0])
	for _, list := range lists[1:] {
		if len(list) < minLen {
			minLen = len(list)
		}
	}
	
	result := make([][]interface{}, minLen)
	for i := 0; i < minLen; i++ {
		row := make([]interface{}, len(lists))
		for j, list := range lists {
			row[j] = list[i]
		}
		result[i] = row
	}
	
	return result
}

// Pairs returns consecutive pairs from a list.
//
// Example:
//   apoc.coll.pairs([1,2,3,4]) => [[1,2], [2,3], [3,4]]
func Pairs(list []interface{}) [][]interface{} {
	if len(list) < 2 {
		return [][]interface{}{}
	}
	
	result := make([][]interface{}, len(list)-1)
	for i := 0; i < len(list)-1; i++ {
		result[i] = []interface{}{list[i], list[i+1]}
	}
	return result
}

// PairsMin returns non-overlapping pairs from a list.
//
// Example:
//   apoc.coll.pairsMin([1,2,3,4,5]) => [[1,2], [3,4]]
func PairsMin(list []interface{}) [][]interface{} {
	result := make([][]interface{}, 0)
	for i := 0; i < len(list)-1; i += 2 {
		result = append(result, []interface{}{list[i], list[i+1]})
	}
	return result
}

// ToSet removes duplicates from a list, preserving order.
//
// Example:
//   apoc.coll.toSet([1,2,1,3,2,4]) => [1,2,3,4]
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

// Sort sorts a list in ascending order.
//
// Example:
//   apoc.coll.sort([3,1,4,1,5,9,2,6]) => [1,1,2,3,4,5,6,9]
func Sort(list []interface{}) []interface{} {
	result := make([]interface{}, len(list))
	copy(result, list)
	
	sort.Slice(result, func(i, j int) bool {
		return compare(result[i], result[j]) < 0
	})
	
	return result
}

// SortMaps sorts a list of maps by a given key.
//
// Example:
//   apoc.coll.sortMaps([{age:30},{age:20},{age:25}], 'age') 
//   => [{age:20},{age:25},{age:30}]
func SortMaps(list []interface{}, key string) []interface{} {
	result := make([]interface{}, len(list))
	copy(result, list)
	
	sort.Slice(result, func(i, j int) bool {
		mi, oki := result[i].(map[string]interface{})
		mj, okj := result[j].(map[string]interface{})
		if !oki || !okj {
			return false
		}
		return compare(mi[key], mj[key]) < 0
	})
	
	return result
}

// Reverse reverses a list.
//
// Example:
//   apoc.coll.reverse([1,2,3,4,5]) => [5,4,3,2,1]
func Reverse(list []interface{}) []interface{} {
	result := make([]interface{}, len(list))
	for i, item := range list {
		result[len(list)-1-i] = item
	}
	return result
}

// Contains checks if a list contains a value.
//
// Example:
//   apoc.coll.contains([1,2,3,4,5], 3) => true
//   apoc.coll.contains([1,2,3,4,5], 6) => false
func Contains(list []interface{}, value interface{}) bool {
	for _, item := range list {
		if reflect.DeepEqual(item, value) {
			return true
		}
	}
	return false
}

// ContainsAll checks if a list contains all values from another list.
//
// Example:
//   apoc.coll.containsAll([1,2,3,4,5], [2,4]) => true
//   apoc.coll.containsAll([1,2,3], [2,6]) => false
func ContainsAll(list []interface{}, values []interface{}) bool {
	for _, value := range values {
		if !Contains(list, value) {
			return false
		}
	}
	return true
}

// ContainsAny checks if a list contains any value from another list.
//
// Example:
//   apoc.coll.containsAny([1,2,3], [3,4,5]) => true
//   apoc.coll.containsAny([1,2,3], [4,5,6]) => false
func ContainsAny(list []interface{}, values []interface{}) bool {
	for _, value := range values {
		if Contains(list, value) {
			return true
		}
	}
	return false
}

// ContainsDuplicates checks if a list has any duplicate values.
//
// Example:
//   apoc.coll.containsDuplicates([1,2,3,2,4]) => true
//   apoc.coll.containsDuplicates([1,2,3,4,5]) => false
func ContainsDuplicates(list []interface{}) bool {
	seen := make(map[string]bool)
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		if seen[key] {
			return true
		}
		seen[key] = true
	}
	return false
}

// ContainsSorted checks if a value exists in a sorted list (binary search).
//
// Example:
//   apoc.coll.containsSorted([1,2,3,4,5], 3) => true
func ContainsSorted(list []interface{}, value interface{}) bool {
	left, right := 0, len(list)-1
	
	for left <= right {
		mid := (left + right) / 2
		cmp := compare(list[mid], value)
		
		if cmp == 0 {
			return true
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	
	return false
}

// Different returns elements in list1 that are not in list2.
//
// Example:
//   apoc.coll.different([1,2,3,4], [2,4,6]) => [1,3]
func Different(list1, list2 []interface{}) []interface{} {
	set2 := make(map[string]bool)
	for _, item := range list2 {
		set2[fmt.Sprintf("%v", item)] = true
	}
	
	result := make([]interface{}, 0)
	for _, item := range list1 {
		if !set2[fmt.Sprintf("%v", item)] {
			result = append(result, item)
		}
	}
	
	return result
}

// Disjunction returns elements that are in either list but not both.
//
// Example:
//   apoc.coll.disjunction([1,2,3], [2,3,4]) => [1,4]
func Disjunction(list1, list2 []interface{}) []interface{} {
	set1 := make(map[string]bool)
	set2 := make(map[string]bool)
	
	for _, item := range list1 {
		set1[fmt.Sprintf("%v", item)] = true
	}
	for _, item := range list2 {
		set2[fmt.Sprintf("%v", item)] = true
	}
	
	result := make([]interface{}, 0)
	for _, item := range list1 {
		key := fmt.Sprintf("%v", item)
		if !set2[key] {
			result = append(result, item)
		}
	}
	for _, item := range list2 {
		key := fmt.Sprintf("%v", item)
		if !set1[key] {
			result = append(result, item)
		}
	}
	
	return ToSet(result)
}

// DropDuplicateNeighbors removes consecutive duplicate values.
//
// Example:
//   apoc.coll.dropDuplicateNeighbors([1,1,2,2,3,3,2,2]) => [1,2,3,2]
func DropDuplicateNeighbors(list []interface{}) []interface{} {
	if len(list) == 0 {
		return []interface{}{}
	}
	
	result := []interface{}{list[0]}
	for i := 1; i < len(list); i++ {
		if !reflect.DeepEqual(list[i], list[i-1]) {
			result = append(result, list[i])
		}
	}
	
	return result
}

// Duplicates returns all duplicate values in a list.
//
// Example:
//   apoc.coll.duplicates([1,2,3,2,4,3,5]) => [2,3]
func Duplicates(list []interface{}) []interface{} {
	counts := make(map[string]int)
	order := make(map[string]interface{})
	
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		counts[key]++
		if counts[key] == 1 {
			order[key] = item
		}
	}
	
	result := make([]interface{}, 0)
	for key, count := range counts {
		if count > 1 {
			result = append(result, order[key])
		}
	}
	
	return result
}

// DuplicatesWithCount returns duplicate values with their counts.
//
// Example:
//   apoc.coll.duplicatesWithCount([1,2,3,2,4,3,3]) 
//   => [{item:2,count:2},{item:3,count:3}]
func DuplicatesWithCount(list []interface{}) []map[string]interface{} {
	counts := make(map[string]int)
	order := make(map[string]interface{})
	
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		counts[key]++
		if counts[key] == 1 {
			order[key] = item
		}
	}
	
	result := make([]map[string]interface{}, 0)
	for key, count := range counts {
		if count > 1 {
			result = append(result, map[string]interface{}{
				"item":  order[key],
				"count": count,
			})
		}
	}
	
	return result
}

// Fill creates a list with a value repeated n times.
//
// Example:
//   apoc.coll.fill('x', 5) => ['x','x','x','x','x']
func Fill(item interface{}, count int) []interface{} {
	result := make([]interface{}, count)
	for i := 0; i < count; i++ {
		result[i] = item
	}
	return result
}

// Flatten flattens nested lists into a single list.
//
// Example:
//   apoc.coll.flatten([[1,2],[3,4],[5]]) => [1,2,3,4,5]
//   apoc.coll.flatten([[1,[2,3]],4]) => [1,2,3,4]
func Flatten(list []interface{}, recursive bool) []interface{} {
	result := make([]interface{}, 0)
	
	for _, item := range list {
		if sublist, ok := item.([]interface{}); ok {
			if recursive {
				result = append(result, Flatten(sublist, true)...)
			} else {
				result = append(result, sublist...)
			}
		} else {
			result = append(result, item)
		}
	}
	
	return result
}

// Frequencies returns a map of values to their occurrence counts.
//
// Example:
//   apoc.coll.frequencies([1,2,2,3,3,3]) 
//   => {1:1, 2:2, 3:3}
func Frequencies(list []interface{}) map[string]int {
	result := make(map[string]int)
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		result[key]++
	}
	return result
}

// FrequenciesAsMap returns frequency data as a list of maps.
//
// Example:
//   apoc.coll.frequenciesAsMap([1,2,2,3,3,3])
//   => [{item:1,count:1},{item:2,count:2},{item:3,count:3}]
func FrequenciesAsMap(list []interface{}) []map[string]interface{} {
	counts := make(map[string]int)
	order := make([]string, 0)
	items := make(map[string]interface{})
	
	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		if counts[key] == 0 {
			order = append(order, key)
			items[key] = item
		}
		counts[key]++
	}
	
	result := make([]map[string]interface{}, 0)
	for _, key := range order {
		result = append(result, map[string]interface{}{
			"item":  items[key],
			"count": counts[key],
		})
	}
	
	return result
}

// IndexOf returns the index of the first occurrence of a value.
// Returns -1 if not found.
//
// Example:
//   apoc.coll.indexOf([1,2,3,4,5], 3) => 2
//   apoc.coll.indexOf([1,2,3,4,5], 6) => -1
func IndexOf(list []interface{}, value interface{}) int {
	for i, item := range list {
		if reflect.DeepEqual(item, value) {
			return i
		}
	}
	return -1
}

// Insert inserts a value at a specific index.
//
// Example:
//   apoc.coll.insert([1,2,4,5], 2, 3) => [1,2,3,4,5]
func Insert(list []interface{}, index int, value interface{}) []interface{} {
	if index < 0 || index > len(list) {
		return list
	}
	
	result := make([]interface{}, len(list)+1)
	copy(result[:index], list[:index])
	result[index] = value
	copy(result[index+1:], list[index:])
	
	return result
}

// InsertAll inserts multiple values at a specific index.
//
// Example:
//   apoc.coll.insertAll([1,2,5,6], 2, [3,4]) => [1,2,3,4,5,6]
func InsertAll(list []interface{}, index int, values []interface{}) []interface{} {
	if index < 0 || index > len(list) {
		return list
	}
	
	result := make([]interface{}, len(list)+len(values))
	copy(result[:index], list[:index])
	copy(result[index:index+len(values)], values)
	copy(result[index+len(values):], list[index:])
	
	return result
}

// Intersection returns elements that exist in all lists.
//
// Example:
//   apoc.coll.intersection([1,2,3,4], [2,3,4,5], [3,4,5,6]) => [3,4]
func Intersection(lists ...[]interface{}) []interface{} {
	if len(lists) == 0 {
		return []interface{}{}
	}
	if len(lists) == 1 {
		return lists[0]
	}
	
	// Count occurrences across all lists
	counts := make(map[string]int)
	items := make(map[string]interface{})
	
	for _, list := range lists {
		seen := make(map[string]bool)
		for _, item := range list {
			key := fmt.Sprintf("%v", item)
			if !seen[key] {
				counts[key]++
				items[key] = item
				seen[key] = true
			}
		}
	}
	
	// Keep items that appear in all lists
	result := make([]interface{}, 0)
	for key, count := range counts {
		if count == len(lists) {
			result = append(result, items[key])
		}
	}
	
	return result
}

// IsEmpty checks if a list is empty.
//
// Example:
//   apoc.coll.isEmpty([]) => true
//   apoc.coll.isEmpty([1,2,3]) => false
func IsEmpty(list []interface{}) bool {
	return len(list) == 0
}

// IsNotEmpty checks if a list is not empty.
//
// Example:
//   apoc.coll.isNotEmpty([1,2,3]) => true
//   apoc.coll.isNotEmpty([]) => false
func IsNotEmpty(list []interface{}) bool {
	return len(list) > 0
}

// Occurrences counts how many times a value appears in a list.
//
// Example:
//   apoc.coll.occurrences([1,2,3,2,4,2,5], 2) => 3
func Occurrences(list []interface{}, value interface{}) int {
	count := 0
	for _, item := range list {
		if reflect.DeepEqual(item, value) {
			count++
		}
	}
	return count
}

// RandomItem returns a random item from a list.
//
// Example:
//   apoc.coll.randomItem([1,2,3,4,5]) => 3 (random)
func RandomItem(list []interface{}) interface{} {
	if len(list) == 0 {
		return nil
	}
	// Note: In production, use crypto/rand for better randomness
	return list[int(math.Floor(float64(len(list))*0.5))] // Placeholder
}

// RandomItems returns n random items from a list.
//
// Example:
//   apoc.coll.randomItems([1,2,3,4,5], 3) => [2,4,5] (random)
func RandomItems(list []interface{}, count int) []interface{} {
	if count >= len(list) {
		return list
	}
	// Note: In production, implement proper random sampling
	return list[:count] // Placeholder
}

// Remove removes a value at a specific index.
//
// Example:
//   apoc.coll.remove([1,2,3,4,5], 2) => [1,2,4,5]
func Remove(list []interface{}, index int) []interface{} {
	if index < 0 || index >= len(list) {
		return list
	}
	
	result := make([]interface{}, len(list)-1)
	copy(result[:index], list[:index])
	copy(result[index:], list[index+1:])
	
	return result
}

// RemoveAll removes all occurrences of a value.
//
// Example:
//   apoc.coll.removeAll([1,2,3,2,4,2,5], 2) => [1,3,4,5]
func RemoveAll(list []interface{}, value interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, item := range list {
		if !reflect.DeepEqual(item, value) {
			result = append(result, item)
		}
	}
	return result
}

// Set replaces a value at a specific index.
//
// Example:
//   apoc.coll.set([1,2,3,4,5], 2, 99) => [1,2,99,4,5]
func Set(list []interface{}, index int, value interface{}) []interface{} {
	if index < 0 || index >= len(list) {
		return list
	}
	
	result := make([]interface{}, len(list))
	copy(result, list)
	result[index] = value
	
	return result
}

// Shuffle randomly shuffles a list.
//
// Example:
//   apoc.coll.shuffle([1,2,3,4,5]) => [3,1,5,2,4] (random)
func Shuffle(list []interface{}) []interface{} {
	result := make([]interface{}, len(list))
	copy(result, list)
	// Note: In production, implement proper Fisher-Yates shuffle
	return result // Placeholder
}

// Slice returns a sublist from start to end index.
//
// Example:
//   apoc.coll.slice([1,2,3,4,5], 1, 4) => [2,3,4]
func Slice(list []interface{}, start, end int) []interface{} {
	if start < 0 {
		start = 0
	}
	if end > len(list) {
		end = len(list)
	}
	if start >= end {
		return []interface{}{}
	}
	
	result := make([]interface{}, end-start)
	copy(result, list[start:end])
	
	return result
}

// Split splits a list into chunks of a given size.
//
// Example:
//   apoc.coll.split([1,2,3,4,5,6,7], 3) => [[1,2,3],[4,5,6],[7]]
func Split(list []interface{}, size int) [][]interface{} {
	if size <= 0 {
		return [][]interface{}{list}
	}
	
	result := make([][]interface{}, 0)
	for i := 0; i < len(list); i += size {
		end := i + size
		if end > len(list) {
			end = len(list)
		}
		chunk := make([]interface{}, end-i)
		copy(chunk, list[i:end])
		result = append(result, chunk)
	}
	
	return result
}

// Subtract returns elements in list1 that are not in list2.
//
// Example:
//   apoc.coll.subtract([1,2,3,4,5], [2,4,6]) => [1,3,5]
func Subtract(list1, list2 []interface{}) []interface{} {
	return Different(list1, list2)
}

// SumLongs sums integer values in a list.
//
// Example:
//   apoc.coll.sumLongs([1,2,3,4,5]) => 15
func SumLongs(list []interface{}) int64 {
	var sum int64
	for _, item := range list {
		if num, ok := toInt64(item); ok {
			sum += num
		}
	}
	return sum
}

// Union returns the union of multiple lists (unique values).
//
// Example:
//   apoc.coll.union([1,2,3], [3,4,5], [5,6,7]) => [1,2,3,4,5,6,7]
func Union(lists ...[]interface{}) []interface{} {
	seen := make(map[string]bool)
	result := make([]interface{}, 0)
	
	for _, list := range lists {
		for _, item := range list {
			key := fmt.Sprintf("%v", item)
			if !seen[key] {
				seen[key] = true
				result = append(result, item)
			}
		}
	}
	
	return result
}

// UnionAll returns the union of multiple lists (with duplicates).
//
// Example:
//   apoc.coll.unionAll([1,2], [2,3], [3,4]) => [1,2,2,3,3,4]
func UnionAll(lists ...[]interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, list := range lists {
		result = append(result, list...)
	}
	return result
}

// Helper functions

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	}
	return 0, false
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	}
	return 0, false
}

func compare(a, b interface{}) int {
	// Try numeric comparison
	if na, oka := toFloat64(a); oka {
		if nb, okb := toFloat64(b); okb {
			if na < nb {
				return -1
			} else if na > nb {
				return 1
			}
			return 0
		}
	}
	
	// Try string comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
}
