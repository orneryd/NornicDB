// Package agg provides APOC aggregation functions.
//
// This package implements all apoc.agg.* functions for custom
// aggregations in Cypher queries.
package agg

import (
	"fmt"
	"sort"
)

// First returns the first non-null value.
//
// Example:
//   apoc.agg.first([null, null, 'hello', 'world']) => 'hello'
func First(values []interface{}) interface{} {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

// Last returns the last non-null value.
//
// Example:
//   apoc.agg.last(['hello', 'world', null, null]) => 'world'
func Last(values []interface{}) interface{} {
	var last interface{}
	for _, value := range values {
		if value != nil {
			last = value
		}
	}
	return last
}

// Nth returns the nth value (0-indexed).
//
// Example:
//   apoc.agg.nth([1,2,3,4,5], 2) => 3
func Nth(values []interface{}, n int) interface{} {
	if n >= 0 && n < len(values) {
		return values[n]
	}
	return nil
}

// Slice returns a slice of values.
//
// Example:
//   apoc.agg.slice([1,2,3,4,5], 1, 4) => [2,3,4]
func Slice(values []interface{}, start, end int) []interface{} {
	if start < 0 {
		start = 0
	}
	if end > len(values) {
		end = len(values)
	}
	if start >= end {
		return []interface{}{}
	}
	
	result := make([]interface{}, end-start)
	copy(result, values[start:end])
	return result
}

// Product returns the product of all numeric values.
//
// Example:
//   apoc.agg.product([2, 3, 4]) => 24
func Product(values []interface{}) float64 {
	product := 1.0
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			product *= num
		}
	}
	return product
}

// Median returns the median value.
//
// Example:
//   apoc.agg.median([1,2,3,4,5]) => 3.0
func Median(values []interface{}) float64 {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	if len(nums) == 0 {
		return 0
	}
	
	sort.Float64s(nums)
	
	mid := len(nums) / 2
	if len(nums)%2 == 0 {
		return (nums[mid-1] + nums[mid]) / 2
	}
	return nums[mid]
}

// Percentile returns the nth percentile.
//
// Example:
//   apoc.agg.percentile([1,2,3,4,5], 0.5) => 3.0
func Percentile(values []interface{}, percentile float64) float64 {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	if len(nums) == 0 {
		return 0
	}
	
	sort.Float64s(nums)
	
	index := percentile * float64(len(nums)-1)
	lower := int(index)
	upper := lower + 1
	
	if upper >= len(nums) {
		return nums[lower]
	}
	
	weight := index - float64(lower)
	return nums[lower]*(1-weight) + nums[upper]*weight
}

// StdDev returns the standard deviation.
//
// Example:
//   apoc.agg.stdev([2,4,4,4,5,5,7,9]) => 2.0
func StdDev(values []interface{}) float64 {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	if len(nums) == 0 {
		return 0
	}
	
	// Calculate mean
	sum := 0.0
	for _, num := range nums {
		sum += num
	}
	mean := sum / float64(len(nums))
	
	// Calculate variance
	variance := 0.0
	for _, num := range nums {
		diff := num - mean
		variance += diff * diff
	}
	variance /= float64(len(nums))
	
	// Return standard deviation
	return sqrt(variance)
}

// Mode returns the most frequent value.
//
// Example:
//   apoc.agg.mode([1,2,2,3,3,3,4]) => 3
func Mode(values []interface{}) interface{} {
	frequencies := make(map[string]int)
	items := make(map[string]interface{})
	
	for _, value := range values {
		key := fmt.Sprintf("%v", value)
		frequencies[key]++
		items[key] = value
	}
	
	maxFreq := 0
	var mode interface{}
	for key, freq := range frequencies {
		if freq > maxFreq {
			maxFreq = freq
			mode = items[key]
		}
	}
	
	return mode
}

// Statistics returns comprehensive statistics.
//
// Example:
//   apoc.agg.statistics([1,2,3,4,5])
//   => {min:1, max:5, mean:3, stdev:1.41, ...}
func Statistics(values []interface{}) map[string]interface{} {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	if len(nums) == 0 {
		return map[string]interface{}{}
	}
	
	// Calculate statistics
	sum := 0.0
	min := nums[0]
	max := nums[0]
	
	for _, num := range nums {
		sum += num
		if num < min {
			min = num
		}
		if num > max {
			max = num
		}
	}
	
	mean := sum / float64(len(nums))
	
	variance := 0.0
	for _, num := range nums {
		diff := num - mean
		variance += diff * diff
	}
	variance /= float64(len(nums))
	stdev := sqrt(variance)
	
	return map[string]interface{}{
		"min":      min,
		"max":      max,
		"mean":     mean,
		"sum":      sum,
		"stdev":    stdev,
		"variance": variance,
		"count":    len(nums),
	}
}

// Graph creates a graph structure from aggregated data.
//
// Example:
//   apoc.agg.graph(paths) => {nodes: [...], relationships: [...]}
func Graph(paths []interface{}) map[string]interface{} {
	nodes := make(map[int64]interface{})
	relationships := make(map[int64]interface{})
	
	for _, path := range paths {
		if pathMap, ok := path.(map[string]interface{}); ok {
			if pathNodes, ok := pathMap["nodes"].([]interface{}); ok {
				for _, node := range pathNodes {
					if nodeMap, ok := node.(map[string]interface{}); ok {
						if id, ok := nodeMap["id"].(int64); ok {
							nodes[id] = node
						}
					}
				}
			}
			
			if pathRels, ok := pathMap["relationships"].([]interface{}); ok {
				for _, rel := range pathRels {
					if relMap, ok := rel.(map[string]interface{}); ok {
						if id, ok := relMap["id"].(int64); ok {
							relationships[id] = rel
						}
					}
				}
			}
		}
	}
	
	nodeList := make([]interface{}, 0, len(nodes))
	for _, node := range nodes {
		nodeList = append(nodeList, node)
	}
	
	relList := make([]interface{}, 0, len(relationships))
	for _, rel := range relationships {
		relList = append(relList, rel)
	}
	
	return map[string]interface{}{
		"nodes":         nodeList,
		"relationships": relList,
	}
}

// MinItems returns the n smallest values.
//
// Example:
//   apoc.agg.minItems([5,2,8,1,9], 3) => [1,2,5]
func MinItems(values []interface{}, n int) []interface{} {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	sort.Float64s(nums)
	
	if n > len(nums) {
		n = len(nums)
	}
	
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		result[i] = nums[i]
	}
	
	return result
}

// MaxItems returns the n largest values.
//
// Example:
//   apoc.agg.maxItems([5,2,8,1,9], 3) => [9,8,5]
func MaxItems(values []interface{}, n int) []interface{} {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	sort.Float64s(nums)
	
	if n > len(nums) {
		n = len(nums)
	}
	
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		result[i] = nums[len(nums)-1-i]
	}
	
	return result
}

// Histogram creates a histogram of values.
//
// Example:
//   apoc.agg.histogram([1,2,2,3,3,3,4,4,4,4], 2)
//   => [{bucket:0, count:1}, {bucket:2, count:9}]
func Histogram(values []interface{}, bucketSize float64) []map[string]interface{} {
	nums := make([]float64, 0)
	for _, value := range values {
		if num, ok := toFloat64(value); ok {
			nums = append(nums, num)
		}
	}
	
	if len(nums) == 0 {
		return []map[string]interface{}{}
	}
	
	buckets := make(map[int]int)
	for _, num := range nums {
		bucket := int(num / bucketSize)
		buckets[bucket]++
	}
	
	result := make([]map[string]interface{}, 0)
	for bucket, count := range buckets {
		result = append(result, map[string]interface{}{
			"bucket": float64(bucket) * bucketSize,
			"count":  count,
		})
	}
	
	return result
}

// Frequencies returns value frequencies.
//
// Example:
//   apoc.agg.frequencies([1,2,2,3,3,3])
//   => [{value:1, count:1}, {value:2, count:2}, {value:3, count:3}]
func Frequencies(values []interface{}) []map[string]interface{} {
	frequencies := make(map[string]int)
	items := make(map[string]interface{})
	order := make([]string, 0)
	
	for _, value := range values {
		key := fmt.Sprintf("%v", value)
		if frequencies[key] == 0 {
			order = append(order, key)
			items[key] = value
		}
		frequencies[key]++
	}
	
	result := make([]map[string]interface{}, 0)
	for _, key := range order {
		result = append(result, map[string]interface{}{
			"value": items[key],
			"count": frequencies[key],
		})
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

func sqrt(x float64) float64 {
	if x < 0 {
		return 0
	}
	
	// Newton's method
	z := x / 2
	for i := 0; i < 10; i++ {
		z = z - (z*z-x)/(2*z)
	}
	return z
}
