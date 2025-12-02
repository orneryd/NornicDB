// Package stats provides APOC statistics functions.
//
// This package implements all apoc.stats.* functions for calculating
// statistical measures and distributions.
package stats

import (
	"math"
	"sort"
)

// Degrees returns degree statistics for nodes.
//
// Example:
//
//	apoc.stats.degrees('KNOWS') => {min: 0, max: 100, mean: 5.2, ...}
func Degrees(relType string) map[string]interface{} {
	// Placeholder - would calculate from database
	return map[string]interface{}{
		"min":    0,
		"max":    0,
		"mean":   0.0,
		"median": 0.0,
		"stdDev": 0.0,
	}
}

// Mean calculates arithmetic mean.
//
// Example:
//
//	apoc.stats.mean([1,2,3,4,5]) => 3.0
func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}

	return sum / float64(len(values))
}

// Median calculates median value.
//
// Example:
//
//	apoc.stats.median([1,2,3,4,5]) => 3.0
func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}

	return sorted[mid]
}

// Mode calculates mode (most frequent value).
//
// Example:
//
//	apoc.stats.mode([1,2,2,3,3,3]) => 3
func Mode(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	freq := make(map[float64]int)
	for _, v := range values {
		freq[v]++
	}

	maxFreq := 0
	var mode float64

	for v, f := range freq {
		if f > maxFreq {
			maxFreq = f
			mode = v
		}
	}

	return mode
}

// StdDev calculates standard deviation.
//
// Example:
//
//	apoc.stats.stdDev([1,2,3,4,5]) => 1.414...
func StdDev(values []float64) float64 {
	return math.Sqrt(Variance(values))
}

// Variance calculates variance.
//
// Example:
//
//	apoc.stats.variance([1,2,3,4,5]) => 2.0
func Variance(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	mean := Mean(values)
	sumSquares := 0.0

	for _, v := range values {
		diff := v - mean
		sumSquares += diff * diff
	}

	return sumSquares / float64(len(values))
}

// Percentile calculates percentile value.
//
// Example:
//
//	apoc.stats.percentile([1,2,3,4,5], 0.95) => 4.8
func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	index := p * float64(len(sorted)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper {
		return sorted[lower]
	}

	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// Quartiles calculates quartiles (Q1, Q2, Q3).
//
// Example:
//
//	apoc.stats.quartiles([1,2,3,4,5]) => {Q1: 2, Q2: 3, Q3: 4}
func Quartiles(values []float64) map[string]float64 {
	return map[string]float64{
		"Q1": Percentile(values, 0.25),
		"Q2": Percentile(values, 0.50),
		"Q3": Percentile(values, 0.75),
	}
}

// IQR calculates interquartile range.
//
// Example:
//
//	apoc.stats.iqr([1,2,3,4,5]) => 2.0
func IQR(values []float64) float64 {
	q := Quartiles(values)
	return q["Q3"] - q["Q1"]
}

// Min returns minimum value.
//
// Example:
//
//	apoc.stats.min([1,2,3,4,5]) => 1
func Min(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}

	return min
}

// Max returns maximum value.
//
// Example:
//
//	apoc.stats.max([1,2,3,4,5]) => 5
func Max(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}

	return max
}

// Range returns range (max - min).
//
// Example:
//
//	apoc.stats.range([1,2,3,4,5]) => 4
func Range(values []float64) float64 {
	return Max(values) - Min(values)
}

// Sum returns sum of values.
//
// Example:
//
//	apoc.stats.sum([1,2,3,4,5]) => 15
func Sum(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}

// Count returns count of values.
//
// Example:
//
//	apoc.stats.count([1,2,3,4,5]) => 5
func Count(values []float64) int {
	return len(values)
}

// Skewness calculates skewness.
//
// Example:
//
//	apoc.stats.skewness([1,2,3,4,5]) => skewness value
func Skewness(values []float64) float64 {
	if len(values) < 3 {
		return 0
	}

	mean := Mean(values)
	stdDev := StdDev(values)

	if stdDev == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += math.Pow((v-mean)/stdDev, 3)
	}

	n := float64(len(values))
	return (n / ((n - 1) * (n - 2))) * sum
}

// Kurtosis calculates kurtosis.
//
// Example:
//
//	apoc.stats.kurtosis([1,2,3,4,5]) => kurtosis value
func Kurtosis(values []float64) float64 {
	if len(values) < 4 {
		return 0
	}

	mean := Mean(values)
	stdDev := StdDev(values)

	if stdDev == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += math.Pow((v-mean)/stdDev, 4)
	}

	n := float64(len(values))
	return ((n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))) * sum -
		(3 * (n - 1) * (n - 1)) / ((n - 2) * (n - 3))
}

// Correlation calculates Pearson correlation coefficient.
//
// Example:
//
//	apoc.stats.correlation([1,2,3], [2,4,6]) => 1.0
func Correlation(x, y []float64) float64 {
	if len(x) != len(y) || len(x) == 0 {
		return 0
	}

	meanX := Mean(x)
	meanY := Mean(y)

	numerator := 0.0
	denomX := 0.0
	denomY := 0.0

	for i := 0; i < len(x); i++ {
		diffX := x[i] - meanX
		diffY := y[i] - meanY
		numerator += diffX * diffY
		denomX += diffX * diffX
		denomY += diffY * diffY
	}

	if denomX == 0 || denomY == 0 {
		return 0
	}

	return numerator / math.Sqrt(denomX*denomY)
}

// Covariance calculates covariance.
//
// Example:
//
//	apoc.stats.covariance([1,2,3], [2,4,6]) => covariance value
func Covariance(x, y []float64) float64 {
	if len(x) != len(y) || len(x) == 0 {
		return 0
	}

	meanX := Mean(x)
	meanY := Mean(y)

	sum := 0.0
	for i := 0; i < len(x); i++ {
		sum += (x[i] - meanX) * (y[i] - meanY)
	}

	return sum / float64(len(x))
}

// ZScore calculates z-scores for all values.
//
// Example:
//
//	apoc.stats.zScore([1,2,3,4,5]) => z-scores
func ZScore(values []float64) []float64 {
	mean := Mean(values)
	stdDev := StdDev(values)

	if stdDev == 0 {
		result := make([]float64, len(values))
		return result
	}

	result := make([]float64, len(values))
	for i, v := range values {
		result[i] = (v - mean) / stdDev
	}

	return result
}

// Normalize normalizes values to 0-1 range.
//
// Example:
//
//	apoc.stats.normalize([1,2,3,4,5]) => [0, 0.25, 0.5, 0.75, 1.0]
func Normalize(values []float64) []float64 {
	min := Min(values)
	max := Max(values)

	if max == min {
		result := make([]float64, len(values))
		for i := range result {
			result[i] = 1.0
		}
		return result
	}

	result := make([]float64, len(values))
	for i, v := range values {
		result[i] = (v - min) / (max - min)
	}

	return result
}

// Histogram creates histogram bins.
//
// Example:
//
//	apoc.stats.histogram([1,2,3,4,5], 5) => bin counts
func Histogram(values []float64, bins int) []int {
	if len(values) == 0 || bins <= 0 {
		return []int{}
	}

	min := Min(values)
	max := Max(values)
	binWidth := (max - min) / float64(bins)

	histogram := make([]int, bins)

	for _, v := range values {
		binIndex := int((v - min) / binWidth)
		if binIndex >= bins {
			binIndex = bins - 1
		}
		histogram[binIndex]++
	}

	return histogram
}

// Outliers detects outliers using IQR method.
//
// Example:
//
//	apoc.stats.outliers([1,2,3,4,5,100]) => [100]
func Outliers(values []float64) []float64 {
	q := Quartiles(values)
	iqr := q["Q3"] - q["Q1"]

	lowerBound := q["Q1"] - 1.5*iqr
	upperBound := q["Q3"] + 1.5*iqr

	outliers := make([]float64, 0)
	for _, v := range values {
		if v < lowerBound || v > upperBound {
			outliers = append(outliers, v)
		}
	}

	return outliers
}

// Summary provides comprehensive statistics.
//
// Example:
//
//	apoc.stats.summary([1,2,3,4,5]) => {mean, median, stdDev, ...}
func Summary(values []float64) map[string]interface{} {
	q := Quartiles(values)

	return map[string]interface{}{
		"count":    len(values),
		"sum":      Sum(values),
		"mean":     Mean(values),
		"median":   Median(values),
		"mode":     Mode(values),
		"stdDev":   StdDev(values),
		"variance": Variance(values),
		"min":      Min(values),
		"max":      Max(values),
		"range":    Range(values),
		"Q1":       q["Q1"],
		"Q2":       q["Q2"],
		"Q3":       q["Q3"],
		"IQR":      IQR(values),
		"skewness": Skewness(values),
		"kurtosis": Kurtosis(values),
	}
}
