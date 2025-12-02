// Package scoring provides APOC scoring and ranking functions.
//
// This package implements all apoc.scoring.* functions for calculating
// similarity scores and rankings.
package scoring

import (
	"math"
	"sort"
	"strings"
)

// Existence calculates existence score based on property presence.
//
// Example:
//
//	apoc.scoring.existence(5, true) => 1.0
func Existence(value interface{}, exists bool) float64 {
	if exists && value != nil {
		return 1.0
	}
	return 0.0
}

// Pareto calculates Pareto score.
//
// Example:
//
//	apoc.scoring.pareto(8, 2, 10, 0.2) => score
func Pareto(minimumThreshold, eightyPercentValue, maximumValue float64, weight float64) float64 {
	if eightyPercentValue <= minimumThreshold {
		return 0
	}

	if eightyPercentValue >= maximumValue {
		return weight
	}

	// Pareto principle: 80/20 rule
	ratio := (eightyPercentValue - minimumThreshold) / (maximumValue - minimumThreshold)
	return ratio * weight
}

// Cosine calculates cosine similarity between two vectors.
//
// Example:
//
//	apoc.scoring.cosine([1,2,3], [4,5,6]) => 0.974...
func Cosine(vector1, vector2 []float64) float64 {
	if len(vector1) != len(vector2) || len(vector1) == 0 {
		return 0
	}

	dotProduct := 0.0
	norm1 := 0.0
	norm2 := 0.0

	for i := 0; i < len(vector1); i++ {
		dotProduct += vector1[i] * vector2[i]
		norm1 += vector1[i] * vector1[i]
		norm2 += vector2[i] * vector2[i]
	}

	if norm1 == 0 || norm2 == 0 {
		return 0
	}

	return dotProduct / (math.Sqrt(norm1) * math.Sqrt(norm2))
}

// Euclidean calculates Euclidean distance.
//
// Example:
//
//	apoc.scoring.euclidean([1,2], [4,6]) => 5.0
func Euclidean(vector1, vector2 []float64) float64 {
	if len(vector1) != len(vector2) {
		return math.MaxFloat64
	}

	sum := 0.0
	for i := 0; i < len(vector1); i++ {
		diff := vector1[i] - vector2[i]
		sum += diff * diff
	}

	return math.Sqrt(sum)
}

// Manhattan calculates Manhattan distance.
//
// Example:
//
//	apoc.scoring.manhattan([1,2], [4,6]) => 7.0
func Manhattan(vector1, vector2 []float64) float64 {
	if len(vector1) != len(vector2) {
		return math.MaxFloat64
	}

	sum := 0.0
	for i := 0; i < len(vector1); i++ {
		sum += math.Abs(vector1[i] - vector2[i])
	}

	return sum
}

// Jaccard calculates Jaccard similarity.
//
// Example:
//
//	apoc.scoring.jaccard([1,2,3], [2,3,4]) => 0.5
func Jaccard(set1, set2 []interface{}) float64 {
	if len(set1) == 0 && len(set2) == 0 {
		return 1.0
	}

	// Convert to sets
	map1 := make(map[interface{}]bool)
	map2 := make(map[interface{}]bool)

	for _, item := range set1 {
		map1[item] = true
	}

	for _, item := range set2 {
		map2[item] = true
	}

	// Calculate intersection and union
	intersection := 0
	for item := range map1 {
		if map2[item] {
			intersection++
		}
	}

	union := len(map1) + len(map2) - intersection

	if union == 0 {
		return 0
	}

	return float64(intersection) / float64(union)
}

// Overlap calculates overlap coefficient.
//
// Example:
//
//	apoc.scoring.overlap([1,2,3], [2,3,4,5]) => 0.667
func Overlap(set1, set2 []interface{}) float64 {
	if len(set1) == 0 || len(set2) == 0 {
		return 0
	}

	map1 := make(map[interface{}]bool)
	for _, item := range set1 {
		map1[item] = true
	}

	intersection := 0
	for _, item := range set2 {
		if map1[item] {
			intersection++
		}
	}

	minSize := len(set1)
	if len(set2) < minSize {
		minSize = len(set2)
	}

	return float64(intersection) / float64(minSize)
}

// Dice calculates Dice coefficient.
//
// Example:
//
//	apoc.scoring.dice([1,2,3], [2,3,4]) => 0.667
func Dice(set1, set2 []interface{}) float64 {
	if len(set1) == 0 && len(set2) == 0 {
		return 1.0
	}

	if len(set1) == 0 || len(set2) == 0 {
		return 0
	}

	map1 := make(map[interface{}]bool)
	for _, item := range set1 {
		map1[item] = true
	}

	intersection := 0
	for _, item := range set2 {
		if map1[item] {
			intersection++
		}
	}

	return 2.0 * float64(intersection) / float64(len(set1)+len(set2))
}

// Pearson calculates Pearson correlation coefficient.
//
// Example:
//
//	apoc.scoring.pearson([1,2,3,4], [2,4,6,8]) => 1.0
func Pearson(x, y []float64) float64 {
	if len(x) != len(y) || len(x) == 0 {
		return 0
	}

	n := float64(len(x))

	// Calculate means
	sumX := 0.0
	sumY := 0.0
	for i := 0; i < len(x); i++ {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / n
	meanY := sumY / n

	// Calculate correlation
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

// TF calculates term frequency.
//
// Example:
//
//	apoc.scoring.tf('hello', 'hello world hello') => 0.667
func TF(term, document string) float64 {
	words := strings.Fields(strings.ToLower(document))
	if len(words) == 0 {
		return 0
	}

	term = strings.ToLower(term)
	count := 0
	for _, word := range words {
		if word == term {
			count++
		}
	}

	return float64(count) / float64(len(words))
}

// IDF calculates inverse document frequency.
//
// Example:
//
//	apoc.scoring.idf('hello', 10, 3) => 1.204
func IDF(term string, totalDocs, docsWithTerm int) float64 {
	if docsWithTerm == 0 {
		return 0
	}

	return math.Log(float64(totalDocs) / float64(docsWithTerm))
}

// TFIDF calculates TF-IDF score.
//
// Example:
//
//	apoc.scoring.tfidf('hello', 'hello world', 10, 3) => score
func TFIDF(term, document string, totalDocs, docsWithTerm int) float64 {
	tf := TF(term, document)
	idf := IDF(term, totalDocs, docsWithTerm)
	return tf * idf
}

// BM25 calculates BM25 ranking score.
//
// Example:
//
//	apoc.scoring.bm25(termFreq, docLength, avgDocLength, k1, b) => score
func BM25(termFreq, docLength, avgDocLength, k1, b float64) float64 {
	numerator := termFreq * (k1 + 1)
	denominator := termFreq + k1*(1-b+b*(docLength/avgDocLength))
	return numerator / denominator
}

// PageRank calculates PageRank score.
//
// Example:
//
//	apoc.scoring.pageRank(incomingLinks, dampingFactor) => score
func PageRank(incomingScores []float64, dampingFactor float64) float64 {
	sum := 0.0
	for _, score := range incomingScores {
		sum += score
	}

	return (1-dampingFactor) + dampingFactor*sum
}

// Normalize normalizes scores to 0-1 range.
//
// Example:
//
//	apoc.scoring.normalize([1,2,3,4,5]) => [0, 0.25, 0.5, 0.75, 1.0]
func Normalize(scores []float64) []float64 {
	if len(scores) == 0 {
		return scores
	}

	min := scores[0]
	max := scores[0]

	for _, score := range scores {
		if score < min {
			min = score
		}
		if score > max {
			max = score
		}
	}

	if max == min {
		result := make([]float64, len(scores))
		for i := range result {
			result[i] = 1.0
		}
		return result
	}

	result := make([]float64, len(scores))
	for i, score := range scores {
		result[i] = (score - min) / (max - min)
	}

	return result
}

// Rank ranks items by score.
//
// Example:
//
//	apoc.scoring.rank([{id:1, score:0.8}, {id:2, score:0.9}]) => ranked list
func Rank(items []map[string]interface{}) []map[string]interface{} {
	sorted := make([]map[string]interface{}, len(items))
	copy(sorted, items)

	sort.Slice(sorted, func(i, j int) bool {
		scoreI, _ := sorted[i]["score"].(float64)
		scoreJ, _ := sorted[j]["score"].(float64)
		return scoreI > scoreJ
	})

	// Add rank
	for i := range sorted {
		sorted[i]["rank"] = i + 1
	}

	return sorted
}

// TopK returns top K items by score.
//
// Example:
//
//	apoc.scoring.topK(items, 10) => top 10 items
func TopK(items []map[string]interface{}, k int) []map[string]interface{} {
	ranked := Rank(items)

	if k > len(ranked) {
		k = len(ranked)
	}

	return ranked[:k]
}

// Percentile calculates percentile rank.
//
// Example:
//
//	apoc.scoring.percentile(75, [50,60,70,80,90]) => 0.6
func Percentile(value float64, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	count := 0
	for _, v := range values {
		if v < value {
			count++
		}
	}

	return float64(count) / float64(len(values))
}

// ZScore calculates z-score (standard score).
//
// Example:
//
//	apoc.scoring.zScore(75, 70, 5) => 1.0
func ZScore(value, mean, stdDev float64) float64 {
	if stdDev == 0 {
		return 0
	}

	return (value - mean) / stdDev
}

// MinMax scales value to range.
//
// Example:
//
//	apoc.scoring.minMax(75, 50, 100, 0, 1) => 0.5
func MinMax(value, min, max, newMin, newMax float64) float64 {
	if max == min {
		return newMin
	}

	normalized := (value - min) / (max - min)
	return newMin + normalized*(newMax-newMin)
}

// Sigmoid applies sigmoid function.
//
// Example:
//
//	apoc.scoring.sigmoid(0) => 0.5
func Sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}

// Softmax applies softmax function.
//
// Example:
//
//	apoc.scoring.softmax([1,2,3]) => [0.09, 0.24, 0.67]
func Softmax(values []float64) []float64 {
	if len(values) == 0 {
		return values
	}

	// Find max for numerical stability
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}

	// Compute exp(x - max)
	expValues := make([]float64, len(values))
	sum := 0.0
	for i, v := range values {
		expValues[i] = math.Exp(v - max)
		sum += expValues[i]
	}

	// Normalize
	result := make([]float64, len(values))
	for i, exp := range expValues {
		result[i] = exp / sum
	}

	return result
}
