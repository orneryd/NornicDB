// Package ir provides standard information-retrieval metrics for benchmark runners.
package ir

import "math"

// Metrics is the standard metric set used by retrieval benchmarks.
type Metrics struct {
	RecallAt10  float64 `json:"recall_at_10"`
	RecallAt100 float64 `json:"recall_at_100"`
	NDCGAt10    float64 `json:"ndcg_at_10"`
	MRRAt10     float64 `json:"mrr_at_10"`
	MAPAt100    float64 `json:"map_at_100"`
}

// Compute calculates graded nDCG and binary relevance metrics for one query.
// Results are ordered document IDs; qrels maps document ID to its relevance grade.
func Compute(results []string, qrels map[string]int) Metrics {
	return Metrics{
		RecallAt10:  recallAt(results, qrels, 10),
		RecallAt100: recallAt(results, qrels, 100),
		NDCGAt10:    ndcgAt(results, qrels, 10),
		MRRAt10:     mrrAt(results, qrels, 10),
		MAPAt100:    mapAt(results, qrels, 100),
	}
}

func recallAt(results []string, qrels map[string]int, limit int) float64 {
	positive := positiveCount(qrels)
	if positive == 0 {
		return 0
	}
	hits := 0
	for _, id := range first(results, limit) {
		if qrels[id] > 0 {
			hits++
		}
	}
	return float64(hits) / float64(positive)
}

func ndcgAt(results []string, qrels map[string]int, limit int) float64 {
	dcg := 0.0
	for index, id := range first(results, limit) {
		grade := qrels[id]
		if grade > 0 {
			dcg += (math.Pow(2, float64(grade)) - 1) / math.Log2(float64(index)+2)
		}
	}
	grades := make([]int, 0, len(qrels))
	for _, grade := range qrels {
		if grade > 0 {
			grades = append(grades, grade)
		}
	}
	for left := 0; left < len(grades); left++ {
		for right := left + 1; right < len(grades); right++ {
			if grades[right] > grades[left] {
				grades[left], grades[right] = grades[right], grades[left]
			}
		}
	}
	ideal := 0.0
	for index, grade := range firstInts(grades, limit) {
		ideal += (math.Pow(2, float64(grade)) - 1) / math.Log2(float64(index)+2)
	}
	if ideal == 0 {
		return 0
	}
	return dcg / ideal
}

func mrrAt(results []string, qrels map[string]int, limit int) float64 {
	for index, id := range first(results, limit) {
		if qrels[id] > 0 {
			return 1 / float64(index+1)
		}
	}
	return 0
}

func mapAt(results []string, qrels map[string]int, limit int) float64 {
	positive := positiveCount(qrels)
	if positive == 0 {
		return 0
	}
	hits := 0
	precisionSum := 0.0
	for index, id := range first(results, limit) {
		if qrels[id] > 0 {
			hits++
			precisionSum += float64(hits) / float64(index+1)
		}
	}
	return precisionSum / float64(positive)
}

func positiveCount(qrels map[string]int) int {
	count := 0
	for _, grade := range qrels {
		if grade > 0 {
			count++
		}
	}
	return count
}

func first(values []string, limit int) []string {
	if limit < len(values) {
		return values[:limit]
	}
	return values
}

func firstInts(values []int, limit int) []int {
	if limit < len(values) {
		return values[:limit]
	}
	return values
}
