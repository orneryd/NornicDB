package ir

import (
	"fmt"
	"math/rand/v2"
	"sort"
)

// Interval is a percentile confidence interval for an absolute metric delta.
type Interval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

// Comparison is a paired evaluation of candidate results against a baseline.
type Comparison struct {
	Queries       int      `json:"queries"`
	Baseline      Metrics  `json:"baseline"`
	Candidate     Metrics  `json:"candidate"`
	AbsoluteDelta Metrics  `json:"absolute_delta"`
	RecallAt10CI  Interval `json:"recall_at_10_ci_95"`
	RecallAt100CI Interval `json:"recall_at_100_ci_95"`
	NDCGAt10CI    Interval `json:"ndcg_at_10_ci_95"`
}

// Compare evaluates two runs on identical qrels and calculates deterministic
// paired-bootstrap 95% confidence intervals for the primary metrics.
func Compare(qrels Qrels, baseline, candidate map[string][]string, seed uint64, resamples int) (Comparison, error) {
	if len(qrels) == 0 {
		return Comparison{}, fmt.Errorf("qrels must not be empty")
	}
	if resamples < 1 {
		return Comparison{}, fmt.Errorf("resamples must be positive")
	}
	queryIDs := make([]string, 0, len(qrels))
	for queryID := range qrels {
		queryIDs = append(queryIDs, queryID)
	}
	sort.Strings(queryIDs)
	baselinePerQuery := make([]Metrics, len(queryIDs))
	candidatePerQuery := make([]Metrics, len(queryIDs))
	for index, queryID := range queryIDs {
		baselinePerQuery[index] = Compute(baseline[queryID], qrels[queryID])
		candidatePerQuery[index] = Compute(candidate[queryID], qrels[queryID])
	}
	baselineMetrics := averageMetrics(baselinePerQuery)
	candidateMetrics := averageMetrics(candidatePerQuery)
	delta := subtractMetrics(candidateMetrics, baselineMetrics)
	recallAt10Deltas, recallAt100Deltas, ndcgDeltas := bootstrapDeltas(baselinePerQuery, candidatePerQuery, seed, resamples)
	return Comparison{
		Queries:       len(queryIDs),
		Baseline:      baselineMetrics,
		Candidate:     candidateMetrics,
		AbsoluteDelta: delta,
		RecallAt10CI:  percentileInterval(recallAt10Deltas),
		RecallAt100CI: percentileInterval(recallAt100Deltas),
		NDCGAt10CI:    percentileInterval(ndcgDeltas),
	}, nil
}

func bootstrapDeltas(baseline, candidate []Metrics, seed uint64, resamples int) ([]float64, []float64, []float64) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	recallAt10Deltas := make([]float64, resamples)
	recallAt100Deltas := make([]float64, resamples)
	ndcgDeltas := make([]float64, resamples)
	for sample := 0; sample < resamples; sample++ {
		baselineRecallAt10, candidateRecallAt10 := 0.0, 0.0
		baselineRecallAt100, candidateRecallAt100 := 0.0, 0.0
		baselineNDCG, candidateNDCG := 0.0, 0.0
		for draw := 0; draw < len(baseline); draw++ {
			index := rng.IntN(len(baseline))
			baselineRecallAt10 += baseline[index].RecallAt10
			candidateRecallAt10 += candidate[index].RecallAt10
			baselineRecallAt100 += baseline[index].RecallAt100
			candidateRecallAt100 += candidate[index].RecallAt100
			baselineNDCG += baseline[index].NDCGAt10
			candidateNDCG += candidate[index].NDCGAt10
		}
		count := float64(len(baseline))
		recallAt10Deltas[sample] = (candidateRecallAt10 - baselineRecallAt10) / count
		recallAt100Deltas[sample] = (candidateRecallAt100 - baselineRecallAt100) / count
		ndcgDeltas[sample] = (candidateNDCG - baselineNDCG) / count
	}
	return recallAt10Deltas, recallAt100Deltas, ndcgDeltas
}

func averageMetrics(metrics []Metrics) Metrics {
	if len(metrics) == 0 {
		return Metrics{}
	}
	total := Metrics{}
	for _, metric := range metrics {
		total.RecallAt10 += metric.RecallAt10
		total.RecallAt100 += metric.RecallAt100
		total.NDCGAt10 += metric.NDCGAt10
		total.MRRAt10 += metric.MRRAt10
		total.MAPAt100 += metric.MAPAt100
	}
	count := float64(len(metrics))
	return Metrics{RecallAt10: total.RecallAt10 / count, RecallAt100: total.RecallAt100 / count, NDCGAt10: total.NDCGAt10 / count, MRRAt10: total.MRRAt10 / count, MAPAt100: total.MAPAt100 / count}
}

func subtractMetrics(left, right Metrics) Metrics {
	return Metrics{RecallAt10: left.RecallAt10 - right.RecallAt10, RecallAt100: left.RecallAt100 - right.RecallAt100, NDCGAt10: left.NDCGAt10 - right.NDCGAt10, MRRAt10: left.MRRAt10 - right.MRRAt10, MAPAt100: left.MAPAt100 - right.MAPAt100}
}

func percentileInterval(values []float64) Interval {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return Interval{Lower: percentile(sorted, 0.025), Upper: percentile(sorted, 0.975)}
}

func percentile(sorted []float64, probability float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(probability * float64(len(sorted)-1))
	return sorted[index]
}
