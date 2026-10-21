package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// vectorScoreReader is a bounded scoring lease, shared by direct node
// accumulation and pool scoring. Its policy selects values, not cancellation
// semantics: CPU vectors and compressed codes use the same batch gate.
type vectorScoreReader struct {
	work   searchBatch
	query  []float32
	index  *VectorIndex
	getter VectorGetter
	codes  func(string) (float64, bool)
}

func (r *vectorScoreReader) score(id string) (float64, bool, error) {
	if r.work.boundary() {
		if err := r.work.check(); err != nil {
			return 0, false, err
		}
	}
	score, ok := r.value(id)
	return score, ok, nil
}

// value is used inside an already checked bounded batch. Direct member
// accumulation instead uses score, which advances the same boundary counter.
func (r *vectorScoreReader) value(id string) (float64, bool) {
	if r.codes != nil {
		return r.codes(id)
	}
	var vec []float32
	var ok bool
	if r.index != nil {
		vec, ok = r.index.getVectorRefLocked(id)
	} else {
		vec, ok = r.getter.GetVector(id)
	}
	if !ok {
		return 0, false
	}
	return float64(vector.DotProduct(r.query, vec)), true
}

// withVectorScoreReader owns the entire borrowed reader lifetime. Checking
// after the callback also catches cancellation during its last value read.
// No partial pool escapes on cancellation, and all exits release the reader.
func withVectorScoreReader(ctx context.Context, query []float32, getter VectorGetter, codes func(string) (float64, bool), release func(), visit func(*vectorScoreReader) error) error {
	if release != nil {
		defer release()
	}
	read := vectorScoreReader{work: newSearchBatch(ctx), query: vector.Normalize(query), getter: getter, codes: codes}
	if err := read.work.check(); err != nil {
		return err
	}
	if index, ok := getter.(*VectorIndex); ok {
		index.mu.RLock()
		defer index.mu.RUnlock()
		read.index = index
	}
	if err := visit(&read); err != nil {
		return err
	}
	return read.work.check()
}

func scoreVectorPool(ctx context.Context, query []float32, getter VectorGetter, codes func(string) (float64, bool), release func(), candidates []Candidate) ([]ScoredCandidate, error) {
	scored := make([]ScoredCandidate, 0, len(candidates))
	err := withVectorScoreReader(ctx, query, getter, codes, release, func(read *vectorScoreReader) error {
		for start := 0; start < len(candidates); start += searchBatchSize {
			if err := read.work.check(); err != nil {
				return err
			}
			batch := candidates[start:min(start+searchBatchSize, len(candidates))]
			// Select value computation once for the batch. Scalar code reads
			// are cheap enough that an extra call through value dominates them.
			// Both policies still share this lease, gate and final commit check.
			if read.codes != nil {
				for _, candidate := range batch {
					if score, ok := read.codes(candidate.ID); ok {
						scored = append(scored, ScoredCandidate{ID: candidate.ID, Score: score})
					}
				}
				continue
			}
			for _, candidate := range batch {
				if score, ok := read.value(candidate.ID); ok {
					scored = append(scored, ScoredCandidate{ID: candidate.ID, Score: score})
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return scored, nil
}
