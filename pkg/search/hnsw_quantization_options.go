package search

import (
	"context"
	"fmt"
	"math"
)

type hnswQuantizationOptionsKey struct{}
type hnswQuantizationOptions struct {
	ignore       bool
	rescore      bool
	oversampling float64
}

func quantizationOptions(ctx context.Context) hnswQuantizationOptions {
	if opts, ok := ctx.Value(hnswQuantizationOptionsKey{}).(hnswQuantizationOptions); ok {
		return opts
	}
	return hnswQuantizationOptions{rescore: true, oversampling: 1}
}

func withQuantizationOptions(ctx context.Context, opts *SearchOptions) (context.Context, error) {
	if opts.QuantizationOversampling != 0 && (opts.QuantizationOversampling < 1 || math.IsNaN(opts.QuantizationOversampling) || math.IsInf(opts.QuantizationOversampling, 0)) {
		return nil, fmt.Errorf("quantization oversampling must be finite and at least 1")
	}
	if !opts.QuantizationIgnore && opts.QuantizationRescore == nil && opts.QuantizationOversampling == 0 {
		return ctx, nil
	}
	return context.WithValue(ctx, hnswQuantizationOptionsKey{}, effectiveHNSWQuantizationOptions(opts)), nil
}

// Cache identity and pipeline scoring must resolve the same request defaults.
func effectiveHNSWQuantizationOptions(opts *SearchOptions) hnswQuantizationOptions {
	q := hnswQuantizationOptions{ignore: opts.QuantizationIgnore, rescore: true, oversampling: opts.QuantizationOversampling}
	if q.oversampling == 0 {
		q.oversampling = 1
	}
	if opts.QuantizationRescore != nil {
		q.rescore = *opts.QuantizationRescore
	}
	return q
}

func (h *HNSWCandidateGen) quantizationActive(ctx context.Context) bool {
	if quantizationOptions(ctx).ignore {
		return false
	}
	h.hnswIndex.mu.RLock()
	defer h.hnswIndex.mu.RUnlock()
	cfg := h.hnswIndex.config.Quantization
	return cfg.Mode != HNSWQuantizationNone && (cfg.Mode != HNSWQuantizationProduct || h.hnswIndex.liveCount >= 256)
}

// quantizedScoreReader pins codes for public named/property vector selection.
// It lets rescore=false remain approximate through the final public consumer,
// rather than silently recalculating original coordinates after the pipeline.
func (h *HNSWIndex) quantizedScoreReader(query []float32) (func(string) (float64, bool), func()) {
	h.mu.RLock()
	if h.quantized == nil {
		h.mu.RUnlock()
		return nil, func() {}
	}
	scorer := h.quantized.query(query)
	return func(id string) (float64, bool) {
		internal, ok := h.idToInternal[id]
		if !ok || h.deleted[internal] {
			return 0, false
		}
		return float64(scorer.dot(internal)), true
	}, func() { scorer.release(); h.mu.RUnlock() }
}

// Public metadata selection and property supplements share this scoring policy.
// Call after snapshotting metadata: the returned reader may hold the graph's
// read lock and must not invert the indexMu -> graph mutation lock order.
func (s *Service) vectorQueryScoreReader(query []float32, quantizedIndex *HNSWIndex) (func(string) (float64, bool), func()) {
	if quantizedIndex != nil {
		reader, release := quantizedIndex.quantizedScoreReader(query)
		if reader != nil {
			return reader, release
		}
		release()
	}
	return func(id string) (float64, bool) { return s.scoreVectorIDDot(query, id) }, func() {}
}
