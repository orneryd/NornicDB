package search

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// SetQuantization configures approximate graph scoring. Construction, including
// lexical ordering and diversity, continues to use original vectors. Use the
// vector search pipeline to rescore candidates and apply exact score thresholds.
// For example, scalar compression keeps one byte per coordinate plus a scale:
//
//	err := index.SetQuantization(ctx, HNSWQuantizationConfig{Mode: HNSWQuantizationScalar})
//
// An empty Mode restores original-coordinate scoring. Mutations update codes
// under the same lock as graph/vector mutations. Changing mode rebuilds codes.
func (h *HNSWIndex) SetQuantization(ctx context.Context, cfg HNSWQuantizationConfig) error {
	if ctx == nil {
		ctx = context.Background()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.setQuantizationLocked(ctx, cfg)
}

func (h *HNSWIndex) setQuantizationLocked(ctx context.Context, cfg HNSWQuantizationConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error
	cfg, err = normalizeHNSWQuantizationConfig(cfg)
	if err != nil {
		return err
	}
	if cfg.Mode == HNSWQuantizationNone {
		h.quantized = nil
		h.config.Quantization = cfg
		return nil
	}
	if cfg.Mode == HNSWQuantizationProduct && h.liveCount < 256 {
		h.config.Quantization = cfg
		h.quantized = nil
		return nil
	}
	if h.quantized != nil && h.quantized.config == cfg {
		return nil
	}
	var training [][]float32
	if cfg.Mode == HNSWQuantizationProduct {
		ids := h.quantizationTrainingIDsLocked(cfg.TrainingSampleMax)
		training = make([][]float32, 0, len(ids))
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			training = append(training, vector.Normalize(h.vectorAtLocked(id)))
		}
	}
	q, err := newHNSWQuantizedVectors(ctx, cfg, h.dimensions, training)
	if err != nil {
		return err
	}
	q.grow(len(h.internalToID))
	errors := make([]error, len(h.internalToID))
	err = parallelOver(ctx, len(h.internalToID), func(id int) {
		if h.deleted[id] {
			return
		}
		v := h.vectorAtLocked(uint32(id))
		if len(v) != h.dimensions {
			errors[id] = fmt.Errorf("missing original coordinates for HNSW vector %q", h.internalToID[id])
			return
		}
		// grow preallocates all rows; workers write disjoint stable IDs and
		// never resize the model while readers/mutations are excluded.
		q.set(uint32(id), v)
	})
	if err != nil {
		return err
	}
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	h.quantized = q
	h.config.Quantization = cfg
	return nil
}

// QuantizationBytes reports encoded coordinates, scales and codebooks only.
// It excludes graph metadata, original coordinates, storage caches and RSS.
func (h *HNSWIndex) QuantizationBytes() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.quantized == nil {
		return 0
	}
	return h.quantized.bytes()
}

func (h *HNSWIndex) quantizationTrainingIDsLocked(limit int) []uint32 {
	type seed struct {
		id   uint32
		hint nodeLexicalHint
	}
	seeds := make([]seed, 0, len(h.buildLexicalHints))
	for id := range h.internalToID {
		if h.deleted[id] {
			continue
		}
		if hint, ok := h.buildLexicalHintLocked(uint32(id)); ok {
			seeds = append(seeds, seed{uint32(id), hint})
		}
	}
	slices.SortFunc(seeds, func(a, b seed) int {
		if n := cmp.Compare(a.hint.Rank, b.hint.Rank); n != 0 {
			return n
		}
		return cmp.Compare(h.internalToID[a.id], h.internalToID[b.id])
	})
	out := make([]uint32, 0, min(limit, h.liveCount))
	seeded := make(map[uint32]struct{}, min(limit, len(seeds)))
	for _, s := range seeds {
		if len(out) == limit {
			break
		}
		out = append(out, s.id)
		seeded[s.id] = struct{}{}
	}
	floor := len(out)
	seen := 0
	rng := rand.New(rand.NewSource(ivfpqTrainSeed))
	for id := range h.internalToID {
		if h.deleted[id] {
			continue
		}
		if _, ok := seeded[uint32(id)]; ok {
			continue
		}
		seen++
		if len(out) < limit {
			out = append(out, uint32(id))
			continue
		}
		if slots := limit - floor; slots > 0 {
			if j := rng.Intn(seen); j < slots {
				out[floor+j] = uint32(id)
			}
		}
	}
	return out
}

type hnswQuantizedQueryKey struct{}

func (h *HNSWIndex) graphDistanceLocked(query []float32, id uint32, scorer *hnswQuantizedQuery) float32 {
	if scorer != nil {
		return 1 - scorer.dot(id)
	}
	return 1 - vector.DotProductSIMD(query, h.vectorAtLocked(id))
}

// prepareQuantizationReadLocked upgrades only when a configured model must be
// built (initial small collection growth or Clear). It returns with RLock held.
func (h *HNSWIndex) prepareQuantizationReadLocked(ctx context.Context) error {
	cfg := h.config.Quantization
	if h.quantized != nil || cfg.Mode == HNSWQuantizationNone {
		return nil
	}
	h.mu.RUnlock()
	h.mu.Lock()
	var err error
	if h.quantized == nil {
		err = h.setQuantizationLocked(ctx, h.config.Quantization)
	}
	h.mu.Unlock()
	h.mu.RLock()
	return err
}
