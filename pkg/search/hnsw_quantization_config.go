package search

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

// HNSWQuantizationConfigFromEnv resolves coordinate compression (scalar int8 by default) without
// changing the fast/balanced/accurate graph preset or the legacy IVF-PQ mode.
//
// NORNICDB_VECTOR_HNSW_QUANTIZATION selects none, scalar (default), pq or binary.
// NORNICDB_VECTOR_HNSW_BINARY_QUERY_BITS selects 1 (default), 4 or 8.
// NORNICDB_VECTOR_HNSW_PQ_COMPRESSION selects 4/8/16/32/64 (default 16).
// NORNICDB_VECTOR_HNSW_PQ_TRAINING_SAMPLE_MAX and _TRAINING_ITERATIONS control
// PQ training (defaults 10000 and 10). Invalid models fail activation rather
// than silently selecting an uncompressed benchmark path.
//
// Example: NORNICDB_VECTOR_HNSW_QUANTIZATION=scalar keeps the chosen graph
// preset and uses int8 coordinates for candidate discovery, then exact reranking.
func HNSWQuantizationConfigFromEnv() HNSWQuantizationConfig {
	mode := strings.ToLower(strings.TrimSpace(envutil.Get("NORNICDB_VECTOR_HNSW_QUANTIZATION", "scalar")))
	switch mode {
	case "none":
		mode = ""
	case "int8":
		mode = "scalar"
	case "product":
		mode = "pq"
	}
	cfg := HNSWQuantizationConfig{Mode: HNSWQuantization(mode)}
	if cfg.Mode == HNSWQuantizationBinary {
		cfg.BinaryQueryBits = envutil.GetInt("NORNICDB_VECTOR_HNSW_BINARY_QUERY_BITS", 1)
	}
	if cfg.Mode == HNSWQuantizationProduct {
		cfg.PQCompression = envutil.GetInt("NORNICDB_VECTOR_HNSW_PQ_COMPRESSION", 16)
		cfg.TrainingSampleMax = envutil.GetInt("NORNICDB_VECTOR_HNSW_PQ_TRAINING_SAMPLE_MAX", 10000)
		cfg.TrainingIterations = envutil.GetInt("NORNICDB_VECTOR_HNSW_PQ_TRAINING_ITERATIONS", 10)
	}
	return cfg
}

func (h *HNSWIndex) quantizedStrategyName() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.quantized == nil {
		if h.config.Quantization.Mode != HNSWQuantizationNone {
			return "HNSW (quantization pending)"
		}
		return "HNSW"
	}
	switch h.quantized.config.Mode {
	case HNSWQuantizationScalar:
		return fmt.Sprintf("HNSW scalar int8 (%d encoded bytes)", h.quantized.bytes())
	case HNSWQuantizationBinary:
		if h.quantized.config.BinaryQueryBits > 1 {
			return fmt.Sprintf("HNSW binary query int%d (%d encoded bytes)", h.quantized.config.BinaryQueryBits, h.quantized.bytes())
		}
		return fmt.Sprintf("HNSW binary (%d encoded bytes)", h.quantized.bytes())
	case HNSWQuantizationProduct:
		return fmt.Sprintf("HNSW PQ x%d (%d encoded bytes)", h.quantized.config.PQCompression, h.quantized.bytes())
	default:
		return "HNSW"
	}
}

func normalizeHNSWQuantizationConfig(cfg HNSWQuantizationConfig) (HNSWQuantizationConfig, error) {
	switch cfg.Mode {
	case HNSWQuantizationNone:
		return HNSWQuantizationConfig{}, nil
	case HNSWQuantizationScalar:
		return HNSWQuantizationConfig{Mode: cfg.Mode}, nil
	case HNSWQuantizationBinary:
		if cfg.BinaryQueryBits == 0 {
			cfg.BinaryQueryBits = 1
		}
		if cfg.BinaryQueryBits != 1 && cfg.BinaryQueryBits != 4 && cfg.BinaryQueryBits != 8 {
			return cfg, fmt.Errorf("binary query bits must be 1, 4 or 8")
		}
		return HNSWQuantizationConfig{Mode: cfg.Mode, BinaryQueryBits: cfg.BinaryQueryBits}, nil
	case HNSWQuantizationProduct:
		if cfg.PQCompression == 0 {
			cfg.PQCompression = 16
		}
		switch cfg.PQCompression {
		case 4, 8, 16, 32, 64:
		default:
			return cfg, fmt.Errorf("unsupported HNSW PQ compression ratio %d", cfg.PQCompression)
		}
		if cfg.TrainingSampleMax <= 0 {
			cfg.TrainingSampleMax = 10000
		}
		if cfg.TrainingIterations <= 0 {
			cfg.TrainingIterations = 10
		}
		return cfg, nil
	default:
		return cfg, fmt.Errorf("unsupported HNSW quantization %q", cfg.Mode)
	}
}

func (s *Service) hnswConfig() HNSWConfig {
	cfg := HNSWConfigFromEnv()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.hnswQuantizationOverride != nil {
		cfg.Quantization = *s.hnswQuantizationOverride
	}
	return cfg
}

// ConfigureHNSWQuantization sets a database's graph compression policy, for
// example from its persisted Qdrant collection configuration. It preserves
// lexical hints and original-vector scoring, and does not change global env.
//
//	err := service.ConfigureHNSWQuantization(ctx, HNSWQuantizationConfig{Mode:HNSWQuantizationScalar})
//
// The configured policy also applies after graph rebuilds and restarts when
// the owning collection reloads its durable configuration.
func (s *Service) ConfigureHNSWQuantization(ctx context.Context, cfg HNSWQuantizationConfig) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg, err := normalizeHNSWQuantizationConfig(cfg)
	if err != nil {
		return err
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.mu.RLock()
	same := s.hnswQuantizationOverride != nil && *s.hnswQuantizationOverride == cfg
	s.mu.RUnlock()
	if same {
		return nil
	}
	s.hnswMu.RLock()
	idx := s.hnswIndex
	s.hnswMu.RUnlock()
	if idx != nil {
		if err := idx.SetQuantization(ctx, cfg); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.hnswQuantizationOverride = &cfg
	s.mu.Unlock()
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()
	return nil
}
