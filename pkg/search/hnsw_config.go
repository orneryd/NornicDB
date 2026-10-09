// Package search - HNSW configuration and quality presets.
package search

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strings"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

// HNSWQualityPreset defines quality presets for HNSW tuning.
type HNSWQualityPreset string

const (
	// QualityFast prioritizes speed over recall.
	// Lower efSearch, lower candidate_multiplier for faster queries.
	QualityFast HNSWQualityPreset = "fast"

	// QualityBalanced provides a good balance between speed and recall.
	// Default preset with reasonable defaults.
	QualityBalanced HNSWQualityPreset = "balanced"

	// QualityAccurate prioritizes recall over speed.
	// Higher efSearch and/or bigger candidate pool for better accuracy.
	QualityAccurate HNSWQualityPreset = "accurate"
)

const defaultHNSWSearchBeamFactor = 4

// HNSWConfigFromEnv loads HNSW configuration from environment variables.
//
// Environment Variables:
//   - NORNICDB_VECTOR_ANN_QUALITY: Quality preset (fast|balanced|accurate|compressed, default: fast)
//   - NORNICDB_VECTOR_HNSW_M: Max connections per node (default: based on preset)
//   - NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION: Candidate list size during construction (default: based on preset)
//   - NORNICDB_VECTOR_HNSW_EF_SEARCH: Candidate list size during search (default: based on preset)
//   - NORNICDB_VECTOR_HNSW_BEAM_FACTOR: Search beam multiplier relative to requested candidates (default: 4)
//   - NORNICDB_HNSW_BUILD_GPU_ENABLED: Attempt GPU-assisted construction (default: true)
//   - NORNICDB_HNSW_BUILD_GPU_BATCH_SIZE: GPU build batch size (default: 2048)
//   - NORNICDB_HNSW_BUILD_GPU_CANDIDATE_K: GPU candidate count per vector (default: 128)
//
// Quality Presets:
//   - fast: M=16, efConstruction=100, efSearch=50 (faster, lower recall)
//   - balanced: M=16, efConstruction=200, efSearch=100 (good balance)
//   - accurate: M=32, efConstruction=400, efSearch=200 (slower, higher recall)
//   - compressed: parsed at ANN quality layer; maps to fast HNSW defaults until
//     compressed ANN strategy routing is active
//
// Example:
//
//	// Use fast preset
//	os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "fast")
//	config := HNSWConfigFromEnv()
//
//	// Override specific parameter
//	os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "balanced")
//	os.Setenv("NORNICDB_VECTOR_HNSW_EF_SEARCH", "150")
//	config := HNSWConfigFromEnv()
func HNSWConfigFromEnv() HNSWConfig {
	preset := getQualityPreset()

	// Start with preset defaults
	config := presetDefaults(preset)

	// Apply advanced overrides if set
	if m := envutil.GetInt("NORNICDB_VECTOR_HNSW_M", 0); m > 0 {
		config.M = m
		config.LevelMultiplier = 1.0 / math.Log(float64(m))
	}

	if efConstruction := envutil.GetInt("NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", 0); efConstruction > 0 {
		config.EfConstruction = efConstruction
	}

	if efSearch := envutil.GetInt("NORNICDB_VECTOR_HNSW_EF_SEARCH", 0); efSearch > 0 {
		config.EfSearch = efSearch
	}
	if beamFactor := envutil.GetInt("NORNICDB_VECTOR_HNSW_BEAM_FACTOR", 0); beamFactor > 0 {
		config.SearchBeamFactor = beamFactor
	}

	config.UseGPUBuild = envutil.GetBoolStrict("NORNICDB_HNSW_BUILD_GPU_ENABLED", true)
	config.GPUBuildBatchSize = envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BATCH_SIZE", 2048)
	if config.GPUBuildBatchSize <= 0 {
		config.GPUBuildBatchSize = 2048
	}
	config.GPUBuildCandidateK = envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_CANDIDATE_K", 128)
	if config.GPUBuildCandidateK <= 0 {
		config.GPUBuildCandidateK = 128
	}
	config.GPUBuildDistancePrecision = strings.ToLower(strings.TrimSpace(envutil.Get("NORNICDB_HNSW_BUILD_GPU_DISTANCE_PRECISION", "fp32")))
	if config.GPUBuildDistancePrecision == "" {
		config.GPUBuildDistancePrecision = "fp32"
	}

	return config
}

// getQualityPreset returns the quality preset from environment variable.
func getQualityPreset() HNSWQualityPreset {
	return hnswPresetFromANNQuality(ANNQualityFromEnv())
}

// hnswPresetFromANNQuality maps top-level ANN quality to HNSW-specific
// defaults. Compressed is parsed at the quality layer and currently maps to
// fast HNSW defaults until compressed ANN strategy wiring is active.
func hnswPresetFromANNQuality(quality ANNQuality) HNSWQualityPreset {
	switch quality {
	case ANNQualityFast:
		return QualityFast
	case ANNQualityAccurate:
		return QualityAccurate
	case ANNQualityBalanced:
		return QualityBalanced
	case ANNQualityCompressed:
		return QualityFast
	default:
		return QualityFast
	}
}

// presetDefaults returns HNSW config defaults for the given preset.
func presetDefaults(preset HNSWQualityPreset) HNSWConfig {
	switch preset {
	case QualityFast:
		return HNSWConfig{
			M:                         16,
			EfConstruction:            100,
			EfSearch:                  50,
			SearchBeamFactor:          defaultHNSWSearchBeamFactor,
			LevelMultiplier:           1.0 / math.Log(16.0),
			UseGPUBuild:               true,
			GPUBuildBatchSize:         2048,
			GPUBuildCandidateK:        128,
			GPUBuildDistancePrecision: "fp32",
		}
	case QualityAccurate:
		return HNSWConfig{
			M:                         32,
			EfConstruction:            400,
			EfSearch:                  200,
			SearchBeamFactor:          defaultHNSWSearchBeamFactor,
			LevelMultiplier:           1.0 / math.Log(32.0),
			UseGPUBuild:               true,
			GPUBuildBatchSize:         2048,
			GPUBuildCandidateK:        128,
			GPUBuildDistancePrecision: "fp32",
		}
	case QualityBalanced:
		fallthrough
	default:
		return DefaultHNSWConfig()
	}
}
