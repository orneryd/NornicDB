// Package search - HNSW configuration and quality presets.
package search

import (
	"math"
	"os"
	"strconv"
	"strings"
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

// HNSWConfigFromEnv loads HNSW configuration from environment variables.
//
// Environment Variables:
//   - NORNICDB_VECTOR_ANN_QUALITY: Quality preset (fast|balanced|accurate, default: balanced)
//   - NORNICDB_VECTOR_HNSW_M: Max connections per node (default: based on preset)
//   - NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION: Candidate list size during construction (default: based on preset)
//   - NORNICDB_VECTOR_HNSW_EF_SEARCH: Candidate list size during search (default: based on preset)
//
// Quality Presets:
//   - fast: M=16, efConstruction=100, efSearch=50 (faster, lower recall)
//   - balanced: M=16, efConstruction=200, efSearch=100 (default, good balance)
//   - accurate: M=32, efConstruction=400, efSearch=200 (slower, higher recall)
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
	if m := getEnvInt("NORNICDB_VECTOR_HNSW_M", 0); m > 0 {
		config.M = m
		config.LevelMultiplier = 1.0 / math.Log(float64(m))
	}

	if efConstruction := getEnvInt("NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", 0); efConstruction > 0 {
		config.EfConstruction = efConstruction
	}

	if efSearch := getEnvInt("NORNICDB_VECTOR_HNSW_EF_SEARCH", 0); efSearch > 0 {
		config.EfSearch = efSearch
	}

	return config
}

// getQualityPreset returns the quality preset from environment variable.
func getQualityPreset() HNSWQualityPreset {
	quality := strings.ToLower(os.Getenv("NORNICDB_VECTOR_ANN_QUALITY"))
	switch quality {
	case "fast":
		return QualityFast
	case "accurate":
		return QualityAccurate
	case "balanced", "":
		return QualityBalanced
	default:
		// Unknown preset, default to balanced
		return QualityBalanced
	}
}

// presetDefaults returns HNSW config defaults for the given preset.
func presetDefaults(preset HNSWQualityPreset) HNSWConfig {
	switch preset {
	case QualityFast:
		return HNSWConfig{
			M:               16,
			EfConstruction:  100,
			EfSearch:        50,
			LevelMultiplier: 1.0 / math.Log(16.0),
		}
	case QualityAccurate:
		return HNSWConfig{
			M:               32,
			EfConstruction:  400,
			EfSearch:        200,
			LevelMultiplier: 1.0 / math.Log(32.0),
		}
	case QualityBalanced:
		fallthrough
	default:
		return DefaultHNSWConfig()
	}
}

// getEnvInt reads an integer environment variable.
func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

