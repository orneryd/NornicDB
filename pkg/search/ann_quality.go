package search

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

// ANNQuality selects the high-level ANN strategy mode.
type ANNQuality string

const (
	ANNQualityFast       ANNQuality = "fast"
	ANNQualityBalanced   ANNQuality = "balanced"
	ANNQualityAccurate   ANNQuality = "accurate"
	ANNQualityCompressed ANNQuality = "compressed"
)

// ANNQualityFromEnv parses the global ANN quality selector.
// Unknown values default to fast to preserve historical behavior.
func ANNQualityFromEnv() ANNQuality {
	raw := strings.ToLower(strings.TrimSpace(envutil.Get("NORNICDB_VECTOR_ANN_QUALITY", "")))
	switch raw {
	case string(ANNQualityFast):
		return ANNQualityFast
	case string(ANNQualityBalanced):
		return ANNQualityBalanced
	case string(ANNQualityAccurate):
		return ANNQualityAccurate
	case string(ANNQualityCompressed):
		return ANNQualityCompressed
	case "":
		return ANNQualityFast
	default:
		return ANNQualityFast
	}
}
