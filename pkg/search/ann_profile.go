package search

import (
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strings"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

const (
	compressedProfileSchemaVersion = "1"
)

// CompressedActivationDiagnostic captures deterministic activation decisions.
type CompressedActivationDiagnostic struct {
	Code    string
	Message string
}

// CompressedANNProfile is the resolved runtime contract for compressed ANN mode.
type CompressedANNProfile struct {
	Quality             ANNQuality
	Active              bool
	Dimensions          int
	VectorCount         int
	IVFLists            int
	PQSegments          int
	PQBits              int
	NProbe              int
	RerankTopK          int
	TrainingSampleMax   int
	KMeansMaxIterations int
	OverflowMax         int
	SeedMaxTerms        int
	SeedDocsPerTerm     int
	RoutingMode         string
	Diagnostics         []CompressedActivationDiagnostic
}

// ResolveCompressedANNProfile resolves compressed ANN settings and readiness.
// Shared knobs are reused directly where semantically compatible.
func ResolveCompressedANNProfile(vectorCount, dimensions int, vectorStoreReady bool) CompressedANNProfile {
	profile := CompressedANNProfile{
		Quality:     ANNQualityFromEnv(),
		Dimensions:  dimensions,
		VectorCount: vectorCount,
	}
	if profile.Quality != ANNQualityCompressed {
		profile.Diagnostics = append(profile.Diagnostics, CompressedActivationDiagnostic{
			Code:    "quality_not_compressed",
			Message: "NORNICDB_VECTOR_ANN_QUALITY is not compressed",
		})
		return profile
	}

	profile.KMeansMaxIterations = clampInt(envutil.GetInt("NORNICDB_KMEANS_MAX_ITERATIONS", 5), 5, 500)
	profile.SeedMaxTerms = clampInt(envutil.GetInt("NORNICDB_KMEANS_SEED_MAX_TERMS", 256), 16, 8192)
	profile.SeedDocsPerTerm = clampInt(envutil.GetInt("NORNICDB_KMEANS_SEED_DOCS_PER_TERM", 1), 1, 64)

	routing := strings.TrimSpace(strings.ToLower(envutil.Get("NORNICDB_VECTOR_ROUTING_MODE", "hybrid")))
	if routing == "" {
		routing = "hybrid"
	}
	profile.RoutingMode = routing

	autoLists := autoIVFLists(vectorCount)
	profile.IVFLists = clampInt(envutil.GetInt("NORNICDB_VECTOR_IVF_LISTS", autoLists), 16, 65536)
	sharedClusterHint := envutil.GetInt("NORNICDB_KMEANS_NUM_CLUSTERS", 0)
	if sharedClusterHint > 0 {
		profile.IVFLists = clampInt(sharedClusterHint, 16, 65536)
	}
	if explicitLists := envutil.GetInt("NORNICDB_VECTOR_IVF_LISTS", 0); explicitLists > 0 {
		profile.IVFLists = clampInt(explicitLists, 16, 65536)
	}

	defaultSegments := 16
	if dimensions > 0 && dimensions <= 128 {
		defaultSegments = 8
	} else if dimensions > 128 {
		defaultSegments = clampInt(dimensions/8, 1, 128)
		for dimensions%defaultSegments != 0 {
			defaultSegments--
		}
	}
	profile.PQSegments = clampInt(envutil.GetInt("NORNICDB_VECTOR_PQ_SEGMENTS", defaultSegments), 1, 128)
	profile.PQBits = clampInt(envutil.GetInt("NORNICDB_VECTOR_PQ_BITS", 8), 4, 8)
	defaultNProbe := maxInt(16, (profile.IVFLists+7)/8)
	profile.NProbe = clampInt(envutil.GetInt("NORNICDB_VECTOR_IVFPQ_NPROBE", defaultNProbe), 1, profile.IVFLists)
	profile.RerankTopK = clampInt(envutil.GetInt("NORNICDB_VECTOR_IVFPQ_RERANK_TOPK", 2000), 10, 20000)
	profile.TrainingSampleMax = clampInt(envutil.GetInt("NORNICDB_VECTOR_IVFPQ_TRAINING_SAMPLE_MAX", 200000), 1000, 5000000)
	profile.OverflowMax = clampInt(envutil.GetInt("NORNICDB_VECTOR_IVFPQ_OVERFLOW_MAX", 512), 0, 100000)

	if dimensions <= 0 {
		profile.Diagnostics = append(profile.Diagnostics, CompressedActivationDiagnostic{
			Code:    "invalid_dimensions",
			Message: "vector dimensions must be > 0",
		})
	}
	if !vectorStoreReady {
		profile.Diagnostics = append(profile.Diagnostics, CompressedActivationDiagnostic{
			Code:    "vector_store_unavailable",
			Message: "compressed mode requires vector file store availability for build/persist",
		})
	}
	if profile.PQSegments <= 0 || dimensions%profile.PQSegments != 0 {
		profile.Diagnostics = append(profile.Diagnostics, CompressedActivationDiagnostic{
			Code:    "invalid_segments",
			Message: fmt.Sprintf("dimensions (%d) must be divisible by NORNICDB_VECTOR_PQ_SEGMENTS (%d)", dimensions, profile.PQSegments),
		})
	}
	minVectors := maxInt(profile.IVFLists*4, 512)
	if vectorCount < minVectors {
		profile.Diagnostics = append(profile.Diagnostics, CompressedActivationDiagnostic{
			Code:    "insufficient_vectors",
			Message: fmt.Sprintf("compressed mode needs at least %d vectors for current IVF list count (%d)", minVectors, profile.IVFLists),
		})
	}
	if profile.TrainingSampleMax < profile.IVFLists {
		profile.Diagnostics = append(profile.Diagnostics, CompressedActivationDiagnostic{
			Code:    "training_sample_too_small",
			Message: "NORNICDB_VECTOR_IVFPQ_TRAINING_SAMPLE_MAX is too small for configured IVF lists",
		})
	}

	profile.Active = len(profile.Diagnostics) == 0
	return profile
}

func autoIVFLists(vectorCount int) int {
	if vectorCount <= 0 {
		return 64
	}
	// Reuse k-means auto intuition: sqrt(n/2), clamped.
	val := int(math.Sqrt(float64(vectorCount) / 2.0))
	return clampInt(val, 16, 8192)
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
