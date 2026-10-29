package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// convergenceLoaderEnv lists every variable the typed binding owns while
// loading. Each case sets exactly the variables it cares about and clears the
// rest so the loader is deterministic.
var convergenceLoaderEnv = []string{
	"NORNICDB_EMBEDDING_ENABLED",
	"NORNICDB_EMBEDDING_PROVIDER",
	"NORNICDB_EMBEDDING_MODEL",
	"NORNICDB_EMBEDDING_API_URL",
	"NORNICDB_EMBEDDING_API_KEY",
	"NORNICDB_EMBEDDING_MODE",
	"NORNICDB_EMBEDDING_DIMENSIONS",
	"NORNICDB_EMBEDDING_CACHE_SIZE",
	"NORNICDB_EMBEDDING_PROPERTIES_INCLUDE",
	"NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE",
	"NORNICDB_EMBEDDING_INCLUDE_LABELS",
	"NORNICDB_EMBEDDING_LABELS_INCLUDE",
	"NORNICDB_EMBEDDING_LABELS_EXCLUDE",
	"NORNICDB_EMBEDDING_GPU_LAYERS",
	"NORNICDB_EMBEDDING_WARMUP_INTERVAL",
	"NORNICDB_SEARCH_MIN_SIMILARITY",
	"NORNICDB_SEARCH_BM25_ENABLED",
	"NORNICDB_SEARCH_BM25_WARMING",
	"NORNICDB_SEARCH_BM25_STEMMER",
	"NORNICDB_SEARCH_VECTOR_ENABLED",
	"NORNICDB_SEARCH_VECTOR_WARMING",
	"NORNICDB_SEARCH_RERANK_ENABLED",
	"NORNICDB_SEARCH_RERANK_PROVIDER",
	"NORNICDB_SEARCH_RERANK_MODEL",
	"NORNICDB_SEARCH_RERANK_API_URL",
	"NORNICDB_SEARCH_RERANK_API_KEY",
	"NORNICDB_KMEANS_MIN_EMBEDDINGS",
	"NORNICDB_KMEANS_CLUSTER_INTERVAL",
	"NORNICDB_KMEANS_NUM_CLUSTERS",
	"NORNICDB_AUTO_LINKS_ENABLED",
	"NORNICDB_AUTO_LINKS_THRESHOLD",
	"NORNICDB_AUTO_TLP_ENABLED",
	"NORNICDB_EMBED_WORKER_NUM_WORKERS",
	"NORNICDB_EMBED_SCAN_INTERVAL",
	"NORNICDB_EMBED_BATCH_DELAY",
	"NORNICDB_EMBED_TRIGGER_DEBOUNCE",
	"NORNICDB_EMBED_MAX_RETRIES",
	"NORNICDB_EMBED_CHUNK_SIZE",
	"NORNICDB_EMBED_CHUNK_OVERLAP",
	"NORNICDB_MVCC_LIFECYCLE_INTERVAL",
	"NORNICDB_QUERY_CACHE_SIZE",
	"NORNICDB_QUERY_CACHE_TTL",
	"NORNICDB_MEMORY_TRANSACTION_MAX",
	"NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX",
}

func loadWithConvergenceEnv(t *testing.T, env map[string]string) *Config {
	t.Helper()
	for _, name := range convergenceLoaderEnv {
		t.Setenv(name, "")
	}
	for name, value := range env {
		t.Setenv(name, value)
	}
	return LoadFromEnv()
}

// TestTypedBindingPreservesStartupGuards pins the original guards: zero,
// empty, and sentinel values must not overwrite the defaults.
func TestTypedBindingPreservesStartupGuards(t *testing.T) {
	defaults := LoadDefaults()

	cfg := loadWithConvergenceEnv(t, map[string]string{
		"NORNICDB_EMBEDDING_DIMENSIONS":         "0",
		"NORNICDB_EMBEDDING_CACHE_SIZE":         "0",
		"NORNICDB_EMBEDDING_GPU_LAYERS":         "-999",
		"NORNICDB_EMBED_WORKER_NUM_WORKERS":     "-1",
		"NORNICDB_EMBED_CHUNK_SIZE":             "0",
		"NORNICDB_EMBED_CHUNK_OVERLAP":          "-1",
		"NORNICDB_EMBED_TRIGGER_DEBOUNCE":       "-1s",
		"NORNICDB_QUERY_CACHE_SIZE":             "0",
		"NORNICDB_QUERY_CACHE_TTL":              "0",
		"NORNICDB_SEARCH_BM25_WARMING":          "bogus",
		"NORNICDB_SEARCH_BM25_STEMMER":          "   ",
		"NORNICDB_AUTO_LINKS_ENABLED":           "true",
		"NORNICDB_SEARCH_RERANK_ENABLED":        "1",
		"NORNICDB_MEMORY_TRANSACTION_MAX":       "",
		"NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX": "",
	})

	require.Equal(t, defaults.Memory.EmbeddingDimensions, cfg.Memory.EmbeddingDimensions)
	require.Equal(t, defaults.Memory.EmbeddingCacheSize, cfg.Memory.EmbeddingCacheSize)
	require.Equal(t, defaults.Memory.EmbeddingGPULayers, cfg.Memory.EmbeddingGPULayers)
	require.Equal(t, defaults.EmbeddingWorker.NumWorkers, cfg.EmbeddingWorker.NumWorkers)
	require.Equal(t, defaults.EmbeddingWorker.ChunkSize, cfg.EmbeddingWorker.ChunkSize)
	require.False(t, cfg.EmbeddingWorker.ChunkSizeSet)
	require.Equal(t, defaults.EmbeddingWorker.ChunkOverlap, cfg.EmbeddingWorker.ChunkOverlap)
	require.False(t, cfg.EmbeddingWorker.ChunkOverlapSet)
	require.Equal(t, defaults.EmbeddingWorker.TriggerDebounceDelay, cfg.EmbeddingWorker.TriggerDebounceDelay)
	require.Equal(t, defaults.Memory.QueryCacheSize, cfg.Memory.QueryCacheSize)
	require.Equal(t, defaults.Memory.QueryCacheTTL, cfg.Memory.QueryCacheTTL)
	// Invalid enums are ignored, leaving the default in place.
	require.Equal(t, "startup", cfg.Memory.SearchBM25Warming)
	require.Equal(t, "none", cfg.Memory.SearchBM25Stemmer)
	// These two only ever enable; "true"/"1" is not their trigger spelling.
	require.True(t, cfg.Memory.AutoLinksEnabled)
	require.False(t, cfg.Features.SearchRerankEnabled)
	require.Equal(t, int64(0), cfg.Memory.TransactionMax)
}

// TestTypedBindingPreservesZeroMeaningfulSettings pins the settings whose zero
// is meaningful and therefore must be applied.
func TestTypedBindingPreservesZeroMeaningfulSettings(t *testing.T) {
	cfg := loadWithConvergenceEnv(t, map[string]string{
		"NORNICDB_KMEANS_NUM_CLUSTERS":      "0",
		"NORNICDB_EMBED_TRIGGER_DEBOUNCE":   "0s",
		"NORNICDB_EMBED_CHUNK_OVERLAP":      "0",
		"NORNICDB_EMBED_WORKER_NUM_WORKERS": "0",
		"NORNICDB_AUTO_LINKS_ENABLED":       "false",
	})

	require.Equal(t, 0, cfg.Memory.KmeansNumClusters)
	require.Equal(t, time.Duration(0), cfg.EmbeddingWorker.TriggerDebounceDelay)
	require.Equal(t, 0, cfg.EmbeddingWorker.ChunkOverlap)
	require.True(t, cfg.EmbeddingWorker.ChunkOverlapSet)
	require.Equal(t, 0, cfg.EmbeddingWorker.NumWorkers)
	require.False(t, cfg.Memory.AutoLinksEnabled)
}

// TestTypedBindingSetsExplicitProvenance pins the explicit-flag side effects.
func TestTypedBindingSetsExplicitProvenance(t *testing.T) {
	cfg := loadWithConvergenceEnv(t, map[string]string{
		"NORNICDB_EMBEDDING_MODEL":      "custom-model",
		"NORNICDB_EMBEDDING_API_URL":    "http://custom",
		"NORNICDB_EMBEDDING_DIMENSIONS": "768",
	})

	require.Equal(t, EmbeddingExplicitSettings{Model: true, APIURL: true, Dimensions: true}, cfg.EmbeddingExplicit)
	require.Equal(t, "custom-model", cfg.Memory.EmbeddingModel)
	require.Equal(t, 768, cfg.Memory.EmbeddingDimensions)

	// Zero dimensions never mark the field explicit.
	cfg = loadWithConvergenceEnv(t, map[string]string{"NORNICDB_EMBEDDING_DIMENSIONS": "0"})
	require.False(t, cfg.EmbeddingExplicit.Dimensions)
}

// TestTypedBindingNormalisesAndConvertsUnits pins normalisation and the
// documented millisecond TTL unit.
func TestTypedBindingNormalisesAndConvertsUnits(t *testing.T) {
	cfg := loadWithConvergenceEnv(t, map[string]string{
		"NORNICDB_SEARCH_BM25_WARMING":      " LAZY ",
		"NORNICDB_SEARCH_VECTOR_WARMING":    "Startup",
		"NORNICDB_SEARCH_BM25_STEMMER":      " Snowball.French ",
		"NORNICDB_SEARCH_RERANK_PROVIDER":   "  Local ",
		"NORNICDB_EMBEDDING_MODE":           "  contextualized  ",
		"NORNICDB_QUERY_CACHE_TTL":          "1500",
		"NORNICDB_BOLT_SERVER_ANNOUNCEMENT": "",
	})

	require.Equal(t, "lazy", cfg.Memory.SearchBM25Warming)
	require.Equal(t, "startup", cfg.Memory.SearchVectorWarming)
	require.Equal(t, "snowball.french", cfg.Memory.SearchBM25Stemmer)
	require.Equal(t, "local", cfg.Features.SearchRerankProvider)
	require.Equal(t, "contextualized", cfg.Memory.EmbeddingMode)
	require.Equal(t, 1500*time.Millisecond, cfg.Memory.QueryCacheTTL)
}

// TestTypedBindingPreservesByteSizeErrors pins the original error text and
// that the error aborts loading.
func TestTypedBindingPreservesByteSizeErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"max", map[string]string{"NORNICDB_MEMORY_TRANSACTION_MAX": "not-bytes"}},
		{"total", map[string]string{"NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX": "not-bytes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range convergenceLoaderEnv {
				t.Setenv(name, "")
			}
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			cfg := LoadDefaults()
			err := applyEnvVars(cfg)
			require.Error(t, err)
			var wantName string
			for name := range tc.env {
				wantName = name
			}
			require.Contains(t, err.Error(), "invalid "+wantName+" value \"not-bytes\"")
		})
	}

	// A valid suffix still parses.
	cfg := loadWithConvergenceEnv(t, map[string]string{"NORNICDB_MEMORY_TRANSACTION_MAX": "2m"})
	require.Equal(t, int64(2*1024*1024), cfg.Memory.TransactionMax)
}

// TestTypedBindingStartupOrderKeepsLaterWins pins that the original sequential
// order is preserved: a later-started block still wins over an earlier one.
func TestTypedBindingStartupOrderKeepsLaterWins(t *testing.T) {
	for _, name := range convergenceLoaderEnv {
		t.Setenv(name, "")
	}
	cfg := LoadDefaults()
	_ = os.Unsetenv("NORNICDB_SEARCH_BM25_STEMMER")
	t.Setenv("NORNICDB_SEARCH_BM25_STEMMER", "snowball.english")
	require.NoError(t, applyEnvVars(cfg))
	require.Equal(t, "snowball.english", cfg.Memory.SearchBM25Stemmer)
}

// TestVisitDatabaseDefaultsProjectsStartupValues pins that the projection
// reads the values the loader produced, not raw environment spellings.
func TestVisitDatabaseDefaultsProjectsStartupValues(t *testing.T) {
	cfg := loadWithConvergenceEnv(t, map[string]string{
		"NORNICDB_SEARCH_BM25_WARMING":  " LAZY ",
		"NORNICDB_QUERY_CACHE_TTL":      "2500",
		"NORNICDB_AUTO_TLP_ENABLED":     "true",
		"NORNICDB_EMBED_CHUNK_SIZE":     "1234",
		"NORNICDB_EMBEDDING_DIMENSIONS": "0",
	})

	got := map[string]string{}
	cfg.VisitDatabaseDefaults(func(environmentVariable, value string) {
		got[environmentVariable] = value
	})

	require.Len(t, got, len(convergenceLoaderEnv))
	require.Equal(t, "lazy", got["NORNICDB_SEARCH_BM25_WARMING"])
	require.Equal(t, "2500", got["NORNICDB_QUERY_CACHE_TTL"])
	require.Equal(t, "true", got["NORNICDB_AUTO_TLP_ENABLED"])
	require.Equal(t, "1234", got["NORNICDB_EMBED_CHUNK_SIZE"])
	// Unset dimensions fall back to the startup default, never 512.
	require.Equal(t, "1024", got["NORNICDB_EMBEDDING_DIMENSIONS"])
	require.Equal(t, "none", got["NORNICDB_SEARCH_BM25_STEMMER"])
}

// TestVisitDatabaseDefaultsMirrorsEnvironmentNames pins that every visited
// environment variable is the one the loader reads: setting it through the
// loader changes exactly the projection the visit reports.
func TestVisitDatabaseDefaultsMirrorsEnvironmentNames(t *testing.T) {
	cfg := loadWithConvergenceEnv(t, map[string]string{"NORNICDB_SEARCH_RERANK_MODEL": "rerank-x"})
	got := map[string]string{}
	cfg.VisitDatabaseDefaults(func(environmentVariable, value string) {
		got[environmentVariable] = value
	})
	require.Equal(t, "rerank-x", got["NORNICDB_SEARCH_RERANK_MODEL"])
	require.Contains(t, got, "NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX")
	require.Len(t, got, len(convergenceLoaderEnv))
}
