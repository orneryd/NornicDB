package dbconfig

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// envList is the environment config owns for database-scoped settings.
var convergenceEnv = []string{
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

func clearConvergenceEnv(t *testing.T) {
	t.Helper()
	for _, name := range convergenceEnv {
		t.Setenv(name, "")
	}
}

// TestEffectiveFromGlobalProjectsConfigFields pins the no-environment
// projection of every database-scoped field. An unset/empty value must fall
// back to the Config field, while the raw registry loop still lets a
// real environment value through first.
func TestEffectiveFromGlobalProjectsConfigFields(t *testing.T) {
	clearConvergenceEnv(t)

	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 1024
	global.Memory.SearchBM25Warming = ""
	global.Memory.SearchVectorWarming = " LAZY "
	global.Memory.SearchBM25Stemmer = " Snowball.French "
	global.Memory.QueryCacheTTL = 90 * time.Second
	global.Memory.QueryCacheSize = 321
	global.Memory.TransactionMax = 2048
	global.Memory.TransactionTotalMax = 4096
	global.EmbeddingWorker.PropertiesInclude = []string{"title", "body"}
	global.EmbeddingWorker.EligibleLabels = nil
	global.Memory.EmbeddingGPULayers = -999
	global.Memory.KmeansNumClusters = 0
	global.Memory.AutoLinksEnabled = false
	global.Features.TopologyAutoIntegrationEnabled = true

	effective := Resolve(global, nil).Effective

	require.Equal(t, "1024", effective["db.nornic.embedding.dimensions"])
	require.Equal(t, "title,body", effective["db.nornic.embedding.properties.include"])
	require.Equal(t, "", effective["db.nornic.embedding.labels.include"])
	require.Equal(t, "-999", effective["db.nornic.embedding.gpu.layers"])

	// Warming: anything other than "lazy" projects as "startup"; "LAZY" projects "lazy".
	require.Equal(t, "startup", effective["db.nornic.search.bm25.warming"])
	require.Equal(t, "lazy", effective["db.nornic.search.vector.warming"])
	require.Equal(t, "snowball.french", effective["db.nornic.search.bm25.stemmer"])

	// Milliseconds for the query-cache TTL, integer entries for the size.
	require.Equal(t, "90000", effective["db.nornic.query_cache.ttl"])
	require.Equal(t, "321", effective["db.nornic.query_cache.max_entries"])

	require.Equal(t, "2048", effective["db.memory.transaction.max"])
	// The process-wide transaction pool is a physical-engine key, reported under
	// the canonical dbms name only by the registry's own environment loop.
	require.Equal(t, "4096", effective["dbms.memory.transaction.total.max"])
	require.NotContains(t, effective, "db.nornic.transaction.total.max")

	require.Equal(t, "0", effective["db.nornic.kmeans.num.clusters"])
	require.Equal(t, "false", effective["db.nornic.auto.links.enabled"])
	require.Equal(t, "true", effective["db.nornic.auto.tlp.enabled"])
}

// TestEffectiveFromGlobalReProjectsTypedFields pins the preserved ordering:
// the raw registry loop seeds the map, then every setting that has a typed
// Config field is re-projected from that field. Keys without a typed field
// keep the raw environment spelling.
func TestEffectiveFromGlobalReProjectsTypedFields(t *testing.T) {
	clearConvergenceEnv(t)
	t.Setenv("NORNICDB_SEARCH_BM25_WARMING", "lazy")
	t.Setenv("NORNICDB_QUERY_CACHE_SIZE", "4242")

	global := config.LoadDefaults()
	global.Memory.SearchBM25Warming = "startup"
	global.Memory.QueryCacheSize = 999

	effective := Resolve(global, nil).Effective

	// The Config field is authoritative once the typed visit runs.
	require.Equal(t, "startup", effective["db.nornic.search.bm25.warming"])
	require.Equal(t, "999", effective["db.nornic.query_cache.max_entries"])

	// Projection is read-only: the fields keep their loaded values.
	require.Equal(t, "startup", global.Memory.SearchBM25Warming)
	require.Equal(t, 999, global.Memory.QueryCacheSize)
}

// TestEffectiveFromGlobalEnvironmentOnlyKeysKeepRawValue pins that env-only
// keys (no typed Config field) still surface the raw environment spelling.
func TestEffectiveFromGlobalEnvironmentOnlyKeysKeepRawValue(t *testing.T) {
	clearConvergenceEnv(t)
	t.Setenv("NORNICDB_VECTOR_HNSW_M", "48")

	require.Equal(t, "48", Resolve(config.LoadDefaults(), nil).Effective["db.nornic.vector.hnsw.m"])
}

// TestEffectiveFromGlobalNormalisesLoadedEnvironment pins that the loader and
// the projection agree on the stored, normalised value.
func TestEffectiveFromGlobalNormalisesLoadedEnvironment(t *testing.T) {
	clearConvergenceEnv(t)
	t.Setenv("NORNICDB_SEARCH_BM25_WARMING", " LAZY ")
	t.Setenv("NORNICDB_SEARCH_BM25_STEMMER", " Snowball.Spanish ")
	t.Setenv("NORNICDB_QUERY_CACHE_TTL", "1000")

	loaded := config.LoadFromEnv()
	require.Equal(t, "lazy", loaded.Memory.SearchBM25Warming)
	require.Equal(t, "snowball.spanish", loaded.Memory.SearchBM25Stemmer)
	require.Equal(t, time.Second, loaded.Memory.QueryCacheTTL)

	effective := Resolve(loaded, nil).Effective
	require.Equal(t, "lazy", effective["db.nornic.search.bm25.warming"])
	require.Equal(t, "snowball.spanish", effective["db.nornic.search.bm25.stemmer"])
	require.Equal(t, "1000", effective["db.nornic.query_cache.ttl"])
}

// TestResolveDimensionsDefaultFollowsProjection pins the existing behaviour:
// a non-positive configured size projects and resolves as 1024 (never 512).
func TestResolveDimensionsDefaultFollowsProjection(t *testing.T) {
	clearConvergenceEnv(t)

	for _, configured := range []int{0, -512} {
		global := config.LoadDefaults()
		global.Memory.EmbeddingDimensions = configured

		resolved := Resolve(global, nil)
		require.Equal(t, 1024, resolved.EmbeddingDimensions, "configured=%d", configured)
		require.Equal(t, "1024", resolved.Effective["db.nornic.embedding.dimensions"], "configured=%d", configured)
	}

	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 512
	resolved := Resolve(global, nil)
	require.Equal(t, 512, resolved.EmbeddingDimensions)
	require.Equal(t, "512", resolved.Effective["db.nornic.embedding.dimensions"])
}

// TestVisitProjectionDoesNotMutateConfig pins the read-only contract of the
// typed projection across boundary values.
func TestVisitProjectionDoesNotMutateConfig(t *testing.T) {
	clearConvergenceEnv(t)

	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 0
	global.Memory.SearchBM25Warming = ""
	global.Memory.SearchBM25Stemmer = ""
	global.Memory.QueryCacheTTL = -time.Second
	global.Memory.KmeansNumClusters = 0
	global.Memory.EmbeddingGPULayers = -1
	global.EmbeddingWorker.ChunkSizeSet = false

	before := *global
	global.VisitDatabaseDefaults(func(string, string) {})
	require.Equal(t, before, *global)

	visited := 0
	global.VisitDatabaseDefaults(func(environmentVariable, value string) {
		visited++
		require.NotEmpty(t, environmentVariable)
		_ = value
	})
	require.Len(t, convergenceEnv, visited)
	require.Equal(t, before, *global)
}
