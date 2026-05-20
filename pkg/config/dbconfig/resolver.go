package dbconfig

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// ResolvedDbConfig holds effective per-DB config for search and embedding.
// Used by getOrCreateSearchService and by the admin API "effective" response.
type ResolvedDbConfig struct {
	// EmbeddingDimensions is the vector size (e.g. 1024).
	EmbeddingDimensions int
	// SearchMinSimilarity is the min cosine similarity for vector search (0.0–1.0).
	SearchMinSimilarity float64
	// BM25Engine selects fulltext engine implementation ("v1" or "v2").
	BM25Engine string
	// BM25Enabled controls whether BM25 fulltext search is built and queryable
	// for this database. Per-DB override wins over the global default.
	BM25Enabled bool
	// BM25Warming is "startup" (build at boot) or "lazy" (build on first query).
	// Ignored when BM25Enabled=false; preserved across flips so a future
	// re-enable honours the operator's intended trigger.
	BM25Warming string
	// VectorEnabled controls whether ANY vector search strategy (HNSW,
	// IVF-HNSW, brute-force, GPU brute-force, Metal, Qdrant pass-through) is
	// built and queryable for this database. When false, node embeddings are
	// not iterated into RAM. Per-DB override wins over the global default.
	VectorEnabled bool
	// VectorWarming is "startup" or "lazy". See BM25Warming.
	VectorWarming string
	// Effective is the full effective value for every allowed key (string form for API).
	Effective map[string]string
}

// Resolve merges global config with per-DB overrides and returns the resolved config.
// Overrides are applied on top of global; omitted keys use global default.
//
// Per-DB overrides always win over the global default, in both directions: an
// override of `true` turns on a globally-disabled index; an override of `false`
// turns off a globally-enabled one. This is load-bearing for the multi-tenant
// story (one DB needs search, the rest don't).
func Resolve(global *config.Config, overrides map[string]string) *ResolvedDbConfig {
	r := &ResolvedDbConfig{
		EmbeddingDimensions: global.Memory.EmbeddingDimensions,
		SearchMinSimilarity: global.Memory.SearchMinSimilarity,
		BM25Engine:          normalizeBM25Engine(os.Getenv("NORNICDB_SEARCH_BM25_ENGINE")),
		BM25Enabled:         global.Memory.SearchBM25Enabled,
		BM25Warming:         normalizeWarming(global.Memory.SearchBM25Warming),
		VectorEnabled:       global.Memory.SearchVectorEnabled,
		VectorWarming:       normalizeWarming(global.Memory.SearchVectorWarming),
		Effective:           make(map[string]string),
	}
	if r.EmbeddingDimensions <= 0 {
		r.EmbeddingDimensions = 1024
	}
	// Build effective map from global (we'll overlay overrides below).
	effectiveFromGlobal(global, r.Effective)
	for k, v := range overrides {
		if !IsAllowedKey(k) {
			continue
		}
		applyOverride(r, k, v)
		r.Effective[k] = v
	}
	return r
}

// normalizeWarming returns "startup" or "lazy"; anything else (including empty)
// falls back to "startup" so deployments without the new keys keep today's
// behaviour.
func normalizeWarming(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "lazy":
		return "lazy"
	default:
		return "startup"
	}
}

// parseBoolFallback returns the parsed value when the string is a recognised
// bool literal (true/false/1/0, case-insensitive); otherwise returns fallback.
// Typos can't silently flip a boolean — the caller's existing value wins.
func parseBoolFallback(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1":
		return true
	case "false", "0":
		return false
	default:
		return fallback
	}
}

func effectiveFromGlobal(c *config.Config, m map[string]string) {
	if c == nil {
		return
	}
	// Embeddings
	m["NORNICDB_EMBEDDING_ENABLED"] = boolStr(c.Memory.EmbeddingEnabled)
	m["NORNICDB_EMBEDDING_PROVIDER"] = c.Memory.EmbeddingProvider
	m["NORNICDB_EMBEDDING_MODEL"] = c.Memory.EmbeddingModel
	m["NORNICDB_EMBEDDING_API_URL"] = c.Memory.EmbeddingAPIURL
	m["NORNICDB_EMBEDDING_API_KEY"] = c.Memory.EmbeddingAPIKey
	m["NORNICDB_EMBEDDING_DIMENSIONS"] = strconv.Itoa(c.Memory.EmbeddingDimensions)
	if c.Memory.EmbeddingDimensions <= 0 {
		m["NORNICDB_EMBEDDING_DIMENSIONS"] = "1024"
	}
	m["NORNICDB_EMBEDDING_CACHE_SIZE"] = strconv.Itoa(c.Memory.EmbeddingCacheSize)
	m["NORNICDB_EMBEDDING_PROPERTIES_INCLUDE"] = strings.Join(c.EmbeddingWorker.PropertiesInclude, ",")
	m["NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE"] = strings.Join(c.EmbeddingWorker.PropertiesExclude, ",")
	m["NORNICDB_EMBEDDING_INCLUDE_LABELS"] = boolStr(c.EmbeddingWorker.IncludeLabels)
	m["NORNICDB_EMBEDDING_GPU_LAYERS"] = strconv.Itoa(c.Memory.EmbeddingGPULayers)
	m["NORNICDB_EMBEDDING_WARMUP_INTERVAL"] = c.Memory.EmbeddingWarmupInterval.String()
	// Search
	m["NORNICDB_SEARCH_MIN_SIMILARITY"] = strconv.FormatFloat(c.Memory.SearchMinSimilarity, 'f', -1, 64)
	m["NORNICDB_SEARCH_BM25_ENGINE"] = normalizeBM25Engine(os.Getenv("NORNICDB_SEARCH_BM25_ENGINE"))
	m["NORNICDB_SEARCH_BM25_ENABLED"] = boolStr(c.Memory.SearchBM25Enabled)
	m["NORNICDB_SEARCH_BM25_WARMING"] = normalizeWarming(c.Memory.SearchBM25Warming)
	m["NORNICDB_SEARCH_VECTOR_ENABLED"] = boolStr(c.Memory.SearchVectorEnabled)
	m["NORNICDB_SEARCH_VECTOR_WARMING"] = normalizeWarming(c.Memory.SearchVectorWarming)
	m["NORNICDB_SEARCH_RERANK_ENABLED"] = boolStr(c.Features.SearchRerankEnabled)
	m["NORNICDB_SEARCH_RERANK_PROVIDER"] = c.Features.SearchRerankProvider
	m["NORNICDB_SEARCH_RERANK_MODEL"] = c.Features.SearchRerankModel
	m["NORNICDB_SEARCH_RERANK_API_URL"] = c.Features.SearchRerankAPIURL
	m["NORNICDB_SEARCH_RERANK_API_KEY"] = c.Features.SearchRerankAPIKey
	// K-means (from Memory)
	m["NORNICDB_KMEANS_MIN_EMBEDDINGS"] = strconv.Itoa(c.Memory.KmeansMinEmbeddings)
	m["NORNICDB_KMEANS_CLUSTER_INTERVAL"] = c.Memory.KmeansClusterInterval.String()
	m["NORNICDB_KMEANS_NUM_CLUSTERS"] = strconv.Itoa(c.Memory.KmeansNumClusters)
	// Auto-links
	m["NORNICDB_AUTO_LINKS_ENABLED"] = boolStr(c.Memory.AutoLinksEnabled)
	m["NORNICDB_AUTO_LINKS_THRESHOLD"] = strconv.FormatFloat(c.Memory.AutoLinksSimilarityThreshold, 'f', -1, 64)
	// Embed worker
	m["NORNICDB_EMBED_WORKER_NUM_WORKERS"] = strconv.Itoa(c.EmbeddingWorker.NumWorkers)
	m["NORNICDB_EMBED_SCAN_INTERVAL"] = c.EmbeddingWorker.ScanInterval.String()
	m["NORNICDB_EMBED_BATCH_DELAY"] = c.EmbeddingWorker.BatchDelay.String()
	m["NORNICDB_EMBED_TRIGGER_DEBOUNCE"] = c.EmbeddingWorker.TriggerDebounceDelay.String()
	m["NORNICDB_EMBED_MAX_RETRIES"] = strconv.Itoa(c.EmbeddingWorker.MaxRetries)
	m["NORNICDB_EMBED_CHUNK_SIZE"] = strconv.Itoa(c.EmbeddingWorker.ChunkSize)
	m["NORNICDB_EMBED_CHUNK_OVERLAP"] = strconv.Itoa(c.EmbeddingWorker.ChunkOverlap)
	m["NORNICDB_MVCC_LIFECYCLE_INTERVAL"] = c.Database.MVCCLifecycleCycleInterval.String()
	// Feature flags for Auto-TLP (from Features; K-means clustering is env-only in feature_flags)
	m["NORNICDB_AUTO_TLP_ENABLED"] = boolStr(c.Features.TopologyAutoIntegrationEnabled)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func applyOverride(r *ResolvedDbConfig, key, value string) {
	value = strings.TrimSpace(value)
	meta, ok := AllowedKeysSet()[key]
	if !ok {
		return
	}
	switch meta.Type {
	case "number":
		if i, err := strconv.Atoi(value); err == nil {
			switch key {
			case "NORNICDB_EMBEDDING_DIMENSIONS":
				r.EmbeddingDimensions = i
				if r.EmbeddingDimensions <= 0 {
					r.EmbeddingDimensions = 1024
				}
			case "NORNICDB_SEARCH_MIN_SIMILARITY":
				// stored as string for generic keys; we also set float
				if f, err := strconv.ParseFloat(value, 64); err == nil {
					r.SearchMinSimilarity = f
				}
			}
		} else if f, err := strconv.ParseFloat(value, 64); err == nil {
			if key == "NORNICDB_SEARCH_MIN_SIMILARITY" || key == "NORNICDB_AUTO_LINKS_THRESHOLD" {
				r.SearchMinSimilarity = f
			}
		}
	case "boolean":
		b := value == "true" || value == "1"
		_ = b
		// Only EmbeddingDimensions and SearchMinSimilarity are used in ResolvedDbConfig for now
	case "string", "duration":
		if key == "NORNICDB_SEARCH_MIN_SIMILARITY" {
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				r.SearchMinSimilarity = f
			}
		}
	}
	// Always update EmbeddingDimensions and SearchMinSimilarity when present in overrides
	if key == "NORNICDB_EMBEDDING_DIMENSIONS" {
		if i, err := strconv.Atoi(value); err == nil && i > 0 {
			r.EmbeddingDimensions = i
		}
	}
	if key == "NORNICDB_SEARCH_MIN_SIMILARITY" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			r.SearchMinSimilarity = f
		}
	}
	if key == "NORNICDB_SEARCH_BM25_ENGINE" {
		r.BM25Engine = normalizeBM25Engine(value)
	}
	switch key {
	case "NORNICDB_SEARCH_BM25_ENABLED":
		r.BM25Enabled = parseBoolFallback(value, r.BM25Enabled)
	case "NORNICDB_SEARCH_BM25_WARMING":
		if ok, _ := IsValidEnumValue(key, value); ok {
			r.BM25Warming = normalizeWarming(value)
		}
	case "NORNICDB_SEARCH_VECTOR_ENABLED":
		r.VectorEnabled = parseBoolFallback(value, r.VectorEnabled)
	case "NORNICDB_SEARCH_VECTOR_WARMING":
		if ok, _ := IsValidEnumValue(key, value); ok {
			r.VectorWarming = normalizeWarming(value)
		}
	}
}

func normalizeBM25Engine(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "v1":
		return "v1"
	default:
		return "v2"
	}
}

// ParseDuration parses a duration string (e.g. 5m, 30s). Returns 0 on error.
func ParseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(strings.TrimSpace(s))
	return d
}
