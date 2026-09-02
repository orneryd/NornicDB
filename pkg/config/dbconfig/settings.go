package dbconfig

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type SettingScope string

const (
	ScopeDatabase       SettingScope = "database"
	ScopePhysicalEngine SettingScope = "physical-engine"
)

type RestartLevel string

const (
	RestartNone    RestartLevel = "none"
	RestartProcess RestartLevel = "process"
)

type HotReloadMode string

const (
	HotReloadNone          HotReloadMode = ""
	HotReloadSearchRebuild HotReloadMode = "search-rebuild"
	HotReloadSearchCache   HotReloadMode = "search-cache"
)

// SettingDefinition is the source of truth for setting validation and metadata.
type SettingDefinition struct {
	Name                string
	EnvironmentVariable string
	Type                string
	Category            string
	Description         string
	DefaultValue        string
	Scope               SettingScope
	Dynamic             bool
	RestartLevel        RestartLevel
	HotReload           HotReloadMode
	ZeroSemantics       string
	Deprecated          bool
	Redacted            bool
	ValidValues         []string
}

var settingsRegistry = []SettingDefinition{
	{Name: "db.nornic.embedding.enabled", EnvironmentVariable: "NORNICDB_EMBEDDING_ENABLED", Type: "boolean", Category: "Embeddings", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embedding.provider", EnvironmentVariable: "NORNICDB_EMBEDDING_PROVIDER", Type: "string", Category: "Embeddings", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.embedding.model", EnvironmentVariable: "NORNICDB_EMBEDDING_MODEL", Type: "string", Category: "Embeddings", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.embedding.api.url", EnvironmentVariable: "NORNICDB_EMBEDDING_API_URL", Type: "string", Category: "Embeddings", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.embedding.api.key", EnvironmentVariable: "NORNICDB_EMBEDDING_API_KEY", Type: "string", Category: "Embeddings", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild, Redacted: true},
	{Name: "db.nornic.embedding.dimensions", EnvironmentVariable: "NORNICDB_EMBEDDING_DIMENSIONS", Type: "number", Category: "Embeddings", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.embedding.cache.size", EnvironmentVariable: "NORNICDB_EMBEDDING_CACHE_SIZE", Type: "number", Category: "Embeddings", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embedding.properties.include", EnvironmentVariable: "NORNICDB_EMBEDDING_PROPERTIES_INCLUDE", Type: "string", Category: "Embeddings", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embedding.properties.exclude", EnvironmentVariable: "NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE", Type: "string", Category: "Embeddings", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embedding.include.labels", EnvironmentVariable: "NORNICDB_EMBEDDING_INCLUDE_LABELS", Type: "boolean", Category: "Embeddings", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embedding.gpu.layers", EnvironmentVariable: "NORNICDB_EMBEDDING_GPU_LAYERS", Type: "number", Category: "Embeddings", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.embedding.warmup.interval", EnvironmentVariable: "NORNICDB_EMBEDDING_WARMUP_INTERVAL", Type: "duration", Category: "Embeddings", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.search.min.similarity", EnvironmentVariable: "NORNICDB_SEARCH_MIN_SIMILARITY", Type: "number", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.bm25.engine", EnvironmentVariable: "NORNICDB_SEARCH_BM25_ENGINE", Type: "string", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.bm25.enabled", EnvironmentVariable: "NORNICDB_SEARCH_BM25_ENABLED", Type: "boolean", Category: "Search", Description: "Master switch for BM25 fulltext search on this database. When false, no BM25 build runs and search returns no fulltext results. Default: true.", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.bm25.warming", EnvironmentVariable: "NORNICDB_SEARCH_BM25_WARMING", Type: "enum", Category: "Search", Description: "When BM25 is enabled, choose startup or lazy warming.", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild, ValidValues: []string{"startup", "lazy"}},
	{Name: "db.nornic.search.vector.enabled", EnvironmentVariable: "NORNICDB_SEARCH_VECTOR_ENABLED", Type: "boolean", Category: "Search", Description: "Master switch for vector search on this database. Default: true.", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.vector.warming", EnvironmentVariable: "NORNICDB_SEARCH_VECTOR_WARMING", Type: "enum", Category: "Search", Description: "When vector search is enabled, choose startup or lazy warming.", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild, ValidValues: []string{"startup", "lazy"}},
	{Name: "db.nornic.search.rerank.enabled", EnvironmentVariable: "NORNICDB_SEARCH_RERANK_ENABLED", Type: "boolean", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.rerank.provider", EnvironmentVariable: "NORNICDB_SEARCH_RERANK_PROVIDER", Type: "string", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.rerank.model", EnvironmentVariable: "NORNICDB_SEARCH_RERANK_MODEL", Type: "string", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.rerank.api.url", EnvironmentVariable: "NORNICDB_SEARCH_RERANK_API_URL", Type: "string", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild},
	{Name: "db.nornic.search.rerank.api.key", EnvironmentVariable: "NORNICDB_SEARCH_RERANK_API_KEY", Type: "string", Category: "Search", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchRebuild, Redacted: true},
	{Name: "db.nornic.search.index.persist.delay.sec", EnvironmentVariable: "NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", Type: "number", Category: "Search", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.ann.quality", EnvironmentVariable: "NORNICDB_VECTOR_ANN_QUALITY", Type: "string", Category: "HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.hnsw.m", EnvironmentVariable: "NORNICDB_VECTOR_HNSW_M", Type: "number", Category: "HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.hnsw.ef.construction", EnvironmentVariable: "NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", Type: "number", Category: "HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.hnsw.ef.search", EnvironmentVariable: "NORNICDB_VECTOR_HNSW_EF_SEARCH", Type: "number", Category: "HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.hnsw.metal.min.candidates", EnvironmentVariable: "NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES", Type: "number", Category: "HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.ivf.hnsw.enabled", EnvironmentVariable: "NORNICDB_VECTOR_IVF_HNSW_ENABLED", Type: "boolean", Category: "IVF-HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.ivf.hnsw.min.cluster.size", EnvironmentVariable: "NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", Type: "number", Category: "IVF-HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.ivf.hnsw.max.clusters", EnvironmentVariable: "NORNICDB_VECTOR_IVF_HNSW_MAX_CLUSTERS", Type: "number", Category: "IVF-HNSW", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.cpu.brute.max.n", EnvironmentVariable: "NORNICDB_VECTOR_CPU_BRUTE_MAX_N", Type: "number", Category: "Vector", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.gpu.brute.min.n", EnvironmentVariable: "NORNICDB_VECTOR_GPU_BRUTE_MIN_N", Type: "number", Category: "Vector", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.vector.gpu.brute.max.n", EnvironmentVariable: "NORNICDB_VECTOR_GPU_BRUTE_MAX_N", Type: "number", Category: "Vector", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.kmeans.clustering.enabled", EnvironmentVariable: "NORNICDB_KMEANS_CLUSTERING_ENABLED", Type: "boolean", Category: "K-means", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.kmeans.min.embeddings", EnvironmentVariable: "NORNICDB_KMEANS_MIN_EMBEDDINGS", Type: "number", Category: "K-means", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.kmeans.cluster.interval", EnvironmentVariable: "NORNICDB_KMEANS_CLUSTER_INTERVAL", Type: "duration", Category: "K-means", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.kmeans.num.clusters", EnvironmentVariable: "NORNICDB_KMEANS_NUM_CLUSTERS", Type: "number", Category: "K-means", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.kmeans.max.iterations", EnvironmentVariable: "NORNICDB_KMEANS_MAX_ITERATIONS", Type: "number", Category: "K-means", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.auto.links.enabled", EnvironmentVariable: "NORNICDB_AUTO_LINKS_ENABLED", Type: "boolean", Category: "Auto-links", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.auto.links.threshold", EnvironmentVariable: "NORNICDB_AUTO_LINKS_THRESHOLD", Type: "number", Category: "Auto-links", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.auto.tlp.enabled", EnvironmentVariable: "NORNICDB_AUTO_TLP_ENABLED", Type: "boolean", Category: "Auto-TLP", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.auto.tlp.llm.qc.enabled", EnvironmentVariable: "NORNICDB_AUTO_TLP_LLM_QC_ENABLED", Type: "boolean", Category: "Auto-TLP", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.auto.tlp.llm.augment.enabled", EnvironmentVariable: "NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED", Type: "boolean", Category: "Auto-TLP", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.worker.num.workers", EnvironmentVariable: "NORNICDB_EMBED_WORKER_NUM_WORKERS", Type: "number", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.scan.interval", EnvironmentVariable: "NORNICDB_EMBED_SCAN_INTERVAL", Type: "duration", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.batch.delay", EnvironmentVariable: "NORNICDB_EMBED_BATCH_DELAY", Type: "duration", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.trigger.debounce", EnvironmentVariable: "NORNICDB_EMBED_TRIGGER_DEBOUNCE", Type: "duration", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.max.retries", EnvironmentVariable: "NORNICDB_EMBED_MAX_RETRIES", Type: "number", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.chunk.size", EnvironmentVariable: "NORNICDB_EMBED_CHUNK_SIZE", Type: "number", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.embed.chunk.overlap", EnvironmentVariable: "NORNICDB_EMBED_CHUNK_OVERLAP", Type: "number", Category: "Embed worker", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.mvcc.lifecycle.interval", EnvironmentVariable: "NORNICDB_MVCC_LIFECYCLE_INTERVAL", Type: "duration", Category: "MVCC lifecycle", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.query_cache.max_entries", EnvironmentVariable: "NORNICDB_QUERY_CACHE_SIZE", Type: "number", Category: "Query cache", Description: "Maximum entries in this database's query cache. Correlates with Neo4j's server.memory.query_cache.per_db_cache_num_entries, but is independently configurable per database.", DefaultValue: "1000", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "disabled"},
	{Name: "db.nornic.query_cache.ttl", EnvironmentVariable: "NORNICDB_QUERY_CACHE_TTL", Type: "number", Category: "Query cache", Description: "Query result cache TTL in milliseconds for this database.", DefaultValue: "300000", Scope: ScopeDatabase, RestartLevel: RestartProcess},
	{Name: "db.nornic.query_plan_cache.max_entries", Type: "number", Category: "Query cache", DefaultValue: "500", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "disabled"},
	{Name: "db.nornic.fabric_plan_cache.max_entries", Type: "number", Category: "Query cache", DefaultValue: "500", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "disabled"},
	{Name: "db.nornic.query_analysis_cache.max_entries", Type: "number", Category: "Query cache", DefaultValue: "1000", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "disabled"},
	{Name: "db.nornic.search_result_cache.max_entries", Type: "number", Category: "Search", DefaultValue: "1000", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchCache, ZeroSemantics: "disabled"},
	{Name: "db.nornic.search_result_cache.ttl", Type: "duration", Category: "Search", DefaultValue: "5m", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, HotReload: HotReloadSearchCache},
	{Name: "db.nornic.query_lookup_metadata.max_entries", Type: "number", Category: "Query cache", DefaultValue: "0", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "existing per-cache bounds"},
	{Name: "db.memory.transaction.total.max", Type: "bytes", Category: "Transactions", DefaultValue: "0", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.memory.index.bm25.max", Type: "bytes", Category: "Index capacity", DefaultValue: "0", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.memory.index.vector.max", Type: "bytes", Category: "Index capacity", DefaultValue: "0", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.memory.index.metadata.max", Type: "bytes", Category: "Index capacity", DefaultValue: "0", Scope: ScopeDatabase, RestartLevel: RestartProcess, ZeroSemantics: "unlimited per index"},
	{Name: "db.nornic.index.bm25.storage", Type: "enum", Category: "Index capacity", DefaultValue: "memory", Scope: ScopeDatabase, RestartLevel: RestartProcess, ValidValues: []string{"memory"}},
	{Name: "db.nornic.index.vector.storage", Type: "enum", Category: "Index capacity", DefaultValue: "auto", Scope: ScopeDatabase, RestartLevel: RestartProcess, ValidValues: []string{"auto", "memory", "disk"}},
	{Name: "db.nornic.memory.storage.mode", Type: "enum", Category: "Storage", DefaultValue: "default", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, ValidValues: []string{"default", "low"}},
	{Name: "db.nornic.memory.storage.node_cache.max_entries", EnvironmentVariable: "NORNICDB_BADGER_NODE_CACHE_MAX_ENTRIES", Type: "number", Category: "Storage", DefaultValue: "10000", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess},
	{Name: "db.nornic.memory.storage.edge_type_cache.max_entries", EnvironmentVariable: "NORNICDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES", Type: "number", Category: "Storage", DefaultValue: "50", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess},
	{Name: "db.nornic.recovery.batch.max_bytes", Type: "bytes", Category: "Recovery", DefaultValue: "0", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.recovery.memory.max", Type: "bytes", Category: "Recovery", DefaultValue: "0", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
}

// Settings returns defensive copies of all registered settings.
func Settings() []SettingDefinition {
	settings := make([]SettingDefinition, 0, len(settingsRegistry))
	for _, definition := range settingsRegistry {
		definition.ValidValues = append([]string(nil), definition.ValidValues...)
		settings = append(settings, definition)
	}
	return settings
}

// CanonicalSettingName maps a supported environment-variable alternative to
// its persisted dotted setting name. Unknown names are returned unchanged.
func CanonicalSettingName(name string) string {
	trimmed := strings.TrimSpace(name)
	for _, definition := range settingsRegistry {
		if trimmed == definition.Name || (definition.EnvironmentVariable != "" && trimmed == definition.EnvironmentVariable) {
			return definition.Name
		}
	}
	return trimmed
}

// CanonicalizeOverrides rewrites environment alternatives to canonical names. When both
// forms are present, the canonical database setting wins deterministically.
func CanonicalizeOverrides(overrides map[string]string) map[string]string {
	if len(overrides) == 0 {
		return nil
	}
	canonical := make(map[string]string, len(overrides))
	for _, definition := range settingsRegistry {
		if definition.EnvironmentVariable != "" {
			if value, exists := overrides[definition.EnvironmentVariable]; exists {
				canonical[definition.Name] = value
			}
		}
	}
	for key, value := range overrides {
		name := CanonicalSettingName(key)
		if name == key {
			canonical[name] = value
		}
	}
	return canonical
}

func LookupSetting(name string) (SettingDefinition, bool) {
	name = CanonicalSettingName(name)
	for _, definition := range Settings() {
		if definition.Name == name {
			return definition, true
		}
	}
	return SettingDefinition{}, false
}

func ParseByteSize(raw string) (int64, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return 0, fmt.Errorf("byte size is empty")
	}
	multiplier := int64(1)
	if suffix := value[len(value)-1]; suffix < '0' || suffix > '9' {
		switch suffix {
		case 'k':
			multiplier = 1 << 10
		case 'm':
			multiplier = 1 << 20
		case 'g':
			multiplier = 1 << 30
		case 't':
			multiplier = 1 << 40
		default:
			return 0, fmt.Errorf("invalid byte-size suffix in %q", raw)
		}
		value = strings.TrimSpace(value[:len(value)-1])
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid byte size %q", raw)
	}
	if parsed > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("byte size overflows int64: %q", raw)
	}
	return parsed * multiplier, nil
}

func NormalizeSettingValue(name, raw string) (string, error) {
	definition, ok := LookupSetting(name)
	if !ok {
		return "", fmt.Errorf("unknown setting %s", name)
	}
	value := strings.TrimSpace(raw)
	switch definition.Type {
	case "bytes":
		parsed, err := ParseByteSize(value)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(parsed, 10), nil
	case "boolean":
		parsed, err := strconv.ParseBool(value)
		if value == "1" {
			parsed, err = true, nil
		} else if value == "0" {
			parsed, err = false, nil
		}
		if err != nil {
			return "", fmt.Errorf("invalid boolean %q", raw)
		}
		return strconv.FormatBool(parsed), nil
	case "number":
		if isFloatingPointSetting(definition.Name) {
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
				return "", fmt.Errorf("invalid nonnegative number %q", raw)
			}
			return strconv.FormatFloat(parsed, 'f', -1, 64), nil
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			return "", fmt.Errorf("invalid nonnegative number %q", raw)
		}
		return strconv.FormatInt(parsed, 10), nil
	case "duration":
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 0 {
			return "", fmt.Errorf("invalid duration %q", raw)
		}
		return parsed.String(), nil
	case "enum":
		for _, allowed := range definition.ValidValues {
			if strings.EqualFold(value, allowed) {
				return allowed, nil
			}
		}
		return "", fmt.Errorf("invalid value for %s: got %s (allowed: %s)", name, raw, strings.Join(definition.ValidValues, ","))
	default:
		return value, nil
	}
}

func isFloatingPointSetting(name string) bool {
	switch name {
	case "db.nornic.search.min.similarity", "db.nornic.auto.links.threshold":
		return true
	default:
		return false
	}
}
