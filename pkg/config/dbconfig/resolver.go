package dbconfig

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
)

// ResolvedDbConfig holds effective per-DB config for search and embedding.
// Used by GetOrCreateSearchService and by the admin API "effective" response.
type ResolvedDbConfig struct {
	// EmbeddingDimensions is the vector size (e.g. 1024).
	EmbeddingDimensions int
	// SearchMinSimilarity is the min cosine similarity for vector search (0.0–1.0).
	SearchMinSimilarity float64
	// BM25Enabled controls whether BM25 fulltext search is built and queryable
	// for this database. Per-DB override wins over the global default.
	BM25Enabled bool
	// BM25Warming is "startup" (build at boot) or "lazy" (build on first query).
	// Ignored when BM25Enabled=false; preserved across flips so a future
	// re-enable honours the operator's intended trigger.
	BM25Warming string
	// BM25Stemmer is the selected BM25 stemmer plugin ID or "none".
	BM25Stemmer string
	// VectorEnabled controls whether ANY vector search strategy (HNSW,
	// IVF-HNSW, brute-force, GPU brute-force, Metal, Qdrant pass-through) is
	// built and queryable for this database. When false, node embeddings are
	// not iterated into RAM. Per-DB override wins over the global default.
	VectorEnabled bool
	// VectorWarming is "startup" or "lazy". See BM25Warming.
	VectorWarming               string
	QueryCacheMaxEntries        int
	QueryCacheTTL               time.Duration
	SearchResultCacheMaxEntries int
	SearchResultCacheTTL        time.Duration
	BM25MemoryMaxBytes          int64
	VectorMemoryMaxBytes        int64
	MetadataMemoryMaxBytes      int64
	BM25StorageMode             string
	VectorStorageMode           string
	// TransactionMemoryMaxBytes is db.memory.transaction.max and
	// TransactionMemoryTotalMaxBytes db.memory.transaction.total.max, in
	// bytes (0 = unlimited, #1343).
	TransactionMemoryMaxBytes      int64
	TransactionMemoryTotalMaxBytes int64
	// Effective is the full effective value for every allowed key (string form for API).
	Effective map[string]string
}

// Resolve merges global config with per-DB overrides and returns the resolved config.
//
// Precedence ladder, lowest → highest:
//
//  1. Built-in defaults (config.LoadDefaults).
//  2. YAML / env-loaded `global` config (cfg.Memory.Search* etc).
//  3. Explicit process/CLI overrides (cfg.CLIOverrides).
//  4. Persisted canonical per-DB settings supplied via the `overrides` map.
//
// Supported NORNICDB_* alternatives found in existing _DbConfig rows remain accepted,
// but canonical db.nornic.* values win when both forms are present.
func Resolve(global *config.Config, overrides map[string]string) *ResolvedDbConfig {
	r := &ResolvedDbConfig{
		EmbeddingDimensions:         global.Memory.EmbeddingDimensions,
		SearchMinSimilarity:         global.Memory.SearchMinSimilarity,
		BM25Enabled:                 global.Memory.SearchBM25Enabled,
		BM25Warming:                 config.NormalizeIndexWarming(global.Memory.SearchBM25Warming),
		BM25Stemmer:                 config.NormalizeStemmerSelection(global.Memory.SearchBM25Stemmer),
		VectorEnabled:               global.Memory.SearchVectorEnabled,
		VectorWarming:               config.NormalizeIndexWarming(global.Memory.SearchVectorWarming),
		QueryCacheMaxEntries:        global.Memory.QueryCacheSize,
		QueryCacheTTL:               global.Memory.QueryCacheTTL,
		SearchResultCacheMaxEntries: 1000,
		SearchResultCacheTTL:        5 * time.Minute,
		TransactionMemoryMaxBytes:   global.Memory.TransactionMax,
		BM25StorageMode:             "memory",
		VectorStorageMode:           "auto",
		Effective:                   make(map[string]string),
	}
	if r.EmbeddingDimensions <= 0 {
		r.EmbeddingDimensions = 1024
	}
	if r.QueryCacheTTL <= 0 {
		r.QueryCacheTTL = 5 * time.Minute
	}
	if !global.Memory.QueryCacheEnabled {
		r.QueryCacheMaxEntries = 0
	}
	// Build effective map from global (we'll overlay overrides below).
	effectiveFromGlobal(global, r.Effective)
	// Read-only walk of the private registry: no defensive copy of every
	// definition is needed because nothing here is handed to callers.
	for _, definition := range settingsRegistry {
		if definition.Scope != ScopePhysicalEngine && definition.DefaultValue != "" {
			if _, exists := r.Effective[definition.Name]; !exists {
				r.Effective[definition.Name] = definition.DefaultValue
			}
		}
	}
	// Explicit process/CLI overrides sit above global config but below stored
	// database settings.
	cliOverrides := CanonicalizeOverrides(global.CLIOverrides)
	for k, v := range cliOverrides {
		if !IsAllowedKey(k) {
			continue
		}
		applyOverride(r, k, v)
		r.Effective[CanonicalSettingName(k)] = v
	}
	// Persisted per-database settings are authoritative.
	databaseOverrides := CanonicalizeOverrides(overrides)
	for k, v := range databaseOverrides {
		if !IsAllowedKey(k) {
			continue
		}
		applyOverride(r, k, v)
		r.Effective[CanonicalSettingName(k)] = v
	}
	resolveEmbeddingProviderDefaults(global, r, cliOverrides, databaseOverrides)
	return r
}

func resolveEmbeddingProviderDefaults(global *config.Config, resolved *ResolvedDbConfig, overrideSets ...map[string]string) {
	providerKey := CanonicalSettingName("NORNICDB_EMBEDDING_PROVIDER")
	apiURLKey := CanonicalSettingName("NORNICDB_EMBEDDING_API_URL")
	modelKey := CanonicalSettingName("NORNICDB_EMBEDDING_MODEL")
	modeKey := CanonicalSettingName("NORNICDB_EMBEDDING_MODE")
	dimensionsKey := CanonicalSettingName("NORNICDB_EMBEDDING_DIMENSIONS")

	hasOverride := func(key string) bool {
		for _, overrides := range overrideSets {
			if _, ok := overrides[key]; ok {
				return true
			}
		}
		return false
	}

	providerConfig := embed.ResolveProviderConfigWithProvenance(&embed.Config{
		Provider:   resolved.Effective[providerKey],
		APIURL:     resolved.Effective[apiURLKey],
		Model:      resolved.Effective[modelKey],
		Mode:       resolved.Effective[modeKey],
		Dimensions: resolved.EmbeddingDimensions,
	}, embed.ProviderConfigExplicit{
		APIURL:     global.EmbeddingExplicit.APIURL || hasOverride(apiURLKey),
		Model:      global.EmbeddingExplicit.Model || hasOverride(modelKey),
		Dimensions: global.EmbeddingExplicit.Dimensions || hasOverride(dimensionsKey),
	})
	if providerConfig == nil {
		return
	}
	resolved.EmbeddingDimensions = providerConfig.Dimensions
	resolved.Effective[providerKey] = providerConfig.Provider
	resolved.Effective[apiURLKey] = providerConfig.APIURL
	resolved.Effective[modelKey] = providerConfig.Model
	resolved.Effective[dimensionsKey] = strconv.Itoa(providerConfig.Dimensions)
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
	// Registry definitions are read-only here; the exported Settings copy is unnecessary.
	for _, definition := range settingsRegistry {
		if definition.EnvironmentVariable == "" {
			continue
		}
		if value, ok := os.LookupEnv(definition.EnvironmentVariable); ok {
			m[definition.Name] = value
		}
	}
	// Typed projection of the database-scoped fields config owns: one read-only
	// visit per setting, each value rendered exactly as the startup loader
	// normalises it. Keys with no typed Config field keep the raw environment
	// value written above; every key that does have one is re-projected from the
	// effective Config value (the pre-convergence behaviour).
	c.VisitDatabaseDefaults(func(environmentVariable, value string) {
		m[CanonicalSettingName(environmentVariable)] = value
	})
}

func applyOverride(r *ResolvedDbConfig, key, value string) {
	key = CanonicalSettingName(key)
	value = strings.TrimSpace(value)
	// One registry lookup replaces the previous AllowedKeysSet() build (which
	// materialised every key's metadata) followed by a second LookupSetting.
	definition, ok := LookupSetting(key)
	if !ok || definition.Scope == ScopePhysicalEngine || KeysExcludedFromPerDB[key] {
		return
	}
	behaviorKey := definition.EnvironmentVariable
	if behaviorKey == "" {
		behaviorKey = key
	}
	switch definition.Type {
	case "number":
		if i, err := strconv.Atoi(value); err == nil {
			switch behaviorKey {
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
			if behaviorKey == "NORNICDB_SEARCH_MIN_SIMILARITY" || behaviorKey == "NORNICDB_AUTO_LINKS_THRESHOLD" {
				r.SearchMinSimilarity = f
			}
		}
	case "boolean":
		b := value == "true" || value == "1"
		_ = b
		// Only EmbeddingDimensions and SearchMinSimilarity are used in ResolvedDbConfig for now
	case "string", "duration":
		if behaviorKey == "NORNICDB_SEARCH_MIN_SIMILARITY" {
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				r.SearchMinSimilarity = f
			}
		}
	}
	// Always update EmbeddingDimensions and SearchMinSimilarity when present in overrides
	if behaviorKey == "NORNICDB_EMBEDDING_DIMENSIONS" {
		if i, err := strconv.Atoi(value); err == nil && i > 0 {
			r.EmbeddingDimensions = i
		}
	}
	if behaviorKey == "NORNICDB_SEARCH_MIN_SIMILARITY" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			r.SearchMinSimilarity = f
		}
	}
	if behaviorKey == "NORNICDB_SEARCH_BM25_STEMMER" {
		r.BM25Stemmer = config.NormalizeStemmerSelection(value)
	}
	if key == "db.nornic.search_result_cache.max_entries" {
		if entries, err := strconv.Atoi(value); err == nil && entries >= 0 {
			r.SearchResultCacheMaxEntries = entries
		}
	}
	if key == "db.nornic.search_result_cache.ttl" {
		if ttl, err := time.ParseDuration(value); err == nil && ttl >= 0 {
			r.SearchResultCacheTTL = ttl
		}
	}
	if key == "db.nornic.query_cache.ttl" {
		if milliseconds, err := strconv.ParseInt(value, 10, 64); err == nil && milliseconds > 0 {
			r.QueryCacheTTL = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if key == "db.nornic.query_cache.max_entries" {
		if entries, err := strconv.Atoi(value); err == nil && entries >= 0 {
			r.QueryCacheMaxEntries = entries
		}
	}
	switch key {
	case "db.memory.transaction.max":
		r.TransactionMemoryMaxBytes, _ = config.ParseByteSize(value)
	case "db.memory.transaction.total.max":
		r.TransactionMemoryTotalMaxBytes, _ = config.ParseByteSize(value)
	case "db.nornic.memory.index.bm25.max":
		r.BM25MemoryMaxBytes, _ = config.ParseByteSize(value)
	case "db.nornic.memory.index.vector.max":
		r.VectorMemoryMaxBytes, _ = config.ParseByteSize(value)
	case "db.nornic.memory.index.metadata.max":
		r.MetadataMemoryMaxBytes, _ = config.ParseByteSize(value)
	case "db.nornic.index.bm25.storage":
		r.BM25StorageMode = strings.ToLower(value)
	case "db.nornic.index.vector.storage":
		r.VectorStorageMode = strings.ToLower(value)
	}
	switch behaviorKey {
	case "NORNICDB_SEARCH_BM25_ENABLED":
		r.BM25Enabled = parseBoolFallback(value, r.BM25Enabled)
	case "NORNICDB_SEARCH_BM25_WARMING":
		if ok, _ := IsValidEnumValue(key, value); ok {
			r.BM25Warming = config.NormalizeIndexWarming(value)
		}
	case "NORNICDB_SEARCH_VECTOR_ENABLED":
		r.VectorEnabled = parseBoolFallback(value, r.VectorEnabled)
	case "NORNICDB_SEARCH_VECTOR_WARMING":
		if ok, _ := IsValidEnumValue(key, value); ok {
			r.VectorWarming = config.NormalizeIndexWarming(value)
		}
	}
}
