package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// databaseSettingID identifies one database-scoped setting bound to exactly one
// Config field. This table is the single owner of each setting's environment
// variable name: applyEnvVars reads it while loading, and dbconfig's
// effective-value projection reads it while rendering.
//
// The numeric values are private and opaque; callers must not persist them.
type databaseSettingID int

const (
	// Embeddings
	settingEmbeddingEnabled databaseSettingID = iota
	settingEmbeddingProvider
	settingEmbeddingModel
	settingEmbeddingAPIURL
	settingEmbeddingAPIKey
	settingEmbeddingMode
	settingEmbeddingDimensions
	settingEmbeddingCacheSize
	settingEmbeddingPropertiesInclude
	settingEmbeddingPropertiesExclude
	settingEmbeddingIncludeLabels
	settingEmbeddingLabelsInclude
	settingEmbeddingLabelsExclude
	settingEmbeddingGPULayers
	settingEmbeddingWarmupInterval
	// Search
	settingSearchMinSimilarity
	settingSearchBM25Enabled
	settingSearchBM25Warming
	settingSearchBM25Stemmer
	settingSearchVectorEnabled
	settingSearchVectorWarming
	settingSearchRerankEnabled
	settingSearchRerankProvider
	settingSearchRerankModel
	settingSearchRerankAPIURL
	settingSearchRerankAPIKey
	// K-means
	settingKmeansMinEmbeddings
	settingKmeansClusterInterval
	settingKmeansNumClusters
	// Auto-links and Auto-TLP
	settingAutoLinksEnabled
	settingAutoLinksThreshold
	settingAutoTLPEnabled
	// Embed worker
	settingEmbedWorkerNumWorkers
	settingEmbedScanInterval
	settingEmbedBatchDelay
	settingEmbedTriggerDebounce
	settingEmbedMaxRetries
	settingEmbedChunkSize
	settingEmbedChunkOverlap
	// MVCC lifecycle
	settingMVCCLifecycleInterval
	// Query cache and transaction memory
	settingQueryCacheSize
	settingQueryCacheTTL
	settingMemoryTransactionMax
	settingMemoryTransactionTotalMax
	databaseSettingCount
)

// databaseSetting is the typed identifier table entry. EnvironmentVariable is
// the sole definition of the setting's environment name.
type databaseSetting struct {
	EnvironmentVariable string
}

var databaseSettings = [databaseSettingCount]databaseSetting{
	settingEmbeddingEnabled:           {EnvironmentVariable: "NORNICDB_EMBEDDING_ENABLED"},
	settingEmbeddingProvider:          {EnvironmentVariable: "NORNICDB_EMBEDDING_PROVIDER"},
	settingEmbeddingModel:             {EnvironmentVariable: "NORNICDB_EMBEDDING_MODEL"},
	settingEmbeddingAPIURL:            {EnvironmentVariable: "NORNICDB_EMBEDDING_API_URL"},
	settingEmbeddingAPIKey:            {EnvironmentVariable: "NORNICDB_EMBEDDING_API_KEY"},
	settingEmbeddingMode:              {EnvironmentVariable: "NORNICDB_EMBEDDING_MODE"},
	settingEmbeddingDimensions:        {EnvironmentVariable: "NORNICDB_EMBEDDING_DIMENSIONS"},
	settingEmbeddingCacheSize:         {EnvironmentVariable: "NORNICDB_EMBEDDING_CACHE_SIZE"},
	settingEmbeddingPropertiesInclude: {EnvironmentVariable: "NORNICDB_EMBEDDING_PROPERTIES_INCLUDE"},
	settingEmbeddingPropertiesExclude: {EnvironmentVariable: "NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE"},
	settingEmbeddingIncludeLabels:     {EnvironmentVariable: "NORNICDB_EMBEDDING_INCLUDE_LABELS"},
	settingEmbeddingLabelsInclude:     {EnvironmentVariable: "NORNICDB_EMBEDDING_LABELS_INCLUDE"},
	settingEmbeddingLabelsExclude:     {EnvironmentVariable: "NORNICDB_EMBEDDING_LABELS_EXCLUDE"},
	settingEmbeddingGPULayers:         {EnvironmentVariable: "NORNICDB_EMBEDDING_GPU_LAYERS"},
	settingEmbeddingWarmupInterval:    {EnvironmentVariable: "NORNICDB_EMBEDDING_WARMUP_INTERVAL"},
	settingSearchMinSimilarity:        {EnvironmentVariable: "NORNICDB_SEARCH_MIN_SIMILARITY"},
	settingSearchBM25Enabled:          {EnvironmentVariable: "NORNICDB_SEARCH_BM25_ENABLED"},
	settingSearchBM25Warming:          {EnvironmentVariable: "NORNICDB_SEARCH_BM25_WARMING"},
	settingSearchBM25Stemmer:          {EnvironmentVariable: "NORNICDB_SEARCH_BM25_STEMMER"},
	settingSearchVectorEnabled:        {EnvironmentVariable: "NORNICDB_SEARCH_VECTOR_ENABLED"},
	settingSearchVectorWarming:        {EnvironmentVariable: "NORNICDB_SEARCH_VECTOR_WARMING"},
	settingSearchRerankEnabled:        {EnvironmentVariable: "NORNICDB_SEARCH_RERANK_ENABLED"},
	settingSearchRerankProvider:       {EnvironmentVariable: "NORNICDB_SEARCH_RERANK_PROVIDER"},
	settingSearchRerankModel:          {EnvironmentVariable: "NORNICDB_SEARCH_RERANK_MODEL"},
	settingSearchRerankAPIURL:         {EnvironmentVariable: "NORNICDB_SEARCH_RERANK_API_URL"},
	settingSearchRerankAPIKey:         {EnvironmentVariable: "NORNICDB_SEARCH_RERANK_API_KEY"},
	settingKmeansMinEmbeddings:        {EnvironmentVariable: "NORNICDB_KMEANS_MIN_EMBEDDINGS"},
	settingKmeansClusterInterval:      {EnvironmentVariable: "NORNICDB_KMEANS_CLUSTER_INTERVAL"},
	settingKmeansNumClusters:          {EnvironmentVariable: "NORNICDB_KMEANS_NUM_CLUSTERS"},
	settingAutoLinksEnabled:           {EnvironmentVariable: "NORNICDB_AUTO_LINKS_ENABLED"},
	settingAutoLinksThreshold:         {EnvironmentVariable: "NORNICDB_AUTO_LINKS_THRESHOLD"},
	settingAutoTLPEnabled:             {EnvironmentVariable: "NORNICDB_AUTO_TLP_ENABLED"},
	settingEmbedWorkerNumWorkers:      {EnvironmentVariable: "NORNICDB_EMBED_WORKER_NUM_WORKERS"},
	settingEmbedScanInterval:          {EnvironmentVariable: "NORNICDB_EMBED_SCAN_INTERVAL"},
	settingEmbedBatchDelay:            {EnvironmentVariable: "NORNICDB_EMBED_BATCH_DELAY"},
	settingEmbedTriggerDebounce:       {EnvironmentVariable: "NORNICDB_EMBED_TRIGGER_DEBOUNCE"},
	settingEmbedMaxRetries:            {EnvironmentVariable: "NORNICDB_EMBED_MAX_RETRIES"},
	settingEmbedChunkSize:             {EnvironmentVariable: "NORNICDB_EMBED_CHUNK_SIZE"},
	settingEmbedChunkOverlap:          {EnvironmentVariable: "NORNICDB_EMBED_CHUNK_OVERLAP"},
	settingMVCCLifecycleInterval:      {EnvironmentVariable: "NORNICDB_MVCC_LIFECYCLE_INTERVAL"},
	settingQueryCacheSize:             {EnvironmentVariable: "NORNICDB_QUERY_CACHE_SIZE"},
	settingQueryCacheTTL:              {EnvironmentVariable: "NORNICDB_QUERY_CACHE_TTL"},
	settingMemoryTransactionMax:       {EnvironmentVariable: "NORNICDB_MEMORY_TRANSACTION_MAX"},
	settingMemoryTransactionTotalMax:  {EnvironmentVariable: "NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX"},
}

// VisitDatabaseDefaults reports this config's value for every database-scoped
// setting this package owns to visit, in fixed registration order, without
// mutating the config or environment. Each value already carries the exact
// projection this package renders (booleans as true/false, durations in their
// documented unit, normalised enums).
//
// Callers must treat visit as read-only; the exported function exists only so
// dbconfig can read the projection while config still owns the fields.
func (c *Config) VisitDatabaseDefaults(visit func(environmentVariable, value string)) {
	if c == nil || visit == nil {
		return
	}
	for id := databaseSettingID(0); id < databaseSettingCount; id++ {
		_ = processDatabaseSetting(c, id, visit)
	}
}

// processDatabaseSetting binds exactly one database-scoped setting.
//
// When visit is nil the setting's Config field is assigned once from the
// environment using the original startup parsing, guards, and explicit-flag
// side effects; the error is returned unchanged for applyEnvVars to propagate.
// When visit is non-nil the setting's current projection is emitted to visit
// and Config is never written.
func processDatabaseSetting(c *Config, id databaseSettingID, visit func(environmentVariable, value string)) error {
	environmentVariable := databaseSettings[id].EnvironmentVariable
	switch id {
	case settingEmbeddingEnabled:
		field := &c.Memory.EmbeddingEnabled
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v == "true" || v == "1"
		}
	case settingEmbeddingProvider:
		field := &c.Memory.EmbeddingProvider
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
		}
	case settingEmbeddingModel:
		field := &c.Memory.EmbeddingModel
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
			c.EmbeddingExplicit.Model = true
		}
	case settingEmbeddingAPIURL:
		field := &c.Memory.EmbeddingAPIURL
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
			c.EmbeddingExplicit.APIURL = true
		}
	case settingEmbeddingAPIKey:
		field := &c.Memory.EmbeddingAPIKey
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
		}
	case settingEmbeddingMode:
		field := &c.Memory.EmbeddingMode
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = strings.TrimSpace(v)
		}
	case settingEmbeddingDimensions:
		field := &c.Memory.EmbeddingDimensions
		if visit != nil {
			dimensions := (*field)
			if dimensions <= 0 {
				dimensions = 1024
			}
			visit(environmentVariable, strconv.Itoa(dimensions))
			return nil
		}
		if v := getEnvInt(environmentVariable, 0); v > 0 {
			(*field) = v
			c.EmbeddingExplicit.Dimensions = true
		}
	case settingEmbeddingCacheSize:
		field := &c.Memory.EmbeddingCacheSize
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if v := getEnvInt(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingEmbeddingPropertiesInclude:
		field := &c.EmbeddingWorker.PropertiesInclude
		if visit != nil {
			visit(environmentVariable, strings.Join((*field), ","))
			return nil
		}
		if v := getEnvStringSlice(environmentVariable, nil); len(v) > 0 {
			(*field) = v
		}
	case settingEmbeddingPropertiesExclude:
		field := &c.EmbeddingWorker.PropertiesExclude
		if visit != nil {
			visit(environmentVariable, strings.Join((*field), ","))
			return nil
		}
		if v := getEnvStringSlice(environmentVariable, nil); len(v) > 0 {
			(*field) = v
		}
	case settingEmbeddingIncludeLabels:
		field := &c.EmbeddingWorker.IncludeLabels
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		// The original guard re-reads the same variable to decide whether to parse.
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = getEnvBool(environmentVariable, (*field))
		}
	case settingEmbeddingLabelsInclude:
		field := &c.EmbeddingWorker.EligibleLabels
		if visit != nil {
			visit(environmentVariable, strings.Join((*field), ","))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = getEnvStringSlice(environmentVariable, nil)
		}
	case settingEmbeddingLabelsExclude:
		field := &c.EmbeddingWorker.ExcludedLabels
		if visit != nil {
			visit(environmentVariable, strings.Join((*field), ","))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = getEnvStringSlice(environmentVariable, nil)
		}
	case settingEmbeddingGPULayers:
		field := &c.Memory.EmbeddingGPULayers
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		// -999 is the original "unset" sentinel.
		if v := getEnvInt(environmentVariable, -999); v != -999 {
			(*field) = v
		}
	case settingEmbeddingWarmupInterval:
		field := &c.Memory.EmbeddingWarmupInterval
		if visit != nil {
			visit(environmentVariable, (*field).String())
			return nil
		}
		if v := getEnvDuration(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingSearchMinSimilarity:
		field := &c.Memory.SearchMinSimilarity
		if visit != nil {
			visit(environmentVariable, strconv.FormatFloat((*field), 'f', -1, 64))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = getEnvFloat(environmentVariable, 0.5)
		}
	case settingSearchBM25Enabled:
		field := &c.Memory.SearchBM25Enabled
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v == "true" || v == "1"
		}
	case settingSearchBM25Warming:
		field := &c.Memory.SearchBM25Warming
		if visit != nil {
			visit(environmentVariable, NormalizeIndexWarming((*field)))
			return nil
		}
		if v := strings.TrimSpace(strings.ToLower(getEnv(environmentVariable, ""))); v == "lazy" || v == "startup" {
			(*field) = v
		}
	case settingSearchBM25Stemmer:
		field := &c.Memory.SearchBM25Stemmer
		if visit != nil {
			visit(environmentVariable, NormalizeStemmerSelection((*field)))
			return nil
		}
		if v := strings.TrimSpace(strings.ToLower(getEnv(environmentVariable, ""))); v != "" {
			(*field) = v
		}
	case settingSearchVectorEnabled:
		field := &c.Memory.SearchVectorEnabled
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v == "true" || v == "1"
		}
	case settingSearchVectorWarming:
		field := &c.Memory.SearchVectorWarming
		if visit != nil {
			visit(environmentVariable, NormalizeIndexWarming((*field)))
			return nil
		}
		if v := strings.TrimSpace(strings.ToLower(getEnv(environmentVariable, ""))); v == "lazy" || v == "startup" {
			(*field) = v
		}
	case settingSearchRerankEnabled:
		field := &c.Features.SearchRerankEnabled
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		if getEnv(environmentVariable, "") == "true" {
			(*field) = true
		}
	case settingSearchRerankProvider:
		field := &c.Features.SearchRerankProvider
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = strings.TrimSpace(strings.ToLower(v))
		}
	case settingSearchRerankModel:
		field := &c.Features.SearchRerankModel
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
		}
	case settingSearchRerankAPIURL:
		field := &c.Features.SearchRerankAPIURL
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
		}
	case settingSearchRerankAPIKey:
		field := &c.Features.SearchRerankAPIKey
		if visit != nil {
			visit(environmentVariable, (*field))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			(*field) = v
		}
	case settingKmeansMinEmbeddings:
		field := &c.Memory.KmeansMinEmbeddings
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if v := getEnvInt(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingKmeansClusterInterval:
		field := &c.Memory.KmeansClusterInterval
		if visit != nil {
			visit(environmentVariable, (*field).String())
			return nil
		}
		if v := getEnvDuration(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingKmeansNumClusters:
		field := &c.Memory.KmeansNumClusters
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		// Zero is meaningful (auto cluster count), so the guard is >= 0.
		if v := getEnvInt(environmentVariable, 0); v >= 0 {
			(*field) = v
		}
	case settingAutoLinksEnabled:
		field := &c.Memory.AutoLinksEnabled
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		if getEnv(environmentVariable, "") == "false" {
			(*field) = false
		}
	case settingAutoLinksThreshold:
		field := &c.Memory.AutoLinksSimilarityThreshold
		if visit != nil {
			visit(environmentVariable, strconv.FormatFloat((*field), 'f', -1, 64))
			return nil
		}
		if v := getEnvFloat(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingAutoTLPEnabled:
		field := &c.Features.TopologyAutoIntegrationEnabled
		if visit != nil {
			visit(environmentVariable, strconv.FormatBool((*field)))
			return nil
		}
		if getEnv(environmentVariable, "") == "true" {
			(*field) = true
		}
	case settingEmbedWorkerNumWorkers:
		field := &c.EmbeddingWorker.NumWorkers
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if raw := strings.TrimSpace(getEnv(environmentVariable, "")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
				(*field) = v
			}
		}
	case settingEmbedScanInterval:
		field := &c.EmbeddingWorker.ScanInterval
		if visit != nil {
			visit(environmentVariable, (*field).String())
			return nil
		}
		if v := getEnvDuration(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingEmbedBatchDelay:
		field := &c.EmbeddingWorker.BatchDelay
		if visit != nil {
			visit(environmentVariable, (*field).String())
			return nil
		}
		if v := getEnvDuration(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingEmbedTriggerDebounce:
		field := &c.EmbeddingWorker.TriggerDebounceDelay
		if visit != nil {
			visit(environmentVariable, (*field).String())
			return nil
		}
		// Zero is meaningful (fire immediately), so the guard is >= 0.
		if v := getEnvDuration(environmentVariable, -1); v >= 0 {
			(*field) = v
		}
	case settingEmbedMaxRetries:
		field := &c.EmbeddingWorker.MaxRetries
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if v := getEnvInt(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingEmbedChunkSize:
		field := &c.EmbeddingWorker.ChunkSize
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if raw := strings.TrimSpace(getEnv(environmentVariable, "")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 0 {
				(*field) = v
				c.EmbeddingWorker.ChunkSizeSet = true
			}
		}
	case settingEmbedChunkOverlap:
		field := &c.EmbeddingWorker.ChunkOverlap
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if raw := strings.TrimSpace(getEnv(environmentVariable, "")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
				(*field) = v
				c.EmbeddingWorker.ChunkOverlapSet = true
			}
		}
	case settingMVCCLifecycleInterval:
		field := &c.Database.MVCCLifecycleCycleInterval
		if visit != nil {
			visit(environmentVariable, (*field).String())
			return nil
		}
		if v := getEnvDuration(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingQueryCacheSize:
		field := &c.Memory.QueryCacheSize
		if visit != nil {
			visit(environmentVariable, strconv.Itoa((*field)))
			return nil
		}
		if v := getEnvInt(environmentVariable, 0); v > 0 {
			(*field) = v
		}
	case settingQueryCacheTTL:
		field := &c.Memory.QueryCacheTTL
		if visit != nil {
			// The documented unit is milliseconds, matching the env parser.
			visit(environmentVariable, strconv.FormatInt((*field).Milliseconds(), 10))
			return nil
		}
		if milliseconds := getEnvInt(environmentVariable, 0); milliseconds > 0 {
			(*field) = time.Duration(milliseconds) * time.Millisecond
		}
	case settingMemoryTransactionMax:
		field := &c.Memory.TransactionMax
		if visit != nil {
			visit(environmentVariable, strconv.FormatInt((*field), 10))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			limit, err := ParseByteSize(v)
			if err != nil {
				return fmt.Errorf("invalid %s value %q: %w", environmentVariable, v, err)
			}
			(*field) = limit
		}
	case settingMemoryTransactionTotalMax:
		field := &c.Memory.TransactionTotalMax
		if visit != nil {
			visit(environmentVariable, strconv.FormatInt((*field), 10))
			return nil
		}
		if v := getEnv(environmentVariable, ""); v != "" {
			limit, err := ParseByteSize(v)
			if err != nil {
				return fmt.Errorf("invalid %s value %q: %w", environmentVariable, v, err)
			}
			(*field) = limit
		}
	}
	return nil
}

// NormalizeIndexWarming returns the supported warming policy; empty and
// unrecognized values use startup, matching the database default contract.
func NormalizeIndexWarming(raw string) string {
	if strings.ToLower(strings.TrimSpace(raw)) == "lazy" {
		return "lazy"
	}
	return "startup"
}

// NormalizeStemmerSelection returns a normalized plugin identifier. An empty
// selection uses the language-neutral none plugin.
func NormalizeStemmerSelection(raw string) string {
	selected := strings.ToLower(strings.TrimSpace(raw))
	if selected == "" {
		return "none"
	}
	return selected
}
