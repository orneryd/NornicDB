// Package dbconfig: allowed per-DB config keys for validation and UI.

package dbconfig

import "strings"

// KeyMeta describes one allowed per-database config key.
type KeyMeta struct {
	Key      string `json:"key"`
	Type     string `json:"type"`     // "string", "number", "boolean", "duration"
	Category string `json:"category"` // "Embeddings", "Search", "HNSW", etc.
	// Description is optional. Surfaced as field-level help in the per-DB
	// config UI when present. Looked up via KeyDescription(key).
	Description string `json:"description,omitempty"`
}

// keyDescriptions provides optional per-key help text. Lives in a separate
// map so callers can populate descriptions for new keys without rewriting
// every existing AllowedKeys() literal.
var keyDescriptions = map[string]string{
	"NORNICDB_SEARCH_BM25_ENABLED":   "Master switch for BM25 fulltext search on this database. When false, no BM25 build runs and search returns no fulltext results. Default: true.",
	"NORNICDB_SEARCH_BM25_WARMING":   "When BM25 is enabled, choose `startup` (build at boot, today's default) or `lazy` (defer the build until the first inbound search query). Lazy reduces boot RSS for idle databases at the cost of first-query latency.",
	"NORNICDB_SEARCH_VECTOR_ENABLED": "Master switch for vector search across every ANN strategy (HNSW, IVF-HNSW, brute-force, GPU, Metal, Qdrant pass-through). When false, node embeddings are NOT iterated into the in-memory ANN substrate. The strongest available memory-pressure lever. Default: true.",
	"NORNICDB_SEARCH_VECTOR_WARMING": "When vector search is enabled, choose `startup` or `lazy`. See NORNICDB_SEARCH_BM25_WARMING.",
}

// KeyDescription returns the operator-facing help text for a key, or the
// empty string when none is registered. Stable for callers (UI/help) to
// consume; returning "" lets callers skip rendering the description line.
func KeyDescription(key string) string {
	return keyDescriptions[key]
}

// keyTuple is the internal positional 3-field shape of an allow-list entry.
// Kept as a separate type so existing 3-field literals in allowedKeysRaw
// don't have to learn about the new Description field on KeyMeta — that
// field is merged in by AllowedKeys() from keyDescriptions.
type keyTuple struct {
	key      string
	typ      string
	category string
}

// AllowedKeys returns the list of allowed per-DB config keys and their metadata,
// with Description populated from the keyDescriptions map for keys that have one.
// Used by API validation and by GET /admin/databases/config/keys.
func AllowedKeys() []KeyMeta {
	raw := allowedKeysRaw()
	keys := make([]KeyMeta, len(raw))
	for i, t := range raw {
		keys[i] = KeyMeta{Key: t.key, Type: t.typ, Category: t.category}
		if d, ok := keyDescriptions[t.key]; ok {
			keys[i].Description = d
		}
	}
	return keys
}

// allowedKeysRaw returns the 3-field raw key list. Kept as positional
// literals so adding Description to KeyMeta didn't force every entry to
// be rewritten.
func allowedKeysRaw() []keyTuple {
	return []keyTuple{
		// Embeddings
		{"NORNICDB_EMBEDDING_ENABLED", "boolean", "Embeddings"},
		{"NORNICDB_EMBEDDING_PROVIDER", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_MODEL", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_API_URL", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_API_KEY", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_DIMENSIONS", "number", "Embeddings"},
		{"NORNICDB_EMBEDDING_CACHE_SIZE", "number", "Embeddings"},
		{"NORNICDB_EMBEDDING_PROPERTIES_INCLUDE", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_INCLUDE_LABELS", "boolean", "Embeddings"},
		{"NORNICDB_EMBEDDING_GPU_LAYERS", "number", "Embeddings"},
		{"NORNICDB_EMBEDDING_WARMUP_INTERVAL", "duration", "Embeddings"},
		// Search
		{"NORNICDB_SEARCH_MIN_SIMILARITY", "number", "Search"},
		{"NORNICDB_SEARCH_BM25_ENGINE", "string", "Search"},
		// Per-DB index master switches and warming triggers.
		// Defaults: enabled=true, warming=startup (today's behaviour).
		// Disabling skips build entirely; warming=lazy defers the build to first
		// inbound search query for the database.
		{"NORNICDB_SEARCH_BM25_ENABLED", "boolean", "Search"},
		{"NORNICDB_SEARCH_BM25_WARMING", "enum:startup,lazy", "Search"},
		{"NORNICDB_SEARCH_VECTOR_ENABLED", "boolean", "Search"},
		{"NORNICDB_SEARCH_VECTOR_WARMING", "enum:startup,lazy", "Search"},
		{"NORNICDB_SEARCH_RERANK_ENABLED", "boolean", "Search"},
		{"NORNICDB_SEARCH_RERANK_PROVIDER", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_MODEL", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_API_URL", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_API_KEY", "string", "Search"},
		{"NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", "number", "Search"},
		// HNSW
		{"NORNICDB_VECTOR_ANN_QUALITY", "string", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_M", "number", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", "number", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_EF_SEARCH", "number", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES", "number", "HNSW"},
		// IVF-HNSW
		{"NORNICDB_VECTOR_IVF_HNSW_ENABLED", "boolean", "IVF-HNSW"},
		{"NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "number", "IVF-HNSW"},
		{"NORNICDB_VECTOR_IVF_HNSW_MAX_CLUSTERS", "number", "IVF-HNSW"},
		// Vector GPU
		{"NORNICDB_VECTOR_GPU_BRUTE_MIN_N", "number", "Vector"},
		{"NORNICDB_VECTOR_GPU_BRUTE_MAX_N", "number", "Vector"},
		// K-means
		{"NORNICDB_KMEANS_CLUSTERING_ENABLED", "boolean", "K-means"},
		{"NORNICDB_KMEANS_MIN_EMBEDDINGS", "number", "K-means"},
		{"NORNICDB_KMEANS_CLUSTER_INTERVAL", "duration", "K-means"},
		{"NORNICDB_KMEANS_NUM_CLUSTERS", "number", "K-means"},
		{"NORNICDB_KMEANS_MAX_ITERATIONS", "number", "K-means"},
		// Auto-links
		{"NORNICDB_AUTO_LINKS_ENABLED", "boolean", "Auto-links"},
		{"NORNICDB_AUTO_LINKS_THRESHOLD", "number", "Auto-links"},
		// Auto-TLP
		{"NORNICDB_AUTO_TLP_ENABLED", "boolean", "Auto-TLP"},
		{"NORNICDB_AUTO_TLP_LLM_QC_ENABLED", "boolean", "Auto-TLP"},
		{"NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED", "boolean", "Auto-TLP"},
		// Embed worker
		{"NORNICDB_EMBED_WORKER_NUM_WORKERS", "number", "Embed worker"},
		{"NORNICDB_EMBED_SCAN_INTERVAL", "duration", "Embed worker"},
		{"NORNICDB_EMBED_BATCH_DELAY", "duration", "Embed worker"},
		{"NORNICDB_EMBED_TRIGGER_DEBOUNCE", "duration", "Embed worker"},
		{"NORNICDB_EMBED_MAX_RETRIES", "number", "Embed worker"},
		{"NORNICDB_EMBED_CHUNK_SIZE", "number", "Embed worker"},
		{"NORNICDB_EMBED_CHUNK_OVERLAP", "number", "Embed worker"},
		{"NORNICDB_MVCC_LIFECYCLE_INTERVAL", "duration", "MVCC lifecycle"},
	}
}

// AllowedKeysSet returns a set of allowed key names for validation.
func AllowedKeysSet() map[string]KeyMeta {
	set := make(map[string]KeyMeta)
	for _, m := range AllowedKeys() {
		set[m.Key] = m
	}
	return set
}

// KeysExcludedFromPerDB are not allowed as per-DB overrides (reserved for future use).
var KeysExcludedFromPerDB = map[string]bool{}

// IsAllowedKey returns true if the key can be set as a per-DB override.
func IsAllowedKey(key string) bool {
	if KeysExcludedFromPerDB[key] {
		return false
	}
	_, ok := AllowedKeysSet()[key]
	return ok
}

// EnumValues returns the permitted values for a key whose Type is "enum:v1,v2,...",
// or nil for non-enum keys (or unknown keys).
func EnumValues(key string) []string {
	meta, ok := AllowedKeysSet()[key]
	if !ok {
		return nil
	}
	if !strings.HasPrefix(meta.Type, "enum:") {
		return nil
	}
	raw := strings.TrimPrefix(meta.Type, "enum:")
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// IsValidEnumValue returns (true, "") if value matches one of the enum's
// permitted values (case-insensitive). For non-enum keys it returns (true, "")
// — only enums get value-level validation here. Returns (false, "<csv>") for
// unknown enum values so callers can echo the permitted list back to operators.
func IsValidEnumValue(key, value string) (bool, string) {
	values := EnumValues(key)
	if values == nil {
		return true, ""
	}
	v := strings.ToLower(strings.TrimSpace(value))
	for _, allowed := range values {
		if strings.ToLower(allowed) == v {
			return true, ""
		}
	}
	return false, strings.Join(values, ",")
}
