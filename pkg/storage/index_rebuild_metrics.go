// Plan 04-04-05: D-13c index-name → enum mapping.
//
// classifyIndexName is the pure function that maps an internal index
// identifier (e.g. "label_Person", "edge_between_KNOWS",
// "temporal_user_activity", "embedding_chunk", or any user-named string)
// to one of the closed-enum buckets accepted by
// observability.AllowedStorageIndexes:
//
//	{"label", "edge_between", "temporal", "embedding", "user_created"}
//
// Anything not matching a known prefix buckets to "user_created" — the
// keystone of T-04-02 mitigation. Drives 1k arbitrary user index names →
// cardinality stays at 5.
//
// Pure function, no allocations, safe in hot paths.
package storage

import "strings"

// classifyIndexName maps an internal index identifier to the closed enum
// for the `index` Prometheus label per CONTEXT D-13c.
//
// Adding a new internal-index family = add a new case here AND update
// observability.AllowedStorageIndexes AND amend ADR §2.3.
func classifyIndexName(internal string) string {
	switch {
	case strings.HasPrefix(internal, "label_"):
		return "label"
	case strings.HasPrefix(internal, "edge_between_"):
		return "edge_between"
	case strings.HasPrefix(internal, "temporal_"):
		return "temporal"
	case strings.HasPrefix(internal, "embedding_"):
		return "embedding"
	default:
		return "user_created"
	}
}

// ClassifyIndexName is the exported alias used by the observation sites
// in pkg/cypher / cmd/nornicdb / future plan 04-05 search-engine plumbing.
// The internal-only `classifyIndexName` is the single source of truth;
// this alias keeps the public API surface uppercase per Go convention
// without duplicating the closed-enum logic.
func ClassifyIndexName(internal string) string { return classifyIndexName(internal) }
