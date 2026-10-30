package search

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/localization"
)

const (
	// EnvSearchQueryMaxChunks bounds how many chunks of a long query are
	// embedded and searched (each costs an embedding call, and a rerank call
	// when reranking). Default 32.
	EnvSearchQueryMaxChunks = "NORNICDB_SEARCH_QUERY_MAX_CHUNKS"
	// EnvSearchQueryChunkOverflow chooses what a query with more chunks than
	// that does: "truncate" (the default) searches the first chunks and
	// reports the rest as dropped; "error" fails the search.
	EnvSearchQueryChunkOverflow = "NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW"
	defaultQueryMaxChunks       = 32
)

// QueryMaxChunks is the configured most chunks of one query that are
// embedded and searched.
func QueryMaxChunks() int {
	if value := envutil.GetInt(EnvSearchQueryMaxChunks, defaultQueryMaxChunks); value > 0 {
		return value
	}
	return defaultQueryMaxChunks
}

// LimitQueryChunks keeps the chunks of a query that are searched: all of
// them, or the first QueryMaxChunks. Over the limit it fails instead when
// NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW=error, so a caller never gets a ranking
// for part of its query without knowing it.
func LimitQueryChunks(chunks []string) ([]string, error) {
	limit := QueryMaxChunks()
	if len(chunks) <= limit {
		return chunks, nil
	}
	if strings.EqualFold(strings.TrimSpace(envutil.Get(EnvSearchQueryChunkOverflow, "")), "error") {
		return nil, RequireAllQueryChunks(chunks)
	}
	return chunks[:limit], nil
}

// RequireAllQueryChunks fails a query with more chunks than are searched,
// whatever NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW says: for a caller whose
// results have no place to report dropped chunks.
func RequireAllQueryChunks(chunks []string) error {
	if limit := QueryMaxChunks(); len(chunks) > limit {
		return localizedError(localization.SearchQueryChunksExceeded(len(chunks), limit), nil)
	}
	return nil
}
