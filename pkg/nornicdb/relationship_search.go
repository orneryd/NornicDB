package nornicdb

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// errRelationshipEmbedderUnavailable defers a relationship's embedding until
// an embedder is loaded.
var errRelationshipEmbedderUnavailable = errors.New("no embedder loaded for relationship search")

// relationshipEmbedFunc embeds relationships for relationship search
// (#1065). The text is built as a node's
// (NORNICDB_EMBEDDING_PROPERTIES_INCLUDE / EXCLUDE), with the relationship
// type in place of labels (NORNICDB_EMBEDDING_INCLUDE_LABELS), and split into
// chunks and embedded as a node's is, with the database's embedder.
func (db *DB) relationshipEmbedFunc(dbName string) search.RelationshipEmbedFunc {
	return func(ctx context.Context, edge *storage.Edge) (search.RelationshipEmbedding, error) {
		// Relationship embedding is automatic embedding too: an
		// administrator's pause holds it here, asleep until the resume; a
		// shutdown's error returns the relationship to its pending set.
		if err := db.embeddingPause.wait(ctx); err != nil {
			return search.RelationshipEmbedding{}, err
		}
		db.mu.RLock()
		queue := db.embedQueue
		config := db.embedWorkerConfig
		db.mu.RUnlock()
		if queue == nil || config == nil {
			return search.RelationshipEmbedding{}, errRelationshipEmbedderUnavailable
		}
		embedder, err := db.getOrCreateEmbedderForDB(dbName)
		if err != nil {
			return search.RelationshipEmbedding{}, err
		}
		if embedder == nil {
			return search.RelationshipEmbedding{}, errRelationshipEmbedderUnavailable
		}
		text := relationshipEmbeddingText(edge, config)
		if text == "" {
			return search.RelationshipEmbedding{}, nil
		}
		chunks, embeddings, _, err := queue.embedDocumentWith(embedder, text, storage.NodeID(edge.ID))
		if err != nil {
			return search.RelationshipEmbedding{}, err
		}
		return search.RelationshipEmbedding{Vectors: embeddings, Spans: relationshipChunkSpans(edge, config, chunks, len(embeddings))}, nil
	}
}

// trimTypeLine removes the type line from a chunk's start: the whole line,
// or, for a chunk an overlap or a small chunk size started inside it, the
// line's tail (from any point to its newline).
func trimTypeLine(chunk, prefix string) string {
	for start := 0; start < len(prefix); start++ {
		if strings.HasPrefix(chunk, prefix[start:]) {
			return chunk[len(prefix)-start:]
		}
	}
	return chunk
}

// relationshipChunkSpans is where each chunk of a relationship's embedding
// text lies in its string properties, as a node's chunks are placed
// (embeddingutil.RecordChunkSpans): none unless the chunks match the vectors
// one to one. A leading "type: " line, when labels are included, isn't a
// property, so it is left out of the text the chunks are found in.
func relationshipChunkSpans(edge *storage.Edge, config *EmbedWorkerConfig, chunks []string, vectors int) []embeddingutil.ChunkSpan {
	if len(chunks) == 0 || len(chunks) != vectors {
		return nil
	}
	source := embeddingutil.BuildEmbedText(edge.Properties, nil, relationshipEmbedTextOptions(config))
	if config.IncludeLabels && edge.Type != "" {
		prefix := "type: " + edge.Type + "\n"
		aligned := make([]string, len(chunks))
		for index, chunk := range chunks {
			aligned[index] = trimTypeLine(chunk, prefix)
		}
		chunks = aligned
	}
	return source.ChunkSpans(chunks)
}

// relationshipEmbedPolicy is what selects a relationship's embedding text:
// the node and relationship property lists and whether labels (its type)
// are included. Saved relationship vectors are embedded again when it
// changes (search.SetRelationshipEmbedPolicy).
func relationshipEmbedPolicy(config *Config) string {
	worker := config.EmbeddingWorker
	return strings.Join([]string{
		"include=" + strings.Join(worker.PropertiesInclude, ","),
		"exclude=" + strings.Join(worker.PropertiesExclude, ","),
		"relationship-include=" + strings.Join(worker.RelationshipPropertiesInclude, ","),
		"relationship-exclude=" + strings.Join(worker.RelationshipPropertiesExclude, ","),
		"labels=" + strconv.FormatBool(worker.IncludeLabels),
	}, ";")
}

// relationshipEmbedTextOptions are the property lists for a relationship's
// embedding text: the relationship lists where set
// (NORNICDB_EMBEDDING_RELATIONSHIP_PROPERTIES_INCLUDE / _EXCLUDE), each
// otherwise the node list.
func relationshipEmbedTextOptions(config *EmbedWorkerConfig) *embeddingutil.EmbedTextOptions {
	include, exclude := config.PropertiesInclude, config.PropertiesExclude
	if len(config.RelationshipPropertiesInclude) > 0 {
		include = config.RelationshipPropertiesInclude
	}
	if len(config.RelationshipPropertiesExclude) > 0 {
		exclude = config.RelationshipPropertiesExclude
	}
	return embeddingutil.EmbedTextOptionsFromFields(include, exclude, false)
}

// relationshipEmbeddingText is a relationship's embedding text: its type,
// when labels are included, then its properties as a node's are taken. No
// properties to embed gives no text.
func relationshipEmbeddingText(edge *storage.Edge, config *EmbedWorkerConfig) string {
	options := relationshipEmbedTextOptions(config)
	policy := embeddingutil.TextPolicy(options)
	fed := false
	for key := range edge.Properties {
		if policy.FeedsText(key) {
			fed = true
			break
		}
	}
	if !fed {
		return ""
	}
	text := strings.TrimSpace(embeddingutil.BuildText(edge.Properties, nil, options))
	if config.IncludeLabels && edge.Type != "" {
		return "type: " + edge.Type + "\n" + text
	}
	return text
}

// wakeRelationshipEmbedding wakes every database's relationship embedding,
// once an embedder is loaded.
func (db *DB) wakeRelationshipEmbedding() {
	db.searchServicesMu.RLock()
	defer db.searchServicesMu.RUnlock()
	for _, entry := range db.searchServices {
		if entry != nil && entry.svc != nil {
			entry.svc.WakeRelationshipEmbedding()
		}
	}
}

// indexRelationshipFromEvent queues a refresh of a relationship a storage
// event reports as written, in its database's search service if that exists.
// The refresh reads the stored relationship and updates both its property
// vectors and, when that is on, relationship search.
func (db *DB) indexRelationshipFromEvent(edge *storage.Edge) {
	if edge == nil {
		return
	}
	db.queueRelationshipRefresh(edge.ID)
}

// removeRelationshipFromEvent queues a refresh of a deleted relationship in
// its database's search service, which drops its relationship search entry
// and any vectors indexed from its properties.
func (db *DB) removeRelationshipFromEvent(edgeID storage.EdgeID) {
	db.queueRelationshipRefresh(edgeID)
}

// queueRelationshipRefresh queues the refresh unless the database's service
// indexes nothing for relationships (vector and relationship search both off).
// Storage events don't create a service.
func (db *DB) queueRelationshipRefresh(edgeID storage.EdgeID) {
	dbName, local, ok := splitQualifiedID(string(edgeID))
	if !ok {
		return
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil || entry.svc == nil || (!entry.svc.VectorEnabled() && !entry.svc.RelationshipSearchEnabled()) {
		return
	}
	entry.queueEdgeRefresh(local)
	db.scheduleQueuedRefreshes(entry)
}

// relationshipSearchService is dbName's search service if it exists and
// indexes relationships; storage events don't create one.
func (db *DB) relationshipSearchService(dbName string) *search.Service {
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil || !entry.svc.RelationshipSearchEnabled() {
		return nil
	}
	return entry.svc
}
