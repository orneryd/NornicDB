package nornicdb

import (
	"context"
	"errors"
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
	return func(ctx context.Context, edge *storage.Edge) ([][]float32, error) {
		db.mu.RLock()
		queue := db.embedQueue
		config := db.embedWorkerConfig
		db.mu.RUnlock()
		if queue == nil || config == nil {
			return nil, errRelationshipEmbedderUnavailable
		}
		embedder, err := db.getOrCreateEmbedderForDB(dbName)
		if err != nil {
			return nil, err
		}
		if embedder == nil {
			return nil, errRelationshipEmbedderUnavailable
		}
		text := relationshipEmbeddingText(edge, config)
		if text == "" {
			return nil, nil
		}
		_, embeddings, _, err := queue.embedDocumentWith(embedder, text, storage.NodeID(edge.ID))
		return embeddings, err
	}
}

// relationshipEmbeddingText is a relationship's embedding text: its type,
// when labels are included, then its properties as a node's are taken. No
// properties to embed gives no text.
func relationshipEmbeddingText(edge *storage.Edge, config *EmbedWorkerConfig) string {
	options := embeddingutil.EmbedTextOptionsFromFields(config.PropertiesInclude, config.PropertiesExclude, false)
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

// indexRelationshipFromEvent indexes a relationship a storage event reports,
// in its database's relationship search, if that service exists and has it
// on.
func (db *DB) indexRelationshipFromEvent(edge *storage.Edge) {
	if edge == nil {
		return
	}
	dbName, local, ok := splitQualifiedID(string(edge.ID))
	if !ok {
		return
	}
	svc := db.relationshipSearchService(dbName)
	if svc == nil {
		return
	}
	userEdge := storage.CopyEdge(edge)
	userEdge.ID = storage.EdgeID(local)
	if _, start, ok := splitQualifiedID(string(edge.StartNode)); ok {
		userEdge.StartNode = storage.NodeID(start)
	}
	if _, end, ok := splitQualifiedID(string(edge.EndNode)); ok {
		userEdge.EndNode = storage.NodeID(end)
	}
	svc.IndexRelationship(userEdge)
}

// removeRelationshipFromEvent drops a deleted relationship from its
// database's relationship search.
func (db *DB) removeRelationshipFromEvent(edgeID storage.EdgeID) {
	dbName, local, ok := splitQualifiedID(string(edgeID))
	if !ok {
		return
	}
	if svc := db.relationshipSearchService(dbName); svc != nil {
		svc.RemoveRelationship(storage.EdgeID(local))
	}
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
