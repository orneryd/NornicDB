package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// With NORNICDB_SEARCH_RELATIONSHIPS_ENABLED, a relationship created by
// Cypher or written straight to storage is found by a word in its
// properties, its vectors come from the database's embedder, a deleted one
// leaves the index, and results map to the relationship with its endpoints
// (#1065).
func TestRelationshipSearchEndToEnd(t *testing.T) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = true
	config.Memory.SearchRelationshipsEnabled = true
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetEmbedder(&staticTestEmbedder{dims: config.Memory.EmbeddingDimensions})

	_, err = db.ExecuteCypher(ctx, "CREATE (:Doc {name: 'a'})-[:CITES {reason: 'zephyrine bridge'}]->(:Doc {name: 'b'})", nil)
	require.NoError(t, err)
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
	require.NoError(t, err)
	require.True(t, svc.RelationshipSearchEnabled())
	find := func(word string) []search.SearchResult {
		response, err := svc.Search(ctx, word, nil, &search.SearchOptions{Limit: 10, Relationships: true})
		require.NoError(t, err)
		return response.Results
	}
	require.Eventually(t, func() bool { return len(find("zephyrine")) == 1 }, 5*time.Second, 20*time.Millisecond)
	// A relationship with no property to embed has no vectors.
	embedding, err := db.relationshipEmbedFunc(db.defaultDatabaseName())(ctx, &storage.Edge{ID: "empty", Type: "CITES"})
	require.NoError(t, err)
	require.Nil(t, embedding.Vectors)
	hit := find("zephyrine")[0]
	require.Equal(t, "CITES", hit.Relationship.Type)
	start, err := db.GetStorage().GetNode(hit.Relationship.StartNode)
	require.NoError(t, err)
	require.Equal(t, "a", start.Properties["name"])
	require.Eventually(t, func() bool { return svc.RelationshipEmbeddingCount() == 1 }, 5*time.Second, 20*time.Millisecond)

	mapped := MapSearchResponse(&search.SearchResponse{Results: []search.SearchResult{hit}})
	require.Nil(t, mapped[0].Node)
	require.Equal(t, string(hit.Relationship.ID), mapped[0].Relationship.ID)
	require.Equal(t, "CITES", mapped[0].Relationship.Type)
	require.Equal(t, string(hit.Relationship.StartNode), mapped[0].Relationship.StartNode)
	require.Equal(t, string(hit.Relationship.EndNode), mapped[0].Relationship.EndNode)
	require.Equal(t, "zephyrine bridge", mapped[0].Relationship.Properties["reason"])

	// Straight to storage: the storage event indexes it, and removes it.
	require.NoError(t, db.GetStorage().CreateEdge(&storage.Edge{ID: "direct", StartNode: hit.Relationship.EndNode, EndNode: hit.Relationship.StartNode, Type: "CITES", Properties: map[string]any{"reason": "obsidian"}}))
	require.Eventually(t, func() bool { return len(find("obsidian")) == 1 }, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, db.GetStorage().DeleteEdge("direct"))
	require.Eventually(t, func() bool { return svc.RelationshipEmbeddingCount() == 1 }, 5*time.Second, 20*time.Millisecond)
	require.Empty(t, find("obsidian"))
}

func TestRelationshipEmbeddingText(t *testing.T) {
	edge := &storage.Edge{Type: "CITES", Properties: map[string]any{"reason": "a bridge", "weight": 2}}
	withType := relationshipEmbeddingText(edge, &EmbedWorkerConfig{IncludeLabels: true})
	require.Contains(t, withType, "type: CITES\n")
	require.Contains(t, withType, "a bridge")
	withoutType := relationshipEmbeddingText(edge, &EmbedWorkerConfig{PropertiesInclude: []string{"reason"}})
	require.NotContains(t, withoutType, "CITES")
	require.NotContains(t, withoutType, "weight")
	require.Empty(t, relationshipEmbeddingText(&storage.Edge{Type: "CITES"}, &EmbedWorkerConfig{IncludeLabels: true}), "no properties, no text")
}

// Without an embed queue (embeddings off) the embed function defers; storage
// events for a database without relationship search, or with unprefixed IDs,
// are ignored.
func TestRelationshipSearchEmbedAndEventEdges(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.relationshipEmbedFunc("nornic")(context.Background(), &storage.Edge{ID: "r", Properties: map[string]any{"reason": "x"}})
	require.ErrorIs(t, err, errRelationshipEmbedderUnavailable)
	// A queue whose embedder isn't loaded yet defers the embedding too.
	db.mu.Lock()
	db.embedQueue, db.embedWorkerConfig = &EmbedQueue{}, &EmbedWorkerConfig{}
	db.mu.Unlock()
	_, err = db.relationshipEmbedFunc("nornic")(context.Background(), &storage.Edge{ID: "r", Properties: map[string]any{"reason": "x"}})
	require.ErrorIs(t, err, errRelationshipEmbedderUnavailable)
	db.mu.Lock()
	db.embedQueue, db.embedWorkerConfig = nil, nil
	db.mu.Unlock()
	db.indexRelationshipFromEvent(nil, storage.CaptureVectorWorkOrigin())
	db.indexRelationshipFromEvent(&storage.Edge{ID: "unprefixed"}, storage.CaptureVectorWorkOrigin())
	db.indexRelationshipFromEvent(&storage.Edge{ID: "nornic:r", StartNode: "nornic:a", EndNode: "nornic:b"}, storage.CaptureVectorWorkOrigin())
	db.queueSearchRefreshEvent(searchRelationshipChange, "unprefixed", storage.CaptureVectorWorkOrigin())
	db.queueSearchRefreshEvent(searchRelationshipChange, "nornic:r", storage.CaptureVectorWorkOrigin())
	require.Nil(t, db.relationshipSearchService("nornic"))
}
