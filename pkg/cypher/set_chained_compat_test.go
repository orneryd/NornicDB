package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetChainedClausesWithMapMerge ensures Neo4j-compatible chained SET works when
// one SET clause uses += map merge and another uses a scalar assignment.
func TestSetChainedClausesWithMapMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "file-1",
		Labels: []string{"File", "Node"},
		Properties: map[string]interface{}{
			"path": "/tmp/sample.txt",
		},
	})
	require.NoError(t, err)

	content := "line 1\nMATCH (n) SET n.x = 1\nSET += should stay data\nRETURN n"
	params := map[string]interface{}{
		"path": "/tmp/sample.txt",
		"props": map[string]interface{}{
			"name":    "sample.txt",
			"type":    "text",
			"content": content,
		},
		"lastModified": "2026-02-18T23:24:00Z",
	}

	result, err := exec.Execute(ctx, `
		MATCH (f:File {path: $path})
		SET f += $props
		SET f.last_modified = $lastModified
		RETURN f.content AS content, f.last_modified AS last_modified
	`, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 2)

	assert.Equal(t, content, result.Rows[0][0])
	assert.Equal(t, "2026-02-18T23:24:00Z", result.Rows[0][1])
}

func TestCreateSetChainedClausesWithMapMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	content := "this content includes SET f.x = 1 and should remain text"
	params := map[string]interface{}{
		"id":   "file-created-1",
		"path": "/tmp/created.txt",
		"props": map[string]interface{}{
			"name":    "created.txt",
			"type":    "text",
			"content": content,
		},
		"indexed": "2026-02-18T23:37:00Z",
	}

	result, err := exec.Execute(ctx, `
		CREATE (f:File:Node {id: $id, path: $path})
		SET f += $props
		SET f.indexed_date = $indexed
		RETURN f.content AS content, f.indexed_date AS indexed_date
	`, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 2)
	assert.Equal(t, content, result.Rows[0][0])
	assert.Equal(t, "2026-02-18T23:37:00Z", result.Rows[0][1])
}
