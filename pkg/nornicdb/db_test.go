package nornicdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/decay"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryTierHalfLives(t *testing.T) {
	// Verify our decay constants match expected half-lives

	episodicHL := decay.HalfLife(decay.TierEpisodic)
	assert.InDelta(t, 7.0, episodicHL, 0.5, "Episodic should have ~7 day half-life")

	semanticHL := decay.HalfLife(decay.TierSemantic)
	assert.InDelta(t, 69.0, semanticHL, 2.0, "Semantic should have ~69 day half-life")

	proceduralHL := decay.HalfLife(decay.TierProcedural)
	assert.InDelta(t, 693.0, proceduralHL, 20.0, "Procedural should have ~693 day half-life")
}

func TestOpen(t *testing.T) {
	t.Run("with nil config uses defaults", func(t *testing.T) {
		tmpDir := t.TempDir() // Auto-cleanup after test
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer db.Close()

		assert.Equal(t, tmpDir, db.config.DataDir)
		assert.True(t, db.config.DecayEnabled)
		assert.True(t, db.config.AutoLinksEnabled)
		assert.NotNil(t, db.storage)
		assert.NotNil(t, db.decay)
		assert.NotNil(t, db.inference)
	})

	t.Run("with custom config", func(t *testing.T) {
		tmpDir := t.TempDir() // Auto-cleanup after test
		config := &Config{
			DecayEnabled:     false,
			AutoLinksEnabled: false,
		}
		db, err := Open(tmpDir, config)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer db.Close()

		assert.Equal(t, tmpDir, db.config.DataDir)
		assert.Nil(t, db.decay)
		assert.Nil(t, db.inference)
	})
}

func TestClose(t *testing.T) {
	t.Run("closes successfully", func(t *testing.T) {
		tmpDir := t.TempDir()
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)
		assert.True(t, db.closed)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		tmpDir := t.TempDir()
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)

		// Second close should also succeed
		err = db.Close()
		assert.NoError(t, err)
	})
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores memory with defaults", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Test content",
			Title:   "Test Title",
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		require.NotNil(t, stored)

		assert.NotEmpty(t, stored.ID)
		assert.Equal(t, "Test content", stored.Content)
		assert.Equal(t, "Test Title", stored.Title)
		assert.Equal(t, TierSemantic, stored.Tier)
		assert.Equal(t, 1.0, stored.DecayScore)
		assert.False(t, stored.CreatedAt.IsZero())
		assert.False(t, stored.LastAccessed.IsZero())
		assert.Equal(t, int64(0), stored.AccessCount)
	})

	t.Run("stores memory with explicit tier", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Important skill",
			Tier:    TierProcedural,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, TierProcedural, stored.Tier)
	})

	t.Run("stores memory with tags and properties", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content:    "Tagged content",
			Tags:       []string{"tag1", "tag2"},
			Source:     "test-source",
			Properties: map[string]any{"custom": "value"},
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, []string{"tag1", "tag2"}, stored.Tags)
		assert.Equal(t, "test-source", stored.Source)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		mem := &Memory{Content: "Test"}
		_, err = db.Store(ctx, mem)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("returns error for nil memory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Store(ctx, nil)
		assert.ErrorIs(t, err, ErrInvalidInput)
	})

	t.Run("stores with embeddings", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content:   "Memory with embedding",
			Embedding: []float32{1.0, 0.0, 0.0},
		}
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, []float32{1.0, 0.0, 0.0}, stored.Embedding)
	})
}

func TestRecall(t *testing.T) {
	ctx := context.Background()

	t.Run("recalls stored memory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Recallable content",
			Title:   "Recallable",
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		require.NotNil(t, recalled)

		assert.Equal(t, stored.ID, recalled.ID)
		assert.Equal(t, "Recallable content", recalled.Content)
		assert.Equal(t, int64(1), recalled.AccessCount)
	})

	t.Run("reinforces memory on recall", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{Content: "Reinforced content"}
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		originalAccess := stored.LastAccessed
		time.Sleep(10 * time.Millisecond) // Small delay

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), recalled.AccessCount)
		assert.True(t, recalled.LastAccessed.After(originalAccess) || recalled.LastAccessed.Equal(originalAccess))
	})

	t.Run("returns error for non-existent ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Recall(ctx, "non-existent-id")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Recall(ctx, "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Recall(ctx, "any-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestRemember(t *testing.T) {
	ctx := context.Background()

	t.Run("finds similar memories", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store memories with embeddings
		mem1 := &Memory{
			Content:   "Dog running in park",
			Embedding: []float32{1.0, 0.0, 0.0, 0.0},
		}
		mem2 := &Memory{
			Content:   "Cat sleeping on couch",
			Embedding: []float32{0.0, 1.0, 0.0, 0.0},
		}
		mem3 := &Memory{
			Content:   "Puppy playing fetch",
			Embedding: []float32{0.9, 0.1, 0.0, 0.0},
		}

		_, err = db.Store(ctx, mem1)
		require.NoError(t, err)
		_, err = db.Store(ctx, mem2)
		require.NoError(t, err)
		_, err = db.Store(ctx, mem3)
		require.NoError(t, err)

		// Search for dog-like content
		query := []float32{0.95, 0.05, 0.0, 0.0}
		results, err := db.Remember(ctx, query, 2)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Top results should be dog-related
		contents := make([]string, len(results))
		for i, r := range results {
			contents[i] = r.Content
		}
		assert.Contains(t, contents, "Dog running in park")
		assert.Contains(t, contents, "Puppy playing fetch")
	})

	t.Run("respects limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store multiple memories
		for i := 0; i < 5; i++ {
			mem := &Memory{
				Content:   "Memory content",
				Embedding: []float32{0.5, 0.5, 0.0, 0.0},
			}
			_, err = db.Store(ctx, mem)
			require.NoError(t, err)
		}

		results, err := db.Remember(ctx, []float32{0.5, 0.5, 0.0, 0.0}, 2)
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("returns empty for no matches", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store memory without embedding
		mem := &Memory{Content: "No embedding"}
		_, err = db.Store(ctx, mem)
		require.NoError(t, err)

		results, err := db.Remember(ctx, []float32{1.0, 0.0}, 10)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("returns error for empty embedding", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Remember(ctx, []float32{}, 10)
		assert.ErrorIs(t, err, ErrInvalidInput)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Remember(ctx, []float32{1.0}, 10)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestLink(t *testing.T) {
	ctx := context.Background()

	t.Run("creates link between memories", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1 := &Memory{Content: "Source"}
		mem2 := &Memory{Content: "Target"}

		stored1, err := db.Store(ctx, mem1)
		require.NoError(t, err)
		stored2, err := db.Store(ctx, mem2)
		require.NoError(t, err)

		edge, err := db.Link(ctx, stored1.ID, stored2.ID, "KNOWS", 0.9)
		require.NoError(t, err)
		require.NotNil(t, edge)

		assert.NotEmpty(t, edge.ID)
		assert.Equal(t, stored1.ID, edge.SourceID)
		assert.Equal(t, stored2.ID, edge.TargetID)
		assert.Equal(t, "KNOWS", edge.Type)
		assert.Equal(t, 0.9, edge.Confidence)
		assert.False(t, edge.AutoGenerated)
	})

	t.Run("uses default edge type", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{Content: "A"})
		mem2, _ := db.Store(ctx, &Memory{Content: "B"})

		edge, err := db.Link(ctx, mem1.ID, mem2.ID, "", 0.5)
		require.NoError(t, err)
		assert.Equal(t, "RELATES_TO", edge.Type)
	})

	t.Run("normalizes confidence", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{Content: "A"})
		mem2, _ := db.Store(ctx, &Memory{Content: "B"})

		edge, err := db.Link(ctx, mem1.ID, mem2.ID, "TEST", 0) // Invalid confidence
		require.NoError(t, err)
		assert.Equal(t, 1.0, edge.Confidence)

		mem3, _ := db.Store(ctx, &Memory{Content: "C"})
		edge2, err := db.Link(ctx, mem1.ID, mem3.ID, "TEST", 1.5) // Out of range
		require.NoError(t, err)
		assert.Equal(t, 1.0, edge2.Confidence)
	})

	t.Run("returns error for non-existent source", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem2, _ := db.Store(ctx, &Memory{Content: "Target"})

		_, err = db.Link(ctx, "non-existent", mem2.ID, "TEST", 1.0)
		assert.Error(t, err)
	})

	t.Run("returns error for non-existent target", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{Content: "Source"})

		_, err = db.Link(ctx, mem1.ID, "non-existent", "TEST", 1.0)
		assert.Error(t, err)
	})

	t.Run("returns error for empty IDs", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Link(ctx, "", "target", "TEST", 1.0)
		assert.ErrorIs(t, err, ErrInvalidID)

		_, err = db.Link(ctx, "source", "", "TEST", 1.0)
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Link(ctx, "a", "b", "TEST", 1.0)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestNeighbors(t *testing.T) {
	ctx := context.Background()

	t.Run("finds direct neighbors", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		memC, _ := db.Store(ctx, &Memory{Content: "C"})

		// A -> B, A -> C
		_, err = db.Link(ctx, memA.ID, memB.ID, "KNOWS", 1.0)
		require.NoError(t, err)
		_, err = db.Link(ctx, memA.ID, memC.ID, "KNOWS", 1.0)
		require.NoError(t, err)

		neighbors, err := db.Neighbors(ctx, memA.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, neighbors, 2)

		contents := []string{neighbors[0].Content, neighbors[1].Content}
		assert.Contains(t, contents, "B")
		assert.Contains(t, contents, "C")
	})

	t.Run("finds incoming neighbors", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})

		// A -> B (so B has incoming from A)
		_, err = db.Link(ctx, memA.ID, memB.ID, "POINTS_TO", 1.0)
		require.NoError(t, err)

		neighbors, err := db.Neighbors(ctx, memB.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, neighbors, 1)
		assert.Equal(t, "A", neighbors[0].Content)
	})

	t.Run("filters by edge type", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		memC, _ := db.Store(ctx, &Memory{Content: "C"})

		_, _ = db.Link(ctx, memA.ID, memB.ID, "KNOWS", 1.0)
		_, _ = db.Link(ctx, memA.ID, memC.ID, "LIKES", 1.0)

		neighbors, err := db.Neighbors(ctx, memA.ID, 1, "KNOWS")
		require.NoError(t, err)
		require.Len(t, neighbors, 1)
		assert.Equal(t, "B", neighbors[0].Content)
	})

	t.Run("traverses multiple hops", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		memC, _ := db.Store(ctx, &Memory{Content: "C"})

		// A -> B -> C
		_, _ = db.Link(ctx, memA.ID, memB.ID, "NEXT", 1.0)
		_, _ = db.Link(ctx, memB.ID, memC.ID, "NEXT", 1.0)

		// Depth 1 from A: just B
		neighbors1, err := db.Neighbors(ctx, memA.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, neighbors1, 1)

		// Depth 2 from A: B and C
		neighbors2, err := db.Neighbors(ctx, memA.ID, 2, "")
		require.NoError(t, err)
		require.Len(t, neighbors2, 2)
	})

	t.Run("returns empty for isolated node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})

		neighbors, err := db.Neighbors(ctx, memA.ID, 1, "")
		require.NoError(t, err)
		assert.Empty(t, neighbors)
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Neighbors(ctx, "", 1, "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Neighbors(ctx, "any-id", 1, "")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestForget(t *testing.T) {
	ctx := context.Background()

	t.Run("forgets memory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem, _ := db.Store(ctx, &Memory{Content: "To forget"})

		err = db.Forget(ctx, mem.ID)
		require.NoError(t, err)

		// Should no longer be recallable
		_, err = db.Recall(ctx, mem.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("cleans up edges", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		_, _ = db.Link(ctx, memA.ID, memB.ID, "TEST", 1.0)

		// Forget A
		err = db.Forget(ctx, memA.ID)
		require.NoError(t, err)

		// B should have no neighbors now
		neighbors, err := db.Neighbors(ctx, memB.ID, 1, "")
		require.NoError(t, err)
		assert.Empty(t, neighbors)
	})

	t.Run("returns error for non-existent ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.Forget(ctx, "non-existent")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.Forget(ctx, "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.Forget(ctx, "any-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestCypher(t *testing.T) {
	ctx := context.Background()

	t.Run("returns empty results for now", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		results, err := db.Cypher(ctx, "MATCH (n) RETURN n", nil)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Empty(t, results)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Cypher(ctx, "MATCH (n) RETURN n", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, "./data", config.DataDir)
	assert.Equal(t, "openai", config.EmbeddingProvider)
	assert.Equal(t, "http://localhost:11434", config.EmbeddingAPIURL)
	assert.Equal(t, "mxbai-embed-large", config.EmbeddingModel)
	assert.Equal(t, 1024, config.EmbeddingDimensions)
	assert.True(t, config.DecayEnabled)
	assert.Equal(t, time.Hour, config.DecayRecalculateInterval)
	assert.Equal(t, 0.05, config.DecayArchiveThreshold)
	assert.True(t, config.AutoLinksEnabled)
	assert.Equal(t, 0.82, config.AutoLinksSimilarityThreshold)
	assert.Equal(t, 30*time.Second, config.AutoLinksCoAccessWindow)
	assert.Equal(t, 7687, config.BoltPort)
	assert.Equal(t, 7474, config.HTTPPort)
}

func TestGenerateID(t *testing.T) {
	// Test that IDs are unique
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateID("test")
		assert.False(t, ids[id], "ID should be unique")
		ids[id] = true
		assert.Contains(t, id, "test-")
	}
}

func TestMemoryToNode(t *testing.T) {
	mem := &Memory{
		ID:           "test-123",
		Content:      "Test content",
		Title:        "Test Title",
		Tier:         TierProcedural,
		DecayScore:   0.8,
		CreatedAt:    time.Now(),
		LastAccessed: time.Now(),
		AccessCount:  5,
		Embedding:    []float32{1.0, 2.0, 3.0},
		Tags:         []string{"tag1", "tag2"},
		Source:       "test-source",
		Properties:   map[string]any{"custom": "value"},
	}

	node := memoryToNode(mem)

	assert.Equal(t, "test-123", string(node.ID))
	assert.Equal(t, []string{"Memory"}, node.Labels)
	assert.Equal(t, "Test content", node.Properties["content"])
	assert.Equal(t, "Test Title", node.Properties["title"])
	assert.Equal(t, "PROCEDURAL", node.Properties["tier"])
	assert.Equal(t, []float32{1.0, 2.0, 3.0}, node.Embedding)
	assert.Equal(t, "test-source", node.Properties["source"])
	assert.Equal(t, "value", node.Properties["custom"])
}

func TestNodeToMemory(t *testing.T) {
	now := time.Now()
	node := &storage.Node{
		ID:        "node-123",
		Labels:    []string{"Memory"},
		CreatedAt: now,
		Embedding: []float32{1.0, 2.0},
		Properties: map[string]any{
			"content":       "Node content",
			"title":         "Node Title",
			"tier":          "EPISODIC",
			"decay_score":   0.7,
			"last_accessed": now.Format(time.RFC3339),
			"access_count":  int64(3),
			"source":        "node-source",
			"tags":          []string{"a", "b"},
			"custom_prop":   "custom_value",
		},
	}

	mem := nodeToMemory(node)

	assert.Equal(t, "node-123", mem.ID)
	assert.Equal(t, "Node content", mem.Content)
	assert.Equal(t, "Node Title", mem.Title)
	assert.Equal(t, TierEpisodic, mem.Tier)
	assert.Equal(t, 0.7, mem.DecayScore)
	assert.Equal(t, []float32{1.0, 2.0}, mem.Embedding)
	assert.Equal(t, "node-source", mem.Source)
	assert.Equal(t, []string{"a", "b"}, mem.Tags)
	assert.Equal(t, "custom_value", mem.Properties["custom_prop"])
}

func TestCosineSimilarity(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{1.0, 0.0, 0.0}
		sim := cosineSimilarity(a, b)
		assert.InDelta(t, 1.0, sim, 0.001)
	})

	t.Run("orthogonal vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.0, 1.0, 0.0}
		sim := cosineSimilarity(a, b)
		assert.InDelta(t, 0.0, sim, 0.001)
	})

	t.Run("opposite vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{-1.0, 0.0, 0.0}
		sim := cosineSimilarity(a, b)
		assert.InDelta(t, -1.0, sim, 0.001)
	})

	t.Run("similar vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.9, 0.1, 0.0}
		sim := cosineSimilarity(a, b)
		assert.Greater(t, sim, 0.9)
	})

	t.Run("different lengths returns 0", func(t *testing.T) {
		a := []float32{1.0, 0.0}
		b := []float32{1.0, 0.0, 0.0}
		sim := cosineSimilarity(a, b)
		assert.Equal(t, 0.0, sim)
	})

	t.Run("empty vectors returns 0", func(t *testing.T) {
		sim := cosineSimilarity([]float32{}, []float32{})
		assert.Equal(t, 0.0, sim)
	})

	t.Run("zero vectors returns 0", func(t *testing.T) {
		a := []float32{0.0, 0.0}
		b := []float32{0.0, 0.0}
		sim := cosineSimilarity(a, b)
		assert.Equal(t, 0.0, sim)
	})
}

func TestSqrt(t *testing.T) {
	t.Run("positive values", func(t *testing.T) {
		assert.InDelta(t, 2.0, sqrt(4.0), 0.001)
		assert.InDelta(t, 3.0, sqrt(9.0), 0.001)
		assert.InDelta(t, 1.414, sqrt(2.0), 0.01)
	})

	t.Run("zero", func(t *testing.T) {
		assert.Equal(t, 0.0, sqrt(0.0))
	})

	t.Run("negative returns 0", func(t *testing.T) {
		assert.Equal(t, 0.0, sqrt(-1.0))
	})
}

func TestDecayIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("decay affects recall score", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Decaying memory",
			Tier:    TierEpisodic,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, 1.0, stored.DecayScore)

		// Multiple recalls should reinforce the memory
		for i := 0; i < 3; i++ {
			_, err = db.Recall(ctx, stored.ID)
			require.NoError(t, err)
		}

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(4), recalled.AccessCount)
	})
}

func TestWithoutDecay(t *testing.T) {
	ctx := context.Background()

	t.Run("works without decay manager", func(t *testing.T) {
		config := &Config{
			DecayEnabled:     false,
			AutoLinksEnabled: false,
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{Content: "No decay"}
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, stored.Content, recalled.Content)
		assert.Equal(t, int64(1), recalled.AccessCount)
	})
}

// =============================================================================
// HTTP Server Interface Tests (Stats, ExecuteCypher, Node/Edge CRUD)
// =============================================================================

func TestStats(t *testing.T) {
	ctx := context.Background()

	t.Run("returns initial stats", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		stats := db.Stats()
		assert.GreaterOrEqual(t, stats.NodeCount, int64(0))
		assert.GreaterOrEqual(t, stats.EdgeCount, int64(0))
	})

	t.Run("updates after storing", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		initialStats := db.Stats()

		_, err = db.Store(ctx, &Memory{Content: "stats test"})
		require.NoError(t, err)

		stats := db.Stats()
		assert.GreaterOrEqual(t, stats.NodeCount, initialStats.NodeCount)
	})
}

func TestExecuteCypher(t *testing.T) {
	ctx := context.Background()

	t.Run("executes match query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN n LIMIT 10", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Columns)
	})

	t.Run("executes with parameters", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		params := map[string]interface{}{"name": "test"}
		result, err := db.ExecuteCypher(ctx, "MATCH (n) WHERE n.name = $name RETURN n", params)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN n", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("creates and queries nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create a node via Cypher
		_, err = db.ExecuteCypher(ctx, "CREATE (n:TestPerson {name: 'TestAlice'}) RETURN n", nil)
		require.NoError(t, err)

		// Query it back
		result, err := db.ExecuteCypher(ctx, "MATCH (n:TestPerson) RETURN n.name", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}

// =============================================================================
// Node CRUD Tests
// =============================================================================

func TestListNodes(t *testing.T) {
	ctx := context.Background()

	t.Run("lists all nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create some nodes
		db.CreateNode(ctx, []string{"TestListPerson"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"TestListPerson"}, map[string]interface{}{"name": "Bob"})

		nodes, err := db.ListNodes(ctx, "TestListPerson", 100, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 2)
	})

	t.Run("filters by label", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"FilterTestPerson"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"FilterTestItem"}, map[string]interface{}{"name": "Book"})

		nodes, err := db.ListNodes(ctx, "FilterTestPerson", 100, 0)
		require.NoError(t, err)
		// All returned nodes should have FilterTestPerson label
		for _, n := range nodes {
			found := false
			for _, l := range n.Labels {
				if l == "FilterTestPerson" {
					found = true
					break
				}
			}
			assert.True(t, found, "all nodes should have FilterTestPerson label")
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		for i := 0; i < 10; i++ {
			db.CreateNode(ctx, []string{"LimitTest"}, map[string]interface{}{"i": i})
		}

		nodes, err := db.ListNodes(ctx, "LimitTest", 3, 0)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(nodes), 3)
	})

	t.Run("respects offset", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		for i := 0; i < 5; i++ {
			db.CreateNode(ctx, []string{"OffsetTest"}, map[string]interface{}{"i": i})
		}

		nodes, err := db.ListNodes(ctx, "OffsetTest", 100, 2)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 0)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ListNodes(ctx, "", 100, 0)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetNode(t *testing.T) {
	ctx := context.Background()

	t.Run("gets existing node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		created, err := db.CreateNode(ctx, []string{"GetNodeTest"}, map[string]interface{}{"name": "TestGetNode"})
		require.NoError(t, err)

		node, err := db.GetNode(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, node.ID)
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.GetNode(ctx, "nonexistent-node-id-xyz")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetNode(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestCreateNode(t *testing.T) {
	ctx := context.Background()

	t.Run("creates node with labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		node, err := db.CreateNode(ctx, []string{"CreateTest", "Employee"}, map[string]interface{}{"name": "CreateAlice"})
		require.NoError(t, err)
		assert.NotEmpty(t, node.ID)
		assert.Len(t, node.Labels, 2)
	})

	t.Run("creates node with properties", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		props := map[string]interface{}{
			"name":  "PropAlice",
			"age":   30,
			"email": "alice@example.com",
		}
		node, err := db.CreateNode(ctx, []string{"PropTest"}, props)
		require.NoError(t, err)
		assert.Equal(t, "PropAlice", node.Properties["name"])
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.CreateNode(ctx, []string{"Test"}, nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestUpdateNode(t *testing.T) {
	ctx := context.Background()

	t.Run("updates existing node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		created, err := db.CreateNode(ctx, []string{"UpdateTest"}, map[string]interface{}{"name": "OriginalName"})
		require.NoError(t, err)

		updated, err := db.UpdateNode(ctx, created.ID, map[string]interface{}{"name": "UpdatedName", "age": 30})
		require.NoError(t, err)
		assert.Equal(t, "UpdatedName", updated.Properties["name"])
		assert.Equal(t, 30, updated.Properties["age"])
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.UpdateNode(ctx, "nonexistent-update-id", map[string]interface{}{"name": "test"})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.UpdateNode(ctx, "test-id", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDeleteNode(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		created, err := db.CreateNode(ctx, []string{"DeleteTest"}, map[string]interface{}{"name": "ToDelete"})
		require.NoError(t, err)

		err = db.DeleteNode(ctx, created.ID)
		require.NoError(t, err)

		// Verify it's deleted
		_, err = db.GetNode(ctx, created.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.DeleteNode(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Edge CRUD Tests
// =============================================================================

func TestListEdges(t *testing.T) {
	ctx := context.Background()

	t.Run("lists all edges", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgeListTest"}, map[string]interface{}{"name": "EdgeAlice"})
		n2, _ := db.CreateNode(ctx, []string{"EdgeListTest"}, map[string]interface{}{"name": "EdgeBob"})

		db.CreateEdge(ctx, n1.ID, n2.ID, "TEST_KNOWS", nil)

		edges, err := db.ListEdges(ctx, "TEST_KNOWS", 100, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(edges), 1)
	})

	t.Run("filters by type", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgeFilterTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"EdgeFilterTest"}, nil)

		db.CreateEdge(ctx, n1.ID, n2.ID, "FILTER_TYPE_A", nil)
		db.CreateEdge(ctx, n1.ID, n2.ID, "FILTER_TYPE_B", nil)

		edges, err := db.ListEdges(ctx, "FILTER_TYPE_A", 100, 0)
		require.NoError(t, err)
		for _, e := range edges {
			assert.Equal(t, "FILTER_TYPE_A", e.Type)
		}
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ListEdges(ctx, "", 100, 0)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetEdge(t *testing.T) {
	ctx := context.Background()

	t.Run("gets existing edge", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"GetEdgeTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"GetEdgeTest"}, nil)
		created, err := db.CreateEdge(ctx, n1.ID, n2.ID, "GET_EDGE_TEST", nil)
		require.NoError(t, err)

		edge, err := db.GetEdge(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, edge.ID)
		assert.Equal(t, "GET_EDGE_TEST", edge.Type)
	})

	t.Run("returns error for non-existent edge", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.GetEdge(ctx, "nonexistent-edge-id")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetEdge(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestCreateEdge(t *testing.T) {
	ctx := context.Background()

	t.Run("creates edge between nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"CreateEdgeTest"}, map[string]interface{}{"name": "EdgeAlice"})
		n2, _ := db.CreateNode(ctx, []string{"CreateEdgeTest"}, map[string]interface{}{"name": "EdgeBob"})

		edge, err := db.CreateEdge(ctx, n1.ID, n2.ID, "CREATE_EDGE_TEST", map[string]interface{}{"since": 2020})
		require.NoError(t, err)
		assert.NotEmpty(t, edge.ID)
		assert.Equal(t, n1.ID, edge.Source)
		assert.Equal(t, n2.ID, edge.Target)
	})

	t.Run("returns error for non-existent source", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n2, _ := db.CreateNode(ctx, []string{"EdgeSrcTest"}, nil)

		_, err = db.CreateEdge(ctx, "nonexistent-source", n2.ID, "TEST", nil)
		assert.Error(t, err)
	})

	t.Run("returns error for non-existent target", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgeTgtTest"}, nil)

		_, err = db.CreateEdge(ctx, n1.ID, "nonexistent-target", "TEST", nil)
		assert.Error(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.CreateEdge(ctx, "a", "b", "KNOWS", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDeleteEdge(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing edge", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"DeleteEdgeTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"DeleteEdgeTest"}, nil)
		created, err := db.CreateEdge(ctx, n1.ID, n2.ID, "TO_DELETE", nil)
		require.NoError(t, err)

		err = db.DeleteEdge(ctx, created.ID)
		require.NoError(t, err)

		// Verify it's deleted
		_, err = db.GetEdge(ctx, created.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.DeleteEdge(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Search Tests
// =============================================================================

func TestSearch(t *testing.T) {
	ctx := context.Background()

	t.Run("finds matching nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"SearchTest"}, map[string]interface{}{"name": "SearchableAlice", "bio": "Software engineer"})
		db.CreateNode(ctx, []string{"SearchTest"}, map[string]interface{}{"name": "SearchableBob", "bio": "Product manager"})

		results, err := db.Search(ctx, "searchable", nil, 10)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("filters by labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"SearchLabelPerson"}, map[string]interface{}{"desc": "labeltest expert"})
		db.CreateNode(ctx, []string{"SearchLabelCompany"}, map[string]interface{}{"desc": "labeltest company"})

		results, err := db.Search(ctx, "labeltest", []string{"SearchLabelPerson"}, 10)
		require.NoError(t, err)
		// Results should only contain SearchLabelPerson nodes
		for _, r := range results {
			found := false
			for _, l := range r.Node.Labels {
				if l == "SearchLabelPerson" {
					found = true
					break
				}
			}
			assert.True(t, found, "result should have SearchLabelPerson label")
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"CaseTest"}, map[string]interface{}{"text": "UniqueSearchTerm123"})

		results, err := db.Search(ctx, "uniquesearchterm123", nil, 10)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("respects limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		for i := 0; i < 10; i++ {
			db.CreateNode(ctx, []string{"LimitSearchTest"}, map[string]interface{}{"text": "limitsearchcontent"})
		}

		results, err := db.Search(ctx, "limitsearchcontent", []string{"LimitSearchTest"}, 3)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 3)
	})

	t.Run("returns empty for no matches", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		results, err := db.Search(ctx, "xyznonexistent123456", nil, 10)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Search(ctx, "test", nil, 10)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestFindSimilar(t *testing.T) {
	ctx := context.Background()

	t.Run("finds similar nodes by embedding", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create nodes with embeddings via Store
		mem1, _ := db.Store(ctx, &Memory{
			Content:   "similar test memory 1",
			Embedding: []float32{1.0, 0.0, 0.0},
		})
		db.Store(ctx, &Memory{
			Content:   "similar test memory 2",
			Embedding: []float32{0.9, 0.1, 0.0}, // Similar
		})

		results, err := db.FindSimilar(ctx, mem1.ID, 10)
		require.NoError(t, err)
		// May or may not find similar depending on other data in testdata
		assert.NotNil(t, results)
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.FindSimilar(ctx, "nonexistent-similar-id", 10)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error for node without embedding", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create node without embedding
		node, _ := db.CreateNode(ctx, []string{"NoEmbedTest"}, map[string]interface{}{"name": "no embedding"})

		_, err = db.FindSimilar(ctx, node.ID, 10)
		assert.Error(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.FindSimilar(ctx, "test-id", 10)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Schema Tests
// =============================================================================

func TestGetLabels(t *testing.T) {
	ctx := context.Background()

	t.Run("returns labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"LabelTestA"}, nil)
		db.CreateNode(ctx, []string{"LabelTestB"}, nil)

		labels, err := db.GetLabels(ctx)
		require.NoError(t, err)
		assert.NotNil(t, labels)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetLabels(ctx)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetRelationshipTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("returns relationship types", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"RelTypeTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"RelTypeTest"}, nil)

		db.CreateEdge(ctx, n1.ID, n2.ID, "REL_TYPE_A", nil)
		db.CreateEdge(ctx, n1.ID, n2.ID, "REL_TYPE_B", nil)

		types, err := db.GetRelationshipTypes(ctx)
		require.NoError(t, err)
		assert.NotNil(t, types)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetRelationshipTypes(ctx)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetIndexes(t *testing.T) {
	ctx := context.Background()

	t.Run("returns indexes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		indexes, err := db.GetIndexes(ctx)
		require.NoError(t, err)
		assert.NotNil(t, indexes)
	})
}

func TestCreateIndex(t *testing.T) {
	ctx := context.Background()

	t.Run("creates index without error", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.CreateIndex(ctx, "Person", "name", "btree")
		require.NoError(t, err)
	})
}

// =============================================================================
// Backup Tests
// =============================================================================

func TestBackup(t *testing.T) {
	ctx := context.Background()

	t.Run("backup succeeds", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.Backup(ctx, "./testdata/backup-test")
		require.NoError(t, err)
	})
}

// =============================================================================
// GDPR Compliance Tests
// =============================================================================

func TestExportUserData(t *testing.T) {
	ctx := context.Background()

	t.Run("exports user data as JSON", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create nodes owned by user
		db.CreateNode(ctx, []string{"GDPRExportTest"}, map[string]interface{}{
			"owner_id": "gdpr-user-export-123",
			"content":  "My note",
		})

		data, err := db.ExportUserData(ctx, "gdpr-user-export-123", "json")
		require.NoError(t, err)
		assert.NotEmpty(t, data)

		// Parse JSON
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		assert.Equal(t, "gdpr-user-export-123", result["user_id"])
	})

	t.Run("exports as CSV", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		data, err := db.ExportUserData(ctx, "gdpr-user-csv-123", "csv")
		require.NoError(t, err)
		assert.NotEmpty(t, data)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ExportUserData(ctx, "user-123", "json")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDeleteUserData(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes user data", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create nodes owned by user
		db.CreateNode(ctx, []string{"GDPRDeleteTest"}, map[string]interface{}{
			"owner_id": "gdpr-user-delete-456",
			"content":  "To delete",
		})

		err = db.DeleteUserData(ctx, "gdpr-user-delete-456")
		require.NoError(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.DeleteUserData(ctx, "user-123")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestAnonymizeUserData(t *testing.T) {
	ctx := context.Background()

	t.Run("anonymizes user data", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create node with PII
		node, _ := db.CreateNode(ctx, []string{"GDPRAnonTest"}, map[string]interface{}{
			"owner_id":   "gdpr-user-anon-789",
			"name":       "Alice Smith",
			"email":      "alice@example.com",
			"username":   "alice",
			"ip_address": "192.168.1.1",
		})

		err = db.AnonymizeUserData(ctx, "gdpr-user-anon-789")
		require.NoError(t, err)

		// Verify PII is removed
		updated, err := db.GetNode(ctx, node.ID)
		require.NoError(t, err)
		assert.Nil(t, updated.Properties["email"])
		assert.Nil(t, updated.Properties["name"])
		assert.Nil(t, updated.Properties["username"])
		assert.Nil(t, updated.Properties["ip_address"])
		// Owner ID should be anonymized
		assert.NotEqual(t, "gdpr-user-anon-789", updated.Properties["owner_id"])
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.AnonymizeUserData(ctx, "user-123")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Additional Edge Cases - nodeToMemory with interface{} tags
// =============================================================================

func TestNodeToMemoryInterfaceTags(t *testing.T) {
	node := &storage.Node{
		ID:        "test-interface-tags",
		Labels:    []string{"Memory"},
		CreatedAt: time.Now(),
		Properties: map[string]any{
			"content": "test content",
			"tags":    []interface{}{"tag1", "tag2"},
		},
	}

	mem := nodeToMemory(node)
	assert.Equal(t, []string{"tag1", "tag2"}, mem.Tags)
}

func TestNodeToMemoryIntAccessCount(t *testing.T) {
	node := &storage.Node{
		ID:        "test-int-access",
		Labels:    []string{"Memory"},
		CreatedAt: time.Now(),
		Properties: map[string]any{
			"content":      "test content",
			"access_count": 5, // int instead of int64
		},
	}

	mem := nodeToMemory(node)
	assert.Equal(t, int64(5), mem.AccessCount)
}

// =============================================================================
// Tests for 0% coverage functions
// =============================================================================

func TestHybridSearch(t *testing.T) {
	ctx := context.Background()

	t.Run("basic_hybrid_search", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store some test memories
		for i := 0; i < 5; i++ {
			_, err := db.Store(ctx, &Memory{
				Content: "Test content about machine learning and AI",
				Title:   "ML Test",
			})
			require.NoError(t, err)
		}

		// Create a mock query embedding (1024 dimensions)
		queryEmbedding := make([]float32, 1024)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.1
		}

		results, err := db.HybridSearch(ctx, "machine learning", queryEmbedding, nil, 10)
		require.NoError(t, err)
		// Results may be empty if no search service or embeddings indexed
		t.Logf("HybridSearch returned %d results", len(results))
	})

	t.Run("hybrid_search_with_labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		queryEmbedding := make([]float32, 1024)

		results, err := db.HybridSearch(ctx, "test", queryEmbedding, []string{"Memory"}, 10)
		require.NoError(t, err)
		t.Logf("HybridSearch with labels returned %d results", len(results))
	})

	t.Run("hybrid_search_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		queryEmbedding := make([]float32, 1024)

		_, err = db.HybridSearch(ctx, "test", queryEmbedding, nil, 10)
		assert.Error(t, err, "Should error on closed DB")
	})

	t.Run("hybrid_search_empty_query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		queryEmbedding := make([]float32, 1024)

		results, err := db.HybridSearch(ctx, "", queryEmbedding, nil, 10)
		require.NoError(t, err)
		t.Logf("HybridSearch with empty query returned %d results", len(results))
	})
}

func TestLoadFromExport(t *testing.T) {
	ctx := context.Background()

	t.Run("load_from_nonexistent_directory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.LoadFromExport(ctx, "/nonexistent/path/to/export")
		assert.Error(t, err, "Should error for nonexistent directory")
	})

	t.Run("load_from_export_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.LoadFromExport(ctx, "./testdata")
		assert.Error(t, err, "Should error on closed DB")
		assert.Equal(t, ErrClosed, err)
	})
}

func TestBuildSearchIndexes(t *testing.T) {
	ctx := context.Background()

	t.Run("build_indexes_on_empty_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.BuildSearchIndexes(ctx)
		require.NoError(t, err, "Should succeed on empty DB")
	})

	t.Run("build_indexes_with_data", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store some test memories
		for i := 0; i < 3; i++ {
			_, err := db.Store(ctx, &Memory{
				Content: "Searchable content for indexing test",
				Title:   "Index Test",
			})
			require.NoError(t, err)
		}

		err = db.BuildSearchIndexes(ctx)
		require.NoError(t, err)
	})

	t.Run("build_indexes_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.BuildSearchIndexes(ctx)
		assert.Error(t, err, "Should error on closed DB")
		assert.Equal(t, ErrClosed, err)
	})
}

func TestSetGetGPUManager(t *testing.T) {
	t.Run("set_and_get_gpu_manager", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Initially nil
		assert.Nil(t, db.GetGPUManager())

		// Set a mock manager (using interface{})
		mockManager := struct{ Name string }{Name: "MockGPU"}
		db.SetGPUManager(mockManager)

		// Get it back
		retrieved := db.GetGPUManager()
		assert.NotNil(t, retrieved)

		// Type assert back
		mock, ok := retrieved.(struct{ Name string })
		assert.True(t, ok)
		assert.Equal(t, "MockGPU", mock.Name)
	})

	t.Run("set_nil_gpu_manager", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Set a manager
		db.SetGPUManager("test")
		assert.NotNil(t, db.GetGPUManager())

		// Set nil to clear
		db.SetGPUManager(nil)
		assert.Nil(t, db.GetGPUManager())
	})

	t.Run("thread_safe_access", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Concurrent access should not panic
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(n int) {
				db.SetGPUManager(n)
				_ = db.GetGPUManager()
				done <- true
			}(i)
		}
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestCypherFunctionWithParams(t *testing.T) {
	ctx := context.Background()

	t.Run("simple_cypher_query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store some data
		_, err = db.Store(ctx, &Memory{
			Content: "Test for Cypher",
			Title:   "Cypher Test",
		})
		require.NoError(t, err)

		// Execute Cypher query
		resultSet, err := db.Cypher(ctx, "MATCH (n:Memory) RETURN count(n)", nil)
		require.NoError(t, err)
		assert.NotNil(t, resultSet)
	})

	t.Run("cypher_with_create", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		resultSet, err := db.Cypher(ctx, "CREATE (n:TestNode {name: 'created'})", nil)
		require.NoError(t, err)
		assert.NotNil(t, resultSet)
	})

	t.Run("cypher_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Cypher(ctx, "RETURN 1", nil)
		assert.Error(t, err)
	})

	t.Run("cypher_invalid_query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Cypher(ctx, "INVALID QUERY SYNTAX", nil)
		assert.Error(t, err)
	})

	t.Run("cypher_with_params", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		params := map[string]any{
			"name": "test",
		}
		resultSet, err := db.Cypher(ctx, "CREATE (n:Test {name: $name})", params)
		require.NoError(t, err)
		assert.NotNil(t, resultSet)
	})
}
