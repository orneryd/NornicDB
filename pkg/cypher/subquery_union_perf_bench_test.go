package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkCallSubqueryUnion_FixedCost_CacheMiss profiles the fixed execution
// overhead of CALL { ... UNION ALL ... } with a cache-busted query per iteration.
func BenchmarkCallSubqueryUnion_FixedCost_CacheMiss(b *testing.B) {
	baseStore := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "sp-1",
		Labels: []string{"SystemPrompt"},
		Properties: map[string]interface{}{
			"promptId": "prompt-id",
			"text":     "system text",
		},
	})
	if err != nil {
		b.Fatalf("seed create failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cache-bust to force execution path profiling instead of query-cache hits.
		query := fmt.Sprintf(`
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN 0 AS sortOrder, 'SYSTEM_PROMPT' AS rowType, p.text AS systemPrompt, null AS originalText, null AS score, null AS language, null AS translatedText
  UNION ALL
  WITH p
  RETURN 1 AS sortOrder, 'CANDIDATE' AS rowType, null AS systemPrompt, 'x-%d' AS originalText, 1.0 AS score, 'en' AS language, 'x' AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6`, i)
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			b.Fatalf("execute failed: %v", err)
		}
	}
}

// BenchmarkCallSubqueryUnion_VectorRelBranch profiles the live query shape:
// Branch 1 = pure projection from seed, Branch 2 = vector search + relationship traversal.
// Branch 2 does NOT depend on the seed (p) — static branch caching should kick in.
func BenchmarkCallSubqueryUnion_VectorRelBranch(b *testing.B) {
	baseStore := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create seed SystemPrompt node
	_, err := store.CreateNode(&storage.Node{
		ID:     "sp-1",
		Labels: []string{"SystemPrompt"},
		Properties: map[string]interface{}{
			"promptId": "prompt-id",
			"text":     "system prompt text",
		},
	})
	if err != nil {
		b.Fatalf("create SystemPrompt failed: %v", err)
	}

	// Create OriginalText + TranslatedText nodes with relationships and vector index
	dim := 4
	embedding := make([]float32, dim)
	for d := 0; d < dim; d++ {
		embedding[d] = float32(d+1) * 0.1
	}
	for i := 0; i < 10; i++ {
		otID := storage.NodeID(fmt.Sprintf("ot-%d", i))
		_, err := store.CreateNode(&storage.Node{
			ID:     otID,
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"originalText": fmt.Sprintf("original text %d", i),
			},
			ChunkEmbeddings: [][]float32{embedding},
		})
		if err != nil {
			b.Fatalf("create OriginalText failed: %v", err)
		}
		for _, lang := range []string{"en", "es"} {
			ttID := storage.NodeID(fmt.Sprintf("tt-%d-%s", i, lang))
			_, err := store.CreateNode(&storage.Node{
				ID:     ttID,
				Labels: []string{"TranslatedText"},
				Properties: map[string]interface{}{
					"language":       lang,
					"translatedText": fmt.Sprintf("translated %d %s", i, lang),
				},
			})
			if err != nil {
				b.Fatalf("create TranslatedText failed: %v", err)
			}
			err = store.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("e-%d-%s", i, lang)),
				StartNode: otID,
				EndNode:   ttID,
				Type:      "TRANSLATES_TO",
			})
			if err != nil {
				b.Fatalf("create edge failed: %v", err)
			}
		}
	}

	// Create vector index
	schema := store.GetSchema()
	if err := schema.AddVectorIndex("idx_original_text", "OriginalText", "embedding", dim, "cosine"); err != nil {
		b.Fatalf("create vector index failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf(`
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText
  UNION ALL
  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, [0.1, 0.2, 0.3, 0.%d])
  YIELD node, score
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  WITH node, score, t
  ORDER BY score DESC, t.language
  LIMIT 5
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6`, i)
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			b.Fatalf("execute failed: %v", err)
		}
	}
}
