//go:build integration
// +build integration

package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type staticBenchmarkEmbedder struct {
	vec []float32
}

func (s staticBenchmarkEmbedder) Embed(context.Context, string) ([]float32, error) {
	out := make([]float32, len(s.vec))
	copy(out, s.vec)
	return out, nil
}

const realDataUnionQueryTemplate = `
MATCH (p:SystemPrompt)
WITH p, %d AS __cache_bust
LIMIT 1
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
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  WHERE node.originalText = "Get it delivered"
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    1.0 AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

const realDataSourceIDJoinQueryTemplate = `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId, coalesce(t.textKey, t.textKey128) AS textKey, t.originalText AS originalText
  ORDER BY t.sourceId
  LIMIT 25
}
CALL {
  WITH sourceId
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.translationId = sourceId
  RETURN tt.language AS language, tt.translatedText AS translatedText
}
RETURN sourceId, textKey, originalText, language, translatedText
ORDER BY sourceId, language
`

const realDataLocalCorrelatedJoinQueryTemplate = `
MATCH (t:MongoDocument)
WHERE t.sourceId IS NOT NULL
WITH t
ORDER BY t.sourceId
LIMIT 25
CALL {
  WITH t
  MATCH (tt:MongoDocument)
  WHERE tt.sourceId = t.sourceId
  RETURN tt.sourceId AS sourceId, tt.language AS language, tt.translatedText AS translatedText
}
RETURN t.sourceId AS sourceId, coalesce(t.textKey, t.textKey128) AS textKey, t.originalText AS originalText, language, translatedText
ORDER BY sourceId, language
`

const realDataVectorUnionPromptQueryTemplate = `
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
  CALL db.index.vector.queryNodes('idx_original_text', 5, "GEt It delivered")
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
LIMIT 6
`

func openRealDataBenchmarkExecutor(b *testing.B, databaseName string) (*StorageExecutor, func()) {
	b.Helper()
	if os.Getenv("INTEGRATION_TEST") == "" {
		b.Skip("Set INTEGRATION_TEST=1 to run real-data benchmark")
	}

	dataDir := strings.TrimSpace(os.Getenv("NORNICDB_REAL_DATA_DIR"))
	if dataDir == "" {
		dataDir = "/usr/local/var/nornicdb/data"
	}
	if _, err := os.Stat(dataDir); err != nil {
		b.Skipf("real data dir unavailable: %s (%v)", dataDir, err)
	}

	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngine(dataDir))
	if err != nil {
		b.Fatalf("open badger real data: %v", err)
	}

	store := storage.NewNamespacedEngine(base, databaseName)
	exec := NewStorageExecutor(store)

	cleanup := func() {
		_ = base.Close()
	}
	return exec, cleanup
}

// BenchmarkRealData_UnionSubquery_CacheMiss runs the exact CALL { ... UNION ALL ... }
// query family against the real on-disk dataset and cache-busts each iteration to
// expose fixed first-hit overhead.
//
// Example:
//
//	INTEGRATION_TEST=1 NORNICDB_REAL_DATA_DIR=/usr/local/var/nornicdb/data \
//	  go test -tags=integration ./pkg/cypher \
//	  -run '^$' -bench BenchmarkRealData_UnionSubquery_CacheMiss \
//	  -benchmem -cpuprofile /tmp/fabric_realdata_cpu.prof -memprofile /tmp/fabric_realdata_mem.prof
func BenchmarkRealData_UnionSubquery_CacheMiss(b *testing.B) {
	exec, cleanup := openRealDataBenchmarkExecutor(b, "caremark_translation")
	defer cleanup()

	// Ensure the predicate in the UNION branch can use index-based start-node pruning.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_, _ = exec.Execute(ctx, "CREATE INDEX idx_originaltext_originaltext IF NOT EXISTS FOR (n:OriginalText) ON (n.originalText)", nil)
		cancel()
	}

	// One warm-up execution so startup side effects don't dominate samples.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _ = exec.Execute(ctx, fmt.Sprintf(realDataUnionQueryTemplate, -1), nil)
		cancel()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := fmt.Sprintf(realDataUnionQueryTemplate, i)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		res, err := exec.Execute(ctx, q, nil)
		cancel()
		if err != nil {
			b.Fatalf("execute failed at iter %d: %v", i, err)
		}
		if res == nil || len(res.Columns) == 0 {
			b.Fatalf("unexpected empty result metadata at iter %d", i)
		}
	}
}

// BenchmarkFabricRealData_SourceIDJoin_CacheMiss runs the composite USE/CALL query
// shape when a translations composite is present in the real dataset.
func BenchmarkFabricRealData_SourceIDJoin_CacheMiss(b *testing.B) {
	exec, cleanup := openRealDataBenchmarkExecutor(b, "nornic")
	defer cleanup()

	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _ = exec.Execute(ctx, realDataSourceIDJoinQueryTemplate, nil)
		cancel()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := realDataSourceIDJoinQueryTemplate
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		res, err := exec.Execute(ctx, q, nil)
		cancel()
		if err != nil {
			// In real datasets where "translations" composite isn't configured, skip.
			if strings.Contains(strings.ToLower(err.Error()), "database 'translations' not found") ||
				strings.Contains(strings.ToLower(err.Error()), "invalid use target") {
				b.Skipf("translations composite unavailable in this real dataset: %v", err)
			}
			b.Fatalf("execute failed at iter %d: %v", i, err)
		}
		if res == nil || len(res.Columns) == 0 {
			b.Fatalf("unexpected empty result metadata at iter %d", i)
		}
	}
}

// BenchmarkRealData_LocalCorrelatedJoin_CacheMiss profiles the non-fabric
// correlated CALL path against real data. This is the N+1-sensitive path that
// now uses a batched IN rewrite + hash join fast path in executor_subqueries.
func BenchmarkRealData_LocalCorrelatedJoin_CacheMiss(b *testing.B) {
	exec, cleanup := openRealDataBenchmarkExecutor(b, "caremark_translation")
	defer cleanup()

	{
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_, _ = exec.Execute(ctx, "CREATE INDEX idx_mongodoc_sourceid IF NOT EXISTS FOR (n:MongoDocument) ON (n.sourceId)", nil)
		cancel()
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _ = exec.Execute(ctx, realDataLocalCorrelatedJoinQueryTemplate, nil)
		cancel()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		res, err := exec.Execute(ctx, realDataLocalCorrelatedJoinQueryTemplate, nil)
		cancel()
		if err != nil {
			b.Fatalf("execute failed at iter %d: %v", i, err)
		}
		if res == nil || len(res.Columns) == 0 {
			b.Fatalf("unexpected empty result metadata at iter %d", i)
		}
	}
}

// BenchmarkRealData_VectorUnionPrompt_CacheMiss runs the exact live query shape
// reported as slow:
// MATCH (p:SystemPrompt {promptId:"prompt-id"}) ... CALL { arm1 UNION ALL arm2(vector+traversal) } ...
func BenchmarkRealData_VectorUnionPrompt_CacheMiss(b *testing.B) {
	exec, cleanup := openRealDataBenchmarkExecutor(b, "caremark_translation")
	defer cleanup()
	exec.SetEmbedder(staticBenchmarkEmbedder{vec: []float32{1.0, 0.0, 0.0}})

	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _ = exec.Execute(ctx, realDataVectorUnionPromptQueryTemplate, nil)
		cancel()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		res, err := exec.Execute(ctx, realDataVectorUnionPromptQueryTemplate, nil)
		cancel()
		if err != nil {
			b.Fatalf("execute failed at iter %d: %v", i, err)
		}
		if res == nil || len(res.Columns) == 0 {
			b.Fatalf("unexpected empty result metadata at iter %d", i)
		}
	}
}
