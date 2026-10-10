package cypher

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type blockingNodeIteratorEngine struct {
	storage.Engine
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

func (e *blockingNodeIteratorEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.enteredOnce.Do(func() { close(e.entered) })
	<-e.release
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !fn(node) {
			break
		}
	}
	return nil
}

type projectedCountingEngine struct {
	storage.Engine
	projectedCalls int
	getNodeCalls   int
	projectedProps [][]string
}

func (e *projectedCountingEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.getNodeCalls++
	return e.Engine.GetNode(id)
}

func (e *projectedCountingEngine) GetNodeProjected(id storage.NodeID, properties []string) (*storage.Node, error) {
	e.projectedCalls++
	props := append([]string(nil), properties...)
	e.projectedProps = append(e.projectedProps, props)
	if reader, ok := e.Engine.(storage.ProjectedNodeReader); ok {
		return reader.GetNodeProjected(id, properties)
	}
	node, err := e.Engine.GetNode(id)
	if err != nil || node == nil {
		return node, err
	}
	out := storage.CopyNode(node)
	out.Properties = make(map[string]any, len(properties))
	for _, property := range properties {
		if value, ok := node.Properties[property]; ok {
			out.Properties[property] = value
		}
	}
	return out, nil
}

func TestMatchVectorCosineFastPath_UsesVectorIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	for i, q := range []string{
		"CREATE (:Item {uuid:'a', emb:[1.0,0.0,0.0]})",
		"CREATE (:Item {uuid:'b', emb:[0.0,1.0,0.0]})",
		"CREATE (:Item {uuid:'c', emb:[0.0,0.0,1.0]})",
	} {
		_, err = exec.Execute(ctx, q, nil)
		require.NoErrorf(t, err, "seed query %d failed", i)
	}

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT $k"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}, "k": 2})
	require.NoError(t, err)
	require.Equal(t, []string{"uuid", "score"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "a", res.Rows[0][0])

	trace := exec.LastHotPathTrace()
	require.True(t, trace.CosineVectorIndexFastPath)
}

func TestMatchVectorCosineFastPath_DoesNotCreateSearchServiceWhenUnwired(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	for i := 0; i < 12; i++ {
		vec := []float64{0, 0, 0, 0}
		vec[i%4] = 1
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'e-%02d', group_id:'g', name_embedding:%s})", i, formatInlineFloat64Vector(vec)), nil)
		require.NoError(t, err)
	}

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	params := map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"min_score":     -1.0,
		"limit":         5,
	}

	for i := 0; i < 5; i++ {
		res, err := exec.Execute(ctx, fmt.Sprintf("%s /* query_%d */", query, i), params)
		require.NoError(t, err)
		require.Len(t, res.Rows, 5)
		require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
		require.Nil(t, exec.searchService, "query-time vector cosine fast path must not allocate a throwaway search service")
	}
}

func TestMatchVectorCosineFastPath_DoesNotReplaceMismatchedSearchService(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	attached := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(attached)

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'e-1', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Entity) WITH n, vector.similarity.cosine(n.name_embedding, $q) AS score RETURN n.uuid AS uuid, score ORDER BY score DESC LIMIT 1"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1, 0, 0, 0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "e-1", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Same(t, attached, exec.searchService, "dimension mismatch should fall back to exact scoring, not replace the DB-owned search service")
}

func TestMatchVectorCosineFastPath_RequiresMatchingIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Item {uuid:'a', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT 1"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	trace := exec.LastHotPathTrace()
	require.False(t, trace.CosineVectorIndexFastPath, "must fall back when no matching vector index exists")
	require.Greater(t, counting.labelCalls, 0, "fallback path should use normal MATCH scanning")
}

func TestMatchWithVectorCosineProjection_PreWhereFilterUsesExactCandidateSet(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX evidence_emb_idx FOR (n:Evidence) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	for index := 0; index < 200; index++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Evidence {id:'other-%03d', asset_id:'other', embedding:[1.0,0.0,0.0]})", index), nil)
		require.NoError(t, err)
	}
	for index := 0; index < 8; index++ {
		score := 0.1 + float64(index)/100
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Evidence {id:'wanted-%03d', asset_id:'wanted', embedding:[%.2f,1.0,0.0]})", index, score), nil)
		require.NoError(t, err)
	}

	searchService := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(searchService)
	require.NoError(t, searchService.BuildIndexes(ctx))

	result, err := exec.Execute(ctx, `
MATCH (n:Evidence) WHERE n.asset_id = $asset
WITH n, vector.similarity.cosine(n.embedding, $q) AS score
RETURN n.id AS id, score
ORDER BY score DESC LIMIT 10`, map[string]interface{}{
		"asset": "wanted",
		"q":     []float64{1.0, 0.0, 0.0},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 8)
	for index, row := range result.Rows {
		require.Equal(t, fmt.Sprintf("wanted-%03d", 7-index), row[0])
	}

	directResult, err := exec.Execute(ctx, `
MATCH (n:Evidence) WHERE n.asset_id = $asset
RETURN n.id AS id, vector.similarity.cosine(n.embedding, $q) AS score
ORDER BY score DESC LIMIT 10`, map[string]interface{}{
		"asset": "wanted",
		"q":     []float64{1.0, 0.0, 0.0},
	})
	require.NoError(t, err)
	require.Len(t, directResult.Rows, 8)
	for index, row := range directResult.Rows {
		require.Equal(t, fmt.Sprintf("wanted-%03d", 7-index), row[0])
	}
}

func TestMatchVectorCosineFastPath_FilteredDirectAndInlinePatternsApplyTopK(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX limit_probe_idx FOR (n:LimitProbe) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	for index := 1; index <= 8; index++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:LimitProbe {id:'p%d', group:'selected', embedding:[%.1f,%.1f]})", index, float64(index)/10, 1-float64(index)/10), nil)
		require.NoError(t, err)
	}
	searchService := search.NewServiceWithDimensions(ns, 2)
	exec.SetSearchService(searchService)
	require.NoError(t, searchService.BuildIndexes(ctx))

	params := map[string]interface{}{"g": "selected", "v": []float64{0.95, 0.05}}
	for _, query := range []string{
		"MATCH (n:LimitProbe) WHERE n.group=$g RETURN n.id AS id, vector.similarity.cosine(n.embedding,$v) AS score ORDER BY score DESC LIMIT 3",
		"MATCH (n:LimitProbe {group:$g}) RETURN n.id AS id, vector.similarity.cosine(n.embedding,$v) AS score ORDER BY score DESC LIMIT 3",
		"MATCH (n:LimitProbe) WHERE n.group=$g WITH n, vector.similarity.cosine(n.embedding,$v) AS score ORDER BY score DESC LIMIT 3 RETURN n.id AS id, score",
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
		require.Len(t, result.Rows, 3)
		require.Equal(t, "p8", result.Rows[0][0])
		require.Equal(t, "p7", result.Rows[1][0])
		require.Equal(t, "p6", result.Rows[2][0])
	}
}

func BenchmarkMatchWithVectorCosineProjection_PreWhereFilterExactCandidates(b *testing.B) {
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX evidence_emb_idx FOR (n:Evidence) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)
	for index := 0; index < 2_000; index++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Evidence {id:'other-%04d', asset_id:'other', embedding:[1.0,0.0,0.0]})", index), nil)
		require.NoError(b, err)
	}
	for index := 0; index < 8; index++ {
		score := 0.1 + float64(index)/100
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Evidence {id:'wanted-%03d', asset_id:'wanted', embedding:[%.2f,1.0,0.0]})", index, score), nil)
		require.NoError(b, err)
	}
	searchService := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(searchService)
	require.NoError(b, searchService.BuildIndexes(ctx))

	query := `
MATCH (n:Evidence) WHERE n.asset_id = $asset
WITH n, vector.similarity.cosine(n.embedding, $q) AS score
RETURN n.id AS id, score
ORDER BY score DESC LIMIT 10`
	params := map[string]interface{}{"asset": "wanted", "q": []float64{1.0, 0.0, 0.0}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(b, err)
		if len(result.Rows) != 8 {
			b.Fatalf("got %d rows, want 8", len(result.Rows))
		}
	}
}

func TestMatchWithVectorCosineProjection_NoIndex_UsesExactFastPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	const dim = 1024
	for i := 0; i < 56; i++ {
		vec := make([]float64, dim)
		vec[i%dim] = 1.0
		if i == 1 {
			vec[0] = 0.8
			vec[1] = 0.6
		}
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (:Chunk {uuid:'chunk-%02d', group_id:'gid-1', emb:%s, text:'chunk text %02d'})", i, formatInlineFloat64Vector(vec), i), nil)
		require.NoError(t, err)
	}

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	res, err := exec.Execute(ctx, `
MATCH (c:Chunk) WHERE c.group_id = $gid
WITH c, vector.similarity.cosine(c.emb, $q) AS sim
RETURN c.text, c.uuid, sim ORDER BY sim DESC LIMIT $limit
`, map[string]interface{}{
		"gid":   "gid-1",
		"q":     unitVectorF64(0, dim),
		"limit": 5,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 5)
	require.Equal(t, "chunk-00", res.Rows[0][1])
	require.Equal(t, "chunk-01", res.Rows[1][1])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "strict chunk cosine projection should use the dedicated exact fast path even without a vector index")
}

func TestMatchVectorCosineFastPath_WriteThenSearchLoop_NoScanRegression(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'seed', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	require.Equal(t, 1, searchSvc.CountPropertyVectorEntries("emb"))
	require.NoError(t, searchSvc.BuildIndexes(ctx))
	require.True(t, searchSvc.IsReady())

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT $k"
	params := map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}, "k": 3}

	// Warm the owned vector query pipeline once, then assert write+search loops stay off scan paths.
	_, err = exec.Execute(ctx, query+" /* warmup */", params)
	require.NoError(t, err)
	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	for i := 0; i < 8; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Item {uuid:'new-%d', emb:[1.0,0.0,0.0]})", i), nil)
		require.NoError(t, err)

		res, err := exec.Execute(ctx, fmt.Sprintf("%s /* loop_%d */", query, i), params)
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)

		trace := exec.LastHotPathTrace()
		require.True(t, trace.CosineVectorIndexFastPath, "loop %d should stay on vector-index fast path", i)
	}

	require.Equal(t, 0, counting.allNodesCalls, "write-then-search loop should not regress to full node scans")
	require.Equal(t, 0, counting.labelCalls, "write-then-search loop should not regress to label scans")
	require.Equal(t, 0, counting.streamNodesCalls, "write-then-search loop should not use stream-scan fallback")
}

func TestMatchVectorCosineFastPath_LazyWiredServiceUsesLiveIndexWithoutWarmup(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 4)
	var warmCalls int32
	searchSvc.SetLazyWarming(true, search.WarmFunc(func() {
		atomic.AddInt32(&warmCalls, 1)
		_ = searchSvc.BuildIndexes(context.Background())
	}))
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'best', group_id:'g', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'other', group_id:'g', name_embedding:[0.0,1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	require.False(t, searchSvc.IsReady(), "live-indexed vector properties must not be reported as full warmup readiness")
	require.Equal(t, 2, searchSvc.CountPropertyVectorEntries("name_embedding"), "live-indexed vector properties make indexed queries service-backed before full warmup")

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"limit":         1,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "best", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.False(t, searchSvc.IsReady(), "scalar cosine fast path should not force full warmup when live indexed vectors can answer")
	require.Equal(t, int32(0), atomic.LoadInt32(&warmCalls), "Graphiti-style scalar cosine reads must not trigger lazy BuildIndexes/k-means warmup")
	require.Equal(t, 0, counting.allNodesCalls)
	require.Equal(t, 0, counting.labelCalls)
	require.Equal(t, 0, counting.streamNodesCalls)
}

func TestMatchVectorCosineFastPath_FileBackedPropertyVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")

	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'best', group_id:'g', name:'FalkorDB', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'other', group_id:'g', name:'NornicDB', name_embedding:[0.0,1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 4)
	searchSvc.SetPersistenceEnabled(true)
	t.Cleanup(func() { searchSvc.SetPersistenceEnabled(false) })
	searchSvc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	require.NoError(t, searchSvc.BuildIndexes(ctx))
	require.True(t, searchSvc.IsReady())
	require.Equal(t, 2, searchSvc.CountPropertyVectorEntries("name_embedding"))
	require.Equal(t, 2, searchSvc.EmbeddingCount())
	exec.SetSearchService(searchSvc)

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"limit":         1,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "best", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Equal(t, 0, counting.allNodesCalls)
	require.Equal(t, 0, counting.labelCalls)
	require.Equal(t, 0, counting.streamNodesCalls)
}

func TestMatchVectorCosineFastPath_ProjectedNodeReadSkipsVectorProperty(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	projected := &projectedCountingEngine{Engine: ns}
	exec := NewStorageExecutor(projected)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(projected, 4)
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'best', group_id:'g', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'other', group_id:'g', name_embedding:[0.0,1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	require.Equal(t, 2, searchSvc.CountPropertyVectorEntries("name_embedding"))

	projected.projectedCalls = 0
	projected.getNodeCalls = 0
	projected.projectedProps = nil

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"limit":         1,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "best", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Positive(t, projected.projectedCalls)
	require.Zero(t, projected.getNodeCalls)
	require.NotEmpty(t, projected.projectedProps)
	require.ElementsMatch(t, []string{"group_id", "uuid"}, projected.projectedProps[0])
	require.NotContains(t, projected.projectedProps[0], "name_embedding")

	projected.projectedCalls = 0
	projected.getNodeCalls = 0
	projected.projectedProps = nil

	query = `
MATCH (n:Entity)
WHERE n.name_embedding IS NOT NULL
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err = exec.Execute(ctx, query, map[string]interface{}{
		"search_vector": []float64{1, 0, 0, 0},
		"limit":         1,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "best", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Positive(t, projected.projectedCalls)
	require.Zero(t, projected.getNodeCalls)
	require.NotEmpty(t, projected.projectedProps)
	require.ElementsMatch(t, []string{"uuid"}, projected.projectedProps[0])
	require.NotContains(t, projected.projectedProps[0], "name_embedding")
}

func TestMatchVectorCosineFastPath_UsesLiveIndexDuringBackgroundWarmup(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	blocking := &blockingNodeIteratorEngine{
		Engine:  counting,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(blocking, 4)
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'best', group_id:'g', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'other', group_id:'g', name_embedding:[0.0,1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	require.Equal(t, 2, searchSvc.CountPropertyVectorEntries("name_embedding"))

	buildErr := make(chan error, 1)
	go func() {
		buildErr <- searchSvc.BuildIndexes(context.Background())
	}()
	releaseBuild := func() {
		select {
		case <-blocking.release:
		default:
			close(blocking.release)
		}
	}
	defer releaseBuild()
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for BuildIndexes to enter node iteration")
	}
	require.True(t, searchSvc.BuildInProgress())

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"limit":         1,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "best", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Equal(t, 0, counting.allNodesCalls, "background warmup must not force exact full-scan fallback")
	require.Equal(t, 0, counting.labelCalls, "background warmup must not force exact label-scan fallback")
	require.Equal(t, 0, counting.streamNodesCalls, "background warmup must not force stream-scan fallback")

	releaseBuild()
	select {
	case err := <-buildErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for BuildIndexes to finish")
	}
}

func TestMatchVectorCosineFastPath_ExternalEmbeddingsUseLiveIndexBeforeReady(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 4)
	exec.SetSearchService(searchSvc)

	for i := 0; i < 16; i++ {
		vec := []float64{0, 0, 0, 0}
		vec[i%4] = 1
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'e-%02d', group_id:'g', name_embedding:%s})", i, formatInlineFloat64Vector(vec)), nil)
		require.NoError(t, err)
	}
	require.Equal(t, 16, searchSvc.CountPropertyVectorEntries("name_embedding"))
	require.False(t, searchSvc.IsReady(), "live-indexed vector properties must not be reported as full warmup readiness")

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"min_score":     -1.0,
		"limit":         5,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 5)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Equal(t, 0, counting.allNodesCalls, "live-index query must not full-scan storage before full warmup")
	require.Equal(t, 0, counting.labelCalls, "live-index query must not label-scan storage before full warmup")
	require.Equal(t, 0, counting.streamNodesCalls, "live-index query must not stream-scan storage before full warmup")
}

func TestMatchVectorCosineFastPath_SetNodeVectorPropertyAfterWarmupIsSearchable(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX sn_emb_idx FOR (n:SN) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:SN {uuid:'seed0'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:SN {uuid:'seed0'}) WITH n CALL db.create.setNodeVectorProperty(n,'emb',[0.0,0.0,1.0,0.0]) RETURN n", nil)
	require.NoError(t, err)

	rankQuery := `
MATCH (n:SN)
WHERE n.emb IS NOT NULL
WITH n, vector.similarity.cosine(n.emb, $q) AS s
RETURN n.uuid AS u, s
ORDER BY s DESC
LIMIT 1`
	res, err := exec.Execute(ctx, rankQuery, map[string]interface{}{"q": []float64{0.0, 0.0, 1.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "seed0", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)

	_, err = exec.Execute(ctx, "CREATE (:SN {uuid:'sentinel'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:SN {uuid:'sentinel'}) WITH n CALL db.create.setNodeVectorProperty(n,'emb',[1.0,0.0,0.0,0.0]) RETURN n", nil)
	require.NoError(t, err)

	direct, err := exec.Execute(ctx, "MATCH (n:SN {uuid:'sentinel'}) RETURN vector.similarity.cosine(n.emb,[1.0,0.0,0.0,0.0]) AS s", nil)
	require.NoError(t, err)
	require.Len(t, direct.Rows, 1)
	require.EqualValues(t, 1.0, direct.Rows[0][0])

	res, err = exec.Execute(ctx, rankQuery, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "sentinel", res.Rows[0][0])
	require.EqualValues(t, 1.0, res.Rows[0][1])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
}

func TestTryFastPathMatchVectorCosine_HandlesAscendingOrder(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'best', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'worst', emb:[-1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	result, handled := exec.tryFastPathMatchVectorCosine(
		ctx,
		"MATCH (n:Item) RETURN vector.similarity.cosine(n.emb, [1.0,0.0,0.0]) AS score ORDER BY score ASC LIMIT 5",
		"MATCH (N:ITEM) RETURN VECTOR.SIMILARITY.COSINE(N.EMB, [1.0,0.0,0.0]) AS SCORE ORDER BY SCORE ASC LIMIT 5",
	)
	require.True(t, handled)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 2)
	// Neo4j's cosine scores: opposite vectors 0, the same direction 1.
	require.EqualValues(t, 0.0, result.Rows[0][0])
	require.EqualValues(t, 1.0, result.Rows[1][0])
}

func TestMatchVectorCosineFastPath_AscendingOrderWithParamVector(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'a', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'b', emb:[0.0,1.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'c', emb:[-1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score ASC LIMIT 2"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "c", res.Rows[0][0])
	require.EqualValues(t, 0.0, res.Rows[0][1], "opposite: Neo4j's cosine score 0")
	require.Equal(t, "b", res.Rows[1][0])
	require.EqualValues(t, 0.5, res.Rows[1][1], "orthogonal: Neo4j's cosine score 0.5")
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
}

func TestMatchVectorCosineFastPath_WithProjectionShape_UsesVectorIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX chunk_emb_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	for _, q := range []string{
		"CREATE (:Chunk {uuid:'c1', emb:[1.0,0.0,0.0], group_id:'g'})",
		"CREATE (:Chunk {uuid:'c2', emb:[0.9,0.1,0.0], group_id:'g'})",
		"CREATE (:Chunk {uuid:'c3', emb:[0.0,1.0,0.0], group_id:'g'})",
	} {
		_, err = exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	query := `MATCH (c:Chunk)
WITH c, vector.similarity.cosine(c.emb, $q) AS score
WHERE score > $min
RETURN c.uuid AS uuid, score
ORDER BY score DESC
LIMIT $k`
	_, err = exec.Execute(ctx, query+" /* warmup */", map[string]interface{}{
		"q":   []float64{1.0, 0.0, 0.0},
		"min": 0.6, // Neo4j's cosine score: c3, orthogonal, is 0.5
		"k":   5,
	})
	require.NoError(t, err)
	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"q":   []float64{1.0, 0.0, 0.0},
		"min": 0.6, // Neo4j's cosine score: c3, orthogonal, is 0.5
		"k":   5,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "c1", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Equal(t, 0, counting.allNodesCalls)
}

func TestMatchVectorCosineFastPath_WithProjection_WriteThenSearchLoop_NoScanRegression(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX chunk_emb_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Chunk {uuid:'seed', emb:[1.0,0.0,0.0], group_id:'g'})", nil)
	require.NoError(t, err)
	require.Equal(t, 1, searchSvc.CountPropertyVectorEntries("emb"))
	require.NoError(t, searchSvc.BuildIndexes(ctx))
	require.True(t, searchSvc.IsReady())

	query := `MATCH (c:Chunk)
WITH c, vector.similarity.cosine(c.emb, $q) AS score
WHERE score > $min
RETURN c.uuid AS uuid, score
ORDER BY score DESC
LIMIT $k`
	params := map[string]interface{}{
		"q":   []float64{1.0, 0.0, 0.0},
		"min": -1.0,
		"k":   3,
	}

	_, err = exec.Execute(ctx, query+" /* warmup */", params)
	require.NoError(t, err)
	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	for i := 0; i < 6; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Chunk {uuid:'n-%d', emb:[1.0,0.0,0.0], group_id:'g'})", i), nil)
		require.NoError(t, err)

		res, err := exec.Execute(ctx, fmt.Sprintf("%s /* projection_loop_%d */", query, i), params)
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)
		require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	}

	require.Equal(t, 0, counting.allNodesCalls)
}

func TestFindCosineVectorIndexName_DisambiguatesByEntityType(t *testing.T) {
	schema := storage.NewSchemaManager()
	require.NoError(t, schema.AddVectorIndexForEntity("a_rel_idx", "Thing", "emb", 3, "cosine", storage.ConstraintEntityRelationship))
	require.NoError(t, schema.AddVectorIndexForEntity("z_node_idx", "Thing", "emb", 3, "cosine", storage.ConstraintEntityNode))

	nodeIndex, ok := findCosineVectorIndexName(schema, "Thing", "emb", storage.ConstraintEntityNode)
	require.True(t, ok)
	require.Equal(t, "z_node_idx", nodeIndex)

	relIndex, ok := findCosineVectorIndexName(schema, "Thing", "emb", storage.ConstraintEntityRelationship)
	require.True(t, ok)
	require.Equal(t, "a_rel_idx", relIndex)
}
