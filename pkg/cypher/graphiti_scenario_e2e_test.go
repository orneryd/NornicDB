package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	graphitiGroupID = "graphiti-bench-g1"
)

const graphitiBulkNodeSaveQuery = `
UNWIND $nodes AS node
MERGE (n:Entity {uuid: node.uuid})
SET n:$(node.labels)
SET n = node
WITH n, node CALL db.create.setNodeVectorProperty(n, "name_embedding", node.name_embedding)
RETURN n.uuid AS uuid
`

const graphitiBulkEdgeSaveQuery = `
UNWIND $entity_edges AS edge
MATCH (source:Entity {uuid: edge.source_node_uuid})
MATCH (target:Entity {uuid: edge.target_node_uuid})
MERGE (source)-[e:RELATES_TO {uuid: edge.uuid}]->(target)
SET e = edge
WITH e, edge CALL db.create.setRelationshipVectorProperty(e, "fact_embedding", edge.fact_embedding)
RETURN edge.uuid AS uuid
`

const graphitiBulkChunkSaveQuery = `
MATCH (x:Entity {uuid:$anchor})
UNWIND $chunks AS c
CREATE (ch:Chunk {
  uuid:c.uuid,
  group_id:c.group_id,
  emb:c.emb,
  text:c.text,
  tokens:c.tokens
})
RETURN ch.uuid AS uuid
`

const graphitiNodeSimilarityQuery = `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`

const graphitiEdgeSimilarityQuery = `
MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
WITH DISTINCT e, n, m, vector.similarity.cosine(e.fact_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN e.uuid AS uuid, properties(e) AS attributes, score
ORDER BY score DESC
LIMIT $limit
`

const graphitiChunkSimilarityQuery = `
MATCH (c:Chunk)
WHERE c.group_id = $group_id
WITH c, vector.similarity.cosine(c.emb, $search_vector) AS score
RETURN c.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`

type graphitiScenarioPayload struct {
	nodes  []map[string]interface{}
	edges  []map[string]interface{}
	chunks []map[string]interface{}
}

type graphitiCopyCountingEngine struct {
	storage.Engine
	nodeUpdates int64
	edgeUpdates int64
	nodeGets    int64
	edgeGets    int64
}

func (e *graphitiCopyCountingEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	atomic.AddInt64(&e.nodeGets, 1)
	return e.Engine.GetNode(id)
}

func (e *graphitiCopyCountingEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	atomic.AddInt64(&e.edgeGets, 1)
	return e.Engine.GetEdge(id)
}

func (e *graphitiCopyCountingEngine) UpdateNode(node *storage.Node) error {
	atomic.AddInt64(&e.nodeUpdates, 1)
	return e.Engine.UpdateNode(node)
}

func (e *graphitiCopyCountingEngine) UpdateEdge(edge *storage.Edge) error {
	atomic.AddInt64(&e.edgeUpdates, 1)
	return e.Engine.UpdateEdge(edge)
}

func (e *graphitiCopyCountingEngine) NodeUpdateCount() int64 {
	return atomic.LoadInt64(&e.nodeUpdates)
}

func (e *graphitiCopyCountingEngine) EdgeUpdateCount() int64 {
	return atomic.LoadInt64(&e.edgeUpdates)
}

func (e *graphitiCopyCountingEngine) NodeGetCount() int64 {
	return atomic.LoadInt64(&e.nodeGets)
}

func (e *graphitiCopyCountingEngine) EdgeGetCount() int64 {
	return atomic.LoadInt64(&e.edgeGets)
}

func TestGraphitiScenarioE2E_LargePayloads(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	dim := 32
	entityCount := 320
	edgeCount := 960
	chunkCount := 960
	if testing.Short() {
		entityCount = 80
		edgeCount = 160
		chunkCount = 160
	}

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX chunk_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX ent_ft IF NOT EXISTS FOR (n:Entity) ON EACH [n.name, n.summary]", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(t, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)

	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.nodes))

	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.edges))
	require.True(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch, "bulk relationship ingest should use the generic relationship merge batch fast path")

	res, err = exec.Execute(ctx, graphitiBulkChunkSaveQuery, map[string]interface{}{
		"anchor": "entity-000000",
		"chunks": payload.chunks,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.chunks))

	countEntities := mustCountRows(t, exec, ctx, "MATCH (n:Entity {group_id: $g}) RETURN count(n)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(entityCount), countEntities)
	countEdges := mustCountRows(t, exec, ctx, "MATCH (:Entity)-[e:RELATES_TO]->(:Entity) WHERE e.group_id = $g RETURN count(e)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(edgeCount), countEdges)
	countChunks := mustCountRows(t, exec, ctx, "MATCH (c:Chunk {group_id: $g}) RETURN count(c)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(chunkCount), countChunks)

	res, err = exec.Execute(ctx, graphitiNodeSimilarityQuery, map[string]interface{}{
		"group_id":      graphitiGroupID,
		"search_vector": unitVectorF64(0, dim),
		"min_score":     -1.0,
		"limit":         25,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "node similarity should route through vector-index fast path")
	assertNonIncreasingScores(t, res.Rows, 1)

	res, err = exec.Execute(ctx, graphitiEdgeSimilarityQuery, map[string]interface{}{
		"search_vector": unitVectorF64(0, dim),
		"min_score":     -1.0,
		"limit":         25,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "edge similarity should route through vector-index fast path")
	assertNonIncreasingScores(t, res.Rows, 2)

	res, err = exec.Execute(ctx, graphitiChunkSimilarityQuery, map[string]interface{}{
		"group_id":      graphitiGroupID,
		"search_vector": unitVectorF64(0, dim),
		"limit":         25,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "chunk similarity should route through vector-index fast path")
	assertNonIncreasingScores(t, res.Rows, 1)

	// Neo4j compatibility: 3-arg fulltext query options map.
	res, err = exec.Execute(ctx, `CALL db.index.fulltext.queryNodes("ent_ft", "entity", {limit: 10, skip: 2}) YIELD node, score RETURN node.uuid AS uuid, score`, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(res.Rows), 10)

	res, err = exec.Execute(ctx, `CALL db.index.fulltext.queryRelationships("rel_ft", "relates", {limit: 10, skip: 1}) YIELD relationship, score RETURN relationship.uuid AS uuid, score`, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(res.Rows), 10)
}

func TestGraphitiScenarioE2E_VerbatimCopySkipsRedundantVectorPropertyUpdates(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &graphitiCopyCountingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	const dim = 16
	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 16, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 16, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(counting, dim)
	exec.SetSearchService(searchSvc)

	payload := buildGraphitiScenarioPayload(12, 24, 0, dim)
	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.nodes))
	require.Equal(t, int64(len(payload.nodes)), counting.NodeUpdateCount(), "node copy should only pay the SET n = row update, not an extra vector setter update")
	require.Equal(t, int64(3*len(payload.nodes)), counting.NodeGetCount(), "shared CREATE and SET notifications each index the node; the vector setter reads once without updating")
	require.Equal(t, len(payload.nodes), searchSvc.CountPropertyVectorEntries("name_embedding"))

	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.edges))
	require.Equal(t, int64(0), counting.EdgeUpdateCount(), "setRelationshipVectorProperty must no-op when SET e = row already stored the same vector")
	require.True(t, searchSvc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))
}

func TestGraphitiScenarioE2E_RelationshipSetWithLimitBoundsMVCCCommitState(t *testing.T) {
	if testing.Short() {
		t.Skip("large-vector MVCC regression")
	}

	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	const (
		nodeCount      = 100
		edgeCount      = 3000
		dim            = 1024
		edgeBatchSize  = 100
		oldGroupID     = "old"
		updatedGroupID = "new"
	)

	_, err = exec.Execute(ctx, "CREATE INDEX t_uuid_idx IF NOT EXISTS FOR (n:T) ON (n.uuid)", nil)
	require.NoError(t, err)

	nodes := make([]map[string]interface{}, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, map[string]interface{}{
			"uuid": fmt.Sprintf("t%d", i),
		})
	}
	_, err = exec.Execute(ctx, "UNWIND $nodes AS node CREATE (:T {uuid: node.uuid})", map[string]interface{}{"nodes": nodes})
	require.NoError(t, err)

	vec := deterministicVectorF64(42, dim)
	for start := 0; start < edgeCount; start += edgeBatchSize {
		end := start + edgeBatchSize
		if end > edgeCount {
			end = edgeCount
		}
		edges := make([]map[string]interface{}, 0, end-start)
		for i := start; i < end; i++ {
			edges = append(edges, map[string]interface{}{
				"uuid":           fmt.Sprintf("r%d", i),
				"source_uuid":    fmt.Sprintf("t%d", i%nodeCount),
				"target_uuid":    fmt.Sprintf("t%d", (i+1)%nodeCount),
				"group_id":       oldGroupID,
				"fact_embedding": vec,
			})
		}
		_, err = exec.Execute(ctx, `
UNWIND $edges AS edge
MATCH (source:T {uuid: edge.source_uuid})
MATCH (target:T {uuid: edge.target_uuid})
CREATE (source)-[:REL {uuid: edge.uuid, group_id: edge.group_id, fact_embedding: edge.fact_embedding}]->(target)
`, map[string]interface{}{"edges": edges})
		require.NoError(t, err)
	}
	require.Equal(t, int64(edgeCount), mustCountRows(t, exec, ctx, "MATCH ()-[r:REL]->() WHERE r.group_id='old' RETURN count(r)", nil))

	nodeRes, err := exec.Execute(ctx, "MATCH (n:T) WITH n LIMIT 1 SET n.tag='x' RETURN n.tag", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x"}}, nodeRes.Rows)

	preWriteRows, err := exec.Execute(ctx, "MATCH ()-[r:REL]->() WHERE r.group_id='old' WITH r LIMIT 1 RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, preWriteRows.Rows, 1)

	relRes, err := exec.Execute(ctx, `
MATCH ()-[r:REL]->() WHERE r.group_id='old' WITH r LIMIT 1
SET r.group_id='new' RETURN 1
`, nil)
	require.NoError(t, err)
	require.Len(t, relRes.Rows, 1)
	require.Equal(t, int64(1), mustCountRows(t, exec, ctx, "MATCH ()-[r:REL]->() WHERE r.group_id='new' RETURN count(r)", nil))
	require.Equal(t, int64(edgeCount-1), mustCountRows(t, exec, ctx, "MATCH ()-[r:REL]->() WHERE r.group_id='old' RETURN count(r)", nil))
}

func TestGraphitiScenarioE2E_BulkEdgeSaveIndexedRepeatedEntityMatches(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX entity_uuid_idx IF NOT EXISTS FOR (n:Entity) ON (n.uuid)", nil)
	require.NoError(t, err)

	nodes := []map[string]interface{}{
		{"uuid": "source-1", "labels": []string{"Entity"}, "name_embedding": []float64{1, 0, 0}},
		{"uuid": "source-2", "labels": []string{"Entity"}, "name_embedding": []float64{0, 1, 0}},
		{"uuid": "target-1", "labels": []string{"Entity"}, "name_embedding": []float64{0, 0, 1}},
		{"uuid": "target-2", "labels": []string{"Entity"}, "name_embedding": []float64{1, 1, 0}},
	}
	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(nodes))

	edges := []map[string]interface{}{
		{"uuid": "edge-1", "source_node_uuid": "source-1", "target_node_uuid": "target-1", "group_id": graphitiGroupID, "fact": "source one relates to target one", "fact_embedding": []float64{1, 0, 0}},
		{"uuid": "edge-2", "source_node_uuid": "source-2", "target_node_uuid": "target-2", "group_id": graphitiGroupID, "fact": "source two relates to target two", "fact_embedding": []float64{0, 1, 0}},
	}
	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(edges))
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindRelationshipMergeBatch)
	require.True(t, trace.UnwindMergeChainBatch)

	countEdges := mustCountRows(t, exec, ctx, "MATCH (:Entity)-[e:RELATES_TO]->(:Entity) WHERE e.group_id = $g RETURN count(e)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(len(edges)), countEdges)
}

func TestGraphitiScenarioE2E_BulkEdgeSaveIndexedEntityMatchesSurviveIncompleteIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX entity_uuid_idx IF NOT EXISTS FOR (n:Entity) ON (n.uuid)", nil)
	require.NoError(t, err)

	payload := buildGraphitiScenarioPayload(24, 64, 0, 8)
	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.nodes))

	// Reproduce the stale-substrate shape: the logical nodes exist and are
	// visible by label scan, but the declared property index is missing a
	// subset of uuid entries. Relationship MATCH must use the index only as
	// an accelerator, not as proof that those endpoint nodes are absent.
	schema := ns.GetSchema()
	require.NotNil(t, schema)
	nodes, err := ns.GetNodesByLabel("Entity")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
	removed := 0
	for _, node := range nodes {
		uuid, _ := node.Properties["uuid"].(string)
		if uuid == "" {
			continue
		}
		if uuid == "entity-000003" || uuid == "entity-000007" || uuid == "entity-000011" || uuid == "entity-000017" {
			ids := schema.PropertyIndexLookup("Entity", "uuid", uuid)
			require.NotEmpty(t, ids)
			for _, id := range ids {
				require.NoError(t, schema.PropertyIndexDelete("Entity", "uuid", id, uuid))
			}
			removed++
		}
	}
	require.Equal(t, 4, removed)
	require.Empty(t, schema.PropertyIndexLookup("Entity", "uuid", "entity-000003"))

	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.edges))
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindRelationshipMergeBatch)
	require.True(t, trace.UnwindMergeChainBatch)

	countEdges := mustCountRows(t, exec, ctx, "MATCH (:Entity)-[e:RELATES_TO]->(:Entity) WHERE e.group_id = $g RETURN count(e)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(len(payload.edges)), countEdges)
}

func TestGraphitiScenarioE2E_ExternalVectorIngestDoesNotUseExactScanFallbackBeforeWarmup(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	const dim = 1024
	const episodes = 4
	const nodesPerEpisode = 10
	const edgesPerEpisode = 16
	const searchesPerEpisode = 3

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, dim)
	exec.SetSearchService(searchSvc)

	for episode := 0; episode < episodes; episode++ {
		payload := buildGraphitiEpisodePayload(episode, nodesPerEpisode, edgesPerEpisode, dim)

		res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
		require.NoError(t, err)
		require.Len(t, res.Rows, len(payload.nodes))

		res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
		require.NoError(t, err)
		require.Len(t, res.Rows, len(payload.edges))
		require.True(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch, "episode %d bulk relationship ingest should use the generic relationship merge batch fast path", episode)

		require.False(t, searchSvc.IsReady(), "test must cover pre-warmup ingest, not fully built search readiness")
		require.Equal(t, (episode+1)*nodesPerEpisode, searchSvc.CountPropertyVectorEntries("name_embedding"))
		require.True(t, searchSvc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))

		counting.allNodesCalls = 0
		counting.allEdgesCalls = 0
		counting.labelCalls = 0
		counting.streamNodesCalls = 0

		for searchIdx := 0; searchIdx < searchesPerEpisode; searchIdx++ {
			queryVec := unitVectorF64(episode+searchIdx, dim)
			nodeQuery := fmt.Sprintf("%s\n/* graphiti_node_resolution_episode_%d_search_%d */", graphitiNodeSimilarityQuery, episode, searchIdx)
			nodeRes, err := exec.Execute(ctx, nodeQuery, map[string]interface{}{
				"group_id":      graphitiGroupID,
				"search_vector": queryVec,
				"min_score":     -1.0,
				"limit":         8,
			})
			require.NoError(t, err)
			require.NotEmpty(t, nodeRes.Rows)
			require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "episode %d node search %d should use vector-index fast path", episode, searchIdx)

			edgeQuery := fmt.Sprintf("%s\n/* graphiti_edge_fact_episode_%d_search_%d */", graphitiEdgeSimilarityQuery, episode, searchIdx)
			edgeRes, err := exec.Execute(ctx, edgeQuery, map[string]interface{}{
				"search_vector": queryVec,
				"min_score":     -1.0,
				"limit":         8,
			})
			require.NoError(t, err)
			require.NotEmpty(t, edgeRes.Rows)
		}

		require.Equal(t, 0, counting.allNodesCalls, "episode %d must not use node exact full-scan fallback", episode)
		require.Equal(t, 0, counting.allEdgesCalls, "episode %d must not use edge exact full-scan fallback", episode)
		require.Equal(t, 0, counting.labelCalls, "episode %d must not use node exact label-scan fallback", episode)
		require.Equal(t, 0, counting.streamNodesCalls, "episode %d must not stream-scan nodes for cosine resolution", episode)
	}
}

func TestGraphitiScenarioE2E_ChunkSimilarityWithoutVectorIndex_UsesExactFastPathDuringFreshIngest(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	const dim = 1024
	payload := buildGraphitiScenarioPayload(110, 127, 56, dim)

	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.nodes))

	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.edges))

	res, err = exec.Execute(ctx, graphitiBulkChunkSaveQuery, map[string]interface{}{
		"anchor": "entity-000000",
		"chunks": payload.chunks,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.chunks))

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	res, err = exec.Execute(ctx, graphitiChunkSimilarityQuery, map[string]interface{}{
		"group_id":      graphitiGroupID,
		"search_vector": unitVectorF64(0, dim),
		"limit":         10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "chunk recall should use the dedicated exact cosine fast path even when Chunk.emb has no vector index")
	assertNonIncreasingScores(t, res.Rows, 1)
}

func TestGraphitiScenarioE2E_RelationshipFulltextMultiTokenFacts(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'n-%02d'})", i), nil)
		require.NoError(t, err)
	}

	facts := []string{
		"NSQ topic migration regression requires an idempotent backfill worker",
		"Backfill replay found an NSQ consumer regression during migration validation",
		"The sftp_migration service publishes NSQ topic events for backfill progress",
		"Regression tests cover NSQ migration retries and delayed backfill batches",
		"NSQ queue lag increased when the migration backfill job retried facts",
		"Graphiti facts document the NSQ topic migration and backfill ordering",
		"Backfill checkpoints prevented migration regression for the NSQ consumer",
		"NSQ migration incident notes explain regression triage and backfill repair",
	}
	for i, fact := range facts {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:$a})
MATCH (b:Entity {uuid:$b})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:$name, fact:$fact}]->(b)
`, map[string]interface{}{
			"a":    fmt.Sprintf("n-%02d", i),
			"b":    fmt.Sprintf("n-%02d", i+1),
			"uuid": fmt.Sprintf("nsq-%02d", i),
			"name": "NSQ migration fact",
			"fact": fact,
		})
		require.NoError(t, err)
	}
	for i, fact := range []string{
		"OAuth token rotation completed without data replay",
		"Invoice import observed a timezone normalization warning",
		"Feature flag cleanup removed obsolete email experiments",
	} {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:'n-00'})
MATCH (b:Entity {uuid:'n-09'})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:'unrelated', fact:$fact}]->(b)
`, map[string]interface{}{
			"uuid": fmt.Sprintf("other-%02d", i),
			"fact": fact,
		})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
CALL db.index.fulltext.queryRelationships('rel_ft', 'NSQ migration regression and backfill') YIELD relationship, score
RETURN relationship.uuid AS uuid, score
ORDER BY score DESC
LIMIT 15
`, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(res.Rows), 5)
	for _, row := range res.Rows {
		require.Contains(t, row[0], "nsq-", "multi-token fact search should not return unrelated relationship %v", row[0])
	}

	short, err := exec.Execute(ctx, `
CALL db.index.fulltext.queryRelationships('rel_ft', 'sftp_migration NSQ topic') YIELD relationship, score
RETURN relationship.uuid AS uuid, score
ORDER BY score DESC
LIMIT 5
`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, short.Rows)
}

func TestGraphitiScenarioE2E_RelationshipFulltextTemporalTail_FreshIngestAndAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	const (
		entityCount = 110
		edgeCount   = 127
		chunkCount  = 56
		dim         = 1024
		limit       = 10
	)

	callOnlyQuery := `CALL db.index.fulltext.queryRelationships("edge_name_and_fact", $q, {limit: $limit})
YIELD relationship AS rel, score
RETURN rel.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit`

	tailNoTemporalQuery := `CALL db.index.fulltext.queryRelationships("edge_name_and_fact", $q, {limit: $limit})
YIELD relationship AS rel, score
MATCH (n:Entity)-[e:RELATES_TO {uuid: rel.uuid}]->(m:Entity)
WHERE e.group_id IN $g
WITH e, score, n, m
RETURN e.uuid AS uuid, properties(e) AS attributes, score
ORDER BY score DESC
LIMIT $limit`

	tailTemporalQuery := `CALL db.index.fulltext.queryRelationships("edge_name_and_fact", $q, {limit: $limit}) YIELD relationship AS rel, score
MATCH (n:Entity)-[e:RELATES_TO {uuid: rel.uuid}]->(m:Entity)
WHERE ((e.invalid_at IS NULL) OR (e.invalid_at > $dt)) AND e.group_id IN $g
WITH e, score, n, m
RETURN e.uuid AS uuid, properties(e) AS attributes, score
ORDER BY score DESC
LIMIT $limit`

	type phaseStats struct {
		callOnlyRows    [][]interface{}
		tailNoTempRows  [][]interface{}
		tailTempRows    [][]interface{}
		trace           HotPathTrace
		allNodesCalls   int
		allEdgesCalls   int
		labelCalls      int
		streamNodeCalls int
		callOnlyDur     time.Duration
		noTempDur       time.Duration
		tempDur         time.Duration
	}

	extractUUIDs := func(rows [][]interface{}) []string {
		uuids := make([]string, 0, len(rows))
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			uuid, ok := row[0].(string)
			require.True(t, ok)
			uuids = append(uuids, uuid)
		}
		return uuids
	}

	runPhase := func(base storage.Engine) phaseStats {
		ns := storage.NewNamespacedEngine(base, "test")
		counting := &countingStreamingEngine{Engine: ns}
		exec := NewStorageExecutor(counting)
		exec.cache = nil

		params := map[string]interface{}{
			"q":     "relates",
			"g":     []string{graphitiGroupID},
			"dt":    "2024-01-01T00:00:00Z",
			"limit": int64(limit),
		}

		start := time.Now()
		callOnlyRes, err := exec.Execute(ctx, callOnlyQuery, params)
		require.NoError(t, err)
		callOnlyDur := time.Since(start)
		require.Len(t, callOnlyRes.Rows, limit)

		start = time.Now()
		noTempRes, err := exec.Execute(ctx, tailNoTemporalQuery, params)
		require.NoError(t, err)
		noTempDur := time.Since(start)
		require.Len(t, noTempRes.Rows, limit)
		require.True(t, exec.LastHotPathTrace().CallTailProjectionFastPath)

		start = time.Now()
		tempRes, err := exec.Execute(ctx, tailTemporalQuery, params)
		require.NoError(t, err)
		tempDur := time.Since(start)
		require.NotEmpty(t, tempRes.Rows)
		require.LessOrEqual(t, len(tempRes.Rows), limit)

		return phaseStats{
			callOnlyRows:    callOnlyRes.Rows,
			tailNoTempRows:  noTempRes.Rows,
			tailTempRows:    tempRes.Rows,
			trace:           exec.LastHotPathTrace(),
			allNodesCalls:   counting.allNodesCalls,
			allEdgesCalls:   counting.allEdgesCalls,
			labelCalls:      counting.labelCalls,
			streamNodeCalls: counting.streamNodesCalls,
			callOnlyDur:     callOnlyDur,
			noTempDur:       noTempDur,
			tempDur:         tempDur,
		}
	}

	base1, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	store1 := storage.NewNamespacedEngine(base1, "test")
	seedExec := NewStorageExecutor(store1)
	seedExec.cache = nil

	_, err = seedExec.Execute(ctx, "CREATE FULLTEXT INDEX edge_name_and_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.name, e.fact, e.group_id]", nil)
	require.NoError(t, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)
	for i := range payload.edges {
		payload.edges[i]["created_at"] = "2024-01-01T00:00:00Z"
		payload.edges[i]["valid_at"] = "2024-01-01T00:00:00Z"
		payload.edges[i]["expired_at"] = nil
		payload.edges[i]["episodes"] = []string{fmt.Sprintf("episode-%02d", i%10)}
		payload.edges[i]["fact"] = fmt.Sprintf("entity-%06d relates entity-%06d across graphiti episode %02d", i%entityCount, (i+(i%7)+1)%entityCount, i%10)
		switch i % 3 {
		case 0:
			payload.edges[i]["invalid_at"] = nil
		case 1:
			payload.edges[i]["invalid_at"] = "2026-01-01T00:00:00Z"
		default:
			payload.edges[i]["invalid_at"] = "2023-01-01T00:00:00Z"
		}
	}

	res, err := seedExec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.nodes))
	res, err = seedExec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.edges))
	res, err = seedExec.Execute(ctx, graphitiBulkChunkSaveQuery, map[string]interface{}{"anchor": "entity-000000", "chunks": payload.chunks})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.chunks))

	freshFirst := runPhase(base1)
	freshRepeat := runPhase(base1)
	require.True(t, freshFirst.trace.CallTailProjectionFastPath)
	require.True(t, freshRepeat.trace.CallTailProjectionFastPath)
	require.Equal(t, extractUUIDs(freshFirst.callOnlyRows), extractUUIDs(freshRepeat.callOnlyRows))
	require.Equal(t, extractUUIDs(freshFirst.tailNoTempRows), extractUUIDs(freshRepeat.tailNoTempRows))
	require.Equal(t, extractUUIDs(freshFirst.tailTempRows), extractUUIDs(freshRepeat.tailTempRows))

	require.NoError(t, base1.Close())

	base2, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	defer base2.Close()

	restartFirst := runPhase(base2)
	restartRepeat := runPhase(base2)
	require.True(t, restartFirst.trace.CallTailProjectionFastPath)
	require.True(t, restartRepeat.trace.CallTailProjectionFastPath)

	require.Equal(t, extractUUIDs(freshFirst.callOnlyRows), extractUUIDs(restartFirst.callOnlyRows))
	require.Equal(t, extractUUIDs(freshFirst.tailNoTempRows), extractUUIDs(restartFirst.tailNoTempRows))
	require.Equal(t, extractUUIDs(freshFirst.tailTempRows), extractUUIDs(restartFirst.tailTempRows))
	require.Equal(t, extractUUIDs(restartFirst.callOnlyRows), extractUUIDs(restartRepeat.callOnlyRows))
	require.Equal(t, extractUUIDs(restartFirst.tailNoTempRows), extractUUIDs(restartRepeat.tailNoTempRows))
	require.Equal(t, extractUUIDs(restartFirst.tailTempRows), extractUUIDs(restartRepeat.tailTempRows))

	t.Logf("fresh ingest first: call-only=%s tail-no-invalid=%s tail-invalid=%s scans(nodes=%d edges=%d labels=%d streamNodes=%d)",
		freshFirst.callOnlyDur, freshFirst.noTempDur, freshFirst.tempDur,
		freshFirst.allNodesCalls, freshFirst.allEdgesCalls, freshFirst.labelCalls, freshFirst.streamNodeCalls)
	t.Logf("fresh ingest repeat: call-only=%s tail-no-invalid=%s tail-invalid=%s scans(nodes=%d edges=%d labels=%d streamNodes=%d)",
		freshRepeat.callOnlyDur, freshRepeat.noTempDur, freshRepeat.tempDur,
		freshRepeat.allNodesCalls, freshRepeat.allEdgesCalls, freshRepeat.labelCalls, freshRepeat.streamNodeCalls)
	t.Logf("restart first: call-only=%s tail-no-invalid=%s tail-invalid=%s scans(nodes=%d edges=%d labels=%d streamNodes=%d)",
		restartFirst.callOnlyDur, restartFirst.noTempDur, restartFirst.tempDur,
		restartFirst.allNodesCalls, restartFirst.allEdgesCalls, restartFirst.labelCalls, restartFirst.streamNodeCalls)
	t.Logf("restart repeat: call-only=%s tail-no-invalid=%s tail-invalid=%s scans(nodes=%d edges=%d labels=%d streamNodes=%d)",
		restartRepeat.callOnlyDur, restartRepeat.noTempDur, restartRepeat.tempDur,
		restartRepeat.allNodesCalls, restartRepeat.allEdgesCalls, restartRepeat.labelCalls, restartRepeat.streamNodeCalls)
}

func BenchmarkGraphitiRelationshipFulltextMultiTokenFacts(b *testing.B) {
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(b, err)

	for i := 0; i < 32; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'bench-n-%02d'})", i), nil)
		require.NoError(b, err)
	}

	relevantFacts := []string{
		"NSQ topic migration regression requires an idempotent backfill worker",
		"Backfill replay found an NSQ consumer regression during migration validation",
		"The sftp_migration service publishes NSQ topic events for backfill progress",
		"Regression tests cover NSQ migration retries and delayed backfill batches",
		"NSQ queue lag increased when the migration backfill job retried facts",
		"Graphiti facts document the NSQ topic migration and backfill ordering",
		"Backfill checkpoints prevented migration regression for the NSQ consumer",
		"NSQ migration incident notes explain regression triage and backfill repair",
	}
	unrelatedFacts := []string{
		"OAuth token rotation completed without data replay",
		"Invoice import observed a timezone normalization warning",
		"Feature flag cleanup removed obsolete email experiments",
		"Calendar sync retries converged after a provider throttle",
	}
	for i := 0; i < 128; i++ {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:$a})
MATCH (b:Entity {uuid:$b})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:$name, fact:$fact}]->(b)
`, map[string]interface{}{
			"a":    fmt.Sprintf("bench-n-%02d", i%31),
			"b":    fmt.Sprintf("bench-n-%02d", (i%31)+1),
			"uuid": fmt.Sprintf("bench-nsq-%03d", i),
			"name": "NSQ migration fact",
			"fact": relevantFacts[i%len(relevantFacts)],
		})
		require.NoError(b, err)
	}
	for i := 0; i < 128; i++ {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:$a})
MATCH (b:Entity {uuid:$b})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:'unrelated', fact:$fact}]->(b)
`, map[string]interface{}{
			"a":    fmt.Sprintf("bench-n-%02d", i%31),
			"b":    fmt.Sprintf("bench-n-%02d", (i%31)+1),
			"uuid": fmt.Sprintf("bench-other-%03d", i),
			"fact": unrelatedFacts[i%len(unrelatedFacts)],
		})
		require.NoError(b, err)
	}

	query := `
CALL db.index.fulltext.queryRelationships('rel_ft', 'NSQ migration regression and backfill') YIELD relationship, score
RETURN relationship.uuid AS uuid, score
ORDER BY score DESC
LIMIT 15
`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(b, err)
		require.GreaterOrEqual(b, len(res.Rows), 15)
	}
}

func BenchmarkGraphitiRelationshipFulltextCallTail(b *testing.B) {
	const (
		dim         = 32
		entityCount = 256
		edgeCount   = 1024
		limit       = 128
	)

	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(b, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, 0, dim)
	_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(b, err)
	_, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(b, err)
	edgeTotal, err := ns.EdgeCount()
	require.NoError(b, err)
	require.Equal(b, int64(edgeCount), edgeTotal)
	rawFulltext, err := exec.Execute(ctx, `CALL db.index.fulltext.queryRelationships("rel_ft", "*", {limit: $limit}) YIELD relationship AS rel, score RETURN rel.uuid AS uuid, score`, map[string]interface{}{"limit": limit})
	require.NoError(b, err)
	require.Len(b, rawFulltext.Rows, limit)

	query := `
CALL db.index.fulltext.queryRelationships("rel_ft", "*", {limit: $limit})
YIELD relationship AS rel, score
MATCH (n:Entity)-[e:RELATES_TO {uuid: rel.uuid}]->(m:Entity)
WHERE e.group_id IN $group_ids
WITH e, score, n, m
RETURN
	e.uuid AS uuid,
	n.uuid AS source_node_uuid,
	m.uuid AS target_node_uuid,
	e.group_id AS group_id,
	e.created_at AS created_at,
	e.name AS name,
	e.fact AS fact,
	e.episodes AS episodes,
	e.expired_at AS expired_at,
	e.valid_at AS valid_at,
	e.invalid_at AS invalid_at,
	properties(e) AS attributes
ORDER BY score DESC
LIMIT $limit
`
	params := map[string]interface{}{
		"group_ids": []string{graphitiGroupID},
		"limit":     limit,
	}
	res, err := exec.Execute(ctx, query, params)
	require.NoError(b, err)
	require.Len(b, res.Rows, limit)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err = exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != limit {
			b.Fatalf("expected %d rows, got %d", limit, len(res.Rows))
		}
	}
}

func BenchmarkGraphitiRelationshipFulltextCallTailProjectionFilter(b *testing.B) {
	const (
		dim         = 32
		entityCount = 256
		edgeCount   = 1024
		limit       = 128
	)

	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(b, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, 0, dim)
	_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(b, err)
	_, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(b, err)

	query := `
CALL db.index.fulltext.queryRelationships("rel_ft", "*", {limit: $limit})
YIELD relationship AS rel, score
WITH rel, score
WHERE score >= $min_score AND rel.uuid IS NOT NULL
RETURN
	rel.uuid AS uuid,
	rel.group_id AS group_id,
	rel.name AS name,
	rel.fact AS fact,
	rel.episodes AS episodes,
	rel.expired_at AS expired_at,
	rel.valid_at AS valid_at,
	rel.invalid_at AS invalid_at,
	properties(rel) AS attributes,
	score
ORDER BY score DESC
LIMIT $limit
`
	params := map[string]interface{}{
		"min_score": 0.0,
		"limit":     limit,
	}
	res, err := exec.Execute(ctx, query, params)
	require.NoError(b, err)
	require.Len(b, res.Rows, limit)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err = exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != limit {
			b.Fatalf("expected %d rows, got %d", limit, len(res.Rows))
		}
	}
}

func BenchmarkGraphitiScenarioHotspots(b *testing.B) {
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	dim := 32
	entityCount := 256
	edgeCount := 768
	chunkCount := 768

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX chunk_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)
	_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(b, err)
	_, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(b, err)
	_, err = exec.Execute(ctx, graphitiBulkChunkSaveQuery, map[string]interface{}{
		"anchor": "entity-000000",
		"chunks": payload.chunks,
	})
	require.NoError(b, err)

	queryVec := unitVectorF64(0, dim)
	b.ReportAllocs()

	b.Run("node_similarity_with_projection_filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(ctx, graphitiNodeSimilarityQuery, map[string]interface{}{
				"group_id":      graphitiGroupID,
				"search_vector": queryVec,
				"min_score":     -1.0,
				"limit":         50,
			})
			require.NoError(b, err)
		}
	})

	b.Run("edge_similarity_with_projection_filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(ctx, graphitiEdgeSimilarityQuery, map[string]interface{}{
				"search_vector": queryVec,
				"min_score":     -1.0,
				"limit":         50,
			})
			require.NoError(b, err)
		}
	})

	b.Run("chunk_similarity_projection", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(ctx, graphitiChunkSimilarityQuery, map[string]interface{}{
				"group_id":      graphitiGroupID,
				"search_vector": queryVec,
				"limit":         50,
			})
			require.NoError(b, err)
		}
	})
}

func BenchmarkGraphitiVerbatimCopyNodeBatch(b *testing.B) {
	const (
		dim       = 1024
		batchSize = 50
	)
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)

	searchSvc := search.NewServiceWithDimensions(ns, dim)
	exec.SetSearchService(searchSvc)
	seed := buildGraphitiScenarioPayload(batchSize, 0, 0, dim)
	_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": seed.nodes})
	require.NoError(b, err)
	require.NoError(b, searchSvc.BuildIndexes(ctx))
	require.True(b, searchSvc.IsReady())

	batches := make([][]map[string]interface{}, b.N)
	for i := 0; i < b.N; i++ {
		start := (i + 1) * batchSize
		nodes := make([]map[string]interface{}, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			global := start + j
			nodes = append(nodes, map[string]interface{}{
				"uuid":           fmt.Sprintf("copy-entity-%06d", global),
				"name":           fmt.Sprintf("copy entity %06d", global),
				"summary":        fmt.Sprintf("copy summary %06d", global),
				"group_id":       graphitiGroupID,
				"labels":         []string{"Entity"},
				"name_embedding": deterministicVectorF64(global, dim),
			})
		}
		batches[i] = nodes
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": batches[i]})
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != batchSize {
			b.Fatalf("unexpected rows: got %d want %d", len(res.Rows), batchSize)
		}
	}
}

func BenchmarkGraphitiBulkEdgeSaveIncompleteIndex(b *testing.B) {
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX entity_uuid_idx IF NOT EXISTS FOR (n:Entity) ON (n.uuid)", nil)
	require.NoError(b, err)

	payload := buildGraphitiScenarioPayload(64, 256, 0, 16)
	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(b, err)
	require.Len(b, res.Rows, len(payload.nodes))

	schema := ns.GetSchema()
	require.NotNil(b, schema)
	entityNodes, err := ns.GetNodesByLabel("Entity")
	require.NoError(b, err)
	require.NotEmpty(b, entityNodes)

	removed := 0
	for _, node := range entityNodes {
		uuid, _ := node.Properties["uuid"].(string)
		if uuid == "" {
			continue
		}
		if strings.HasSuffix(uuid, "003") || strings.HasSuffix(uuid, "007") || strings.HasSuffix(uuid, "011") || strings.HasSuffix(uuid, "017") {
			ids := schema.PropertyIndexLookup("Entity", "uuid", uuid)
			require.NotEmpty(b, ids)
			for _, id := range ids {
				require.NoError(b, schema.PropertyIndexDelete("Entity", "uuid", id, uuid))
			}
			removed++
		}
	}
	require.NotZero(b, removed)

	// Warm one-time executor/query setup so the measurement reflects the
	// steady-state repeated ingest path rather than setup overhead.
	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(b, err)
	require.Len(b, res.Rows, len(payload.edges))
	require.True(b, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
		require.NoError(b, err)
		require.Len(b, res.Rows, len(payload.edges))
	}
}

func BenchmarkGraphitiRelationshipVectorFastPath(b *testing.B) {
	const (
		dim         = 32
		entityCount = 512
		edgeCount   = 4096
		chunkCount  = 0
		limit       = 50
	)

	setup := func(b *testing.B, attachSearch bool) (*StorageExecutor, context.Context) {
		b.Helper()

		base, err := storage.NewBadgerEngineInMemory()
		require.NoError(b, err)
		b.Cleanup(func() { _ = base.Close() })

		ns := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(ns)
		exec.cache = nil
		ctx := context.Background()

		_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
		require.NoError(b, err)

		payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)
		_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
		require.NoError(b, err)
		_, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
		require.NoError(b, err)

		if attachSearch {
			searchSvc := search.NewServiceWithDimensions(ns, dim)
			require.NoError(b, searchSvc.BuildIndexes(ctx))
			require.True(b, searchSvc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))
			exec.SetSearchService(searchSvc)
		}

		return exec, ctx
	}

	baselineExec, baselineCtx := setup(b, false)
	indexedExec, indexedCtx := setup(b, true)
	queryVec := unitVectorF64(0, dim)
	params := map[string]interface{}{
		"search_vector": queryVec,
		"min_score":     -1.0,
		"limit":         limit,
	}

	b.ReportAllocs()
	b.Run("storage_scan_baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := baselineExec.Execute(baselineCtx, graphitiEdgeSimilarityQuery, params)
			require.NoError(b, err)
			require.NotEmpty(b, res.Rows)
		}
	})

	b.Run("indexed_relationship_vectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := indexedExec.Execute(indexedCtx, graphitiEdgeSimilarityQuery, params)
			require.NoError(b, err)
			require.NotEmpty(b, res.Rows)
		}
	})
}

func buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim int) graphitiScenarioPayload {
	nodes := make([]map[string]interface{}, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		uuid := fmt.Sprintf("entity-%06d", i)
		nodes = append(nodes, map[string]interface{}{
			"uuid":           uuid,
			"name":           fmt.Sprintf("entity %06d", i),
			"summary":        fmt.Sprintf("synthetic summary for entity %06d", i),
			"group_id":       graphitiGroupID,
			"labels":         []string{"Entity", fmt.Sprintf("Tier%d", i%4)},
			"name_embedding": deterministicVectorF64(i, dim),
			"custom_rank":    int64(i % 11),
		})
	}

	edges := make([]map[string]interface{}, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		src := i % entityCount
		dst := (i + (i % 7) + 1) % entityCount
		edges = append(edges, map[string]interface{}{
			"uuid":             fmt.Sprintf("edge-%06d", i),
			"source_node_uuid": fmt.Sprintf("entity-%06d", src),
			"target_node_uuid": fmt.Sprintf("entity-%06d", dst),
			"name":             "RELATES_TO",
			"fact":             fmt.Sprintf("entity-%06d relates entity-%06d", src, dst),
			"group_id":         graphitiGroupID,
			"fact_embedding":   deterministicVectorF64(10_000+i, dim),
		})
	}

	chunks := make([]map[string]interface{}, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunks = append(chunks, map[string]interface{}{
			"uuid":     fmt.Sprintf("chunk-%06d", i),
			"group_id": graphitiGroupID,
			"emb":      deterministicVectorF64(20_000+i, dim),
			"text":     fmt.Sprintf("chunk text %06d with graphiti-like payload", i),
			"tokens":   int64(128 + (i % 64)),
		})
	}

	return graphitiScenarioPayload{
		nodes:  nodes,
		edges:  edges,
		chunks: chunks,
	}
}

func buildGraphitiEpisodePayload(episode, entityCount, edgeCount, dim int) graphitiScenarioPayload {
	nodes := make([]map[string]interface{}, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		global := episode*entityCount + i
		uuid := fmt.Sprintf("episode-%02d-entity-%06d", episode, i)
		nodes = append(nodes, map[string]interface{}{
			"uuid":           uuid,
			"name":           fmt.Sprintf("episode %02d entity %06d", episode, i),
			"summary":        fmt.Sprintf("DOS episode %02d synthetic entity %06d", episode, i),
			"group_id":       graphitiGroupID,
			"labels":         []string{"Entity", fmt.Sprintf("Episode%d", episode)},
			"name_embedding": deterministicVectorF64(global, dim),
		})
	}

	edges := make([]map[string]interface{}, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		src := i % entityCount
		dst := (i + (i % 5) + 1) % entityCount
		edges = append(edges, map[string]interface{}{
			"uuid":             fmt.Sprintf("episode-%02d-edge-%06d", episode, i),
			"source_node_uuid": fmt.Sprintf("episode-%02d-entity-%06d", episode, src),
			"target_node_uuid": fmt.Sprintf("episode-%02d-entity-%06d", episode, dst),
			"name":             "RELATES_TO",
			"fact":             fmt.Sprintf("DOS episode %02d fact %06d", episode, i),
			"group_id":         graphitiGroupID,
			"fact_embedding":   deterministicVectorF64(100_000+episode*edgeCount+i, dim),
		})
	}

	return graphitiScenarioPayload{nodes: nodes, edges: edges}
}

func deterministicVectorF64(seed, dim int) []float64 {
	v := make([]float64, dim)
	base := seed % dim
	v[base] = 1.0
	v[(base+3)%dim] = 0.35
	v[(base+7)%dim] = 0.2
	normalizeF64(v)
	return v
}

func unitVectorF64(idx, dim int) []float64 {
	v := make([]float64, dim)
	v[idx%dim] = 1.0
	return v
}

func normalizeF64(v []float64) {
	sum := 0.0
	for _, x := range v {
		sum += x * x
	}
	if sum == 0 {
		return
	}
	n := math.Sqrt(sum)
	for i := range v {
		v[i] = v[i] / n
	}
}

func mustCountRows(t testing.TB, exec *StorageExecutor, ctx context.Context, query string, params map[string]interface{}) int64 {
	res, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	return mustInt64(t, res.Rows[0][0])
}

func mustInt64(t testing.TB, v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		require.Failf(t, "unexpected numeric type", "value=%v (%T)", v, v)
		return 0
	}
}

func assertNonIncreasingScores(t testing.TB, rows [][]interface{}, scoreIdx int) {
	prev := math.Inf(1)
	for i, row := range rows {
		require.Greater(t, len(row), scoreIdx, "row %d missing score index %d", i, scoreIdx)
		score, ok := toFloat64(row[scoreIdx])
		require.True(t, ok, "row %d score is not numeric: %T", i, row[scoreIdx])
		require.LessOrEqual(t, score, prev+1e-6, "scores should be sorted descending")
		prev = score
	}
}
