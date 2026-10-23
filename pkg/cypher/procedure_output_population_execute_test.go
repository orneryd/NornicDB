package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const procedureOutputPopulation = 2048

// apocPathPopulationFixture is a start node with a leaf for every member of
// the population.
func apocPathPopulationFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := store.CreateNode(&storage.Node{ID: "start", Labels: []string{"PathStart"}, Properties: map[string]interface{}{"name": "start"}})
	require.NoError(t, err)
	for i := 0; i < procedureOutputPopulation; i++ {
		leaf := storage.NodeID(fmt.Sprintf("leaf-%04d", i))
		_, err := store.CreateNode(&storage.Node{ID: leaf, Labels: []string{"Leaf"}, Properties: map[string]interface{}{"name": string(leaf)}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID("branch-" + string(leaf)), Type: "BRANCH", StartNode: "start", EndNode: leaf}))
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
}

// vectorQueryIndexFixture is the vector population served by a built
// search index, so db.index.vector.queryNodes reads its hits from the index.
func vectorQueryIndexFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	executor := vectorPopulationFixture(t)
	service := search.NewServiceWithDimensions(executor.storage, 3)
	t.Cleanup(func() { _ = service.Close() })
	require.NoError(t, service.BuildIndexes(context.Background()))
	executor.SetSearchService(service)
	return executor
}

// searchPopulationFixture is a population of documents that one term
// matches, behind a built search index.
func searchPopulationFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for i := 0; i < procedureOutputPopulation; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%04d", i)), Labels: []string{"Document"}, Properties: map[string]interface{}{"content": fmt.Sprintf("alpha document %d", i)}})
		require.NoError(t, err)
	}
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	service := search.NewService(store)
	t.Cleanup(func() { _ = service.Close() })
	require.NoError(t, service.BuildIndexes(context.Background()))
	executor.SetSearchService(service)
	return executor
}

// linkPredictionPopulationFixture links a source to every member of the
// population through one shared neighbor: each is a predicted link.
func linkPredictionPopulationFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for _, id := range []string{"source", "hub"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "source-hub", Type: "K", StartNode: "source", EndNode: "hub"}))
	for i := 0; i < procedureOutputPopulation; i++ {
		target := storage.NodeID(fmt.Sprintf("target-%04d", i))
		_, err := store.CreateNode(&storage.Node{ID: target, Labels: []string{"P"}, Properties: map[string]interface{}{"name": string(target)}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID("hub-" + string(target)), Type: "K", StartNode: "hub", EndNode: target}))
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
}

// The procedures' output adapters stop inside their pass over the
// procedure's results, through public Execute: the context expires at the
// adapter's second batch, and the statement fails instead of reporting rows.
func TestProcedureOutputPopulationsExecuteCancellation(t *testing.T) {
	cases := []entityPopulationCase{
		{name: "apoc_path_rows", query: "MATCH (s:PathStart) CALL apoc.path.subgraphNodes(s, {}) YIELD node RETURN node.name AS name", consumer: "apocPathRows", after: 2, rows: procedureOutputPopulation + 1, fixture: apocPathPopulationFixture},
		{name: "apoc_path_subgraph_all", query: "MATCH (s:PathStart) CALL apoc.path.subgraphAll(s, {}) YIELD nodes RETURN size(nodes) AS n", consumer: "callApocPathSubgraphAll", after: 2, rows: 1, fixture: apocPathPopulationFixture},
		{name: "vector_query_exact", query: "CALL db.index.vector.queryNodes('vec_population_nodes', 4096, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.uuid AS uuid, score", consumer: "callDbIndexVectorQueryNodesInput", after: 2, rows: procedureOutputPopulation, fixture: vectorPopulationFixture},
		{name: "vector_query_index", query: "CALL db.index.vector.queryNodes('vec_population_nodes', 4096, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.uuid AS uuid, score", consumer: "callDbIndexVectorQueryNodesInput", after: 2, rows: procedureOutputPopulation, fixture: vectorQueryIndexFixture},
		{name: "retrieve_rows", query: "CALL db.retrieve({query: 'alpha', limit: 4096})", consumer: "runSearchRequest", after: 2, rows: procedureOutputPopulation, fixture: searchPopulationFixture},
		// A continuation page holds at most the result stream's page size (500),
		// one batch: it is canceled at the pass's first observation.
		{name: "retrieve_page", query: "CALL db.retrieve({query: 'alpha', limit: 4096, n: 500})", consumer: "executeSearchContinuationPage", after: 1, rows: 1, fixture: searchPopulationFixture},
		// The graph search fuses a pool its keyword fallback bounds (40 hits
		// here), one batch: it is canceled at the pass's first observation.
		{name: "retrieve_graph_rows", query: "CALL db.retrieveGraph({query: 'alpha', limit: 4096})", consumer: "graphSearchResult", after: 1, rows: 40, fixture: searchPopulationFixture},
		{name: "retrieve_graph_page", query: "CALL db.retrieveGraph({query: 'alpha', limit: 4096, n: 4096})", consumer: "graphSearchPage", after: 1, rows: 1, fixture: searchPopulationFixture},
		{name: "link_prediction_topology", query: "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'source', topK: 4096})", consumer: "formatLinkPredictionResults", after: 2, rows: procedureOutputPopulation, fixture: linkPredictionPopulationFixture},
		{name: "link_prediction_hybrid", query: "CALL gds.linkPrediction.predict.stream({sourceNode: 'source', topK: 4096})", consumer: "formatHybridPredictionResults", after: 2, rows: procedureOutputPopulation, fixture: linkPredictionPopulationFixture},
	}
	runEntityPopulationCases(t, cases, nil)
}

// db.retrieveRelationships' adapter stops the same way over relationship hits.
func TestRelationshipSearchOutputPopulationExecuteCancellation(t *testing.T) {
	fixture := func(t *testing.T) *StorageExecutor {
		t.Helper()
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
		service := search.NewServiceWithDimensions(store, 3)
		t.Cleanup(func() { _ = service.Close() })
		service.EnableRelationshipSearch(nil)
		service.StartRelationshipIndex()
		executor.SetSearchService(service)
		_, err := executor.Execute(context.Background(), "UNWIND range(1, 2048) AS i CREATE (:Doc {i: i})-[:CITES {reason: 'zephyrine bridge ' + toString(i)}]->(:Doc {i: i})", nil)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			result, err := executor.Execute(context.Background(), "CALL db.retrieveRelationships({query: 'zephyrine', limit: 4096})", nil)
			return err == nil && len(result.Rows) == procedureOutputPopulation
		}, 30*time.Second, 50*time.Millisecond)
		return executor
	}
	runEntityPopulationCases(t, []entityPopulationCase{
		{name: "retrieve_relationships", query: "CALL db.retrieveRelationships({query: 'zephyrine', limit: 4096})", consumer: "relationshipSearchResult", after: 2, rows: procedureOutputPopulation, fixture: fixture},
	}, nil)
}
