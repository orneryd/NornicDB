package cypher

// PROFILE for the two vector query procedures.
//
// Exact fallbacks report reader populations; indexed answers report one
// logical index request and actual hydration. Candidate counts do not describe
// ANN entries visited. Provider callbacks and shared background warmup remain
// explicitly unobserved only when the request actually enters those boundaries.
// Ordinary and PROFILE queries must return identical rows and score bits.

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vectorProfileParity runs statement and then PROFILE statement on exec,
// holding both answers to the same columns and the same rows, including the
// exact score bits, and returns the ordinary rows with the profiled plan.
// beforeProfile runs after the ordinary statement, so a fixture can reset the
// reader probe it uses as ground truth for the profiled run only.
func vectorProfileParity(t *testing.T, exec *StorageExecutor, statement string, params map[string]interface{}, beforeProfile func()) ([][]interface{}, *ExecutionPlan) {
	t.Helper()
	plain, err := exec.Execute(t.Context(), statement, params)
	require.NoError(t, err, statement)
	if beforeProfile != nil {
		beforeProfile()
	}
	profiled, err := exec.Execute(t.Context(), "PROFILE "+statement, params)
	require.NoError(t, err, statement)
	require.Equal(t, plain.Columns, profiled.Columns, "PROFILE preserves the procedure's columns")
	require.Equal(t, plain.Rows, profiled.Rows, "PROFILE preserves the public rows and their exact score bits")
	return plain.Rows, executionPlanFromResult(t, profiled)
}

// requireVectorProfileRead selects the one record of a physical read kind and
// holds it to the entities the reader actually handed over: the record's rows
// are those entities and reading one entity is one access unit, as every other
// entity read in PROFILE reports it.
func requireVectorProfileRead(t *testing.T, plan *ExecutionPlan, kind, detail string, delivered int64) *PlanOperator {
	t.Helper()
	record := planOperatorOfType(plan.Root, kind)
	require.NotNil(t, record, "%s must be reported as a plan operator", kind)
	require.True(t, record.RowsObserved, "%s reports the rows it read", kind)
	require.True(t, record.DBHitsObserved, "%s reports the access units it performed", kind)
	require.Equal(t, detail, record.Description, "%s identifies the read it performed", kind)
	require.Equal(t, delivered, record.ActualRows, "%s counts the entities the reader delivered", kind)
	require.Equal(t, delivered, record.DBHits, "reading an entity is one access unit")
	return record
}

// vectorProfileNodes seeds one vector index on label over a population that
// separates what a read sees from what the procedure emits: scored nodes, nodes
// of the label without a vector, and one node of another label. Every vector
// scores differently, so the top of a service answer is not a tie: the other
// label's node ranks below both of the label's vectors and cannot displace
// them in a top-k cut.
func vectorProfileNodes(t testing.TB, exec *StorageExecutor, ns storage.Engine, indexName, label string) {
	t.Helper()
	_, err := exec.Execute(t.Context(),
		"CALL db.index.vector.createNodeIndex('"+indexName+"', '"+label+"', 'embedding', 4, 'cosine')", nil)
	require.NoError(t, err)
	for _, node := range []*storage.Node{
		{ID: "vec-best", Labels: []string{label}, Properties: map[string]interface{}{"uuid": "vec-best", "embedding": []float64{1, 0, 0, 0}}},
		{ID: "vec-second", Labels: []string{label}, Properties: map[string]interface{}{"uuid": "vec-second", "embedding": []float64{0, 1, 0, 0}}},
		{ID: "vec-plain-1", Labels: []string{label}, Properties: map[string]interface{}{"uuid": "vec-plain-1"}},
		{ID: "vec-plain-2", Labels: []string{label}, Properties: map[string]interface{}{"uuid": "vec-plain-2"}},
		{ID: "vec-plain-3", Labels: []string{label}, Properties: map[string]interface{}{"uuid": "vec-plain-3"}},
		{ID: "vec-other", Labels: []string{"VectorProfileOther"}, Properties: map[string]interface{}{"uuid": "vec-other", "embedding": []float64{-1, 0, 0, 0}}},
	} {
		_, err := ns.CreateNode(t.Context(), node)
		require.NoError(t, err, string(node.ID))
	}
}

// TestProfileVectorQueryNodesExactFallbackReportsTheLabelPopulation: without a
// search service the procedure answers from its exact fallback, and PROFILE
// reports the population that fallback read. The label population is every
// node of the index label, including the three the scoring admits nothing from,
// so the record is neither the emitted row count, nor the scored subset, nor the
// whole node population.
func TestProfileVectorQueryNodesExactFallbackReportsTheLabelPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const (
			label     = "VectorProfileDoc"
			indexName = "profile_doc_population_idx"
		)
		ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_vector_nodes_exact")
		exec := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0})
		vectorProfileNodes(t, exec, ns, indexName, label)

		population, err := ns.GetNodesByLabel(t.Context(), label)
		require.NoError(t, err)
		require.Len(t, population, 5, "two scored nodes and three without a vector")

		rows, plan := vectorProfileParity(t, exec,
			"CALL db.index.vector.queryNodes('"+indexName+"', 1, $q) YIELD node, score RETURN node.uuid AS uuid, score",
			map[string]interface{}{"q": []float64{1, 0, 0, 0}}, nil)
		require.Nil(t, exec.search(), "the exact fallback is the source under test, not a wired service")
		require.Equal(t, [][]interface{}{{"vec-best", float64(1)}}, rows,
			"the exact fallback emits its nearest row, not the population it read")

		require.True(t, plan.TotalDBHitsObserved,
			"the procedure reports its own reads instead of an opaque callback")
		require.Empty(t, plan.Unobserved)
		scan := requireVectorProfileRead(t, plan, profileKindNodeByLabelScan, label, int64(len(population)))
		require.NotEqual(t, int64(len(rows)), scan.ActualRows,
			"the record is the population the reader delivered (%d), not the %d emitted rows", len(population), len(rows))
		require.Nil(t, planOperatorOfType(plan.Root, profileKindAllNodesScan),
			"the index label's population was read, so no whole-node scan is claimed")
		require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByIdSeek),
			"the exact fallback scores the images it read; it hydrates no point read")
		require.GreaterOrEqual(t, plan.TotalDBHits, scan.ActualRows,
			"the population the reader delivered is part of the statement's observed work")
	})
}

// TestProfileVectorQueryNodesServiceAnswerReportsCandidateHydration: when the
// wired service answers, the statement reads no node population. It hydrates
// each candidate the service returned with a point read, and PROFILE reports
// those point reads as the node hydration they are. The record is the
// point-read kind the rest of PROFILE uses, so a definition or population-state
// lookup cannot be mistaken for one. The reader probe is the ground truth: the
// statement performed at least the point reads the record claims.
func TestProfileVectorQueryNodesServiceAnswerReportsCandidateHydration(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const (
			label     = "VectorProfileHydratedDoc"
			indexName = "profile_doc_hydration_idx"
		)
		ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_vector_nodes_service")
		spy := storagefault.NewQueryNodeReadSpy(ns)
		exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
		vectorProfileNodes(t, exec, ns, indexName, label)

		service := search.NewService(ns, 4, nil)
		require.NoError(t, service.BuildIndexes(t.Context()))
		t.Cleanup(func() { _ = service.Close() })
		require.True(t, service.IsReady(), "a ready service answers the query")
		require.Positive(t, service.CountPropertyVectorEntries("embedding"),
			"the service holds the index's vectors, so the statement reads the service and not storage")
		exec.SetSearchService(service)

		rows, plan := vectorProfileParity(t, exec,
			"CALL db.index.vector.queryNodes('"+indexName+"', 2, $q) YIELD node, score RETURN node.uuid AS uuid, score",
			map[string]interface{}{"q": []float64{1, 0, 0, 0}},
			func() {
				spy.Full.Store(0)
				spy.Query.Store(0)
			})
		pointReads := spy.Full.Load() + spy.Query.Load()

		require.Len(t, rows, 2)
		require.Equal(t, "vec-best", rows[0][0])
		require.Greater(t, rows[0][1].(float64), 0.99)

		require.True(t, plan.TotalDBHitsObserved,
			"the procedure reports its own reads instead of an opaque callback")
		require.Empty(t, plan.Unobserved)
		request := planOperatorOfType(plan.Root, "NodeVectorIndexQuery")
		require.NotNil(t, request)
		require.EqualValues(t, 2, request.ActualRows, "returned index candidates, not visited ANN entries")
		require.EqualValues(t, 1, request.DBHits, "one logical index query")
		hydration := requireVectorProfileRead(t, plan, profileKindNodeByIdSeek, "", int64(len(rows)))
		require.Positive(t, pointReads, "the candidates are hydrated with real point reads")
		require.GreaterOrEqual(t, pointReads, hydration.ActualRows,
			"the recorded hydration is bounded by the point reads the reader performed")
		require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByLabelScan),
			"a service answer hydrates its candidates; it reads no label population")
		require.Nil(t, planOperatorOfType(plan.Root, profileKindAllNodesScan),
			"a service answer hydrates its candidates; it scans no node population")
		require.GreaterOrEqual(t, plan.TotalDBHits, hydration.ActualRows,
			"the hydration's point reads are part of the statement's observed work")
	})
}

// vectorProfileRelationships seeds one relationship vector index over a
// population that separates what a read sees from what the procedure emits:
// scored relationships of the index type, one relationship of that type without
// a vector, and two of another type. Every vector of the index type scores
// differently, so neither a top-k cut nor a tie order depends on map or stream
// order.
func vectorProfileRelationships(t testing.TB, exec *StorageExecutor, ns storage.Engine, indexName, relType string) {
	t.Helper()
	_, err := exec.Execute(t.Context(),
		"CALL db.index.vector.createRelationshipIndex('"+indexName+"', '"+relType+"', 'embedding', 3, 'cosine')", nil)
	require.NoError(t, err)
	for _, id := range []string{"rel-a", "rel-b", "rel-c", "rel-d"} {
		_, err := ns.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(id), Labels: []string{"VectorProfileEntity"}})
		require.NoError(t, err, id)
	}
	for _, edge := range []*storage.Edge{
		{ID: "rel-best", Type: relType, StartNode: "rel-a", EndNode: "rel-b",
			Properties: map[string]interface{}{"uuid": "rel-best", "embedding": []float64{1, 0, 0}}},
		{ID: "rel-second", Type: relType, StartNode: "rel-a", EndNode: "rel-c",
			Properties: map[string]interface{}{"uuid": "rel-second", "embedding": []float64{0, 1, 0}}},
		{ID: "rel-third", Type: relType, StartNode: "rel-a", EndNode: "rel-d",
			Properties: map[string]interface{}{"uuid": "rel-third", "embedding": []float64{-1, 0, 0}}},
		{ID: "rel-unvectorised", Type: relType, StartNode: "rel-b", EndNode: "rel-c",
			Properties: map[string]interface{}{"uuid": "rel-unvectorised"}},
		{ID: "rel-other-type", Type: "VectorProfileOtherRel", StartNode: "rel-c", EndNode: "rel-d",
			Properties: map[string]interface{}{"uuid": "rel-other-type", "embedding": []float64{1, 0, 0}}},
		{ID: "rel-other-plain", Type: "VectorProfileOtherRel", StartNode: "rel-b", EndNode: "rel-d",
			Properties: map[string]interface{}{"uuid": "rel-other-plain"}},
	} {
		require.NoError(t, ns.CreateEdge(t.Context(), edge), string(edge.ID))
	}
}

// TestProfileVectorQueryRelationshipsExactFallbackReportsTheEdgePopulation:
// without a search service the procedure answers from its exact fallback, which
// streams every relationship. PROFILE reports that whole population, not the k
// emitted rows and not the type- or vector-admitted subset it scored.
func TestProfileVectorQueryRelationshipsExactFallbackReportsTheEdgePopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const (
			relType   = "VectorProfileFact"
			indexName = "profile_rel_population_idx"
		)
		ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_vector_rels_exact")
		exec := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0})
		vectorProfileRelationships(t, exec, ns, indexName, relType)

		edges, err := ns.AllEdges(t.Context())
		require.NoError(t, err)
		require.Len(t, edges, 6, "three scored relationships, one unvectorised and two of another type")

		rows, plan := vectorProfileParity(t, exec,
			"CALL db.index.vector.queryRelationships('"+indexName+"', 2, $q) YIELD relationship, score RETURN relationship.uuid AS uuid, score",
			map[string]interface{}{"q": []float64{1, 0, 0}}, nil)
		require.Nil(t, exec.search(), "the exact fallback is the source under test, not a wired service")
		require.Len(t, rows, 2)
		require.Equal(t, "rel-best", rows[0][0])
		require.Equal(t, float64(1), rows[0][1])

		require.True(t, plan.TotalDBHitsObserved,
			"the procedure reports its own reads instead of an opaque callback")
		require.Empty(t, plan.Unobserved)
		scan := requireVectorProfileRead(t, plan, profileKindAllRelationshipsScan, "", int64(len(edges)))
		require.NotEqual(t, int64(len(rows)), scan.ActualRows,
			"the record is the population the reader streamed (%d), not the %d emitted rows", len(edges), len(rows))
		require.Nil(t, planOperatorOfType(plan.Root, queryprofile.KindRelationshipByIDSeek),
			"the exact fallback projects the images it streamed; it reads no relationship point-wise")
		require.GreaterOrEqual(t, plan.TotalDBHits, scan.ActualRows,
			"the streamed population is part of the statement's observed work")
	})
}

// TestProfileVectorQueryRelationshipsServiceAnswerReportsCandidateHydration:
// when the wired service answers relationship vectors, the statement streams no
// relationship population. It reads each candidate it returned by id and
// PROFILE reports those point reads as the edge hydration they are.
func TestProfileVectorQueryRelationshipsServiceAnswerReportsCandidateHydration(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const (
			relType   = "VectorProfileHydratedFact"
			indexName = "profile_rel_hydration_idx"
		)
		ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_vector_rels_service")
		exec := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0})
		vectorProfileRelationships(t, exec, ns, indexName, relType)

		service := search.NewService(ns, 3, nil)
		require.NoError(t, service.BuildIndexes(t.Context()))
		t.Cleanup(func() { _ = service.Close() })
		require.True(t, service.IsReady(), "a ready service answers the query")
		require.True(t, service.HasRelationshipVectorEntries(relType, "embedding"),
			"the service holds the index type's vectors, so the statement reads the service and not storage")
		exec.SetSearchService(service)

		rows, plan := vectorProfileParity(t, exec,
			"CALL db.index.vector.queryRelationships('"+indexName+"', 2, $q) YIELD relationship, score RETURN relationship.uuid AS uuid, score",
			map[string]interface{}{"q": []float64{1, 0, 0}}, nil)
		require.Len(t, rows, 2)
		require.Equal(t, "rel-best", rows[0][0])
		require.Greater(t, rows[0][1].(float64), 0.99)

		require.True(t, plan.TotalDBHitsObserved,
			"the procedure reports its own reads instead of an opaque callback")
		require.Empty(t, plan.Unobserved)
		request := planOperatorOfType(plan.Root, "RelationshipVectorIndexQuery")
		require.NotNil(t, request)
		require.EqualValues(t, 2, request.ActualRows, "returned index candidates, not streamed relationships")
		require.EqualValues(t, 1, request.DBHits, "one logical index query")
		hydration := requireVectorProfileRead(t, plan, queryprofile.KindRelationshipByIDSeek, "", int64(len(rows)))
		require.Nil(t, planOperatorOfType(plan.Root, profileKindAllRelationshipsScan),
			"a service answer hydrates the candidates it returned; it streams no relationship population")
		require.GreaterOrEqual(t, plan.TotalDBHits, hydration.ActualRows,
			"the hydration's point reads are part of the statement's observed work")
	})
}

// BenchmarkVectorProcedureReadObservation shares the public regression fixture
// and measures uncached ordinary execution; setup/index building are untimed.
func BenchmarkVectorProcedureReadObservation(b *testing.B) {
	for _, relationships := range []bool{false, true} {
		for _, served := range []bool{false, true} {
			b.Run(fmt.Sprintf("relationships=%t/service=%t", relationships, served), func(b *testing.B) {
				ns := storage.NewNamespacedEngine(newTestMemoryEngine(b), "profile_vector_bench")
				exec := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0})
				procedure, entity, dimensions := "queryNodes", "node", 4
				if relationships {
					vectorProfileRelationships(b, exec, ns, "probe", "Probe")
					procedure, entity, dimensions = "queryRelationships", "relationship", 3
				} else {
					vectorProfileNodes(b, exec, ns, "probe", "Probe")
				}
				if served {
					svc := search.NewService(ns, dimensions, nil)
					require.NoError(b, svc.BuildIndexes(b.Context()))
					b.Cleanup(func() { require.NoError(b, svc.Close()) })
					exec.SetSearchService(svc)
				}
				query := "CALL db.index.vector." + procedure + "('probe',2,$q) YIELD " + entity + ",score RETURN " + entity + ".uuid,score"
				q := make([]float64, dimensions)
				q[0] = 1
				params := map[string]interface{}{"q": q}
				warm, err := exec.Execute(b.Context(), query, params)
				require.NoError(b, err)
				require.Len(b, warm.Rows, 2)
				want := warm.Rows
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					result, err := exec.Execute(b.Context(), query, params)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != 2 || result.Rows[0][0] != want[0][0] || result.Rows[1][0] != want[1][0] {
						b.Fatal("changed vector query result")
					}
				}
			})
		}
	}
}

func TestProfileVectorProviderAndWarmupCoverageIsRequestOwned(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const statement = "CALL db.index.vector.queryNodes('probe',1,$q) YIELD node,score RETURN node.uuid,score"
		for _, mode := range []string{"embedding cache", "shared warmup", "empty warmup"} {
			t.Run(mode, func(t *testing.T) {
				ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_vector_lifetime")
				exec := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0})
				vectorProfileNodes(t, exec, ns, "probe", "Probe")
				params := map[string]interface{}{"q": []float64{1, 0, 0, 0}}
				var buildErr chan error
				switch mode {
				case "embedding cache":
					exec.SetEmbedder(&mockQueryEmbedder{embedding: []float32{1, 0, 0, 0}})
					params["q"] = "find best"
				case "shared warmup", "empty warmup":
					svc := search.NewService(ns, 4, nil)
					t.Cleanup(func() { require.NoError(t, svc.Close()) })
					if mode == "shared warmup" {
						buildErr = make(chan error, 1)
						svc.SetLazyWarming(true, search.WarmFunc(func() { buildErr <- svc.BuildIndexes(t.Context()) }))
					} else {
						svc.SetLazyWarming(true, nil)
					}
					exec.SetSearchService(svc)
				}
				first, err := exec.Execute(t.Context(), "PROFILE "+statement, params)
				require.NoError(t, err)
				require.Equal(t, "vec-best", first.Rows[0][0])
				if buildErr != nil {
					require.NoError(t, <-buildErr)
				}
				plan := executionPlanFromResult(t, first)
				switch mode {
				case "embedding cache":
					require.False(t, plan.TotalDBHitsObserved)
					require.Contains(t, plan.Unobserved, "database reads: query embedding provider")
				case "shared warmup":
					require.False(t, plan.TotalDBHitsObserved)
					require.Contains(t, plan.Unobserved, "database reads: shared background search warmup")
				case "empty warmup":
					require.True(t, plan.TotalDBHitsObserved, "no callback or background graph read occurred")
					require.Empty(t, plan.Unobserved)
				}
				ordinary, err := exec.Execute(t.Context(), statement, params)
				require.NoError(t, err)
				require.Equal(t, first.Rows, ordinary.Rows)
				second, err := exec.Execute(t.Context(), "PROFILE "+statement, params)
				require.NoError(t, err)
				require.Equal(t, first.Rows, second.Rows)
				warm := executionPlanFromResult(t, second)
				require.True(t, warm.TotalDBHitsObserved, "cached embedding or completed warmup adds no unknown invocation")
				require.Empty(t, warm.Unobserved)
			})
		}
	})
}

func TestProfileVectorAdmissionObservesItsProvidedService(t *testing.T) {
	exec, _ := newProfileRuntimeTestExecutor(t)
	svc := search.NewService(exec.storage, 3, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	resolutions := 0
	exec.SetSearchServiceResolver("probe", func(string) *search.Service { resolutions++; return nil })
	collector := newProfileRuntimeCollector()
	_, err := exec.vectorIndexServiceAnswers(withProfileRuntimeCollector(t.Context(), collector), svc, "probe", search.VectorIndexTarget{}, 3, vectorReadPassive)
	require.NoError(t, err)
	require.Zero(t, resolutions, "state belongs to the passed service, without resolving a different one")
	reads := collector.accessObservation()
	require.Len(t, reads, 1)
	require.Equal(t, profileKindVectorIndexPopulationStateSeek, reads[0].Kind)
	require.EqualValues(t, 1, reads[0].Rows)
	require.EqualValues(t, 1, reads[0].Hits)
}
