package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipOccurrenceSeekStore struct {
	*storage.NamespacedEngine
	lookups atomic.Int64
}

func (store *relationshipOccurrenceSeekStore) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	store.lookups.Add(1)
	return store.NamespacedEngine.SeekRelationships(ctx, relType, properties, values)
}

// Identical source text can occur in access kinds with different eligibility.
// The assertions observe only public rows/plans and physical storage requests;
// they do not rebuild or alter the compiler's access-site records.
func TestRelationshipAccessOccurrenceEligibilityPublic(t *testing.T) {
	store := &relationshipOccurrenceSeekStore{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "occurrence_eligibility")}
	exec := NewStorageExecutor(store)
	exec.cache = nil
	ctx := context.Background()
	for _, query := range []string{
		"CREATE (:A)-[:T {p:'x'}]->(:B)",
		"CREATE RANGE INDEX occurrence_p FOR ()-[r:T]-() ON (r.p)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	const ordinary = "MATCH (a)-[r:T {p:'x'}]->(b) RETURN 'ordinary' AS kind, 1 AS n"
	const shortest = "MATCH path = shortestPath((a)-[r:T {p:'x'}]->(b)) RETURN 'shortest' AS kind, length(path) AS n"
	const allShortest = "MATCH path = allShortestPaths((a)-[r:T {p:'x'}]->(b)) RETURN 'shortest' AS kind, length(path) AS n"
	const legalOrdinary = "MATCH (a:A)-[r:T]->(b:B) RETURN 'ordinary' AS kind, 1 AS n"
	const legalSeek = "MATCH (a:A)-[r:T]->(b:B) WHERE r.p='x' RETURN 'ordinary' AS kind, 1 AS n"
	const legalShortest = "MATCH path = shortestPath((a:A)-[r:T]->(b:B)) RETURN 'shortest' AS kind, length(path) AS n"
	const legalAllShortest = "MATCH path = allShortestPaths((a:A)-[r:T]->(b:B)) RETURN 'shortest' AS kind, length(path) AS n"
	const expressionShortest = "MATCH (a:A), (b:B) RETURN 'shortest' AS kind, length(shortestPath((a:A)-[r:T]->(b:B))) AS n"
	const expressionAllShortest = "MATCH (a:A), (b:B) RETURN 'shortest' AS kind, size(allShortestPaths((a:A)-[r:T]->(b:B))) AS n"
	const selectedPath = "MATCH path = ANY SHORTEST (a:A)-[r:T {p:'x'}]->(b:B) RETURN 'shortest' AS kind, length(path) AS n"
	const selectedOrdinary = "MATCH (a:A)-[r:T {p:'x'}]->(b:B) RETURN 'ordinary' AS kind, 1 AS n"
	for _, tc := range []struct {
		name, query string
		seek        bool
		rows        [][]interface{}
		inlinePath  bool
		sites       int
	}{
		{"ordinary_control", ordinary, true, [][]interface{}{{"ordinary", int64(1)}}, false, 1},
		{"shortest_control", shortest, false, nil, true, 1},
		{"all_shortest_control", allShortest, false, nil, true, 1},
		{"shortest_then_ordinary", shortest + " UNION ALL " + ordinary, true, nil, true, 2},
		{"ordinary_then_shortest", ordinary + " UNION ALL " + shortest, true, nil, true, 2},
		{"all_shortest_then_ordinary", allShortest + " UNION ALL " + ordinary, true, nil, true, 2},
		{"ordinary_then_all_shortest", ordinary + " UNION ALL " + allShortest, true, nil, true, 2},
		{"nested_call", "CALL { " + shortest + " UNION ALL " + ordinary + " } RETURN kind, n", true, nil, true, 2},
		{"legal_shortest_control", legalShortest, false, [][]interface{}{{"shortest", int64(1)}}, false, 1},
		{"legal_all_shortest_control", legalAllShortest, false, [][]interface{}{{"shortest", int64(1)}}, false, 1},
		{"legal_shortest_then_seek", legalShortest + " UNION ALL " + legalSeek, true, [][]interface{}{{"shortest", int64(1)}, {"ordinary", int64(1)}}, false, 2},
		{"legal_seek_then_all_shortest", legalSeek + " UNION ALL " + legalAllShortest, true, [][]interface{}{{"ordinary", int64(1)}, {"shortest", int64(1)}}, false, 2},
		{"legal_identical_scan_occurrences", legalShortest + " UNION ALL " + legalOrdinary, false, [][]interface{}{{"shortest", int64(1)}, {"ordinary", int64(1)}}, false, 2},
		{"legal_identical_scan_occurrences_reversed", legalOrdinary + " UNION ALL " + legalAllShortest, false, [][]interface{}{{"ordinary", int64(1)}, {"shortest", int64(1)}}, false, 2},
		{"expression_shortest_then_ordinary", expressionShortest + " UNION ALL " + legalOrdinary, false, [][]interface{}{{"shortest", int64(1)}, {"ordinary", int64(1)}}, false, 2},
		{"ordinary_then_expression_all_shortest", legalOrdinary + " UNION ALL " + expressionAllShortest, false, [][]interface{}{{"ordinary", int64(1)}, {"shortest", int64(1)}}, false, 2},
		{"selected_path_control", selectedPath, false, [][]interface{}{{"shortest", int64(1)}}, false, 1},
		{"selected_path_then_ordinary", selectedPath + " UNION ALL " + selectedOrdinary, true, [][]interface{}{{"shortest", int64(1)}, {"ordinary", int64(1)}}, false, 2},
		{"ordinary_then_selected_path", selectedOrdinary + " UNION ALL " + selectedPath, true, [][]interface{}{{"ordinary", int64(1)}, {"shortest", int64(1)}}, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"PROFILE", "EXPLAIN"} {
				t.Run(mode, func(t *testing.T) {
					before := store.lookups.Load()
					result, err := exec.Execute(ctx, mode+" "+tc.query, nil)
					if tc.inlinePath && mode == "PROFILE" {
						// Neo4j's shortest-path contract rejects inline relationship
						// properties. EXPLAIN isolates the reported template collision;
						// supported patterns below prove physical runtime behavior.
						require.ErrorContains(t, err, "contains properties {p:'x'}")
						return
					}
					require.NoError(t, err, tc.query)
					lookups := store.lookups.Load() - before
					if mode == "PROFILE" {
						require.ElementsMatch(t, tc.rows, result.Rows)
						if tc.seek {
							require.Positive(t, lookups, "ordinary MATCH must physically use its available index even beside an identical shortest-path pattern")
						} else {
							require.Zero(t, lookups, "shortest-path occurrence must retain its scan-only access contract")
						}
					} else {
						require.Empty(t, result.Rows)
						require.Zero(t, lookups, "EXPLAIN must not execute either access kind")
					}
					plan, ok := result.Metadata["plan"].(*ExecutionPlan)
					require.True(t, ok, "public plan metadata")
					var seeks, sites int
					var visit func(*PlanOperator)
					visit = func(operator *PlanOperator) {
						if _, ok := operator.Arguments["accessOccurrences"]; ok {
							sites++
						}
						if _, ok := operator.Arguments["accessAttempts"]; ok {
							sites++
						}
						if operator.OperatorType == "RelationshipIndexSeek" {
							seeks++
						}
						for _, child := range operator.Children {
							visit(child)
						}
					}
					visit(plan.Root)
					require.Equal(t, tc.sites, sites, "each access kind must retain its own occurrence/counter record")
					if tc.seek {
						require.Positive(t, seeks, "ordinary occurrence must remain seek-eligible in the public plan")
					} else {
						require.Zero(t, seeks, "shortest-path occurrence must not advertise an unsupported equality seek")
					}
				})
			}
		})
	}
}

// The new path-search entry must obey the same source-removal contract as an
// ordinary MATCH: a cached spelling cannot authorize a removed access site.
func TestRelationshipAccessPathSourceRemovalPublic(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "path_source_removal"))
	exec.cache = nil
	_, err := exec.Execute(context.Background(), "CREATE (:A)-[:T]->(:B)", nil)
	require.NoError(t, err)
	const query = "MATCH (a:A), (b:B) RETURN length(shortestPath((a)-[r:T]->(b))) AS n"
	program := relationshipProgramFor(query)
	for _, removal := range []string{"registered", "removed", "removed_with_cached_path"} {
		removed := removal != "registered"
		source := program
		if removed {
			source = &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}}
			if removal == "removed_with_cached_path" {
				source.paths = program.paths
			}
		}
		ctx := context.WithValue(context.Background(), relationshipAccessProgramKey{}, source)
		ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
		require.Equal(t, removed, len(profile.unregistered) != 0, "source removal must be visible to the actual path-search kernel")
		result, err = exec.Execute(ctx, "EXPLAIN "+query, nil)
		require.NoError(t, err)
		root := result.Metadata["plan"].(*ExecutionPlan).Root
		var sites int
		var visit func(*PlanOperator)
		visit = func(operator *PlanOperator) {
			if _, owned := operator.Arguments["accessOccurrences"]; owned {
				sites++
			}
			for _, child := range operator.Children {
				visit(child)
			}
		}
		visit(root)
		require.Equal(t, !removed, sites != 0, "EXPLAIN cannot rediscover the removed source")
	}
}
