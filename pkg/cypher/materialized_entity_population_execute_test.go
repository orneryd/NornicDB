package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const entityPopulationSize = 2048

// entityPopulationFixture holds node populations the executor collects as
// candidates (labels, an indexed property, join keys) rather than as rows.
func entityPopulationFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for i := 0; i < entityPopulationSize; i++ {
		for _, label := range []string{"JoinA", "JoinB", "EntityPopulation"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("%s-%04d", label, i)), Labels: []string{label}, Properties: map[string]interface{}{"k": int64(i), "id": int64(i)}})
			require.NoError(t, err)
		}
	}
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	_, err := executor.Execute(context.Background(), "CREATE INDEX entity_population_id FOR (n:EntityPopulation) ON (n.id)", nil)
	require.NoError(t, err)
	return executor
}

type entityPopulationCase struct {
	name, query, consumer string
	after                 int
	// file and pass name the consumer's loop line when it has several.
	file, pass string
	rows       int
	setup      func(*testing.T, *StorageExecutor)
	fixture    func(*testing.T) *StorageExecutor
}

// Candidate, join and derived entity populations stop inside the consumer
// that walks them, through public Execute: the context expires only there,
// with populations large enough that a missing observation would drain them.
func TestMaterializedEntityPopulationsExecuteCancellation(t *testing.T) {
	ids := make([]interface{}, entityPopulationSize)
	for i := range ids {
		ids[i] = int64(i)
	}
	const join = "MATCH (a:JoinA), (b:JoinB) WHERE a.k = b.k RETURN a.k, b.k"
	cases := []entityPopulationCase{
		{name: "where_join_seed", query: join, consumer: "buildCombinationsUsingWhereJoin", after: 1, rows: entityPopulationSize},
		{name: "where_join_index", query: join, consumer: "buildCombinationsUsingWhereJoin", after: 3, rows: entityPopulationSize},
		{name: "where_join_expansion", query: join, consumer: "buildCombinationsUsingWhereJoin", after: 5, rows: entityPopulationSize},
		{name: "where_join_combinations", query: join, consumer: "pipelineApplyNodeJoinProduct", after: 2, rows: entityPopulationSize},
		{name: "node_projection_limit", query: "MATCH (n:EntityPopulation) RETURN n.id LIMIT $n", consumer: "projectPipelineSimpleNodeRead", after: 1, rows: entityPopulationSize},
		{name: "index_in_list", query: "MATCH (n:EntityPopulation) WHERE n.id IN $ids RETURN n.id", consumer: "tryCollectNodesFromPropertyIndexInAnyOf", after: 1, rows: entityPopulationSize},
		{name: "entity_refresh_lookups", query: "UNWIND range(1, 2) AS k MATCH (n:EntityPopulation) MERGE (m:EntityPopulation {id: n.id}) ON MATCH SET m.v = k RETURN count(*) AS c", consumer: "refreshRowEntities", after: 1, file: "row_entity_refresh.go", pass: "\t\tif lookups.due() && lookups.canceled() {", rows: 1},
		{name: "merge_scan", query: "MERGE (n:EntityPopulation {k: -1}) RETURN n.k", consumer: "findMergeNode", after: 1, rows: 1},
		{name: "delete_targets", query: "MATCH (n:EntityPopulation) DELETE n", consumer: "DeleteTargets", after: 1},
		{name: "routed_restoration", query: "USE nornic UNWIND range(1,$n) AS `a b` WITH `a b` ORDER BY `a b` RETURN *", consumer: "sortColumnsByName", after: 2, rows: 4096, fixture: func(t *testing.T) *StorageExecutor {
			executor, _ := nestedUseRouterExecutor(t, "nornic")
			return executor
		}},
	}
	runEntityPopulationCases(t, cases, map[string]interface{}{"n": int64(4096), "ids": ids})
}

func runEntityPopulationCases(t *testing.T, cases []entityPopulationCase, parameters map[string]interface{}) {
	for _, tc := range cases {
		fixture := tc.fixture
		if fixture == nil {
			fixture = entityPopulationFixture
		}
		t.Run(tc.name+"/success", func(t *testing.T) {
			executor := fixture(t)
			if tc.setup != nil {
				tc.setup(t, executor)
			}
			result, err := executor.Execute(context.Background(), tc.query, parameters)
			require.NoError(t, err)
			require.Len(t, result.Rows, tc.rows)
		})
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				executor := fixture(t)
				if tc.setup != nil {
					tc.setup(t, executor)
				}
				ctx := &rowConsumerContext{Context: context.Background(), consumer: tc.consumer, deadline: deadline, after: tc.after, done: make(chan struct{})}
				if tc.pass != "" {
					ctx.line = rowConsumerPassLine(t, tc.file, tc.consumer, tc.pass)
				}
				_, err := executor.Execute(ctx, tc.query, parameters)
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
				}
				require.ErrorIs(t, err, want, "a canceled population must not report success")
				require.Equal(t, tc.after, ctx.checks, "must cancel inside the requested consumer")
				require.False(t, ctx.triggered.IsZero())
				require.Less(t, time.Since(ctx.triggered), 2*time.Second, "canceled pass must stop promptly")
			})
		}
	}
}
