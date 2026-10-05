package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClauseSemanticsExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "clause-semantics")
	return NewStorageExecutor(store), context.Background()
}

func executeClauseQueries(t *testing.T, exec *StorageExecutor, ctx context.Context, queries ...string) {
	t.Helper()
	for _, query := range queries {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "query failed: %s", query)
	}
}

func seedDistinctAggregationChain(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	executeClauseQueries(t, exec, ctx,
		`CREATE (:Repository {id: 'pdc-repo'})`,
		`CREATE (:Workload {id: 'pdc-w'})`,
		`CREATE (:Platform {id: 'pdc-platform'})`,
		`CREATE (:WorkloadInstance {id: 'pdc-i1'})`,
		`CREATE (:WorkloadInstance {id: 'pdc-i2'})`,
		`CREATE (:WorkloadInstance {id: 'pdc-i3'})`,
		`MATCH (r:Repository {id: 'pdc-repo'}), (w:Workload {id: 'pdc-w'}) CREATE (r)-[:DEFINES]->(w)`,
		`MATCH (i:WorkloadInstance), (w:Workload {id: 'pdc-w'}) CREATE (i)-[:INSTANCE_OF]->(w)`,
		`MATCH (i:WorkloadInstance), (p:Platform {id: 'pdc-platform'}) CREATE (i)-[:RUNS_ON]->(p)`,
	)
}

func seedDisconnectedRelationshipGraph(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	executeClauseQueries(t, exec, ctx,
		`CREATE (:PDG {id: 's1'})`,
		`CREATE (:PDG {id: 't1'})`,
		`MATCH (s:PDG {id: 's1'}), (t:PDG {id: 't1'}) CREATE (s)-[:RDG]->(t) CREATE (t)-[:RDG]->(s)`,
	)
}

// Regression: initially reported in #362.
func TestDistinctAggregatesUseValueIdentity(t *testing.T) {
	t.Run("node variable after multi-match", func(t *testing.T) {
		exec, ctx := newClauseSemanticsExecutor(t)
		seedDistinctAggregationChain(t, exec, ctx)

		result, err := exec.Execute(ctx, `
			MATCH (r:Repository {id: 'pdc-repo'})-[:DEFINES]->(w:Workload)
			MATCH (w)<-[:INSTANCE_OF]-(i:WorkloadInstance)
			MATCH (i)-[:RUNS_ON]->(p:Platform)
			RETURN count(DISTINCT p) AS count
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	})

	t.Run("scalar over node cross product", func(t *testing.T) {
		exec, ctx := newClauseSemanticsExecutor(t)
		executeClauseQueries(t, exec, ctx,
			`CREATE (:PDG {id: 's1'})`,
			`CREATE (:PDG {id: 't1'})`,
		)

		result, err := exec.Execute(ctx, `MATCH (a:PDG), (x:PDG) WHERE a.id IN ['s1', 't1'] RETURN count(DISTINCT a.id) AS n`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	})

	t.Run("relationship variable", func(t *testing.T) {
		exec, ctx := newClauseSemanticsExecutor(t)
		seedDisconnectedRelationshipGraph(t, exec, ctx)

		result, err := exec.Execute(ctx, `
			MATCH (a:PDG)-[r:RDG]-(b:PDG)
			WHERE a.id IN ['s1', 't1'] AND b.id IN ['s1', 't1']
			RETURN collect(DISTINCT r) AS relationships
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		relationships, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok, "expected relationship list, got %T", result.Rows[0][0])
		require.Len(t, relationships, 2)
	})
}

// Regression: initially reported in #363.
func TestWithDistinctAfterChainedMatches(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	seedDistinctAggregationChain(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (r:Repository {id: 'pdc-repo'})-[:DEFINES]->(w:Workload)
		MATCH (w)<-[:INSTANCE_OF]-(i:WorkloadInstance)
		MATCH (i)-[:RUNS_ON]->(p:Platform)
		WITH DISTINCT p
		RETURN count(p) AS count
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// Regression: initially reported in #364.
func TestAnonymousStartOptionalMatchBindsIncomingRelationship(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	executeClauseQueries(t, exec, ctx,
		`CREATE (:Function {id: 'pdc-fn'})`,
		`CREATE (:Function {id: 'pdc-o1'})`,
		`CREATE (:Function {id: 'pdc-o2'})`,
		`CREATE (:Function {id: 'pdc-c1'})`,
		`CREATE (:Function {id: 'pdc-c2'})`,
		`CREATE (:Function {id: 'pdc-c3'})`,
		`MATCH (e:Function {id: 'pdc-fn'}), (o:Function) WHERE o.id IN ['pdc-o1', 'pdc-o2'] CREATE (e)-[:CALLS]->(o)`,
		`MATCH (e:Function {id: 'pdc-fn'}), (c:Function) WHERE c.id IN ['pdc-c1', 'pdc-c2', 'pdc-c3'] CREATE (c)-[:CALLS]->(e)`,
	)

	result, err := exec.Execute(ctx, `
		MATCH (e:Function {id: 'pdc-fn'})
		OPTIONAL MATCH (e)-[outgoingRel]->()
		OPTIONAL MATCH ()-[incomingRel]->(e)
		RETURN elementId(outgoingRel) AS outgoing, elementId(incomingRel) AS incoming
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 6)
	for _, row := range result.Rows {
		require.NotNil(t, row[0])
		require.NotNil(t, row[1])
	}
}

// Regression: initially reported in #374.
func TestBoundEndIncomingPatternsExpandInDeclaredDirection(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	executeClauseQueries(t, exec, ctx,
		`CREATE (:Function {id: 'pom-fn'})`,
		`CREATE (:Function {id: 'pom-o1'})`,
		`CREATE (:Function {id: 'pom-o2'})`,
		`CREATE (:Function {id: 'pom-c1'})`,
		`CREATE (:Function {id: 'pom-c2'})`,
		`CREATE (:Function {id: 'pom-c3'})`,
		`MATCH (a:Function {id: 'pom-fn'}), (b:Function) WHERE b.id IN ['pom-o1', 'pom-o2'] CREATE (a)-[:CALLS]->(b)`,
		`MATCH (a:Function), (b:Function {id: 'pom-fn'}) WHERE a.id IN ['pom-c1', 'pom-c2', 'pom-c3'] CREATE (a)-[:CALLS]->(b)`,
	)

	t.Run("optional match binds named start and relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Function {id: 'pom-fn'})
			OPTIONAL MATCH (c)-[i:CALLS]->(e)
			RETURN c.id AS source, elementId(i) AS edge
			ORDER BY source
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		require.Equal(t, []string{"pom-c1", "pom-c2", "pom-c3"}, []string{
			result.Rows[0][0].(string), result.Rows[1][0].(string), result.Rows[2][0].(string),
		})
		for _, row := range result.Rows {
			require.NotNil(t, row[1])
		}
	})

	t.Run("optional match after aggregating with preserves values", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Function {id: 'pom-fn'})
			OPTIONAL MATCH (e)-[o:CALLS]->()
			WITH e, count(DISTINCT o) AS outgoing
			OPTIONAL MATCH (c)-[i:CALLS]->(e)
			RETURN outgoing, count(DISTINCT i) AS incoming
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2), int64(3)}}, result.Rows)
	})

	t.Run("pattern comprehension returns incoming bindings", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Function {id: 'pom-fn'})
			RETURN [(c)-[i:CALLS]->(e) | c.id] AS sources
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{"pom-c1", "pom-c2", "pom-c3"}}}, result.Rows)
	})

	t.Run("count subquery counts incoming bindings", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Function {id: 'pom-fn'})
			RETURN COUNT { ()-[:CALLS]->(e) } AS incoming
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	})

	t.Run("pattern expressions discard optional null rows", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Function {id: 'pom-c1'})
			RETURN [(c)-[:CALLS]->(e) | c.id] AS sources,
			       COUNT { ()-[:CALLS]->(e) } AS incoming
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{}, int64(0)}}, result.Rows)
	})
}

// Regression: initially reported in #365.
func TestMixedRelationshipAndNodePatternsCrossProduct(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	seedDisconnectedRelationshipGraph(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (a:PDG)-[r:RDG]->(b:PDG), (x:PDG)
		WHERE a.id IN ['s1', 't1']
		RETURN count(r) AS n
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)

	distinctResult, err := exec.Execute(ctx, `
		MATCH (a:PDG)-[r:RDG]->(b:PDG), (x:PDG)
		WHERE a.id IN ['s1', 't1']
		RETURN count(DISTINCT r) AS n
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, distinctResult.Rows)

	filteredResult, err := exec.Execute(ctx, `
		MATCH (a:PDG)-[r:RDG]->(b:PDG), (x:PDG)
		WHERE a.id = x.id
		RETURN count(r) AS n
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, filteredResult.Rows)
}

// Regression: initially reported in #366.
func TestMatchUnwindMatchMergePreservesBindings(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	executeClauseQueries(t, exec, ctx,
		`CREATE (:Function {id: 'poe-T'})`,
		`CREATE (:Function {id: 'poe-C1'})`,
		`CREATE (:Function {id: 'poe-C2'})`,
	)

	result, err := exec.Execute(ctx, `
		MATCH (t:Function {id: 'poe-T'})
		UNWIND ['poe-C1', 'poe-C2'] AS cid
		MATCH (c:Function {id: cid})
		MERGE (c)-[:CALLS]->(t)
	`, nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Stats.RelationshipsCreated)

	countResult, err := exec.Execute(ctx, `MATCH (:Function)-[r:CALLS]->(:Function {id: 'poe-T'}) RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, countResult.Rows)
}

func TestRelationshipMergeCreatesUnboundEndpoint(t *testing.T) {
	for _, pattern := range []string{
		"(endpoint)-[:CALLS]->(t)",
		"(t)-[:CALLS]->(endpoint)",
		"(endpoint)<-[:CALLS]-(t)",
		"(t)<-[:CALLS]-(endpoint)",
		"(endpoint)-[:CALLS]-(t)",
		"(t)-[:CALLS]-(endpoint)",
	} {
		t.Run(pattern, func(t *testing.T) {
			exec, ctx := newClauseSemanticsExecutor(t)
			executeClauseQueries(t, exec, ctx, `CREATE (:Function {id: 'poe-T'})`)
			query := "MATCH (t:Function {id:'poe-T'}) MERGE " + pattern + " RETURN size(labels(endpoint)) AS labelCount, t.id AS id"
			for iteration := 0; iteration < 2; iteration++ {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0), "poe-T"}}, result.Rows)
				if iteration == 0 {
					require.EqualValues(t, 1, result.Stats.NodesCreated)
					require.EqualValues(t, 1, result.Stats.RelationshipsCreated)
				} else {
					require.Zero(t, result.Stats.NodesCreated)
					require.Zero(t, result.Stats.RelationshipsCreated)
				}
			}
			stored, err := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, stored.Rows)
			stored, err = exec.Execute(ctx, "MATCH ()-[r:CALLS]->() RETURN count(r)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
		})
	}
}

// Regression: initially reported in #367.
func TestNestedAggregateExpression(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	seedDisconnectedRelationshipGraph(t, exec, ctx)

	tests := []struct {
		name  string
		expr  string
		value int64
	}{
		{name: "collect", expr: "size(collect(r))", value: 4},
		{name: "collect distinct", expr: "size(collect(DISTINCT r))", value: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, `
				MATCH (a:PDG)-[r:RDG]-(b:PDG)
				WHERE a.id IN ['s1', 't1'] AND b.id IN ['s1', 't1']
				RETURN `+test.expr+` AS n
			`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.value}}, result.Rows)
		})
	}
}

// Regression: initially reported in #368.
func TestCollectDistinctPropertyAfterChainedMatches(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	seedDistinctAggregationChain(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (r:Repository {id: 'pdc-repo'})-[:DEFINES]->(w:Workload)
		MATCH (w)<-[:INSTANCE_OF]-(i:WorkloadInstance)
		MATCH (i)-[:RUNS_ON]->(p:Platform)
		RETURN collect(DISTINCT p.id) AS ids
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"pdc-platform"}}}, result.Rows)
}

func seedReverseOptionalTraversal(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	executeClauseQueries(t, exec, ctx,
		`CREATE (:Repository {id: 'ag-repo'})`,
		`CREATE (:File {id: 'ag-f', relative_path: 'x.go'})`,
		`CREATE (:Function {id: 'ag-e'})`,
		`MATCH (r:Repository {id: 'ag-repo'}), (f:File {id: 'ag-f'}) CREATE (r)-[:REPO_CONTAINS]->(f)`,
		`MATCH (f:File {id: 'ag-f'}), (e:Function {id: 'ag-e'}) CREATE (f)-[:CONTAINS]->(e)`,
	)
}

// Regression: initially reported in #369.
func TestReverseOptionalMatchBindsEveryHop(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "single two-hop clause",
			query: `
				MATCH (e:Function {id: 'ag-e'})
				OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(r:Repository)
				RETURN f.relative_path AS fp, elementId(r) AS re, r IS NULL AS rnull
			`,
		},
		{
			name: "split clauses",
			query: `
				MATCH (e:Function {id: 'ag-e'})
				OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File)
				OPTIONAL MATCH (f)<-[:REPO_CONTAINS]-(r:Repository)
				RETURN f.relative_path AS fp, elementId(r) AS re, r IS NULL AS rnull
			`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, ctx := newClauseSemanticsExecutor(t)
			seedReverseOptionalTraversal(t, exec, ctx)
			result, err := exec.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, "x.go", result.Rows[0][0])
			require.NotNil(t, result.Rows[0][1])
			assert.Equal(t, false, result.Rows[0][2])
		})
	}
}

// Regression: initially reported in #370.
func TestNullPropertyProjectionReturnsNull(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	executeClauseQueries(t, exec, ctx, `CREATE (:Function {id: 'ag-e'})`)

	result, err := exec.Execute(ctx, `
		MATCH (e:Function {id: 'ag-e'})
		OPTIONAL MATCH (e)<-[:MISSING]-(r:Repository)
		RETURN r.id AS rid
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, result.Rows)

	value := exec.resolveReturnExprFromVarMap(ctx, "r.id", map[string]interface{}{}, "", "", nil, nil)
	require.Nil(t, value)
}

func seedGroupedWorkloadRows(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	executeClauseQueries(t, exec, ctx,
		`CREATE (:CloudAction {id: 'b-act', action: 's3:PutObject'})`,
		`CREATE (:Workload {id: 'b-w1'})`,
		`CREATE (:Workload {id: 'b-w2'})`,
		`CREATE (:Function {id: 'b-fn-one', uid: 'b-fn-one'})`,
		`CREATE (:Function {id: 'b-fn-dup', uid: 'b-fn-dup'})`,
		`CREATE (:Function {id: 'b-fn-amb', uid: 'b-fn-amb'})`,
		`MATCH (a:Function), (b:CloudAction {id: 'b-act'}) CREATE (a)-[:INVOKES_CLOUD_ACTION]->(b)`,
		`MATCH (a:Function {id: 'b-fn-one'}), (b:Workload {id: 'b-w1'}) CREATE (a)-[:RUNS_IN]->(b)`,
		`MATCH (a:Function {id: 'b-fn-dup'}), (b:Workload {id: 'b-w1'}) CREATE (a)-[:RUNS_IN]->(b) CREATE (a)-[:RUNS_IN]->(b)`,
		`MATCH (a:Function {id: 'b-fn-amb'}), (b:Workload) CREATE (a)-[:RUNS_IN]->(b)`,
	)
}

// Regression: initially reported in #371.
func TestAggregatedWithWhereFiltersChainedMatchRows(t *testing.T) {
	exec, ctx := newClauseSemanticsExecutor(t)
	seedGroupedWorkloadRows(t, exec, ctx)
	params := map[string]interface{}{
		"function_uids": []string{"b-fn-one", "b-fn-dup", "b-fn-amb"},
	}
	prefix := `
		MATCH (fn:Function)-[:INVOKES_CLOUD_ACTION]->(action:CloudAction)
		WHERE fn.uid IN $function_uids
		MATCH (fn)-[:RUNS_IN]->(workload:Workload)
		WITH fn, action, collect(DISTINCT workload) AS workloads
	`

	tests := []struct {
		name string
		tail string
		rows [][]interface{}
	}{
		{
			name: "filter by aggregate expression",
			tail: `WHERE size(workloads) = 1 RETURN fn.uid AS f, size(workloads) AS n ORDER BY f`,
			rows: [][]interface{}{{"b-fn-dup", int64(1)}, {"b-fn-one", int64(1)}},
		},
		{
			name: "filter by projected aggregate scalar",
			tail: `WITH fn, action, workloads, size(workloads) AS n WHERE n = 1 RETURN fn.uid AS f, n ORDER BY f`,
			rows: [][]interface{}{{"b-fn-dup", int64(1)}, {"b-fn-one", int64(1)}},
		},
		{
			name: "filter remains independent of return projection",
			tail: `WHERE size(workloads) > 1 RETURN fn.uid AS f ORDER BY f`,
			rows: [][]interface{}{{"b-fn-amb"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, prefix+test.tail, params)
			require.NoError(t, err)
			require.Equal(t, test.rows, result.Rows)
		})
	}
}
