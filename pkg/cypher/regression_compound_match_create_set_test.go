package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRegression_CompoundMatchCreateRelationshipOnly(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reg_match_create_rel_only")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:A {n:1}), (b:B {n:2})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (a:A {n:1}), (b:B {n:2}) CREATE (a)-[:R]->(b) RETURN count(*) AS n", nil)
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 1, res.Stats.RelationshipsCreated)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (a:A)-[:R]->(b:B) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func TestRegression_MatchCreateNewNodeWithRelationshipToExisting(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reg_match_create_node_rel")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Session {id:'sid'})", nil)
	require.NoError(t, err)

	combos, _, err := exec.pipelineApplyMatch(ctx, []pipelineRow{{}}, "MATCH (s:Session {id:'sid'})")
	require.NoError(t, err)
	require.Len(t, combos, 1)
	require.NotNil(t, combos[0]["s"])

	res, err := exec.Execute(ctx, "MATCH (s:Session {id:'sid'}) CREATE (n:Foo)-[:RAISED_IN]->(s) RETURN count(n)", nil)
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 1, res.Stats.NodesCreated)
	require.EqualValues(t, 1, res.Stats.RelationshipsCreated)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (n:Foo)-[:RAISED_IN]->(:Session {id:'sid'}) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func TestRegression_SetSelfReferenceConcatPersistsValue(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reg_set_concat")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Heuristic {content:'hello'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (h:Heuristic) WHERE h.content = 'hello' SET h.content = h.content + ', world'", nil)
	require.NoError(t, err)

	verify, err := exec.Execute(ctx, "MATCH (h:Heuristic) RETURN h.content", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, "hello, world", verify.Rows[0][0])
}

func TestRegression_SetClauseTerminatesAtNextClauseKeyword(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reg_set_clause_boundary")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, "CREATE (t:Foo) SET t.x = 'wyrd' CREATE (u:Foo) RETURN t.x", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "wyrd", res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (f:Foo) RETURN count(f)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 2, verify.Rows[0][0])
}

func TestRegression_InlinePropertyMatchConsistentWithWhereAfterSet(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reg_inline_vs_where")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (h:Heuristic {title:'t1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (h:Heuristic {title:'t1'}) SET h.tested_against = 'v'", nil)
	require.NoError(t, err)

	resWhere, err := exec.Execute(ctx, "MATCH (h:Heuristic) WHERE h.tested_against = 'v' RETURN h.title", nil)
	require.NoError(t, err)
	require.Len(t, resWhere.Rows, 1)
	require.Equal(t, "t1", resWhere.Rows[0][0])

	resInline, err := exec.Execute(ctx, "MATCH (h:Heuristic {tested_against:'v'}) RETURN h.title", nil)
	require.NoError(t, err)
	require.Len(t, resInline.Rows, 1)
	require.Equal(t, "t1", resInline.Rows[0][0])
}
