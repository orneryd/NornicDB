package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRegression_MatchCartesianCreateRelationshipCreatesEdge(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:A {n: 1}), (b:B {n: 2})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (a:A {n: 1}), (b:B {n: 2}) CREATE (a)-[:R]->(b) RETURN count(*) AS n", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 1, res.Stats.RelationshipsCreated)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.EqualValues(t, 1, res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (a:A)-[:R]->(b:B) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func TestRegression_MatchCreateNodeWithRelationshipToBoundNode(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Session {id: 'sid'})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (s:Session {id: 'sid'}) CREATE (n:Foo)-[:RAISED_IN]->(s) RETURN count(n)", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 1, res.Stats.NodesCreated)
	require.EqualValues(t, 1, res.Stats.RelationshipsCreated)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (:Foo)-[:RAISED_IN]->(:Session {id:'sid'}) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func TestRegression_SetSelfReferenceConcatenationPersists(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (h:Heuristic {content:"hello"})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `MATCH (h:Heuristic) WHERE h.content = "hello" SET h.content = h.content + ", world"`, nil)
	require.NoError(t, err)

	verify, err := exec.Execute(ctx, "MATCH (h:Heuristic) RETURN h.content", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, "hello, world", verify.Rows[0][0])
}

func TestRegression_SetStopsAtNextClauseKeyword(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `CREATE (t:Foo) SET t.x = "wyrd" CREATE (u:Foo) RETURN t.x`, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "wyrd", res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (f:Foo) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 2, verify.Rows[0][0])
}

func TestRegression_InlinePropertyMatchMatchesWhereForm(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (h:Heuristic {title:"t1", tested_against:"v"})`, nil)
	require.NoError(t, err)

	whereRes, err := exec.Execute(ctx, `MATCH (h:Heuristic) WHERE h.tested_against = "v" RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Len(t, whereRes.Rows, 1)

	inlineRes, err := exec.Execute(ctx, `MATCH (h:Heuristic {tested_against:"v"}) RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Len(t, inlineRes.Rows, 1)
	require.Equal(t, whereRes.Rows[0][0], inlineRes.Rows[0][0])

	_, err = exec.Execute(ctx, `MATCH (h:Heuristic {title:"t1"}) SET h.tested_against = "v2"`, nil)
	require.NoError(t, err)

	whereRes2, err := exec.Execute(ctx, `MATCH (h:Heuristic) WHERE h.tested_against = "v2" RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Len(t, whereRes2.Rows, 1)

	inlineRes2, err := exec.Execute(ctx, `MATCH (h:Heuristic {tested_against:"v2"}) RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Len(t, inlineRes2.Rows, 1)
	require.Equal(t, whereRes2.Rows[0][0], inlineRes2.Rows[0][0])
}
