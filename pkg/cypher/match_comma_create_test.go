package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newRegression191Executor(t *testing.T, async bool) *StorageExecutor {
	t.Helper()
	baseStore := newTestMemoryEngine(t)
	var engine storage.Engine = baseStore
	if async {
		engine = baseStore
	}
	return NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
}

func TestRegression191Followup_MatchCommaCreateRelationship(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit_memory", true: "implicit_async"}[async], func(t *testing.T) {
			exec := newRegression191Executor(t, async)
			ctx := context.Background()

			_, err := exec.Execute(ctx, `CREATE (t:Task {project:"dimension"})`, nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, `CREATE (s:Session {id:"2026-06-02_13-54-13"})`, nil)
			require.NoError(t, err)

			res, err := exec.Execute(ctx, `
		MATCH (t:Task {project:"dimension"}),
		      (s:Session {id:"2026-06-02_13-54-13"})
		WHERE NOT (t)-[:RAISED_IN]->(s)
		CREATE (t)-[:RAISED_IN]->(s)
		RETURN count(*) AS edges_added
	`, nil)
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)
			require.EqualValues(t, 1, res.Rows[0][0])
			require.Equal(t, 1, res.Stats.RelationshipsCreated)

			verify, err := exec.Execute(ctx, `
		MATCH (t:Task)-[r:RAISED_IN]->(s:Session)
		RETURN count(r) AS c
	`, nil)
			require.NoError(t, err)
			require.Len(t, verify.Rows, 1)
			require.EqualValues(t, 1, verify.Rows[0][0])
		})
	}
}

func TestRegression191Followup_MatchCreateNodeToMatchedNode(t *testing.T) {
	exec := newRegression191Executor(t, true)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (s:Session {id:"sid"})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (s:Session {id:"sid"})
		CREATE (n:Foo)-[:RAISED_IN]->(s)
		RETURN n
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, 1, res.Stats.NodesCreated)
	require.Equal(t, 1, res.Stats.RelationshipsCreated)

	verifyNodes, err := exec.Execute(ctx, `MATCH (n:Foo) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, verifyNodes.Rows, 1)
	require.EqualValues(t, 1, verifyNodes.Rows[0][0])

	verifyRels, err := exec.Execute(ctx, `MATCH (:Foo)-[r:RAISED_IN]->(:Session) RETURN count(r)`, nil)
	require.NoError(t, err)
	require.Len(t, verifyRels.Rows, 1)
	require.EqualValues(t, 1, verifyRels.Rows[0][0])
}

func TestRegression191Followup_SetSelfStringConcat(t *testing.T) {
	exec := newRegression191Executor(t, true)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (h:Heuristic {content:"hello"})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (h:Heuristic)
		WHERE h.content = "hello"
		SET h.content = h.content + ", world"
	`, nil)
	require.NoError(t, err)

	got, err := exec.Execute(ctx, `MATCH (h:Heuristic) RETURN h.content`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 1)
	require.Equal(t, "hello, world", got.Rows[0][0])
}

func TestRegression191Followup_CreateSetClauseBoundary(t *testing.T) {
	exec := newRegression191Executor(t, true)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `CREATE (t:Foo) SET t.x = "wyrd" CREATE (u:Foo) RETURN t.x`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "wyrd", res.Rows[0][0])

	verify, err := exec.Execute(ctx, `MATCH (f:Foo) RETURN count(f)`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 2, verify.Rows[0][0])
}

func TestRegression191Followup_CreateSetClauseBoundaryExplicitTransaction(t *testing.T) {
	exec := newRegression191Executor(t, true)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `BEGIN`, nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, `CREATE (t:Foo) SET t.x = "wyrd" CREATE (u:Foo) RETURN t.x`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "wyrd", res.Rows[0][0])
	_, err = exec.Execute(ctx, `COMMIT`, nil)
	require.NoError(t, err)

	verify, err := exec.Execute(ctx, `MATCH (f:Foo) RETURN count(f)`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 2, verify.Rows[0][0])
}

func TestRegression191Followup_InlinePropertyMatchAfterSet(t *testing.T) {
	exec := newRegression191Executor(t, true)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (h:Heuristic {title:"T"})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `MATCH (h:Heuristic {title:"T"}) SET h.tested_against = "v"`, nil)
	require.NoError(t, err)

	whereRes, err := exec.Execute(ctx, `MATCH (h:Heuristic) WHERE h.tested_against = "v" RETURN count(h)`, nil)
	require.NoError(t, err)
	require.Len(t, whereRes.Rows, 1)
	require.EqualValues(t, 1, whereRes.Rows[0][0])

	inlineRes, err := exec.Execute(ctx, `MATCH (h:Heuristic {tested_against:"v"}) RETURN count(h)`, nil)
	require.NoError(t, err)
	require.Len(t, inlineRes.Rows, 1)
	require.EqualValues(t, 1, inlineRes.Rows[0][0])
}
