package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFindStandaloneSetInMergeSegmentFrom_Branches(t *testing.T) {
	segment := "MERGE (n:Node {id:'1'}) ON CREATE SET n.created = true ON MATCH SET n.seen = true SET n.final = true RETURN n"

	idx := findStandaloneSetInMergeSegmentFrom(segment, -10)
	require.Greater(t, idx, 0)
	require.Equal(t, "SET n.final = true RETURN n", segment[idx:])

	require.Equal(t, -1, findStandaloneSetInMergeSegmentFrom(segment, idx+1))
	require.Equal(t, -1, findStandaloneSetInMergeSegmentFrom("MERGE (n:Node {asset:'x'}) RETURN n", 0))
}

func TestApplyWithProjection_ExpressionFallbackBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_with_more_cov"))
	ctx := context.Background()

	n := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Node"},
		Properties: map[string]interface{}{"name": "alice"},
	}
	nodeCtx := map[string]*storage.Node{"n": n}
	relCtx := map[string]*storage.Edge{}
	scalarCtx := map[string]interface{}{"score": int64(7)}

	remaining, outNodes, outRels, outScalars := exec.applyWithProjection(ctx, "toUpper(n.name) AS upper RETURN upper", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, "RETURN upper", remaining)
	require.Empty(t, outNodes)
	require.Empty(t, outRels)
	require.Equal(t, map[string]interface{}{"upper": "ALICE"}, outScalars)

	// Unknown expression resolves to itself and must be skipped instead of leaking literal text.
	remaining, outNodes, outRels, outScalars = exec.applyWithProjection(ctx, "ghost.prop AS g RETURN g", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, "RETURN g", remaining)
	require.Empty(t, outNodes)
	require.Empty(t, outRels)
	require.Empty(t, outScalars)
}

func TestExecuteMergeWithChain_ChainBreakSkipsIntermediateClauses(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_chain_skip_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `
		MERGE (a:Node {id:'a-skip'})
		MERGE (b:Node {id:'b-skip'})
		WITH a, b
		MATCH (m:Missing {id:'none'})
		FOREACH (i IN [1,2] | CREATE (tmp:Tmp {k:i}))
		RETURN a.id AS aid, b.id AS bid
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid", "bid"}, res.Columns)
	require.Empty(t, res.Rows)

	// Initial MERGE clauses still execute before the chain break.
	verifyNodes, err := exec.Execute(ctx, "MATCH (n:Node) WHERE n.id IN ['a-skip','b-skip'] RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, verifyNodes.Rows[0][0])

	// FOREACH must be skipped after chain break.
	verifyTmp, err := exec.Execute(ctx, "MATCH (t:Tmp) RETURN count(t)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verifyTmp.Rows[0][0])
}

func TestApplyWithProjection_EmptyProjectionKeepsContext(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_with_empty_cov"))
	ctx := context.Background()
	node := &storage.Node{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "x"}}
	nodeCtx := map[string]*storage.Node{"n": node}
	relCtx := map[string]*storage.Edge{}
	scalarCtx := map[string]interface{}{"s": int64(1)}

	remaining, outNodes, outRels, outScalars := exec.applyWithProjection(ctx, "MATCH (n) RETURN n", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, "MATCH (n) RETURN n", remaining)
	require.Equal(t, nodeCtx, outNodes)
	require.Empty(t, outRels)
	require.Equal(t, scalarCtx, outScalars)
}

func TestExecuteMergeWithContext_RelationshipChainAndSet(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_ctx_rel_chain_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	a := &storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "a"}}
	b := &storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "b"}}
	c := &storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "c"}}
	_, err := store.CreateNode(a)
	require.NoError(t, err)
	_, err = store.CreateNode(b)
	require.NoError(t, err)
	_, err = store.CreateNode(c)
	require.NoError(t, err)

	nodeCtx := map[string]*storage.Node{"a": a, "b": b, "c": c}
	relCtx := map[string]*storage.Edge{}

	q := "MERGE (a)-[r:KNOWS]->(b) SET r.weight = 1 MERGE (b)-[s:KNOWS]->(c) RETURN r.weight AS rw"
	res, err := exec.executeMergeWithContext(ctx, q, nodeCtx, relCtx)
	require.NoError(t, err)
	require.Equal(t, []string{"rw"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])
	require.EqualValues(t, 2, res.Stats.RelationshipsCreated)

	verify, err := exec.Execute(ctx, "MATCH (x:Person {id:'a'})-[r:KNOWS]->(y:Person {id:'b'}) RETURN r.weight", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])

	// Second execution should reuse existing relationships (MERGE semantics).
	res2, err := exec.executeMergeWithContext(ctx, q, nodeCtx, relCtx)
	require.NoError(t, err)
	require.EqualValues(t, 0, res2.Stats.RelationshipsCreated)
}

func TestExecuteMergeWithChain_RelationshipBranchesAndChainBreak(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_chain_rel_branches_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `
		MERGE (a:A {id:'a'})
		MERGE (b:B {id:'b'})
		MERGE (a)-[:R0]->(b)
		WITH a
		MATCH (b:B {id:'b'}) MERGE (a)-[:R1]->(b)
		MERGE (a)-[:R2]->(b)
		RETURN a.id AS aid
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a", res.Rows[0][0])
	require.EqualValues(t, 3, res.Stats.RelationshipsCreated)

	verify, err := exec.Execute(ctx, "MATCH (a:A {id:'a'})-[r]->(b:B {id:'b'}) RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, verify.Rows[0][0])

	// Malformed MATCH rejects the statement before any MERGE writes.
	_, err = exec.Execute(ctx, `
		MERGE (x:A {id:'x'})
		WITH x
		MATCH (bad
		RETURN x.id AS xid
	`, nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
	verify, err = exec.Execute(ctx, "MATCH (x:A {id:'x'}) RETURN count(x)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])
}
