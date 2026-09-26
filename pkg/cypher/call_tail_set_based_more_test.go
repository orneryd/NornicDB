package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteCallTailSetBased_RelationshipRewriteWithScalar(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "call_tail_set_based_rel_rewrite")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"N"}, Properties: map[string]interface{}{"id": id}})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ab", Type: "REL", StartNode: "a", EndNode: "b"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "bc", Type: "REL", StartNode: "b", EndNode: "c"}))

	a, err := store.GetNode("a")
	require.NoError(t, err)
	b, err := store.GetNode("b")
	require.NoError(t, err)

	seed := &ExecuteResult{
		Columns: []string{"n", "score"},
		Rows: [][]interface{}{
			{a, int64(10)},
			{b, int64(20)},
		},
	}

	res, ok := exec.executeCallTailSetBased(
		ctx,
		seed,
		"MATCH (n)-[r]->(m) WITH n, count(r) AS c, score RETURN n.id AS id, c, score ORDER BY id",
		[]int{0, 1},
		[]string{"id", "c", "score"},
	)
	require.True(t, ok)
	require.Equal(t, []string{"id", "c", "score"}, res.Columns)
	require.Equal(t, [][]interface{}{{"a", int64(1), int64(10)}, {"b", int64(1), int64(20)}}, res.Rows)
}

func TestExecuteCallTailSetBased_RelationshipRewriteErrorAndConstraintDetection(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_set_based_rel_err"))
	ctx := context.Background()

	node := &storage.Node{ID: storage.NodeID("n1")}
	seed := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{node}}}

	res, ok := exec.executeCallTailSetBased(ctx, seed, "MATCH (n)-[r]->(m) WITH n RETURN (", []int{0}, nil)
	require.False(t, ok)
	require.Nil(t, res)

	require.True(t, callTailHasRelationshipTypeConstraint(`MATCH (a)-[:R]->(b) RETURN a`))
	require.True(t, callTailHasRelationshipTypeConstraint(`MATCH (a)-[r:R]->(b) RETURN a`))
	require.False(t, callTailHasRelationshipTypeConstraint(`MATCH (a)-[r]->(b) RETURN ':' + a.name`))
}

func TestCallTailReturnOptionsAndVersionNonDevCommit(t *testing.T) {
	ret, orderBy, limit, skip := splitCallTailProjectionModifiers("n ORDER BY n.name ASC LIMIT not_int SKIP 3")
	require.Equal(t, "n", ret)
	require.Equal(t, "ORDER BY n.name ASC", orderBy)
	require.Equal(t, "not_int", limit)
	require.Equal(t, "3", skip)

	ret, orderBy, limit, skip = splitCallTailProjectionModifiers("n SKIP 1 LIMIT 2")
	require.Equal(t, "n", ret)
	require.Equal(t, "", orderBy)
	require.Equal(t, "2", limit)
	require.Equal(t, "1", skip)

	originalCommit := buildinfo.Commit
	defer func() { buildinfo.Commit = originalCommit }()
	buildinfo.Commit = "278e403abcd"

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_version_nondev"))
	res, err := exec.callNornicDbVersion()
	require.NoError(t, err)
	require.Equal(t, []string{"version", "build", "edition"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "278e403", res.Rows[0][1])
}
