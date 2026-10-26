package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPreparedCountDeltaProjectsPrivateImagesWithoutCopies(t *testing.T) {
	vector := make([]float32, 16384)
	vector[0] = 42
	p := &PreparedEntityCommit{namespace: "test", changes: []StoredEntityChange{{AfterNode: &Node{ID: "test:hidden", VisibilitySuppressed: true, ChunkEmbeddings: [][]float32{vector}}}, {BeforeNode: &Node{ID: "test:deleted"}}, {AfterEdge: &Edge{ID: "test:e", VisibilitySuppressed: true}}, {AfterNode: &Node{ID: "test-other:foreign"}}, {AfterEdge: &Edge{ID: "foreign:e"}}}}
	nodes, edges := p.CountDelta()
	require.Zero(t, nodes)
	require.EqualValues(t, 1, edges)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		n, e := p.CountDelta()
		if n != 0 || e != 1 {
			panic("projection changed")
		}
	}))
	cloned := p.Changes()
	cloned[0].AfterNode.ChunkEmbeddings[0][0] = -1
	cloned[0].AfterNode.ID = "other:changed"
	require.Equal(t, float32(42), p.changes[0].AfterNode.ChunkEmbeddings[0][0])
	nodes, edges = p.CountDelta()
	require.Zero(t, nodes)
	require.EqualValues(t, 1, edges)
}
func TestPreparedCountDeltaIncludesCascadeAndHiddenStoredEntities(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err = b.CreateNode(&Node{ID: id, VisibilitySuppressed: true})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", VisibilitySuppressed: true}))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
	p, err := tx.PrepareEntityCommit(context.Background(), "count-cascade")
	require.NoError(t, err)
	defer p.Abort()
	nodes, edges := p.CountDelta()
	require.EqualValues(t, -1, nodes)
	require.EqualValues(t, -1, edges)
	fullNodes, fullEdges := StoredEntityCountDelta("test", p.Changes())
	require.Equal(t, nodes, fullNodes)
	require.Equal(t, edges, fullEdges)
}
