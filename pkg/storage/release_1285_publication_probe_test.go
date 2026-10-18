package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelease1285DirectPublication(t *testing.T) {
	for _, writer := range []string{"create", "update"} {
		for _, mutation := range []string{"delete", "shrink"} {
			t.Run(writer+"/"+mutation, func(t *testing.T) {
				e := newTestEngine(t)
				id := NodeID("test:direct-publication")
				if writer == "update" {
					_, err := e.CreateNode(&Node{ID:id, Labels:[]string{"Doc"}, Properties:map[string]any{"text":"same"}})
					require.NoError(t, err)
				}
				interposed := false
				restore := setCommitTailHook(func() {
					setCommitTailHook(nil)
					interposed = true
					if mutation == "delete" { require.NoError(t, e.DeleteNode(id)) } else {
						require.NoError(t, e.UpdateNode(&Node{ID:id, Labels:[]string{"Doc"}, Properties:map[string]any{"text":"same"}, ChunkEmbeddings:[][]float32{{13,17}}}))
					}
				})
				defer restore()
				if writer == "create" { _, err := e.CreateNode(sol1474Large(id)); require.NoError(t, err) } else { require.NoError(t, e.UpdateNode(sol1474Large(id))) }
				require.True(t, interposed, "public competing operation ran after the writer's body commit")
				require.Zero(t, sol1474Keys(t, e, id), "an earlier body writer must not publish a range after delete/shrink")
			})
		}
	}
}

func TestRelease1285LargeCommitRollback(t *testing.T) {
	e := openLargeCommitTestEngine(t, t.TempDir())
	defer e.Close()
	id := NodeID("test:rollback-owner")
	_, err := e.CreateNode(sol1474Large(id))
	require.NoError(t, err)
	injected := errors.New("owner probe batch failure")
	restore := setLargeCommitBatchHook(func(batch int) error { if batch == 2 { return injected }; return nil })
	defer restore()
	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	n := sol1474Large(id)
	n.ChunkEmbeddings = [][]float32{{29,31}}
	require.NoError(t, tx.UpdateNode(n))
	stageLargeCreate(t, tx, 6000)
	require.ErrorIs(t, tx.Commit(), injected)
	restore()
	require.Equal(t, 16, sol1474Keys(t, e, id))
	got, err := e.GetNode(id)
	require.NoError(t, err)
	require.True(t, sameChunkEmbeddings(sol1474Large(id).ChunkEmbeddings, got.ChunkEmbeddings))
	require.Zero(t, countBigNodes(t, e))
	requireNoLargeCommitIntent(t, e)
}
