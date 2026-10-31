package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOrderReleaseEvictsCachedImages: a direct write releases its write order
// once committed, before its commit tail updates the caches. A relationship
// create ordered right after a node delete, in that window, must not read the
// deleted node's cached image and acknowledge a relationship to it.
func TestOrderReleaseEvictsCachedImages(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind(nil))
	_, err := b.GetNode("test:b") // cache test:b's image
	require.NoError(t, err)

	var once sync.Once
	created := make(chan error, 1)
	restore := setCommitTailHook(func() {
		once.Do(func() {
			go func() {
				tx, err := b.BeginTransaction()
				if err == nil {
					err = tx.SetImplicit(true)
				}
				if err == nil {
					err = tx.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "LINK"})
				}
				if err == nil {
					err = tx.Commit()
				} else if tx != nil {
					_ = tx.Rollback()
				}
				created <- err
			}()
			select {
			case err := <-created:
				created <- err
			case <-time.After(5 * time.Second):
			}
		})
	})
	defer restore()

	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode("test:b", CascadeEdges))
	require.NoError(t, tx.Commit())
	restore()
	createErr := <-created

	require.NoError(t, b.FlushWriteBehind(nil))
	edges, err := b.AllEdges()
	require.NoError(t, err)
	for _, e := range edges {
		for _, id := range []NodeID{e.StartNode, e.EndNode} {
			_, err := b.GetNode(id)
			require.NoError(t, err, "relationship %s acknowledged (create error %v) to endpoint %s deleted before it", e.ID, createErr, id)
		}
	}
	require.Error(t, createErr, "a relationship to a node deleted before it must not be acknowledged")
}
