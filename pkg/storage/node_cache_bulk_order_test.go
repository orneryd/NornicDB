package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNodeCacheBulkDeleteRevokesPublisher(t *testing.T) {
	for _, all := range []bool{false, true} {
		name := "prefix"
		if all {
			name = "all"
		}
		t.Run(name, func(t *testing.T) {
			b := NewMemoryEngine()
			t.Cleanup(func() { _ = b.Close() })
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			restore := setCommitTailHook(func() { close(entered); <-release })
			t.Cleanup(func() { unblock(); restore() })
			node := &Node{ID: "test:bulk-publication", ChunkEmbeddings: [][]float32{{1, 0}}}
			created := make(chan error, 1)
			go func() { _, err := b.CreateNode(node); created <- err }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("create did not reach its durable boundary")
			}
			var err error
			if all {
				err = b.db.DropAll()
			} else {
				err = b.db.DropPrefix(nodeKey(node.ID), embeddingPrefix(node.ID))
			}
			require.NoError(t, err)
			unblock()
			require.NoError(t, <-created)
			_, err = b.GetNode(node.ID)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetQueryNode(node.ID)
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
