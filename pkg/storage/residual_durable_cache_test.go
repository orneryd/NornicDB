package storage

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A public write is durably visible before its derived stage runs. A warm
// point read at that boundary must agree with the newly committed body.
func TestResidualDurableNodeCache(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.CreateNode(&Node{ID: "test:durable", Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = b.GetStoredNode("test:durable")
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(setPublicationStageHook(func(uint64) {
		close(entered)
		<-release
	}))
	done := make(chan error, 1)
	go func() {
		done <- b.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:durable", Properties: map[string]any{"v": int64(2)}}})
	}()
	<-entered
	node, readErr := b.GetStoredNode("test:durable")
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, readErr)
	require.Equal(t, int64(2), node.Properties["v"])
}
