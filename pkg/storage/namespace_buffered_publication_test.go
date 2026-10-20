package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNamespaceBufferedReplayReleasesPublicationBeforeListeners(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	called := make(chan struct{})
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "test:publication_probe" {
			return
		}
		defer close(called)
		if !engine.writeBarrier.TryLock() {
			t.Error("buffered listener entered with the physical publication barrier held")
			return
		}
		engine.writeBarrier.Unlock()
		require.Zero(t, engine.writeBehind.PendingOps(), "retire the generation before listener reentry")
		require.NoError(t, engine.FlushWriteBehind(), "listener may flush without waiting on its own generation")
	})
	bufferedCreate(t, engine, &Node{ID: "test:publication_probe", Labels: []string{"Probe"}})
	require.NoError(t, engine.FlushWriteBehind())
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("buffered publication listener did not complete")
	}
}
