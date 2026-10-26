// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventNotificationsSettledThroughPublisher(t *testing.T) {
	base := createTestBadgerEngine(t)
	view := NewNamespacedEngine(&WALEngine{engine: &TracedEngine{Engine: base}}, "tenant")
	require.True(t, EventNotificationsSettled(view))
	require.False(t, EventNotificationsSettled(struct{ Engine }{}))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	base.OnNodeCreated(func(*Node) { close(entered); <-release })
	finished := make(chan error, 1)
	go func() {
		_, err := base.CreateNode(&Node{ID: "tenant:one"})
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("publisher callback did not begin")
	}
	// The value is readable while its callback is unfinished. Metadata must
	// report that distinction without waiting for the blocked observer.
	_, err := base.GetNode("tenant:one")
	require.NoError(t, err)
	require.False(t, EventNotificationsSettled(view))
	unblock()
	require.NoError(t, <-finished)
	require.True(t, EventNotificationsSettled(view))
	buffered := newWriteBehindTestEngine(t, time.Hour)
	require.False(t, EventNotificationsSettled(buffered), "buffered ACKs cannot certify per-operation event coverage")
}
