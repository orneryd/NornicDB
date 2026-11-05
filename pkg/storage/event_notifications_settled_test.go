// Author: Codex implementation lane, 2026-10-26 (NOR-148); ported to release/v1.4.4.
package storage

import (
	"context"
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
		_, err := base.CreateNode(context.Background(), &Node{ID: "tenant:one"})
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("publisher callback did not begin")
	}
	// The value is readable while its callback is unfinished. Metadata must
	// report that distinction without waiting for the blocked observer.
	_, err := base.GetNode(context.Background(), "tenant:one")
	require.NoError(t, err)
	require.False(t, EventNotificationsSettled(view))
	unblock()
	require.NoError(t, <-finished)
	require.True(t, EventNotificationsSettled(view))
	buffered := newWriteBehindTestEngine(t, time.Hour)
	require.False(t, EventNotificationsSettled(buffered), "buffered ACKs cannot certify per-operation event coverage")
}

// ReadPublicationSettled reports the absence of an in-flight logical
// publication: a native view is settled unless a publication is running, and a
// buffered ACK's hold is released after it advances the graph token, so reuse
// cannot be certified against a publication still in progress.
func TestReadPublicationSettledFollowsPublisher(t *testing.T) {
	base := createTestBadgerEngine(t)
	require.True(t, ReadPublicationSettled(base), "an idle native view has published every acknowledged write")
	require.True(t, ReadPublicationSettled(NewNamespacedEngine(&WALEngine{engine: base}, "tenant")))
	require.False(t, ReadPublicationSettled(struct{ Engine }{}), "an unproven engine reports nothing")

	buffered := newWriteBehindTestEngine(t, time.Hour)
	bufferedWrite(t, buffered, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(context.Background(), &Node{ID: "test:published", Labels: []string{"Doc"}})
		return err
	})
	deadline := time.Now().Add(10 * time.Second)
	for !ReadPublicationSettled(buffered) {
		require.True(t, time.Now().Before(deadline), "the buffered acknowledgment's publication hold was never released")
		time.Sleep(time.Millisecond)
	}
}
