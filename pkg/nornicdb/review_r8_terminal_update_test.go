package nornicdb_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReviewDLQTerminalWriteCannotOverrideUpdate(t *testing.T) {
	p := &dlqPublicProvider{poison: true}
	w, s := dlqPublicFixture(t, p, func(c *nornicdb.EmbedWorkerConfig) { c.MaxRetries = 0 })
	started, resume := make(chan struct{}), make(chan struct{})
	var gate, release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }) })
	s.beforeWrite = func(n *storage.Node) {
		if failed, _ := n.EmbedMeta["embedding_failed"].(bool); failed {
			gate.Do(func() { close(started); <-resume })
		}
	}
	dlqPublicAdd(t, s, "poison")
	w.StartWorkers()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("terminal commit did not start")
	}
	n, err := s.GetNode("poison")
	require.NoError(t, err)
	stamp := n.UpdatedAt
	// A supported idempotent upsert is still a new attempt, even with the
	// caller's unchanged body timestamp. Enqueue is the public notification.
	require.NoError(t, s.UpdateNode(storage.NodeUpdateRequest{After: n}))
	current, err := s.GetNode("poison")
	require.NoError(t, err)
	require.True(t, stamp.Equal(current.UpdatedAt), "exercise idempotent same-version update")
	w.Enqueue("poison")
	p.mu.Lock()
	p.poison = false
	p.mu.Unlock()
	release.Do(func() { close(resume) })
	require.Eventually(t, func() bool { return w.Stats().Processed == 1 || w.Stats().Parked == 1 }, 3*time.Second, time.Millisecond)
	w.Close()
	current, err = s.GetNode("poison")
	require.NoError(t, err)
	failures, err := w.ParkedEmbeddingFailures(context.Background(), 10)
	require.NoError(t, err)
	t.Logf("processed=%d parked=%d durableFailures=%d vectors=%d meta=%v", w.Stats().Processed, w.Stats().Parked, len(failures), len(current.ChunkEmbeddings), current.EmbedMeta)
	require.NotEqual(t, true, current.EmbedMeta["embedding_failed"], "a terminal result from the pre-update claim must be rejected")
	require.NotEmpty(t, current.ChunkEmbeddings, "updated node must return to MAIN and use its new attempt")
}
