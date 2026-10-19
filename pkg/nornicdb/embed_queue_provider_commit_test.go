package nornicdb

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type providerCommitQueue struct {
	*rejectingSidecarQueue
	started, release chan struct{}
}

func (q *providerCommitQueue) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	close(q.started)
	<-q.release
	return q.rejectingSidecarQueue.UpdateNodeEmbeddingSidecar(node)
}

func TestEmbedWorkerProviderChangeSerializesCommit(t *testing.T) {
	for _, setter := range []string{"single", "resolver"} {
		for _, retry := range []bool{false, true} {
			t.Run(setter+map[bool]string{false: "/first", true: "/retained"}[retry], func(t *testing.T) {
				worker, queue, old := nodeSaveRetryFixture(t, "whole", 1, 0)
				if retry {
					queue.rejections = 1
					require.True(t, worker.processNextBatch())
					makeNodeSaveRetriesReady(worker)
				}
				gate := &providerCommitQueue{queue, make(chan struct{}), make(chan struct{})}
				worker.storage = gate
				published := make(chan bool, 1)
				worker.onEmbedded = func(*storage.Node) { published <- worker.currentEmbedder() == old }
				written := make(chan struct{})
				go func() { worker.processNextBatch(); close(written) }()
				<-gate.started
				locked := worker.providerWriteMu.TryLock()
				if locked {
					worker.providerWriteMu.Unlock()
					t.Error("write/publication boundary did not hold the provider barrier")
				}
				replacement := newMockEmbedder()
				replacement.model = "replacement"
				started, changed := make(chan struct{}), make(chan struct{})
				go func() {
					close(started)
					if setter == "single" {
						worker.SetEmbedder(replacement)
					} else {
						worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return replacement, nil })
					}
					close(changed)
				}()
				<-started
				select {
				case <-changed:
					t.Error("provider change completed while old sidecar commit was blocked")
				case <-time.After(20 * time.Millisecond):
				}
				close(gate.release)
				<-written
				<-changed
				require.True(t, <-published, "publication belongs before provider transition")
				active, err := worker.resolveEmbedder("node-000")
				require.NoError(t, err)
				require.Same(t, replacement, active)
			})
		}
	}
}

func TestEmbedWorkerProviderChangeRejectsValidatedRetry(t *testing.T) {
	worker, queue, old := nodeSaveRetryFixture(t, "whole", 1, 1)
	require.True(t, worker.processNextBatch())
	makeNodeSaveRetriesReady(worker)
	node, retained, _ := worker.claimNextNodeSource()
	require.NotNil(t, retained)
	worker.SetEmbedder(newMockEmbedder())
	node.ChunkEmbeddings, node.EmbedMeta = retained.vectors, retained.meta
	require.True(t, worker.writeEmbeddedNode(node, old, retained))
	worker.releaseNodeClaim(node.ID)
	require.Equal(t, 1, queue.attempts[node.ID], "obsolete validated claim must not reach storage")
	require.True(t, worker.processNextBatch(), "replacement provider must retain pending ownership")
	require.Equal(t, 2, queue.attempts[node.ID])
}
