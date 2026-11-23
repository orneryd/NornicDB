package nornicdb

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-018: TestEmbedWorkerProviderChangeSerializesCommit/single/retry failed
// under -race: "publication belongs before provider transition". The worker's
// onEmbedded publication is delivered by storage.PersistEmbeddingOutcome after
// the persist closure in commitNodeOutcome (embed_queue_attempt.go) has
// returned, i.e. after providerWriteMu.RUnlock, so a SetEmbedder waiting on
// providerWriteMu can run before the publication.
//
//   TestL7018BarrierHeldAtPublication  diagnostic: inside onEmbedded, can the
//     provider barrier be taken for writing? (true = not held by the commit).
//   TestL7018DelayedPublication        minimal reproduction: the test's own
//     scenario, with the publication callback waiting 200 ms before it reads
//     the embedder; expected FAIL if publication is outside the barrier.

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL7018BarrierHeldAtPublication(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "retry"}[retry], func(t *testing.T) {
			worker, queue, _ := nodeSaveRetryFixture(t, "whole", 1, 0)
			if retry {
				queue.rejections = 1
				require.True(t, worker.processNextBatch())
				makeNodeSaveRetriesReady(worker)
			}
			free := make(chan bool, 1)
			worker.onEmbedded = func(*storage.Node) {
				taken := worker.providerWriteMu.TryLock()
				if taken {
					worker.providerWriteMu.Unlock()
				}
				free <- taken
			}
			worker.processNextBatch()
			select {
			case taken := <-free:
				t.Logf("L7_018 provider barrier free at publication: %v", taken)
				require.False(t, taken, "the commit holds the provider barrier while it publishes")
			case <-time.After(10 * time.Second):
				t.Fatal("no publication")
			}
		})
	}
}

func TestL7018DelayedPublication(t *testing.T) {
	for _, setter := range []string{"single", "resolver"} {
		for _, retry := range []bool{false, true} {
			t.Run(setter+map[bool]string{false: "/first", true: "/retry"}[retry], func(t *testing.T) {
				worker, queue, old := nodeSaveRetryFixture(t, "whole", 1, 0)
				if retry {
					queue.rejections = 1
					require.True(t, worker.processNextBatch())
					makeNodeSaveRetriesReady(worker)
				}
				gate := &providerCommitQueue{queue, make(chan struct{}), make(chan struct{})}
				worker.storage = gate
				published := make(chan bool, 1)
				worker.onEmbedded = func(*storage.Node) {
					time.Sleep(200 * time.Millisecond)
					published <- worker.currentEmbedder() == old
				}
				written := make(chan struct{})
				go func() { worker.processNextBatch(); close(written) }()
				<-gate.started
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
				time.Sleep(20 * time.Millisecond)
				close(gate.release)
				<-written
				<-changed
				require.True(t, <-published, "publication belongs before provider transition")
			})
		}
	}
}
