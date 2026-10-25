package storage

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// P2-1: replay publication without listeners must not copy embedding payloads.
// Compare controlled payload sizes so intrinsic bookkeeping is not confused
// with notification-copy allocations or a guessed absolute allocation budget.
func TestDispatcherUnobservedReplayAllocationDoesNotGrowWithPayload(t *testing.T) {
	b, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer b.Close()
	schema := b.GetSchemaForNamespace("test")
	allocations := func(chunks [][]float32) float64 {
		node := &Node{ID: "test:unobserved", ChunkEmbeddings: chunks}
		ops := []Operation{{Type: OpUpdateNode, Node: node, OldNode: node}}
		return testing.AllocsPerRun(10, func() {
			b.publishCommitOperations("test", schema, ops, false)
		})
	}
	small := allocations(nil)
	chunks := make([][]float32, 32)
	for i := range chunks {
		chunks[i] = []float32{float32(i)}
	}
	large := allocations(chunks)
	runtime.KeepAlive(chunks)
	require.Equal(t, small, large, "unobserved replay allocations must not grow with embedding chunks")
}

func TestDispatcherOutcomeEligibilityWithoutListeners(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "bound"}[bound], func(t *testing.T) {
			b, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer b.Close()
			node := &Node{ID: "test:outcome"}
			if bound {
				node.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, node.EmbeddingRevision(), nil, nil))
			}
			n := b.reserveCommitOperations([]Operation{{Type: OpUpdateNode, Node: node, embeddingOutcome: true}})
			b.completeCommitNotification(n)
			require.NoError(t, b.FlushWriteBehind())
			if bound {
				require.NotNil(t, n, "a bound attempt needs its completion token even without listeners")
			} else {
				require.Nil(t, n, "an unbound outcome has no notification consumer")
			}
		})
	}
}

// P2-2: a direct write landing its own older buffer may wait on replay, while
// an unrelated synchronous write has no dependency on that generation.
func TestDispatcherSlowLanderAllowsUnrelatedSynchronousWrites(t *testing.T) {
	for _, direct := range []bool{true, false} {
		t.Run(map[bool]string{true: "direct", false: "explicit"}[direct], func(t *testing.T) {
			b := writeBehindOrderEngine(t)
			b.writeBehind.Close()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error {
				return tx.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "buffered"}})
			}))
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			apply := b.writeBehind.apply
			b.writeBehind.apply = func(g *CommitBuffer) error {
				close(entered)
				<-release
				return apply(g)
			}
			landing := make(chan error, 1)
			go func() { landing <- b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "newer"}}) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("direct lander did not reach replay")
			}
			independent := make(chan error, 1)
			go func() {
				node := &Node{ID: "test:b", Properties: map[string]any{"v": "independent"}}
				if direct {
					independent <- b.UpdateNode(node)
					return
				}
				tx, err := b.BeginTransaction()
				if err == nil {
					err = tx.UpdateNode(node)
					if err == nil {
						err = tx.Commit()
					} else {
						_ = tx.Rollback()
					}
				}
				independent <- err
			}()
			completed := false
			select {
			case err := <-independent:
				completed = true
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Error("unrelated writer serialized behind another entity's replay")
			}
			unblock()
			require.NoError(t, <-landing)
			if !completed {
				require.NoError(t, <-independent)
			}
			b.writeBehind.apply = apply
		})
	}
}
