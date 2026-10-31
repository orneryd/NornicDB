package storage

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func publicationProofPayload(t *testing.T, b *BadgerEngine) *Node {
	t.Helper()
	_, err := b.CreateNode(&Node{ID: "test:proof", Properties: map[string]any{"text": "same input"}})
	require.NoError(t, err)
	node, err := b.GetNode("test:proof")
	require.NoError(t, err)
	node, err = SnapshotNodeForEmbedding(node)
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	node.EmbedMeta = map[string]any{"chunk_count": 1}
	return node
}

// This is a public sidecar save and real ordered listener/worker delivery. Its
// accepted proof reaches the canonical capture reader without a new read txn.
func TestEmbeddingPublicationProofPublicDelivery(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "bound"}[bound], func(t *testing.T) {
			b := newSidecarTestBadger(t)
			node := publicationProofPayload(t, b)
			var listeners, workers int
			b.OnNodeUpdated(func(accepted *Node) {
				listeners++
				require.True(t, accepted.runtimeState().embeddingPublication.matches(b, accepted.ID, accepted.runtimeState().embeddingRevision))
				allocs := testing.AllocsPerRun(20, func() {
					same, err := b.embeddingRevisionIsCurrent(accepted.ID, accepted.runtimeState().embeddingRevision, accepted.runtimeState().embeddingPublication)
					if err != nil || !same {
						panic("current accepted outcome was rejected")
					}
				})
				require.Zero(t, allocs, "uncontended publication must not create a managed read transaction")
			})
			if bound {
				node.SetEmbeddingAttempt(NewEmbeddingAttempt(CaptureVectorWorkOrigin(), 1, 1, node.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(*Node) { workers++ }))
			}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
			require.Equal(t, 1, listeners)
			if bound {
				require.Equal(t, 1, workers)
			}
			stored, err := b.GetNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, node.ChunkEmbeddings, stored.ChunkEmbeddings)
		})
	}
}

// A publication in the post-commit tail must not be mistaken for the accepted
// outcome's own boundary. The body is unchanged, so the fresh fallback accepts.
func TestEmbeddingPublicationProofComesFromCommit(t *testing.T) {
	b := newSidecarTestBadger(t)
	node := publicationProofPayload(t, b)
	var once atomic.Bool
	var committed embeddingPublicationProof
	restore := setCommitTailHook(func() {
		if !once.CompareAndSwap(false, true) {
			return
		}
		committed = node.runtimeState().embeddingPublication
		require.NotZero(t, committed.commitTs, "proof must exist before the post-commit tail")
		require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("publication-proof-test"), []byte("later publication"))
		}))
	})
	defer restore()
	var delivered bool
	b.OnNodeUpdated(func(accepted *Node) {
		delivered = true
		require.Equal(t, committed, accepted.runtimeState().embeddingPublication, "never sample a replacement boundary after committing")
		require.False(t, accepted.runtimeState().embeddingPublication.matches(b, accepted.ID, accepted.runtimeState().embeddingRevision))
		same, err := b.embeddingRevisionIsCurrent(accepted.ID, accepted.runtimeState().embeddingRevision, accepted.runtimeState().embeddingPublication)
		require.NoError(t, err)
		require.True(t, same, "an intervening unrelated commit requires fresh validation, not rejection")
	})
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
	require.True(t, delivered)
}

func TestEmbeddingPublicationProofFallsBackAfterBodyChanges(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "update", true: "delete"}[remove], func(t *testing.T) {
			b := newSidecarTestBadger(t)
			node := publicationProofPayload(t, b)
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
			proof, revision := node.runtimeState().embeddingPublication, node.runtimeState().embeddingRevision
			if remove {
				require.NoError(t, b.DeleteNode(node.ID))
			} else {
				updated, err := b.GetNode(node.ID)
				require.NoError(t, err)
				updated.Properties["text"] = "new source"
				require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: updated}))
			}
			require.False(t, proof.matches(b, node.ID, revision))
			same, err := b.embeddingRevisionIsCurrent(node.ID, revision, proof)
			require.False(t, same)
			if remove {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Reentrant public writes queue behind the listener. The next accepted outcome
// gets its own proof; it cannot inherit the first event's invalidated boundary.
func TestEmbeddingPublicationProofRefreshesAfterReentrantWrite(t *testing.T) {
	b := newSidecarTestBadger(t)
	node := publicationProofPayload(t, b)
	var proofs []embeddingPublicationProof
	b.OnNodeUpdated(func(accepted *Node) {
		proofs = append(proofs, accepted.runtimeState().embeddingPublication)
		require.True(t, accepted.runtimeState().embeddingPublication.matches(b, accepted.ID, accepted.runtimeState().embeddingRevision))
		if len(proofs) != 1 {
			return
		}
		_, err := b.CreateNode(&Node{ID: "test:reentrant"})
		require.NoError(t, err)
		next := CopyNode(accepted)
		next.ChunkEmbeddings = [][]float32{{3, 4}}
		require.NoError(t, b.UpdateNodeEmbeddingSidecar(next))
	})
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
	require.Len(t, proofs, 2)
	require.Greater(t, proofs[1].commitTs, proofs[0].commitTs)
	require.False(t, proofs[0].matches(b, node.ID, node.runtimeState().embeddingRevision))
	require.True(t, proofs[1].matches(b, node.ID, node.runtimeState().embeddingRevision))
}

// An older assigned writer may finish after the sidecar's physical write.
// Acceptance did not observe it, so that sidecar cannot certify currentness.
func TestEmbeddingPublicationProofUnobservedPredecessor(t *testing.T) {
	b := newSidecarTestBadger(t)
	node := publicationProofPayload(t, b)
	prior, err := b.db.oracle.assign()
	require.NoError(t, err)
	defer b.db.oracle.finish(prior)
	previous := b.db.newTxnAt(prior-1, true)
	defer previous.Discard()
	require.NoError(t, previous.Set([]byte("publication-proof-predecessor"), []byte("earlier writer")))
	done := make(chan error, 1)
	go func() {
		done <- b.commitEngineWrite(func(cw *commitWriter) error {
			cw.embeddingOutcome = node
			return cw.writeOnce(func(txn *badger.Txn) error {
				if err := b.checkEmbeddingSourceInTxn(txn, node); err != nil {
					return err
				}
				return txn.Set([]byte("publication-proof-outcome"), []byte("later writer"))
			})
		})
	}()
	require.Eventually(t, func() bool {
		b.db.oracle.mu.Lock()
		defer b.db.oracle.mu.Unlock()
		return b.db.oracle.nextTs > prior+1
	}, 3*time.Second, time.Millisecond)
	require.NoError(t, previous.CommitAt(prior, nil))
	b.db.oracle.finish(prior)
	require.NoError(t, <-done)
	require.Zero(t, node.runtimeState().embeddingPublication.commitTs, "an unobserved predecessor prevents certification")
	same, err := b.embeddingRevisionIsCurrent(node.ID, node.runtimeState().embeddingRevision, node.runtimeState().embeddingPublication)
	require.NoError(t, err)
	require.True(t, same, "missing proof uses the fresh metadata path")
}

func TestEmbeddingPublicationProofMaintenanceBoundary(t *testing.T) {
	for _, operation := range []string{"drop_prefix", "drop_all", "load_older_versions"} {
		t.Run(operation, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			node := publicationProofPayload(t, b)
			var backup bytes.Buffer
			if operation == "load_older_versions" {
				_, err := b.db.Backup(&backup, 0)
				require.NoError(t, err)
			}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
			proof := node.runtimeState().embeddingPublication
			require.True(t, proof.matches(b, node.ID, node.runtimeState().embeddingRevision))
			switch operation {
			case "drop_prefix":
				require.NoError(t, b.db.DropPrefix(prefixDropOptions{}, nodeKey(node.ID)))
			case "drop_all":
				require.NoError(t, b.db.DropAll())
			case "load_older_versions":
				require.NoError(t, b.db.Load(bytes.NewReader(backup.Bytes()), 1))
			}
			require.False(t, proof.matches(b, node.ID, node.runtimeState().embeddingRevision), "bulk mutation invalidates the publication interval")
			same, err := b.embeddingRevisionIsCurrent(node.ID, node.runtimeState().embeddingRevision, proof)
			if operation == "load_older_versions" {
				require.NoError(t, err)
				require.True(t, same)
			} else {
				require.ErrorIs(t, err, ErrNotFound)
				require.False(t, same)
			}
		})
	}
}
