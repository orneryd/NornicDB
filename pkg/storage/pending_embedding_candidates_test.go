package storage

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func pendingCandidateNode(i int) *Node {
	return &Node{ID: NodeID(fmt.Sprintf("test:candidate-%06d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": fmt.Sprintf("document %d", i)}}
}

// Count Badger's real iterator creations, including iterations hidden inside
// helpers, rather than checking an implementation-specific cache statistic.
func TestPendingClaimBoundedWork(t *testing.T) {
	metric := expvar.Get("badger_iterator_num_user").(*expvar.Int)
	for _, n := range []int{1, 128, 1024} {
		t.Run(fmt.Sprintf("pending=%d", n), func(t *testing.T) {
			e := newTestBadgerEngineForPending(t)
			for i := 0; i < n; i++ {
				_, err := e.CreateNode(pendingCandidateNode(i))
				require.NoError(t, err)
			}
			require.NotNil(t, e.FindNodeNeedingEmbedding()) // one startup/recovery load
			before := metric.Value()
			for i := 0; i < n; i++ {
				node := e.FindNodeNeedingEmbedding()
				require.NotNil(t, node)
				require.Equal(t, pendingCandidateNode(i).ID, node.ID)
				e.MarkNodeEmbedded(node.ID)
			}
			work := metric.Value() - before
			t.Logf("ACTIVE_WORK pending=%d claims=%d range_iterators=%d", n, n, work)
			require.Zero(t, work, "active claims must use point reads, independent of pending/store size")
			require.Nil(t, e.FindNodeNeedingEmbedding())
			require.EqualValues(t, 1, metric.Value()-before-work, "one empty-boundary reconciliation recovers lost hints")
		})
	}
}

func TestPendingCandidatesCommittedWriters(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding()) // initialize an empty catalog
	_, err := e.CreateNode(pendingCandidateNode(3))
	require.NoError(t, err)
	require.Equal(t, pendingCandidateNode(3).ID, e.FindNodeNeedingEmbedding().ID)
	e.MarkNodeEmbedded(pendingCandidateNode(3).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())

	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(pendingCandidateNode(2))
	require.NoError(t, err)
	require.Nil(t, e.FindNodeNeedingEmbedding(), "uncommitted write is not published")
	require.NoError(t, tx.Commit())
	require.Equal(t, pendingCandidateNode(2).ID, e.FindNodeNeedingEmbedding().ID)
	e.MarkNodeEmbedded(pendingCandidateNode(2).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())

	tx, err = e.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(pendingCandidateNode(1))
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Nil(t, e.FindNodeNeedingEmbedding())
	_, err = e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	updated := pendingCandidateNode(0)
	updated.Properties["content"] = "updated before claim"
	require.NoError(t, e.UpdateNode(updated))
	next := e.FindNodeNeedingEmbedding()
	require.NotNil(t, next)
	require.Equal(t, pendingCandidateNode(0).ID, next.ID)
	require.Equal(t, "updated before claim", next.Properties["content"])
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	e.AddToPendingEmbeddings(pendingCandidateNode(0).ID)
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
}

func TestPendingCandidatesFailedEngineWrite(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	sentinel := errors.New("abort pending-marker write")
	err = e.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, e.setPendingEmbeddingInTxn(txn, pendingCandidateNode(0).ID))
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	leaked := 0
	e.pendingEmbeddingAdds.Range(func(_, _ any) bool { leaked++; return true })
	require.Zero(t, leaked)
	e.AddToPendingEmbeddings(pendingCandidateNode(0).ID)
	require.NotNil(t, e.FindNodeNeedingEmbedding())
}

func TestPendingCandidatesRecoveryAndRestore(t *testing.T) {
	path := t.TempDir()
	e, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	if e != nil {
		fixtureowner.Own(t, e)
	}
	require.NoError(t, err)
	e.SetEmbeddingsEnabled(true)
	_, err = e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "pending.backup")
	require.NoError(t, e.Backup(backup))
	require.NoError(t, e.Close())
	e, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	require.NoError(t, err)
	defer e.Close()
	e.SetEmbeddingsEnabled(true)
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.NoError(t, e.Restore(backup))
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.Equal(t, 1, e.RefreshPendingEmbeddingsIndex())
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
}

func TestPendingCandidatesDeletedAndChangedSource(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	_, err = e.CreateNode(pendingCandidateNode(1))
	require.NoError(t, err)
	old := e.FindNodeNeedingEmbedding()
	require.NotNil(t, old)
	require.NoError(t, e.DeleteNode(old.ID))
	node := e.FindNodeNeedingEmbedding()
	require.NotNil(t, node)
	require.Equal(t, pendingCandidateNode(1).ID, node.ID)
	e.MarkNodeEmbedded(node.ID) // worker claim; source changes before completion
	changed := pendingCandidateNode(1)
	changed.Properties["content"] = "changed source"
	require.NoError(t, e.UpdateNode(changed))
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	require.ErrorIs(t, e.UpdateNodeEmbeddingSidecar(node), ErrEmbeddingSourceChanged)
	require.Equal(t, "changed source", e.FindNodeNeedingEmbedding().Properties["content"])
}

func TestPendingCandidatesLargeCommitPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%t", fail), func(t *testing.T) {
			e := openLargeCommitTestEngine(t, t.TempDir())
			defer e.Close()
			e.SetEmbeddingsEnabled(true)
			for i := 0; i < 3; i++ {
				_, err := e.CreateNode(pendingCandidateNode(i))
				require.NoError(t, err)
				e.MarkNodeEmbedded(pendingCandidateNode(i).ID)
			}
			require.Nil(t, e.FindNodeNeedingEmbedding())
			sentinel := errors.New("interrupt pending-marker batches")
			batches := 0
			restore := setLargeCommitBatchHook(func(batch int) error {
				batches++
				require.Nil(t, e.FindNodeNeedingEmbedding(), "unpublished batches must not enqueue work")
				if fail && batch == 2 {
					return sentinel
				}
				return nil
			})
			defer restore()
			// Values must stay below the 512-byte low-memory value-log threshold;
			// otherwise only their pointers count against the transaction size.
			units := make([]func(*badger.Txn) error, 10000)
			for i := range units {
				i := i
				units[i] = func(txn *badger.Txn) error {
					if i%4999 == 0 {
						if err := e.setPendingEmbeddingInTxn(txn, pendingCandidateNode(i/4999).ID); err != nil {
							return err
						}
					}
					return txn.Set([]byte(fmt.Sprintf("pending-probe-%06d", i)), []byte(strings.Repeat("x", 256)))
				}
			}
			err := e.withUpdateUnits(units)
			require.Greater(t, batches, 1, "fixture must split into multiple atomic batches")
			if fail {
				require.ErrorIs(t, err, sentinel)
				require.Nil(t, e.FindNodeNeedingEmbedding())
			} else {
				require.NoError(t, err)
				for i := 0; i < 3; i++ {
					node := e.FindNodeNeedingEmbedding()
					require.NotNil(t, node)
					require.Equal(t, pendingCandidateNode(i).ID, node.ID)
					e.MarkNodeEmbedded(node.ID)
				}
				require.Nil(t, e.FindNodeNeedingEmbedding())
			}
			leaked := 0
			e.pendingEmbeddingAdds.Range(func(_, _ any) bool { leaked++; return true })
			require.Zero(t, leaked)
		})
	}
}

func TestPendingCandidatesConcurrentEnqueue(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	const n = 100
	var writers, claimers sync.WaitGroup
	var claimMu sync.Mutex // same shared ownership as EmbedWorker's claimMu
	work := make(chan struct{}, n)
	claimed := make(chan NodeID, n)
	for c := 0; c < 2; c++ {
		claimers.Add(1)
		go func() {
			defer claimers.Done()
			for range work {
				claimMu.Lock()
				node := e.FindNodeNeedingEmbedding()
				if node == nil {
					t.Error("committed enqueue was lost")
				} else {
					claimed <- node.ID
					e.MarkNodeEmbedded(node.ID)
				}
				claimMu.Unlock()
			}
		}()
	}
	for writer := 0; writer < 4; writer++ {
		writer := writer
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := writer; i < n; i += 4 {
				_, err := e.CreateNode(pendingCandidateNode(i))
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
				work <- struct{}{}
			}
		}()
	}
	writers.Wait()
	close(work)
	claimers.Wait()
	close(claimed)
	seen := map[NodeID]bool{}
	for id := range claimed {
		require.False(t, seen[id])
		seen[id] = true
	}
	require.Len(t, seen, n)
	require.Nil(t, e.FindNodeNeedingEmbedding())
}

func TestPendingCandidatesWALReplay(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	payload, err := json.Marshal(WALNodeData{Node: pendingCandidateNode(0)})
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(e, WALEntry{Operation: OpCreateNode, Database: "test", Data: payload}))
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
}

func TestPendingCandidatesRestartAfterMarkerBeforeHint(t *testing.T) {
	path := t.TempDir()
	e, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	if e != nil {
		fixtureowner.Own(t, e)
	}
	require.NoError(t, err)
	e.SetEmbeddingsEnabled(true)
	_, err = e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	txn, readTs, err := e.db.beginTxn(true)
	require.NoError(t, err)
	require.NoError(t, e.setPendingEmbeddingInTxn(txn, pendingCandidateNode(0).ID))
	require.NoError(t, e.db.commit(txn))
	txn.Discard()
	e.db.endRead(readTs)
	require.NoError(t, e.db.syncIfPersistent())
	// Model the exact crash boundary: the marker is durable but the commit
	// writer's volatile hint publication has not run. Discard all RAM state.
	require.NoError(t, e.Close())
	e, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	require.NoError(t, err)
	defer e.Close()
	e.SetEmbeddingsEnabled(true)
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
}

func TestPendingCandidatesRestartLargeBacklog(t *testing.T) {
	path := t.TempDir()
	e, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	if e != nil {
		fixtureowner.Own(t, e)
	}
	require.NoError(t, err)
	e.SetEmbeddingsEnabled(true)
	const n = 2048
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = pendingCandidateNode(i)
	}
	require.NoError(t, e.BulkCreateNodes(nodes))
	require.NoError(t, e.Close())
	e, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	require.NoError(t, err)
	defer e.Close()
	e.SetEmbeddingsEnabled(true)
	for i := 0; i < n; i++ {
		node := e.FindNodeNeedingEmbedding()
		require.NotNil(t, node)
		require.Equal(t, nodes[i].ID, node.ID)
		e.MarkNodeEmbedded(node.ID)
	}
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.Zero(t, e.PendingEmbeddingsCount())
}

func TestPendingCandidatesInvalidationAndFailedHints(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	e.InvalidatePendingEmbeddingsIndex()
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
	txn, readTs, err := e.db.beginTxn(true)
	require.NoError(t, err)
	txn.Discard()
	e.db.endRead(readTs)
	require.Error(t, e.setPendingEmbeddingInTxn(txn, pendingCandidateNode(1).ID))
	require.NoError(t, e.Close())
	e.InvalidatePendingEmbeddingsIndex()
	require.ErrorIs(t, e.pendingCandidates.load(e), ErrStorageClosed)
}

func TestPendingCandidatesFailedReadInvalidatesCatalog(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	require.NotNil(t, e.FindNodeNeedingEmbedding())
	e.db.oracle.fail(errors.New("injected unavailable store"))
	require.Nil(t, e.FindNodeNeedingEmbedding(), "failed atomic lookup must not return a candidate")
	require.Nil(t, e.FindNodeNeedingEmbedding(), "failed catalog reload must remain retryable")
}
