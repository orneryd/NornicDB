package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type canonicalPreparedInner struct {
	Engine
	beforeSubmit func()
}

func (e *canonicalPreparedInner) GetInnerEngine() Engine { return e.Engine }
func (e *canonicalPreparedInner) CreateNode(ctx context.Context, node *Node) (NodeID, error) {
	namespace, _, _ := ParseDatabasePrefix(string(node.ID))
	prepared, settle, err := PrepareEntityChanges(ctx, e.Engine, namespace, []Operation{{Type: OpCreateNode, Node: node}}, CommitModeDirect)
	if err != nil {
		return "", err
	}
	defer prepared.Abort()
	owner, ok := EntityCommitSubmitterFor(e.Engine)
	if !ok {
		return "", ErrInvalidData
	}
	if e.beforeSubmit != nil {
		e.beforeSubmit()
	}
	receipt, err := owner.Submitter.SubmitEntityCommit(ctx, prepared, settle)
	if err != nil {
		return "", err
	}
	outcome := receipt.WaitOutcome(ctx)
	return node.ID, outcome.Err
}

// Component composition of real public storage preparation/submission APIs.
// This does not claim a public Manager fixture was executed in this checkout.
func TestOuterWALPreservesCanonicalPreparedInnerJournal(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, func(engine *MemoryEngine) Engine { return &canonicalPreparedInner{Engine: engine} })
	_, err := wrapped.CreateNode(t.Context(), &Node{ID: "test:inner-prepared"})
	require.NoError(t, err)
	_, err = native.GetNode(t.Context(), "test:inner-prepared")
	require.NoError(t, err)
	require.Len(t, walClientEntries(t, wal), 1, "accepted prepared inner must not silently bypass the outer WAL")
}

// The exclusive owner uses the same gate as restore/automatic compaction. This
// component scheduling control does not claim an actual restore payload ran.
func TestOuterWALPreparedInnerTransfersLeasePastQueuedExclusiveOwner(t *testing.T) {
	var inner *canonicalPreparedInner
	_, wal, wrapped := newWALClientFixture(t, func(engine *MemoryEngine) Engine { inner = &canonicalPreparedInner{Engine: engine}; return inner })
	waiting := make(chan struct{}, 1)
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &wrapped.mutationMu {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})
	defer restore()
	writerDone := make(chan struct{})
	waitCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	inner.beforeSubmit = func() {
		go func() {
			defer close(writerDone)
			if wrapped.mutationMu.LockContext(waitCtx) == nil {
				wrapped.mutationMu.Unlock()
			}
		}()
		select {
		case <-waiting:
		case <-waitCtx.Done():
			t.Error("exclusive owner did not reach admission")
		}
	}
	_, err := wrapped.CreateNode(waitCtx, &Node{ID: "test:prepared-queued"})
	require.NoError(t, err)
	select {
	case <-writerDone:
	case <-waitCtx.Done():
		t.Fatal("transferred lease did not retire")
	}
	require.Len(t, walClientEntries(t, wal), 1)
}

func TestWALNativePublicationSharesObserverLogicalIdentity(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	var observedID string
	var publications int
	require.NoError(t, native.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(publication StoredEntityPublication) (func(), error) {
			observedID = publication.LogicalID
			return func() { publications++ }, nil
		}, nil
	}, nil))
	_, err := wrapped.CreateNode(t.Context(), &Node{ID: "test:shared-logical-id"})
	require.NoError(t, err)
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	var data walEntityCommitData
	require.NoError(t, decodeRecoveryPayload(entries[0].Data, &data))
	envelope, _, err := DecodeEntityTransaction(data.Payload)
	require.NoError(t, err)
	require.NotEmpty(t, observedID)
	require.Equal(t, envelope.LogicalTxID, observedID)
	require.Equal(t, 1, publications)
	require.NoError(t, ReplayWALEntry(native, entries[0]))
	require.Equal(t, 1, publications, "same receipt cannot publish observer delta twice")
}
