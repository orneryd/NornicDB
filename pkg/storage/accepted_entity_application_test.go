// Author: NOR-288 implementation lane (Codex), 2026-10-26.
package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

type acceptedUnavailableSubmitter struct {
	*BadgerEngine
	unavailable atomic.Bool
	failure     error
}

func TestAcceptedNativeConflictSurvivesCrashRestart(t *testing.T) {
	dir := t.TempDir()
	options := writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true}
	base := newWriteBehindTestEngine(t, time.Hour, options)
	base.SetDecayEnabled(true)
	wal := base.bufferedCommitWAL
	_, err := base.CreateNode(&Node{ID: "test:old", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	require.NoError(t, base.PutAccessMeta("test:old", &knowledgepolicy.AccessMetaEntry{TargetID: "test:old", TargetScope: knowledgepolicy.ScopeNode}))
	var events atomic.Int32
	base.OnNodeCreated(func(*Node) { events.Add(1) })
	base.OnNodeDeleted(func(NodeID) { events.Add(1) })
	tx, err := BeginEntityTransaction(base, CommitModeDirect)
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode("test:old", CascadeEdges))
	_, err = tx.CreateNode(&Node{ID: "test:new", Properties: map[string]any{"v": "after"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	require.NoError(t, base.PutAccessMeta("test:old", &knowledgepolicy.AccessMetaEntry{TargetID: "test:old", TargetScope: knowledgepolicy.ScopeNode, MutationCount: 1}))
	receipt, err := NewWALEngine(base, wal).SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	require.EqualValues(t, 2, events.Load(), "the discarded native attempt emits no events; the accepted replacement publishes once before success")
	require.Equal(t, CommitDiscarded, p.terminal.State, "the first native frame must really conflict")
	require.ErrorIs(t, p.terminal.Err, ErrConflict)
	// The application releases its lifecycle enrolment before it resolves the
	// receipt, so the applying worker has returned once Wait has.
	sequence := wal.Sequence()
	crashClose(t, base, wal)
	base = newWriteBehindTestEngine(t, time.Hour, options)
	wal = base.bufferedCommitWAL
	_, err = RecoverBufferedCommitsAtOpen(base, wal)
	require.NoError(t, err)
	require.Equal(t, sequence, wal.Sequence(), "restart never creates a new accepted record")
	_, err = base.GetStoredNode("test:old")
	require.ErrorIs(t, err, ErrNotFound)
	node, err := base.GetStoredNode("test:new")
	require.NoError(t, err)
	require.Equal(t, "after", node.Properties["v"])
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, base.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:new", Properties: map[string]any{"v": "writable"}}}))
}

func TestAcceptedSettlementCallbackCanCloseEngine(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	walDir := t.TempDir()
	wal, err := NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "close-settlement", Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}}}}})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	closed := make(chan error, 1)
	replayEnvelope, replayOperations, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	receipt, err := SubmitAcceptedEntityTransaction(context.Background(), base, replayEnvelope, replayOperations, func(CommitOutcome) { closed <- base.Close() })
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := receipt.Wait(ctx); err != nil {
		// The broken owner waits for itself inside Close. A second Close in
		// cleanup would conceal this bounded failure behind a package timeout.
		t.Errorf("accepted settlement callback could not close its engine: %v", err)
		return
	}
	require.NoError(t, <-closed)
	recovered, replay, err := RecoverFromWALWithResult(walDir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = recovered.Close() })
	require.Empty(t, replay.Errors)
	node, err := recovered.GetStoredNode("test:accepted")
	require.NoError(t, err)
	require.Equal(t, "committed", node.Properties["v"])
}

// The record really reaches the file and fsync; only acknowledgement fails.
// This crosses WALEngine's uncertain-append branch without fabricating a receipt.
type acceptedRecordedWriteError struct {
	file    *os.File
	failure error
}

func (w *acceptedRecordedWriteError) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if err != nil {
		return n, err
	}
	return n, errors.Join(w.failure, w.file.Sync())
}

func (s *acceptedUnavailableSubmitter) GetInnerEngine() Engine { return s.BadgerEngine }
func (s *acceptedUnavailableSubmitter) SubmitEntityCommit(ctx context.Context, p *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	if s.unavailable.Load() {
		return nil, s.failure
	}
	return s.BadgerEngine.SubmitEntityCommit(ctx, p, settle)
}

func TestAcceptedApplicationPendingRecoverySettlesOnce(t *testing.T) {
	for _, scenario := range []string{"conflict-budget", "permanent-error", "restart", "outer-submit", "uncertain-append"} {
		t.Run(scenario, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			base, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			walDir := t.TempDir()
			wal, err := NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
			require.NoError(t, err)
			unavailable := &acceptedUnavailableSubmitter{BadgerEngine: base, failure: errors.New("native submit unavailable")}
			owner := NewWALEngine(unavailable, wal)
			t.Cleanup(func() { _ = owner.Close() })
			var reject atomic.Bool
			var attempts, publications, settlements atomic.Int32
			failure := errors.New("permanent native preparation error")
			if scenario == "conflict-budget" {
				failure = ErrConflict
			}
			if scenario == "outer-submit" {
				failure = unavailable.failure
				unavailable.unavailable.Store(true)
			}
			if scenario == "uncertain-append" {
				failure = errors.New("durable append acknowledgement failed")
				wal.writer.Reset(&acceptedRecordedWriteError{file: wal.file, failure: failure})
			}
			require.NoError(t, base.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
				return func(StoredEntityPublication) (func(), error) {
					attempts.Add(1)
					if reject.Load() {
						return nil, failure
					}
					return func() { publications.Add(1) }, nil
				}, nil
			}, nil))
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			_, err = tx.CreateNode(&Node{ID: "test:accepted", Properties: map[string]any{"v": "accepted"}})
			require.NoError(t, err)
			prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "pending-recovery"})
			require.NoError(t, err)
			t.Cleanup(prepared.Abort)
			payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: prepared.LogicalID(), Operations: prepared.Operations()})
			require.NoError(t, err)
			reject.Store(true)
			started := time.Now()
			receipt, err := owner.SubmitEntityCommit(context.Background(), prepared, func(CommitOutcome) { settlements.Add(1) })
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			outcome := receipt.WaitOutcome(ctx)
			require.Equal(t, CommitRecoveryPending, outcome.State)
			var accepted *AcceptedCommitError
			require.ErrorAs(t, outcome.Err, &accepted)
			require.NotErrorIs(t, outcome.Err, ErrConflict)
			require.ErrorIs(t, accepted.Cause(), failure)
			require.True(t, strings.Contains(outcome.Err.Error(), "committed-pending or unknown"))
			want := int32(1)
			if scenario == "outer-submit" || scenario == "uncertain-append" {
				want = 0
			}
			if scenario == "conflict-budget" {
				want = acceptedApplicationAttempts
				require.GreaterOrEqual(t, time.Since(started), 200*time.Millisecond, "native conflicts must yield between bounded applications")
			}
			require.Equal(t, want, attempts.Load())
			require.Zero(t, settlements.Load())
			require.Zero(t, publications.Load())
			require.EqualValues(t, 1, wal.Sequence(), "only the accepted command is journaled")
			_, err = base.CreateNode(&Node{ID: "test:accepted"})
			require.ErrorIs(t, err, ErrConflict, "pending command keeps its authority")
			reject.Store(false)
			unavailable.unavailable.Store(false)
			if scenario == "restart" {
				require.NoError(t, owner.Close())
				recovered, result, err := RecoverFromWALWithResult(walDir, "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = recovered.Close() })
				require.Empty(t, result.Errors)
				base = recovered.BadgerEngine
			} else {
				if scenario == "uncertain-append" {
					wal.mu.Lock()
					wal.writer.Reset(wal.file)
					wal.mu.Unlock()
					n, err := RecoverBufferedCommitsAtOpen(base, wal)
					require.NoError(t, err)
					require.Equal(t, 1, n, "recovery must discover the durably written accepted command")
				} else {
					replayEnvelope, replayOperations, err := DecodeEntityTransaction(payload)
					require.NoError(t, err)
					recovery, err := SubmitAcceptedEntityTransaction(ctx, owner, replayEnvelope, replayOperations, nil)
					require.NoError(t, err)
					require.NoError(t, recovery.Wait(ctx))
				}
				require.NoError(t, receipt.Wait(ctx), "original waiter joins recovery's terminal owner")
				require.EqualValues(t, 1, publications.Load())
				require.EqualValues(t, 1, settlements.Load())
				require.EqualValues(t, 1, wal.Sequence(), "recovery never re-journals accepted bytes")
				require.NoError(t, ApplyAcceptedEntityTransaction(owner, payload))
				require.EqualValues(t, 1, publications.Load(), "duplicate application is idempotent")
			}
			node, err := base.GetStoredNode("test:accepted")
			require.NoError(t, err)
			require.Equal(t, "accepted", node.Properties["v"])
			count, err := base.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			require.NoError(t, base.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:accepted", Properties: map[string]any{"v": "writable"}}}), "settlement releases accepted authority")
		})
	}
}
