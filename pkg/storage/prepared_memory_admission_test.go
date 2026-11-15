package storage

// B04/report21: the memory policy's refusal of a prepared entity commit must
// be definite and must precede every journal record of that logical commit.
//
// The owners were sequential: WALEngine.SubmitEntityCommit journaled the
// commit's semantic record (OpEntityCommit) and only then did the native
// application reach the write buffer's admission, commitBufferedLocked
// (badger_write_behind.go), which reserves the commit's forecast on the
// engine's memory.Policy. A commit the policy refused therefore left a
// semantic record the recovery at open applied, and the refusal reached the
// caller as an accepted outcome (AcceptedCommitError), not as a rejection.
//
// commitBufferedLocked now reserves the commit's forecast and only then runs
// the prepared handoff that lets the journal owner hold the frame, so the
// refusal is definite: preparation itself returns memory.ErrLimitExceeded and
// nothing of the commit is journaled.
//
// The tests below decide the required outcome of that path and keep the
// neighbouring contracts (a commit within the limit, the explicit direct route,
// and a discarded or canceled frame) unchanged.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

const (
	preparedMemoryNamespace = "test"
	preparedMemoryLabel     = "PreparedMemory"
	// preparedMemoryLimit is the fixture's soft limit, one MiB.
	preparedMemoryLimit = int64(1 << 20)
	// preparedMemoryNodes and preparedMemoryNodeText stage the refused commit:
	// ten nodes of 300 KiB forecast a buffered commit of roughly nine MiB.
	preparedMemoryNodes    = 10
	preparedMemoryNodeText = 300 << 10
	// preparedMemorySmallText stages the within-limit control: ten nodes of
	// 10 KiB forecast about 300 KiB.
	preparedMemorySmallText = 10 << 10
)

// preparedMemorySampler is the deterministic physical reading the policy tests
// in writebehind_memory_policy_test.go use: a constant, never-pressured sample,
// so only the commit's forecast decides admission.
func preparedMemorySampler() (memory.Sample, error) {
	return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
}

// preparedMemoryPolicy is the one policy owner of these tests: a 1 MiB soft
// limit, the deterministic sampler, and a freshness long enough that the single
// Refresh below decides every reservation the test makes.
func preparedMemoryPolicy(t *testing.T) *memory.Policy {
	t.Helper()
	policy := memory.NewPolicy(preparedMemoryLimit, 0, time.Hour, preparedMemorySampler, time.Now)
	require.NoError(t, policy.Refresh())
	return policy
}

// preparedMemoryEngine opens the persistent write-behind fixture of
// badger_write_behind_test.go with the manual drain the admission fixtures use
// (writebehind_memory_policy_test.go, commit_admission_test.go): the buffer's
// worker is stopped, so a generation stays in memory until the test flushes it,
// and the journal is the engine's own buffered-commit WAL.
func preparedMemoryEngine(t *testing.T, dir string, policy *memory.Policy) (*BadgerEngine, *WAL) {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{
		dataDir:      dir,
		wal:          true,
		manualDrain:  true,
		memoryPolicy: policy,
	})
	return engine, engine.bufferedCommitWAL
}

// preparedMemoryStage stages n nodes of text bytes each, labelled
// preparedMemoryLabel, in tx.
func preparedMemoryStage(t *testing.T, tx *BadgerTransaction, n, text int) {
	t.Helper()
	ctx := context.Background()
	payload := strings.Repeat("x", text)
	for i := range n {
		_, err := tx.CreateNode(ctx, &Node{
			ID:         NodeID(fmt.Sprintf("test:prepared-memory-%02d", i)),
			Labels:     []string{preparedMemoryLabel},
			Properties: map[string]any{"text": payload},
		})
		require.NoError(t, err)
	}
}

// preparedMemoryPrepare stages n nodes of text bytes in a transaction of mode
// and prepares the logical commit. A commit the policy refuses is refused by
// this preparation, before the frame exists: it returns nil and the definite
// memory error. Otherwise it returns the frame parked at its preparation
// hand-off, with its forecast reserved and held; the cleanups abort the frame,
// which is a no-op once it was submitted.
func preparedMemoryPrepare(t *testing.T, b *BadgerEngine, mode EntityCommitMode, logicalID string, n, text int) (*PreparedEntityCommit, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, mode)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace(preparedMemoryNamespace))
	preparedMemoryStage(t, tx, n, text)
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
	if err != nil {
		require.Nil(t, prepared, "a refused preparation hands no frame to its journal owner")
		return nil, err
	}
	t.Cleanup(prepared.Abort)
	return prepared, nil
}

// preparedMemorySubmission is everything one submission made observable: the
// error the submitter returned before accepting the commit, or the accepted
// receipt and its terminal outcome. accepted carries the AcceptedCommitError
// that keeps a native cause behind the durable-acceptance barrier
// (commit_receipt_prototype.go), which is never a definite refusal.
type preparedMemorySubmission struct {
	err      error
	receipt  *CommitReceipt
	outcome  CommitOutcome
	accepted *AcceptedCommitError
}

// preparedMemorySubmit submits prepared through the WAL engine, as an outer
// journal owner does, and waits for the accepted outcome when the submission
// accepted the commit. It reports what it observed and asserts nothing.
func preparedMemorySubmit(t *testing.T, b *BadgerEngine, w *WAL, prepared *PreparedEntityCommit) preparedMemorySubmission {
	t.Helper()
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(context.Background(), prepared, nil)
	submission := preparedMemorySubmission{err: err, receipt: receipt}
	if err != nil || receipt == nil {
		t.Logf("submission: error %v, receipt %v", err, receipt)
		return submission
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	submission.outcome = receipt.WaitOutcome(ctx)
	var accepted *AcceptedCommitError
	if errors.As(submission.outcome.Err, &accepted) {
		submission.accepted = accepted
	}
	t.Logf("submission: accepted outcome state=%d err=%v accepted-barrier=%v", submission.outcome.State, submission.outcome.Err, submission.accepted != nil)
	return submission
}

// requirePreparedMemoryRefusal is the required outcome of a commit the policy
// refuses: a definite error naming the memory cause and no accepted receipt, so
// no accepted command is left for the journal's recovery to apply. An accepted
// outcome reports its own state and native cause instead of this refusal.
func requirePreparedMemoryRefusal(t *testing.T, s preparedMemorySubmission) {
	t.Helper()
	if s.accepted != nil {
		t.Logf("accepted outcome %d retains the native cause behind AcceptedCommitError: %v", s.accepted.State, s.accepted.Err)
	}
	require.Nil(t, s.receipt, "a refused commit owns no accepted receipt (outcome state %d, native cause %v)", s.outcome.State, s.outcome.Err)
	require.Error(t, s.err, "SubmitEntityCommit must report the refusal, not only an accepted receipt")
	require.ErrorIs(t, s.err, memory.ErrLimitExceeded)
	var accepted *AcceptedCommitError
	require.False(t, errors.As(s.err, &accepted), "an accepted outcome is not a definite refusal")
}

// preparedMemoryRecords counts the journal's records that name logicalID, by
// kind: the semantic entity commit the WAL engine journals (OpEntityCommit) and
// the write buffer's physical record of the same commit (OpBufferedCommit).
func preparedMemoryRecords(t *testing.T, dir, logicalID string) (semantic, buffered int) {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
	require.NoError(t, err)
	for _, entry := range entries {
		switch entry.Operation {
		case OpEntityCommit:
			var data walEntityCommitData
			require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
			envelope, _, err := DecodeEntityTransaction(data.Payload)
			require.NoError(t, err)
			if envelope.LogicalTxID == logicalID {
				semantic++
			}
		case OpBufferedCommit:
			data, err := decodeBufferedWALRecord(entry)
			require.NoError(t, err)
			if data.LogicalTxID == logicalID {
				buffered++
			}
		}
	}
	return semantic, buffered
}

// preparedMemoryStored drains the buffer and returns the nodes labelled
// preparedMemoryLabel the store then holds.
func preparedMemoryStored(t *testing.T, b *BadgerEngine) int {
	t.Helper()
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	nodes, err := b.GetNodesByLabel(context.Background(), preparedMemoryLabel)
	require.NoError(t, err)
	return len(nodes)
}

// preparedMemoryRecovered crashes b and its journal (crashClose: the write
// buffer is stopped and dropped without a drain; the store and the journal are
// closed) and opens them again with the recovery nornicdb.Open runs. The
// reopened engine carries no memory policy, so it shows what the journal alone
// makes durable, independent of the live admission decision.
func preparedMemoryRecovered(t *testing.T, b *BadgerEngine, w *WAL, dir string) *BadgerEngine {
	t.Helper()
	crashClose(t, b, w)
	reopened, reopenedWAL := preparedMemoryEngine(t, dir, nil)
	_, err := RecoverBufferedCommitsAtOpen(reopened, reopenedWAL)
	require.NoError(t, err)
	return reopened
}

// Ten nodes of 300 KiB in one implicit transaction forecast a buffered commit
// of about 9 MiB, nine times the fixture's 1 MiB policy. The commit must be
// refused definitely before the journal holds either record of it: no
// OpEntityCommit semantic record and no OpBufferedCommit physical record for the
// logical ID, no reservation left on the policy, no node in the store, and no
// node after crashClose and the recovery at open.
//
// The reservation precedes the prepared handoff, so the refusal is
// preparation's own definite error: the frame never reaches its journal owner.
// Submission below therefore only happens for a frame preparation returned.
func TestPreparedEntityCommitMemoryRefusalPrecedesJournal(t *testing.T) {
	dir := t.TempDir()
	policy := preparedMemoryPolicy(t)
	b, w := preparedMemoryEngine(t, dir, policy)
	const logicalID = "prepared-memory-refusal"
	prepared, prepareErr := preparedMemoryPrepare(t, b, CommitModeImplicit, logicalID, preparedMemoryNodes, preparedMemoryNodeText)
	t.Logf("preparation: frame=%v err=%v reserved=%d", prepared != nil, prepareErr, policy.Reserved())
	policyBefore := policy.Reserved()
	submission := preparedMemorySubmission{err: prepareErr}
	if prepared != nil {
		submission = preparedMemorySubmit(t, b, w, prepared)
	}
	semantic, buffered := preparedMemoryRecords(t, dir, logicalID)
	reserved := policy.Reserved()
	stored := preparedMemoryStored(t, b)
	t.Logf("reserved before=%d after=%d; journal semantic=%d buffered=%d for %q; stored=%d",
		policyBefore, reserved, semantic, buffered, logicalID, stored)
	reopened := preparedMemoryRecovered(t, b, w, dir)
	recovered := preparedMemoryStored(t, reopened)
	t.Logf("after crashClose and the recovery at open: %d nodes labelled %s", recovered, preparedMemoryLabel)

	requirePreparedMemoryRefusal(t, submission)
	require.Zero(t, semantic, "the journal must hold no semantic record of the refused commit")
	require.Zero(t, buffered, "the journal must hold no buffered record of the refused commit")
	require.Zero(t, policy.Reserved(), "a refused commit leaves no reservation")
	require.Zero(t, stored, "the store must hold no node of the refused commit")
	require.Zero(t, recovered, "the recovery at open must not apply a refused commit")
}

// The same fixture and policy admit a commit within the limit: ten nodes of
// 10 KiB forecast about 300 KiB. The commit is accepted, applies, holds its
// forecast until the generation is flushed, and releases it when the generation
// retires. This is the control that the refusal above is a limit decision, not
// a rejected route.
func TestPreparedEntityCommitWithinMemoryLimitCommits(t *testing.T) {
	dir := t.TempDir()
	policy := preparedMemoryPolicy(t)
	b, w := preparedMemoryEngine(t, dir, policy)
	const logicalID = "prepared-memory-within-limit"
	prepared, err := preparedMemoryPrepare(t, b, CommitModeImplicit, logicalID, preparedMemoryNodes, preparedMemorySmallText)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	// The prepared frame waits for its journal owner with the real lease of its
	// forecast already held: the same lease the acknowledged generation keeps.
	held := policy.Reserved()
	require.Positive(t, held, "a within-limit prepared commit holds its forecast while it waits")
	submission := preparedMemorySubmit(t, b, w, prepared)
	require.NoError(t, submission.err, "a commit within the policy is accepted")
	require.NotNil(t, submission.receipt, "an accepted commit returns its receipt")
	require.NoError(t, submission.outcome.Err, "an applied commit carries no outcome error")
	require.Equal(t, CommitApplied, submission.outcome.State)
	semantic, buffered := preparedMemoryRecords(t, dir, logicalID)
	reserved := policy.Reserved()
	t.Logf("held at preparation=%d; journal semantic=%d buffered=%d for %q; reserved=%d", held, semantic, buffered, logicalID, reserved)
	require.Equal(t, 1, semantic, "the journal holds the commit's semantic record")
	require.Equal(t, 1, buffered, "and its buffered record")
	require.Equal(t, held, reserved, "the acknowledged generation receives the one reservation, not a second")
	require.Equal(t, preparedMemoryNodes, preparedMemoryStored(t, b))
	require.Zero(t, policy.Reserved(), "retiring the generation releases the forecast")
}

// An explicit prepared commit takes the direct route: bufferedCommit is false
// for CommitModeExplicit (badger_transaction_publication.go), so
// commitBufferedLocked and its reservation are not reached. The same ten nodes
// of 300 KiB commit and survive the recovery at open, even though the buffered
// route would refuse them. The explicit route's existing policy contract is
// unchanged.
func TestPreparedExplicitEntityCommitIsNotMemoryRefused(t *testing.T) {
	dir := t.TempDir()
	policy := preparedMemoryPolicy(t)
	b, w := preparedMemoryEngine(t, dir, policy)
	const logicalID = "prepared-memory-explicit"
	prepared, err := preparedMemoryPrepare(t, b, CommitModeExplicit, logicalID, preparedMemoryNodes, preparedMemoryNodeText)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	submission := preparedMemorySubmit(t, b, w, prepared)
	require.NoError(t, submission.err, "the explicit direct route is not memory-refused")
	require.NotNil(t, submission.receipt)
	require.NoError(t, submission.outcome.Err)
	require.Equal(t, CommitApplied, submission.outcome.State)
	semantic, buffered := preparedMemoryRecords(t, dir, logicalID)
	reserved := policy.Reserved()
	t.Logf("journal semantic=%d buffered=%d for %q; reserved=%d", semantic, buffered, logicalID, reserved)
	require.Equal(t, 1, semantic, "the journal holds the commit's semantic record")
	require.Zero(t, buffered, "the direct route appends no buffered record")
	require.Zero(t, policy.Reserved(), "the direct route holds no buffer forecast")
	require.Equal(t, preparedMemoryNodes, preparedMemoryStored(t, b))
	reopened := preparedMemoryRecovered(t, b, w, dir)
	require.Equal(t, preparedMemoryNodes, preparedMemoryStored(t, reopened), "the committed nodes survive the recovery at open")
}

// A prepared frame that is discarded, and one the journal refuses because the
// caller's context is canceled, both end before any record of the commit: no
// semantic or buffered record, no reservation on the policy, and no node in the
// store or after the recovery at open. Neither path is a memory decision, but
// both stage a within-limit commit, so the lease the frame holds while it waits
// is real and its release is observable.
func TestPreparedEntityCommitMemoryLeaseReleasedOnDiscard(t *testing.T) {
	for _, c := range []struct {
		name    string
		logical string
		submit  bool
	}{
		{name: "discard", logical: "prepared-memory-discard"},
		{name: "canceled submit", logical: "prepared-memory-canceled", submit: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			policy := preparedMemoryPolicy(t)
			b, w := preparedMemoryEngine(t, dir, policy)
			prepared, err := preparedMemoryPrepare(t, b, CommitModeImplicit, c.logical, preparedMemoryNodes, preparedMemorySmallText)
			require.NoError(t, err)
			require.NotNil(t, prepared)
			held := policy.Reserved()
			require.Positive(t, held, "the discarded frame holds its forecast while it waits")
			if c.submit {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				receipt, err := NewWALEngine(b, w).SubmitEntityCommit(ctx, prepared, nil)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, receipt, "a canceled caller context accepts nothing")
				require.Equal(t, held, policy.Reserved(), "a canceled submission leaves the parked frame's lease held")
				t.Logf("canceled submission: err=%v receipt=%v reserved=%d", err, receipt, policy.Reserved())
			}
			prepared.Abort()
			require.Zero(t, policy.Reserved(), "discarding the frame releases its forecast")
			semantic, buffered := preparedMemoryRecords(t, dir, c.logical)
			reserved := policy.Reserved()
			stored := preparedMemoryStored(t, b)
			t.Logf("after discard: journal semantic=%d buffered=%d for %q; reserved=%d; stored=%d",
				semantic, buffered, c.logical, reserved, stored)
			reopened := preparedMemoryRecovered(t, b, w, dir)
			recovered := preparedMemoryStored(t, reopened)

			require.Zero(t, semantic, "the journal holds no semantic record")
			require.Zero(t, buffered, "the journal holds no buffered record")
			require.Zero(t, policy.Reserved(), "a discarded frame leaves no reservation")
			require.Zero(t, stored, "the store holds no node")
			require.Zero(t, recovered, "the recovery at open applies no node")
		})
	}
}
