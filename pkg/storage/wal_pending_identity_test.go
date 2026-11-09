package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The first accepted command remains pending at a deterministic native boundary.
// Both databases must receive their own journal entries and outcomes even when
// their logical IDs match. The distinct-ID case controls the fixture; retries
// within each database must still join the existing accepted command.
func TestWALPendingEntityCommitIdentityIsDatabaseScoped(t *testing.T) {
	for _, betaID := range []string{"shared-identity", "distinct-beta-identity"} {
		t.Run(betaID, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			fixture := newWALPendingIdentityFixture(t)
			const (
				alphaDatabase = "alpha"
				betaDatabase  = "beta"
				alphaID       = "shared-identity"
			)

			alpha := fixture.prepare(t, ctx, alphaDatabase, "alpha:a", alphaID)
			beta := fixture.prepare(t, ctx, betaDatabase, "beta:b", betaID)
			// The two accepted commands are distinct work in distinct databases with
			// either shared or distinct logical IDs, which native acceptance permits.
			require.Equal(t, alphaID, alpha.LogicalID())
			require.Equal(t, betaID, beta.LogicalID())
			require.Equal(t, alphaDatabase, alpha.Namespace())
			require.Equal(t, betaDatabase, beta.Namespace())
			require.NotEmpty(t, alpha.Operations())
			require.NotEmpty(t, beta.Operations())

			// Publish the first database's commit: it journals, then waits at the
			// accepted native boundary, so it stays in the WAL pending registry.
			alphaSettled := make(chan CommitOutcome, 1)
			alphaReceipt, err := fixture.owner.SubmitEntityCommit(ctx, alpha, func(outcome CommitOutcome) { alphaSettled <- outcome })
			require.NoError(t, err)
			require.NotNil(t, alphaReceipt)
			require.Equal(t, []string{alphaDatabase + "/" + alphaID}, fixture.parked.waitForApplications(t, ctx, 1),
				"the first database's accepted commit must be journaled and awaiting native application")
			assert.Equal(t, []string{alphaDatabase + "/" + alphaID}, journalEntityCommitIdentities(t, fixture.wal),
				"the first database's accepted commit must have exactly one journal record")

			// A retry inside the same database (a second prepared frame for the same
			// accepted identity, as an uncertain client retry produces) must join the
			// pending receipt instead of journaling a second record. This is the
			// accepted-once behavior the pending registry exists for and it must
			// survive the identity scoping.
			alphaRetry := fixture.prepare(t, ctx, alphaDatabase, "alpha:retry", alphaID)
			alphaRetryReceipt, err := fixture.owner.SubmitEntityCommit(ctx, alphaRetry, nil)
			require.NoError(t, err)
			require.Same(t, alphaReceipt, alphaRetryReceipt,
				"an in-flight retry in the same database must join that database's pending receipt")
			assert.Equal(t, []string{alphaDatabase + "/" + alphaID}, journalEntityCommitIdentities(t, fixture.wal),
				"an in-flight retry in the same database must not journal a second record")

			// The second database submits while the first
			// database's commit is demonstrably pending.
			betaSettled := make(chan CommitOutcome, 1)
			betaReceipt, err := fixture.owner.SubmitEntityCommit(ctx, beta, func(outcome CommitOutcome) { betaSettled <- outcome })
			require.NoError(t, err)
			require.NotNil(t, betaReceipt)
			assert.NotSame(t, alphaReceipt, betaReceipt,
				"database %q must not receive database %q's pending receipt for logical identity %q",
				betaDatabase, alphaDatabase, alphaID)
			assert.Equal(t, map[string]int{
				alphaDatabase + "/" + alphaID: 1,
				betaDatabase + "/" + betaID:   1,
			}, pendingEntityCommitIdentities(fixture.owner),
				"the WAL pending owner must key accepted commits by database and logical identity")
			assert.Equal(t, []string{alphaDatabase + "/" + alphaID, betaDatabase + "/" + betaID},
				journalEntityCommitIdentities(t, fixture.wal),
				"each database's accepted commit needs its own journal record")

			fixture.release()
			alphaOutcome := waitAcceptedOutcome(t, ctx, alphaReceipt, alphaDatabase)
			assert.Equal(t, CommitApplied, alphaOutcome.State, "database %q commit outcome: %v", alphaDatabase, acceptedCause(alphaOutcome.Err))
			betaOutcome := waitAcceptedOutcome(t, ctx, betaReceipt, betaDatabase)
			assert.Equal(t, CommitApplied, betaOutcome.State, "database %q commit outcome: %v", betaDatabase, acceptedCause(betaOutcome.Err))

			alphaNode, err := fixture.native.GetStoredNode(t.Context(), "alpha:a")
			assert.NoError(t, err, "database %q write must be published", alphaDatabase)
			if err == nil {
				assert.Equal(t, alphaDatabase, alphaNode.Properties["database"])
			}
			betaNode, err := fixture.native.GetStoredNode(t.Context(), "beta:b")
			assert.NoError(t, err, "database %q write must be published", betaDatabase)
			if err == nil {
				assert.Equal(t, betaDatabase, betaNode.Properties["database"])
			}
			_, err = fixture.native.GetStoredNode(t.Context(), "alpha:retry")
			assert.ErrorIs(t, err, ErrNotFound,
				"an in-flight duplicate in the same database must not be applied a second time")

			// Each accepted command owns its own settlement: the second database's
			// receipt is settled by its own application, not by the first database's.
			select {
			case outcome := <-alphaSettled:
				assert.Equal(t, CommitApplied, outcome.State)
			default:
				assert.Fail(t, "database alpha's accepted command never settled its receipt")
			}
			select {
			case outcome := <-betaSettled:
				assert.Equal(t, CommitApplied, outcome.State)
			default:
				assert.Fail(t, "database beta's accepted command never settled its receipt")
			}

			// A durable duplicate of either database's identity (the accepted record is
			// already applied, so the native owner reports a recorded receipt) must not
			// journal another record or apply its staged change.
			journalBefore := journalEntityCommitIdentities(t, fixture.wal)
			for _, retry := range []struct{ database, nodeID, logicalID string }{
				{alphaDatabase, "alpha:durable", alphaID},
				{betaDatabase, "beta:durable", betaID},
			} {
				retryPrepared := fixture.prepare(t, ctx, retry.database, retry.nodeID, retry.logicalID)
				// Ownership transfers to the accepted command; the frame must not be
				// aborted after submission.
				retryReceipt, err := fixture.owner.SubmitEntityCommit(ctx, retryPrepared, nil)
				require.NoError(t, err)
				outcome := retryReceipt.WaitOutcome(ctx)
				assert.Equal(t, CommitApplied, outcome.State,
					"a durable duplicate in %q must report the recorded outcome, saw state %d: %v",
					retry.database, outcome.State, acceptedCause(outcome.Err))
				_, err = fixture.native.GetStoredNode(t.Context(), NodeID(retry.nodeID))
				assert.ErrorIs(t, err, ErrNotFound,
					"a durable duplicate in %q must not be applied", retry.database)
			}
			assert.Equal(t, journalBefore, journalEntityCommitIdentities(t, fixture.wal),
				"durable duplicates must not add journal records")
		})
	}
}

// walPendingIdentityFixture owns one WAL engine whose inner native owner holds
// every accepted application until release.
type walPendingIdentityFixture struct {
	native  *BadgerEngine
	parked  *parkedWALEntitySubmitter
	wal     *WAL
	owner   *WALEngine
	release func()
}

func newWALPendingIdentityFixture(t *testing.T) *walPendingIdentityFixture {
	t.Helper()
	var parked *parkedWALEntitySubmitter
	native, wal, owner := newWALClientFixture(t, func(engine *MemoryEngine) Engine {
		parked = newParkedWALEntitySubmitter(engine)
		return parked
	})
	var once sync.Once
	release := func() { once.Do(func() { close(parked.release) }) }
	// Cleanup order (LIFO): release every parked native application and join it,
	// then close the WAL engine, the WAL and the native engine.
	t.Cleanup(func() {
		release()
		parked.inflight.Wait()
	})
	return &walPendingIdentityFixture{native: native.BadgerEngine, parked: parked, wal: wal, owner: owner, release: release}
}

// prepare stages one entity commit of database in a fresh native transaction
// and runs the native commit pipeline up to its prephysical boundary, exactly
// as an accepted client commit does.
func (f *walPendingIdentityFixture) prepare(t *testing.T, ctx context.Context, database, nodeID, logicalID string) *PreparedEntityCommit {
	t.Helper()
	tx, err := BeginEntityTransaction(ctx, f.native, CommitModeDirect)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace(database))
	require.NoError(t, tx.SetValidationSetting(DeferredConstraintValidation, true))
	_, err = tx.CreateNode(ctx, &Node{
		ID:         NodeID(nodeID),
		Labels:     []string{"Record"},
		Properties: map[string]any{"database": database},
	})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
	require.NoError(t, err)
	t.Cleanup(func() {
		if !prepared.accepted.Load() {
			prepared.Abort()
		}
	})
	return prepared
}

// parkedWALEntitySubmitter is the WAL engine's inner native owner. It holds
// every native application at the accepted boundary until the test releases it,
// so a journaled accepted commit stays in the WAL pending registry without a
// sleep and without an injected pending-map value.
type parkedWALEntitySubmitter struct {
	Engine
	mu       sync.Mutex
	seen     []string
	notify   chan struct{}
	release  chan struct{}
	inflight sync.WaitGroup
}

func newParkedWALEntitySubmitter(engine Engine) *parkedWALEntitySubmitter {
	return &parkedWALEntitySubmitter{
		Engine:  engine,
		notify:  make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (p *parkedWALEntitySubmitter) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	p.inflight.Add(1)
	defer p.inflight.Done()
	p.mu.Lock()
	p.seen = append(p.seen, prepared.Namespace()+"/"+prepared.LogicalID())
	select {
	case p.notify <- struct{}{}:
	default:
	}
	release := p.release
	p.mu.Unlock()
	select {
	case <-release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	route, found := EntityCommitSubmitterFor(p.Engine)
	if !found {
		return nil, ErrInvalidData
	}
	return route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
}

// waitForApplications blocks until n native applications reached the accepted
// boundary and returns the observed "<database>/<logicalID>" identities.
func (p *parkedWALEntitySubmitter) waitForApplications(t *testing.T, ctx context.Context, n int) []string {
	t.Helper()
	for {
		p.mu.Lock()
		seen := append([]string(nil), p.seen...)
		notify := p.notify
		p.mu.Unlock()
		if len(seen) >= n {
			return seen
		}
		select {
		case <-notify:
		case <-ctx.Done():
			t.Fatalf("waiting for %d parked native applications, saw %v: %v", n, seen, ctx.Err())
			return seen
		}
	}
}

// pendingEntityCommitIdentities reports the "<database>/<logicalID>" identity of
// every accepted commit currently held in the WAL pending registry. It reads the
// accepted handle's immutable identity, not the registry key.
func pendingEntityCommitIdentities(owner *WALEngine) map[string]int {
	identities := map[string]int{}
	owner.pendingEntityCommits.Range(func(_, value any) bool {
		pending, ok := value.(*walAcceptedEntityCommit)
		if !ok || pending.handle == nil {
			identities["unknown"]++
			return true
		}
		database, logicalID := pending.handle.Identity()
		identities[database+"/"+logicalID]++
		return true
	})
	return identities
}

// journalEntityCommitIdentities returns the sorted "<database>/<logicalID>"
// identity of every entity_commit record in the active WAL.
func journalEntityCommitIdentities(t *testing.T, wal *WAL) []string {
	t.Helper()
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	identities := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Operation != OpEntityCommit {
			continue
		}
		var payload walEntityCommitData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &payload))
		envelope, _, err := DecodeEntityTransaction(payload.Payload)
		require.NoError(t, err)
		require.Equal(t, entry.Database, envelope.Namespace,
			"a journal record's database must match its accepted identity")
		identities = append(identities, envelope.Namespace+"/"+envelope.LogicalTxID)
	}
	sort.Strings(identities)
	return identities
}

// waitAcceptedOutcome waits, on the bounded test context, for an accepted
// receipt to reach a terminal outcome.
func waitAcceptedOutcome(t *testing.T, ctx context.Context, receipt *CommitReceipt, database string) CommitOutcome {
	t.Helper()
	require.NotNil(t, receipt)
	outcome := receipt.WaitOutcome(ctx)
	require.NotEqual(t, CommitRecoveryPending, outcome.State,
		"database %q accepted commit did not settle on its own receipt: %v", database, acceptedCause(outcome.Err))
	return outcome
}

// acceptedCause returns the native diagnostic behind an accepted outcome
// barrier so a failure report names the underlying cause.
func acceptedCause(err error) error {
	var accepted *AcceptedCommitError
	if errors.As(err, &accepted) {
		return accepted.Err
	}
	return err
}
