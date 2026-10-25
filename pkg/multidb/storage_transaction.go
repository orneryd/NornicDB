package multidb

import (
	"context"
	"errors"
	"fmt"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// managedEntityOwner retains capabilities before transparent decorators are
// unwrapped. The first submitter is authoritative; an inner native owner must
// never bypass an outer journal or replication boundary.
type managedEntityOwner struct {
	route storage.EntityCommitRoute
	found bool
	begin interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	}
	observer        storedEntityObserverInitializer
	registry        storage.ManagedEntityRuntimeRegistry
	boundary        storage.StoredEntityReadBoundary
	namespace       *storage.NamespacedEngine
	writeBoundaries []storage.EntityWriteBoundary
}

func managedEntityOwnerFor(engine storage.Engine) managedEntityOwner {
	route, found := storage.EntityCommitSubmitterFor(engine)
	owner := managedEntityOwner{route: route, found: found}
	for engine != nil {
		if policy, ok := engine.(storage.EntityWriteBoundary); ok {
			owner.writeBoundaries = append(owner.writeBoundaries, policy)
		}
		if owner.boundary == nil {
			owner.boundary, _ = engine.(storage.StoredEntityReadBoundary)
		}
		if owner.begin == nil {
			owner.begin, _ = engine.(interface {
				BeginTransaction() (*storage.BadgerTransaction, error)
			})
		}
		if owner.registry == nil {
			owner.registry, _ = engine.(storage.ManagedEntityRuntimeRegistry)
		}
		if owner.observer == nil {
			owner.observer, _ = engine.(storedEntityObserverInitializer)
		}
		if owner.namespace == nil {
			owner.namespace, _ = engine.(*storage.NamespacedEngine)
		}
		wrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		next := wrapper.GetInnerEngine()
		if next == engine {
			break
		}
		engine = next
	}
	return owner
}
func (t *sizeTrackingEngine) managedUsageState() (*DatabaseInfo, *Limits, error) {
	if t.runtimeErr != nil {
		return nil, nil, t.runtimeErr
	}
	if t.runtime == nil || t.runtime.retired.Load() {
		return nil, nil, ErrDatabaseNotFound
	}
	return t.runtime.accounting, t.runtime.policy.snapshot(), nil
}
func (t *sizeTrackingEngine) managedCapabilityError() error {
	return fmt.Errorf("database %q requires ManagedEntityEngine atomic publication capabilities: %w", t.dbName, storage.ErrInvalidData)
}

// admitPrepared is the one admission owner for explicit transactions and direct
// command preparation. It returns native ownership and terminal settlement,
// without submitting or waiting. Preaccept failures close the original snapshot.
func (t *sizeTrackingEngine) admitPrepared(ctx context.Context, tx *storage.BadgerTransaction, owner managedEntityOwner) (prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome), prepareErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var info *DatabaseInfo
	reserved := false
	defer func() {
		if prepareErr != nil {
			if prepared != nil {
				prepared.Abort()
			} else {
				_ = tx.Rollback()
			}
			if reserved {
				info.discardStorageUsage(prepared.LogicalID())
			}
			prepareErr = nornicerrors.MarkCommitRolledBack(prepareErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	summary := tx.PendingChanges(false)
	if summary.Namespace != t.dbName {
		return nil, nil, storage.ErrCrossNamespaceTransaction
	}
	if owner.route.CheckAdmission != nil {
		if err := owner.route.CheckAdmission(); err != nil {
			return nil, nil, err
		}
	}
	if !owner.found || owner.observer == nil || owner.registry == nil {
		return nil, nil, t.managedCapabilityError()
	}
	if summary.Operations == 0 {
		// An identical hidden relationship can be a true no-op. Finish its native
		// read-only view; the leader must acknowledge it without an empty log batch.
		return nil, nil, tx.CommitContext(ctx)
	}
	var limits *Limits
	var err error
	info, limits, err = t.managedUsageState()
	if err != nil {
		return nil, nil, err
	}
	info.sizeMu.RLock()
	trackBytes := info.sizeInitialized
	info.sizeMu.RUnlock()
	trackBytes = trackBytes || (limits != nil && limits.Storage.MaxBytes > 0)
	// Snapshot installation precedes native preparation and manager admission.
	if err := t.runtime.initializeStorageUsageWithContext(ctx, trackBytes, owner.boundary); err != nil {
		return nil, nil, err
	}
	prepared, err = tx.PrepareEntityCommit(ctx, tx.ID)
	if err != nil {
		return nil, nil, err
	}
	var changes []storage.StoredEntityChange
	var delta storageUsage
	if trackBytes {
		changes, err = prepared.ChangesWithContext(ctx)
		if err != nil {
			return prepared, nil, err
		}
		delta, err = storageChangesUsageWithContext(ctx, t.dbName, changes, true)
	} else {
		delta.nodes, delta.edges, err = prepared.CountDeltaWithContext(ctx)
	}
	if err != nil {
		return prepared, nil, err
	}
	release, err := t.writeMu.acquire(ctx, true, summary.HoldsMergeKey)
	if err != nil {
		return prepared, nil, err
	}
	defer release()
	info, limits, err = t.managedUsageState()
	if err != nil {
		return prepared, nil, err
	}
	info.sizeMu.RLock()
	bytesReady := info.sizeInitialized
	info.sizeMu.RUnlock()
	if limits != nil && limits.Storage.MaxBytes > 0 && !bytesReady {
		return prepared, nil, storage.ErrConflict
	}
	if bytesReady && !trackBytes {
		changes, err = prepared.ChangesWithContext(ctx)
		if err != nil {
			return prepared, nil, err
		}
		delta, err = storageChangesUsageWithContext(ctx, t.dbName, changes, true)
		if err != nil {
			return prepared, nil, err
		}
	}
	storageLimits := StorageLimits{}
	if limits != nil {
		storageLimits = limits.Storage
	}
	if err := info.reserveStorageUsage(t.dbName, prepared.LogicalID(), delta, storageLimits); err != nil {
		return prepared, nil, err
	}
	reserved = true
	checker := t.checker
	if checker == nil {
		checker = &databaseLimitChecker{manager: t.manager, databaseName: t.dbName, localPolicy: t.runtime.policy}
	}
	if limits != nil && limits.Rate.MaxWritesPerSecond > 0 {
		for i := 0; i < summary.Operations; i++ {
			if err := checker.CheckWriteRate(); err != nil {
				return prepared, nil, err
			}
		}
	}
	logicalID := prepared.LogicalID()
	return prepared, func(outcome storage.CommitOutcome) {
		if outcome.State == storage.CommitDiscarded {
			info.discardStorageUsage(logicalID)
		}
	}, nil
}

// CommitTransaction keeps ordinary read-only/schema and untracked native paths,
// while accepted entity work survives caller cancellation on its own receipt.
func (t *sizeTrackingEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) (commitErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	accepted, ordinaryAttempted := false, false
	defer func() {
		if !accepted {
			_ = tx.Rollback()
			// Before an attempted ordinary commit or ownership transfer every failure
			// is a known rollback; Bolt must retain the socket for RESET and reuse.
			if !ordinaryAttempted {
				commitErr = nornicerrors.MarkCommitRolledBack(commitErr)
			}
		}
	}()
	ordinary := func(afterPublication func()) error {
		ordinaryAttempted = true
		err := tx.CommitContextWithPublication(ctx, afterPublication)
		accepted = err == nil
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nornicerrors.MarkCommitRolledBack(err)
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	summary := tx.PendingChanges(false)
	if summary.Operations == 0 {
		return ordinary(nil)
	}
	if summary.Namespace != t.dbName {
		return storage.ErrCrossNamespaceTransaction
	}
	_, limits, err := t.managedUsageState()
	if err != nil {
		return err
	}
	owner := managedEntityOwnerFor(t.Engine)
	if owner.route.CheckAdmission != nil {
		if err := owner.route.CheckAdmission(); err != nil {
			return err
		}
	}
	if !t.runtime.requiresManaged(limits) && owner.found && !owner.route.RequiresSubmission {
		release, err := t.writeMu.acquire(ctx, false, summary.HoldsMergeKey)
		if err != nil {
			return err
		}
		_, limits, err = t.managedUsageState()
		if err != nil {
			release()
			return err
		}
		if !t.runtime.requiresManaged(limits) {
			defer func() {
				if release != nil {
					release()
				}
			}()
			return ordinary(func() { release(); release = nil })
		}
		release()
	}
	prepared, settle, err := t.admitPrepared(ctx, tx, owner)
	if err != nil {
		return err
	}
	if prepared == nil {
		return nil
	}
	receipt, err := owner.route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
	if err != nil {
		prepared.Abort()
		settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
		return err
	}
	accepted = true
	outcome := receipt.WaitOutcome(ctx)
	if outcome.State == storage.CommitDiscarded {
		return nornicerrors.MarkCommitRolledBack(outcome.Err)
	}
	return outcome.Err
}
