package multidb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type catalogDurability uint8

const (
	catalogPhysicalPublication catalogDurability = iota
	catalogDurableDrop
)

// DROP's durability acknowledgement belongs to the same physical receipt that
// publishes the catalog image; it is independent of the caller's wait lifetime.
type catalogTransitionPolicy struct {
	durability catalogDurability
	dropName   string
}

// transitionCatalog serializes proposals until the authoritative storage receipt
// settles. A canceled waiter neither releases accepted authority nor publishes it.
// No manager read lock is retained across storage preparation or settlement.
func (m *DatabaseManager) transitionCatalog(ctx context.Context, policy catalogTransitionPolicy, edit func(*DatabaseManager) error) error {
	if ctx == nil {
		return fmt.Errorf("catalog mutation requires an operation context: %w", storage.ErrInvalidData)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.catalogGate:
	}
	release := func() { m.catalogGate <- struct{}{} }
	if err := ctx.Err(); err != nil {
		release()
		return err
	}
	owner := managedEntityOwnerFor(m.inner)
	if !owner.found || owner.begin == nil || owner.route.Submitter == nil {
		release()
		return fmt.Errorf("catalog storage requires entity commit receipts: %w", storage.ErrInvalidData)
	}
	if owner.route.CheckAdmission != nil {
		if err := owner.route.CheckAdmission(); err != nil {
			release()
			return err
		}
	}
	m.mu.RLock()
	candidate := &DatabaseManager{inner: m.inner, config: m.config, databases: make(map[string]*DatabaseInfo, len(m.databases)), pendingDrops: maps.Clone(m.pendingDrops), serverID: m.serverID, serverCreatedAt: m.serverCreatedAt, remoteCredentialCipher: m.remoteCredentialCipher}
	for name, info := range m.databases {
		candidate.databases[name] = cloneDatabaseInfo(info, false, databaseSnapshotFull)
	}
	m.mu.RUnlock()
	if err := edit(candidate); err != nil {
		release()
		return err
	}
	node, err := candidate.catalogMetadataNode()
	if err != nil {
		release()
		return err
	}
	tx, err := storage.BeginEntityTransaction(m.inner, storage.CommitModeDirect)
	if err != nil {
		release()
		return err
	}
	if err = tx.SetNamespace(m.config.SystemDatabase); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	qualified, err := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase).NodeWriteInput(node)
	if err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	previous, readErr := tx.GetNode(qualified.ID)
	operation := storage.Operation{Type: storage.OpUpdateNode, Node: qualified}
	var apply func() error
	if errors.Is(readErr, storage.ErrNotFound) {
		operation.Type = storage.OpCreateNode
		apply = func() error { _, err := tx.CreateNode(qualified); return err }
	} else if readErr != nil {
		_ = tx.Rollback()
		release()
		return readErr
	} else {
		qualified.CreatedAt = previous.CreatedAt
		operation.OldNode = previous
		apply = func() error { return tx.UpdateGraphNode(qualified, storage.ClientNodeEdits) }
	}
	for i := len(owner.writeBoundaries) - 1; i >= 0; i-- {
		policy, next := owner.writeBoundaries[i], apply
		apply = func() error { return policy.WithEntityChange(operation, next) }
	}
	if err = apply(); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	logicalID := uuid.NewString()
	prepared, err := tx.PrepareEntityCommit(ctx, storage.EntityCommitRequest{LogicalID: logicalID})
	if err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	var publicationErr error
	handoff := &dropHandoff{name: policy.dropName}
	receipt, err := owner.route.Submitter.SubmitEntityCommit(ctx, prepared, func(outcome storage.CommitOutcome) {
		if outcome.State == storage.CommitApplied {
			freshDrop := false
			if policy.durability == catalogDurableDrop && candidate.pendingDrops[policy.dropName] {
				m.mu.RLock()
				freshDrop = !m.pendingDrops[policy.dropName]
				m.mu.RUnlock()
			}
			publicationErr = m.publishCatalog(candidate)
			if freshDrop {
				defer handoff.intentApplied(m)
			}
			if policy.durability == catalogDurableDrop {
				syncErr := m.syncDropMetadata()
				if syncErr != nil {
					// Applied metadata is never restored. The existing DROP
					// owner retains only the unavailable-name durability fence.
					m.mu.Lock()
					m.transitionPendingDrop(policy.dropName, true)
					m.mu.Unlock()
				}
				publicationErr = compose.Outcomes(publicationErr, syncErr)
			}
		}
		release()
	})
	if err != nil {
		prepared.Abort()
		release()
		return err
	}
	outcome := receipt.WaitOutcome(ctx)
	if outcome.State == storage.CommitDiscarded {
		return compose.Outcomes(errors.New("catalog commit discarded"), outcome.Err)
	}
	if outcome.State == storage.CommitRecoveryPending {
		handoff.waiterDetached(m)
		return storage.AcceptedOutcome(outcome.State, logicalID, outcome.Err)
	}
	err = compose.Outcomes(outcome.Err, publicationErr)
	if err != nil {
		return storage.AcceptedOutcome(storage.CommitApplied, logicalID, err)
	}
	return nil
}

// publishCatalog is the sole publication boundary for accepted catalog changes.
// Existing accounting/runtime identities stay attached to their physical lifetime.
func (m *DatabaseManager) publishCatalog(candidate *DatabaseManager) (result error) {
	m.mu.Lock()
	changed := false
	defer func() { m.unlockStatementCacheIndexes(changed, false) }()
	for name, old := range m.databases {
		next := candidate.databases[name]
		if next == nil || !m.pendingDrops[name] && candidate.pendingDrops[name] {
			delete(m.engines, name)
			m.dropDatabaseStatementCachesLocked(name)
			changed = true
			if old.Type == "standard" {
				if runtime := old.managed.Load(); runtime != nil {
					runtime.retireDatabase()
				}
			}
		}
		if next == nil {
			m.applyChangeCapture(name, nil)
			delete(m.limitCheckers, name)
			continue
		}
		if next.Status != old.Status && next.Status != "online" {
			delete(m.engines, name)
			m.dropDatabaseStatementCachesLocked(name)
			changed = true
		}
		for _, alias := range old.Aliases {
			if !slices.Contains(next.Aliases, alias) {
				m.invalidateStatementCacheReferenceLocked(alias)
				changed = true
			}
		}
		for _, ref := range canonicalConstituents(old.Constituents, "") {
			_, replacement, exists := findCanonicalConstituent(next.Constituents, ref.Alias)
			if !exists || replacement != ref {
				m.invalidateStatementCacheReferenceLocked(name + "." + ref.Alias)
				changed = true
			}
		}
	}
	published := make(map[string]*DatabaseInfo, len(candidate.databases))
	for name, next := range candidate.databases {
		current := m.databases[name]
		fresh := current == nil
		if fresh {
			current = next
		} else {
			current.ID, current.Name, current.CreatedAt, current.CreatedBy = next.ID, next.Name, next.CreatedAt, next.CreatedBy
			current.Status, current.Type, current.IsDefault, current.UpdatedAt = next.Status, next.Type, next.IsDefault, next.UpdatedAt
			current.Aliases, current.Constituents, current.Options = next.Aliases, next.Constituents, next.Options
			if !reflect.DeepEqual(current.limitsSnapshot(), next.Limits) {
				current.Limits = cloneLimits(next.Limits)
				if runtime := current.managed.Load(); runtime != nil {
					runtime.policyUpdateMu.Lock()
					runtime.policy.updateLimits(next.Limits)
					runtime.policyUpdateMu.Unlock()
				}
			}
		}
		published[name] = current
		if fresh && !m.statementCachesClosed && current.Type == "standard" && !candidate.pendingDrops[name] {
			runtime, err := bindManagedRuntime(current, current.Limits, name, managedEntityOwnerFor(m.inner))
			if err != nil {
				result = compose.Outcomes(result, err)
			} else {
				runtime.activateDatabase(current.Limits)
			}
		}
		// Accepted metadata can settle after Close; runtime effects belong to
		// the open manager lifetime and must not restart a stopped CDC owner.
		if !m.statementCachesClosed {
			m.applyChangeCapture(name, next.Options)
		}
	}
	m.databases = published
	for name := range m.pendingDrops {
		if !candidate.pendingDrops[name] {
			m.transitionPendingDrop(name, false)
		}
	}
	for name, pending := range candidate.pendingDrops {
		if pending {
			m.transitionPendingDrop(name, true)
		}
	}
	m.serverID, m.serverCreatedAt = candidate.serverID, candidate.serverCreatedAt
	m.rebuildRuntimeConstituents()
	if !m.statementCachesClosed {
		result = compose.Outcomes(result, m.registerGraphRuntimeOwners())
	}
	return result
}
