package multidb

import (
	"context"
	"errors"
	"strings"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// CommitTransaction preserves database quotas and byte accounting when Cypher
// commits a raw transaction. Admission covers all staged changes and remains
// serialized with ordinary writes until publication completes.
func (t *sizeTrackingEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	changes := tx.PendingChanges(false)
	if ctx == nil {
		ctx = context.Background()
	}
	committed := false
	var release func()
	defer func() {
		if release != nil {
			release()
		}
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	commit := func(afterPublication func()) error {
		err := tx.CommitContextWithPublication(ctx, afterPublication)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nornicerrors.MarkCommitRolledBack(err)
		}
		return err
	}
	if changes.Operations == 0 {
		// Read-only commits bypass write admission but still own cancellation
		// cleanup, including the snapshot and any acquired MERGE key.
		return commit(func() { committed = true })
	}
	if changes.Namespace != t.dbName {
		return nornicerrors.MarkCommitRolledBack(storage.ErrCrossNamespaceTransaction)
	}
	checker := t.checker
	if checker == nil {
		checker = &databaseLimitChecker{manager: t.manager, databaseName: t.dbName}
	}
	limits := checker.limitsSnapshot()
	initialized, err := t.manager.storageSizeInitialized(t.dbName)
	if err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	needsExclusive := func(l *Limits, initialized bool) bool {
		return initialized || (l != nil && (l.Storage.MaxNodes > 0 || l.Storage.MaxEdges > 0 || l.Storage.MaxBytes > 0 || l.Rate.MaxWritesPerSecond > 0))
	}
	exclusive := needsExclusive(limits, initialized)
	for {
		release, err = t.writeMu.acquire(ctx, exclusive, changes.HoldsMergeKey)
		if err != nil {
			return nornicerrors.MarkCommitRolledBack(err)
		}
		// Initialization can win before our shared lease. Recheck inside the gate
		// so a first size read cannot race a write that skips incremental accounting.
		limits = checker.limitsSnapshot()
		initialized, err = t.manager.storageSizeInitialized(t.dbName)
		if err != nil {
			return nornicerrors.MarkCommitRolledBack(err)
		}
		if exclusive || !needsExclusive(limits, initialized) {
			break
		}
		release()
		release = nil
		exclusive = true
	}
	publish := func(nodeBytes, edgeBytes int64) func() {
		return func() {
			committed = true
			if initialized || (limits != nil && limits.Storage.MaxBytes > 0) {
				t.manager.applyStorageSizeDelta(t.dbName, nodeBytes, edgeBytes)
			}
			release()
			release = nil
		}
	}
	if !exclusive {
		return commit(publish(0, 0))
	}
	trackBytes := initialized || (limits != nil && limits.Storage.MaxBytes > 0)
	if trackBytes || (limits != nil && (limits.Storage.MaxNodes > 0 || limits.Storage.MaxEdges > 0)) {
		changes = tx.PendingChanges(true)
	}
	var currentBytes int64
	if trackBytes {
		currentBytes, err = checker.getCurrentStorageSize(t.Engine)
		if err != nil {
			return err
		}
	}
	nodes, nodeBytes, err := transactionEntityDelta(changes.Nodes,
		func(id storage.NodeID) (*storage.Node, error) {
			return t.Engine.GetNode(storage.NodeID(strings.TrimPrefix(string(id), t.dbName+":")))
		},
		func(node *storage.Node) (int64, error) {
			node.ID = storage.NodeID(strings.TrimPrefix(string(node.ID), t.dbName+":"))
			return calculateNodeSize(node)
		}, trackBytes)
	if err != nil {
		return err
	}
	edges, edgeBytes, err := transactionEntityDelta(changes.Edges,
		func(id storage.EdgeID) (*storage.Edge, error) {
			return t.Engine.GetEdge(storage.EdgeID(strings.TrimPrefix(string(id), t.dbName+":")))
		},
		func(edge *storage.Edge) (int64, error) {
			edge.ID = storage.EdgeID(strings.TrimPrefix(string(edge.ID), t.dbName+":"))
			edge.StartNode = storage.NodeID(strings.TrimPrefix(string(edge.StartNode), t.dbName+":"))
			edge.EndNode = storage.NodeID(strings.TrimPrefix(string(edge.EndNode), t.dbName+":"))
			return calculateEdgeSize(edge)
		}, trackBytes)
	if err != nil {
		return err
	}
	if limits != nil {
		if nodes > 0 && limits.Storage.MaxNodes > 0 {
			count, err := t.Engine.NodeCount()
			if err != nil {
				return err
			}
			if count+nodes > limits.Storage.MaxNodes {
				return nornicerrors.MarkCommitRolledBack(localizedError(localization.MultidbMaxNodesReached(t.dbName, count+nodes, limits.Storage.MaxNodes), ErrStorageLimitExceeded))
			}
		}
		if edges > 0 && limits.Storage.MaxEdges > 0 {
			count, err := t.Engine.EdgeCount()
			if err != nil {
				return err
			}
			if count+edges > limits.Storage.MaxEdges {
				return nornicerrors.MarkCommitRolledBack(localizedError(localization.MultidbMaxEdgesReached(t.dbName, count+edges, limits.Storage.MaxEdges), ErrStorageLimitExceeded))
			}
		}
		if delta := nodeBytes + edgeBytes; delta > 0 && limits.Storage.MaxBytes > 0 && currentBytes+delta > limits.Storage.MaxBytes {
			return nornicerrors.MarkCommitRolledBack(localizedError(localization.MultidbMaxBytesExceeded(t.dbName, currentBytes, limits.Storage.MaxBytes, delta), ErrStorageLimitExceeded))
		}
	}
	// Match ordinary writes: one token per buffered storage operation, including
	// deletes. Rejected attempts retain consumed rate tokens, not storage usage.
	if limits != nil && limits.Rate.MaxWritesPerSecond > 0 {
		for i := 0; i < changes.Operations; i++ {
			if err := checker.CheckWriteRate(); err != nil {
				return nornicerrors.MarkCommitRolledBack(err)
			}
		}
	}
	return commit(publish(nodeBytes, edgeBytes))
}

// Compare final staged entities with the current committed state, rather than
// the transaction's older snapshot. Each ID contributes once; nil is deletion.
func transactionEntityDelta[K comparable, V any](pending map[K]*V, get func(K) (*V, error), size func(*V) (int64, error), trackBytes bool) (count, bytes int64, err error) {
	for id, next := range pending {
		previous, getErr := get(id)
		if getErr != nil && !errors.Is(getErr, storage.ErrNotFound) {
			return 0, 0, getErr
		}
		if getErr == nil {
			count--
			if trackBytes {
				n, err := size(previous)
				if err != nil {
					return 0, 0, err
				}
				bytes -= n
			}
		}
		if next != nil {
			count++
			if trackBytes {
				n, err := size(next)
				if err != nil {
					return 0, 0, err
				}
				bytes += n
			}
		}
	}
	return count, bytes, nil
}
