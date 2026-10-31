package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// SetLifecycleController injects the MVCC lifecycle controller.
func (b *BadgerEngine) SetLifecycleController(controller MVCCLifecycleController) {
	b.mu.Lock()
	b.lifecycleController = controller
	b.mu.Unlock()
}

// StartLifecycleManager starts the injected lifecycle manager.
func (b *BadgerEngine) StartLifecycleManager(ctx context.Context) {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller != nil {
		controller.StartLifecycle(ctx)
	}
}

// RegisterSnapshotReader delegates reader registration when lifecycle is enabled.
func (b *BadgerEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	b.mvccPruneMu.Lock()
	defer b.mvccPruneMu.Unlock()
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil {
		return func() {}
	}
	return controller.RegisterSnapshotReader(info)
}

// LifecycleStatus reports lifecycle status when enabled.
func (b *BadgerEngine) LifecycleStatus() map[string]interface{} {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil {
		return map[string]interface{}{"enabled": false}
	}
	return controller.LifecycleStatus()
}

// TriggerPruneNow runs an immediate lifecycle prune.
func (b *BadgerEngine) TriggerPruneNow(ctx context.Context) error {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil {
		return nil
	}
	return controller.TriggerPruneNow(ctx)
}

// PauseLifecycle pauses lifecycle work.
func (b *BadgerEngine) PauseLifecycle() {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller != nil {
		controller.PauseLifecycle()
	}
}

// ResumeLifecycle resumes lifecycle work.
func (b *BadgerEngine) ResumeLifecycle() {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller != nil {
		controller.ResumeLifecycle()
	}
}

// SetLifecycleSchedule updates lifecycle cadence when supported by the controller.
func (b *BadgerEngine) SetLifecycleSchedule(interval time.Duration) error {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if scheduler, ok := controller.(interface{ SetLifecycleSchedule(time.Duration) error }); ok {
		return scheduler.SetLifecycleSchedule(interval)
	}
	return nil
}

// TopLifecycleDebtKeys returns the highest-debt keys when supported by the controller.
func (b *BadgerEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if provider, ok := controller.(interface {
		TopLifecycleDebtKeys(int) []MVCCLifecycleDebtKey
	}); ok {
		return provider.TopLifecycleDebtKeys(limit)
	}
	return nil
}

func (b *BadgerEngine) acquireSnapshotReader(info SnapshotReaderInfo) (func(), error) {
	b.mvccPruneMu.Lock()
	defer b.mvccPruneMu.Unlock()
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil || !controller.IsLifecycleEnabled() {
		b.activeMVCCSnapshotReaders.Add(1)
		return func() { b.activeMVCCSnapshotReaders.Add(-1) }, nil
	}
	return controller.AcquireSnapshotReader(info)
}

func (b *BadgerEngine) evaluateSnapshotReader(info SnapshotReaderInfo) (bool, bool) {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil || !controller.IsLifecycleEnabled() {
		return false, false
	}
	return controller.EvaluateSnapshotReader(info)
}

// IterateMVCCHeads iterates all persisted MVCC heads.
func (b *BadgerEngine) IterateMVCCHeads(ctx context.Context, yield func(logicalKey []byte, head MVCCHead) error) error {
	return b.withView(func(txn *badger.Txn) error {
		for _, prefix := range []byte{prefixMVCCNodeHead, prefixMVCCEdgeHead} {
			opts := badgerIteratorOptions()
			opts.Prefix = []byte{prefix}
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				select {
				case <-ctx.Done():
					it.Close()
					return ctx.Err()
				default:
				}
				key := append([]byte(nil), it.Item().Key()...)
				logical := append([]byte{headPrefixToLogicalPrefix(prefix)}, key[1:]...)
				var head MVCCHead
				if err := it.Item().Value(func(val []byte) error {
					var decodeErr error
					head, decodeErr = decodeMVCCHead(val)
					return decodeErr
				}); err != nil {
					it.Close()
					return err
				}
				if err := b.applyMVCCPruneFloorInTxn(txn, logical, &head); err != nil {
					it.Close()
					return err
				}
				if err := yield(logical, head); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}
		return nil
	})
}

// IterateMVCCVersions iterates all versions for a logical key.
func (b *BadgerEngine) IterateMVCCVersions(ctx context.Context, logicalKey []byte, yield func(version MVCCVersion, tombstoned bool, sizeBytes int64) error) error {
	if len(logicalKey) < 2 {
		return ErrInvalidData
	}
	return b.withView(func(txn *badger.Txn) error {
		prefix := mvccVersionPrefixForLogicalKey(logicalKey)
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			key := append([]byte(nil), it.Item().Key()...)
			_, version, err := extractMVCCLogicalKeyAndVersion(key)
			if err != nil {
				return err
			}
			tombstoned := false
			if err := it.Item().Value(func(val []byte) error {
				switch logicalKey[0] {
				case prefixMVCCNode:
					record, decodeErr := decodeMVCCNodeRecord(val)
					if decodeErr != nil {
						return decodeErr
					}
					tombstoned = record.Tombstoned
				case prefixMVCCEdge:
					record, decodeErr := decodeMVCCEdgeRecord(val)
					if decodeErr != nil {
						return decodeErr
					}
					tombstoned = record.Tombstoned
				default:
					return fmt.Errorf("unknown logical key prefix: %x", logicalKey[0])
				}
				return nil
			}); err != nil {
				return err
			}
			if err := yield(version, tombstoned, int64(it.Item().ValueSize())); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteMVCCVersion deletes a single MVCC version for a logical key.
func (b *BadgerEngine) DeleteMVCCVersion(ctx context.Context, logicalKey []byte, version MVCCVersion) error {
	fullKey, err := mvccVersionKeyForLogicalKey(logicalKey, version)
	if err != nil {
		return err
	}
	// Ordered after the entity's acknowledged write-behind commits, as
	// WriteMVCCHead is; the version the entity's head names is never deleted
	// (a commit landed after the caller read its fence).
	return b.withUpdate(updateRequest{ctx: ctx, scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		if len(logicalKey) == 1+8 {
			head, err := b.loadMVCCHeadForLogicalKeyInTxn(txn, logicalKey)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			if err == nil && head.Version.Compare(version) == 0 {
				return ErrConflict
			}
		}
		return txn.Delete(fullKey)
	}, b.mvccLogicalEntities(logicalKey))
}

// WriteMVCCHead writes an updated MVCC head for a logical key. A head never
// moves backwards: when the stored head is newer than head (a commit of the
// entity landed after the caller read it, as the lifecycle pruner does), its
// version and tombstone stay and only the floor is raised. It is ordered
// after the entity's acknowledged write-behind commits
// (orderAfterBufferedCommits), so none of them lands over it afterwards.
func (b *BadgerEngine) WriteMVCCHead(ctx context.Context, logicalKey []byte, head MVCCHead) error {
	return b.withUpdate(updateRequest{ctx: ctx, scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		current, err := b.loadMVCCHeadForLogicalKeyInTxn(txn, logicalKey)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if err == nil && current.Version.Compare(head.Version) > 0 {
			head.Version, head.Tombstoned = current.Version, current.Tombstoned
			if current.FloorVersion.Compare(head.FloorVersion) > 0 {
				head.FloorVersion = current.FloorVersion
			}
		}
		return b.writeMVCCHeadForLogicalKeyInTxn(txn, logicalKey, head.Version, head.Tombstoned, head.FloorVersion)
	}, b.mvccLogicalEntities(logicalKey))
}

// mvccLogicalEntities names, for the write order, the entity an MVCC logical
// key belongs to (an adjacency key: its node and relationship); a number no
// entity holds names none.
func (b *BadgerEngine) mvccLogicalEntities(logical []byte) writtenEntities {
	if len(logical) == 1+8+8 && (logical[0] == prefixMVCCOutgoingAdj || logical[0] == prefixMVCCIncomingAdj) {
		var e writtenEntities
		if id, ok := b.idDict.nodeReverseLookup(binary.BigEndian.Uint64(logical[1:9])); ok {
			e.nodes = append(e.nodes, id)
		}
		if id, ok := b.idDict.edgeReverseLookup(binary.BigEndian.Uint64(logical[9:])); ok {
			e.edges = append(e.edges, id)
		}
		return e
	}
	if len(logical) != 1+8 {
		return writtenEntities{}
	}
	num := binary.BigEndian.Uint64(logical[1:])
	switch logical[0] {
	case prefixMVCCNode:
		if id, ok := b.idDict.nodeReverseLookup(num); ok {
			return writtenEntities{nodes: []NodeID{id}}
		}
	case prefixMVCCEdge:
		if id, ok := b.idDict.edgeReverseLookup(num); ok {
			return writtenEntities{edges: []EdgeID{id}}
		}
	}
	return writtenEntities{}
}

// ReadMVCCHead loads the current MVCC head for a logical key.
func (b *BadgerEngine) ReadMVCCHead(ctx context.Context, logicalKey []byte) (MVCCHead, error) {
	var head MVCCHead
	err := b.withView(func(txn *badger.Txn) error {
		var innerErr error
		head, innerErr = b.loadMVCCHeadForLogicalKeyInTxn(txn, logicalKey)
		if innerErr != nil {
			return innerErr
		}
		return b.applyMVCCPruneFloorInTxn(txn, logicalKey, &head)
	})
	return head, err
}
func headPrefixToLogicalPrefix(prefix byte) byte {
	if prefix == prefixMVCCNodeHead {
		return prefixMVCCNode
	}
	return prefixMVCCEdge
}

// mvccVersionPrefixForLogicalKey returns the scan prefix that matches
// every persisted MVCC version key for the given logical entity.
//
// Wire layout (post-refactor): [prefix(1)][numID(8)][sortVersion(16)].
// The logical key IS the [prefix(1)][numID(8)] head — there is no 0x00
// separator. An earlier draft of this helper appended a 0x00 byte
// thinking the version key used the legacy variable-length format, but
// the post-refactor compact layout doesn't, and the spurious 0x00 made
// the scan match no keys at all (the real version's leading byte is
// the high bit of the timestamp, never 0x00 for any timestamp the
// engine has ever written). Tests TestBadgerEngine_IterateMVCC_*
// regress this.
func mvccVersionPrefixForLogicalKey(logicalKey []byte) []byte {
	if len(logicalKey) < 2 {
		return nil
	}
	return append([]byte{}, logicalKey...)
}

func mvccVersionKeyForLogicalKey(logicalKey []byte, version MVCCVersion) ([]byte, error) {
	if len(logicalKey) != 1+8 {
		return nil, ErrInvalidData
	}
	numID := binary.BigEndian.Uint64(logicalKey[1:])
	switch logicalKey[0] {
	case prefixMVCCNode:
		return mvccNodeVersionKey(numID, version), nil
	case prefixMVCCEdge:
		return mvccEdgeVersionKey(numID, version), nil
	default:
		return nil, fmt.Errorf("unknown logical key prefix: %x", logicalKey[0])
	}
}
