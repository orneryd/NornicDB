package storage

import (
	"github.com/dgraph-io/badger/v4"
)

// prefixMVCCMetaManualSuppression keys an entity suppressed on demand
// (kp.suppress, #1064): [prefixMVCCMeta, this, kind, id...], kind
// 'n' for a node and 'e' for a relationship. It is apart from the entity's
// body, so no other write can clear or set it, and from decay, so it holds
// whether or not decay is enabled.
const prefixMVCCMetaManualSuppression = byte(0x11)

const (
	manualSuppressionNode byte = 'n'
	manualSuppressionEdge byte = 'e'
)

func manualSuppressionKey(kind byte, id string) []byte {
	key := make([]byte, 0, 3+len(id))
	key = append(key, prefixMVCCMeta, prefixMVCCMetaManualSuppression, kind)
	return append(key, id...)
}

// loadManualSuppressions reads the entities suppressed on demand into
// memory, replacing what it held. Called on engine open and restore.
func (b *BadgerEngine) loadManualSuppressions(db badgerViewer) error {
	b.manualSuppressionMu.Lock()
	b.manualSuppressed = nil
	b.manualSuppressionCount.Store(0)
	b.manualSuppressionMu.Unlock()
	prefix := []byte{prefixMVCCMeta, prefixMVCCMetaManualSuppression}
	return db.View(func(txn *badger.Txn) error {
		opts := badgerPrefixIteratorOptions(prefix)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) > 3 {
				b.markManualSuppression(key[2], string(key[3:]), true)
			}
		}
		return nil
	})
}

// markManualSuppression records (or forgets) an on-demand suppression in
// memory.
func (b *BadgerEngine) markManualSuppression(kind byte, id string, suppressed bool) {
	key := string(kind) + id
	b.manualSuppressionMu.Lock()
	defer b.manualSuppressionMu.Unlock()
	if b.manualSuppressed == nil {
		b.manualSuppressed = make(map[string]struct{})
	}
	if suppressed {
		b.manualSuppressed[key] = struct{}{}
	} else {
		delete(b.manualSuppressed, key)
	}
	b.manualSuppressionCount.Store(int64(len(b.manualSuppressed)))
}

// manuallySuppressed reports whether an entity is suppressed on demand. It
// costs an atomic load while none is.
func (b *BadgerEngine) manuallySuppressed(kind byte, id string) bool {
	if b.manualSuppressionCount.Load() == 0 {
		return false
	}
	b.manualSuppressionMu.RLock()
	_, suppressed := b.manualSuppressed[string(kind)+id]
	b.manualSuppressionMu.RUnlock()
	return suppressed
}

// visibilityFiltered reports whether reads may hide an entity: decay is
// enabled or an entity is suppressed on demand, and no reveal scope is open.
// Read paths that take a shortcut when nothing can be hidden check it.
func (b *BadgerEngine) visibilityFiltered() bool {
	return (b.decayEnabled || b.manualSuppressionCount.Load() > 0) && !b.revealAll.Load()
}

// HasManualSuppressions reports whether any entity is suppressed on demand,
// for callers that skip visibility filtering when nothing can be hidden.
func (b *BadgerEngine) HasManualSuppressions() bool {
	return b.manualSuppressionCount.Load() > 0
}

// SetNodeManuallySuppressed hides a node on demand, or shows it again: hidden
// from MATCH, traversal and search, its data kept and visible under reveal(),
// whether or not decay is enabled (#1064). Showing it again re-indexes it for
// search. ErrNotFound when no such node is stored.
func (b *BadgerEngine) SetNodeManuallySuppressed(id NodeID, suppressed bool) error {
	revision := b.namespaceLifetimeRevision()
	notification, finish, err := b.beginNotifyingWrite(namespaceForNodeID(id))
	if err != nil {
		return err
	}
	defer finish()
	if err := b.checkNamespaceLifetimeRevision(namespaceForNodeID(id), revision); err != nil {
		return err
	}
	var node *Node
	err = b.setManualSuppression(manualSuppressionNode, string(id), nodeKey(id), suppressed, func(txn *badger.Txn, val []byte) error {
		decoded, err := b.decodeNodeWithEmbeddings(txn, val, id)
		node = decoded
		return err
	}, notification)
	if err != nil {
		return err
	}
	b.graphMutationVersions.changed(namespaceForNodeID(id))
	if !suppressed && node != nil {
		// Search drops a node it can't load; index it again.
		notification.ops = []Operation{{Type: OpUpdateNode, Node: node}}
	}
	return nil
}

// SetEdgeManuallySuppressed is SetNodeManuallySuppressed for a relationship.
func (b *BadgerEngine) SetEdgeManuallySuppressed(id EdgeID, suppressed bool) error {
	revision := b.namespaceLifetimeRevision()
	release, err := b.beginWrite(namespaceForEdgeID(id))
	if err != nil {
		return err
	}
	defer release()
	if err := b.checkNamespaceLifetimeRevision(namespaceForEdgeID(id), revision); err != nil {
		return err
	}
	err = b.setManualSuppression(manualSuppressionEdge, string(id), edgeKey(id), suppressed, nil)
	if err != nil {
		return err
	}
	b.graphMutationVersions.changed(namespaceForEdgeID(id))
	return nil
}

// setManualSuppression writes (or deletes) an entity's suppression key, once
// its record is found, and mirrors it in memory. decode, when set, reads the
// record.
func (b *BadgerEngine) setManualSuppression(kind byte, id string, recordKey []byte, suppressed bool, decode func(*badger.Txn, []byte) error, notification ...*commitNotification) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	err := b.withNotifyingUpdate(func(txn *badger.Txn) error {
		item, err := txn.Get(recordKey)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if decode != nil {
			if err := item.Value(func(val []byte) error { return decode(txn, val) }); err != nil {
				return err
			}
		}
		if suppressed {
			return txn.Set(manualSuppressionKey(kind, id), []byte{})
		}
		return txn.Delete(manualSuppressionKey(kind, id))
	}, notification...)
	if err != nil {
		return err
	}
	b.markManualSuppression(kind, id, suppressed)
	return nil
}

// forgetManualSuppression drops a deleted entity's on-demand suppression, so
// an entity created again with the same ID (a MERGEd relationship's ID is
// derived from its endpoints and properties) isn't hidden.
func (b *BadgerEngine) forgetManualSuppression(kind byte, id string) {
	if !b.manuallySuppressed(kind, id) {
		return
	}
	b.markManualSuppression(kind, id, false)
	// A delete is reported from inside its commit; the key goes in a
	// transaction of its own.
	go func() {
		_ = b.withUpdate(func(txn *badger.Txn) error {
			return txn.Delete(manualSuppressionKey(kind, id))
		})
	}()
}

// ManualSuppressionEngine hides and shows nodes and relationships on demand
// (kp.suppress / unsuppress, #1064).
type ManualSuppressionEngine interface {
	SetNodeManuallySuppressed(id NodeID, suppressed bool) error
	SetEdgeManuallySuppressed(id EdgeID, suppressed bool) error
}

// SetNodeManuallySuppressed suppresses or shows a node of this namespace.
func (n *NamespacedEngine) SetNodeManuallySuppressed(id NodeID, suppressed bool) error {
	inner, ok := n.inner.(ManualSuppressionEngine)
	if !ok {
		return ErrNotImplemented
	}
	return inner.SetNodeManuallySuppressed(n.prefixNodeID(id), suppressed)
}

// SetEdgeManuallySuppressed suppresses or shows a relationship of this
// namespace.
func (n *NamespacedEngine) SetEdgeManuallySuppressed(id EdgeID, suppressed bool) error {
	inner, ok := n.inner.(ManualSuppressionEngine)
	if !ok {
		return ErrNotImplemented
	}
	return inner.SetEdgeManuallySuppressed(n.prefixEdgeID(id), suppressed)
}

// SetNodeManuallySuppressed passes through to the stored engine: the
// suppression is storage metadata, like decay's suppression flag, not a
// logged graph write.
func (w *WALEngine) SetNodeManuallySuppressed(id NodeID, suppressed bool) error {
	inner, ok := w.engine.(ManualSuppressionEngine)
	if !ok {
		return ErrNotImplemented
	}
	return inner.SetNodeManuallySuppressed(id, suppressed)
}

// SetEdgeManuallySuppressed is SetNodeManuallySuppressed for a relationship.
func (w *WALEngine) SetEdgeManuallySuppressed(id EdgeID, suppressed bool) error {
	inner, ok := w.engine.(ManualSuppressionEngine)
	if !ok {
		return ErrNotImplemented
	}
	return inner.SetEdgeManuallySuppressed(id, suppressed)
}
