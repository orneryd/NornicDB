package storage

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
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
	b.suppressionEnds = nil
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

// dropSuppressionEnd discards the pending end of an entity's stored
// suppression, which the caller's suppression write superseded.
func (b *BadgerEngine) dropSuppressionEnd(kind byte, id string) bool {
	b.manualSuppressionMu.Lock()
	key := string(kind) + id
	_, pending := b.suppressionEnds[key]
	delete(b.suppressionEnds, key)
	b.manualSuppressionMu.Unlock()
	return pending
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

// manualSuppressionKind preserves the persisted metadata kind bytes.
func manualSuppressionKind(kind StoredEntityKind) (byte, error) {
	switch kind {
	case StoredEntityNodeKind:
		return manualSuppressionNode, nil
	case StoredEntityEdgeKind:
		return manualSuppressionEdge, nil
	default:
		return 0, ErrInvalidData
	}
}

// SetManualSuppression hides or shows a stored node or relationship without
// deleting its data. Hidden entities remain available under reveal(). Changes
// publish an update for search consumers, whether or not decay is enabled.
// Unknown kinds return ErrInvalidData; missing entities return ErrNotFound.
func (b *BadgerEngine) SetManualSuppression(entityKind StoredEntityKind, id string, suppressed bool) error {
	kind, err := manualSuppressionKind(entityKind)
	if err != nil {
		return err
	}
	var node *Node
	var edge *Edge
	recordKey := edgeKey(EdgeID(id))
	if kind == manualSuppressionNode {
		recordKey = nodeKey(NodeID(id))
	}
	decode := func(txn *badger.Txn, value []byte) (err error) {
		if kind == manualSuppressionNode {
			node, err = b.decodeNodeWithEmbeddings(txn, value, NodeID(id))
		} else {
			edge, err = b.decodeEdgeBodyWithID(namespaceForEdgeID(EdgeID(id)), value, EdgeID(id))
		}
		return err
	}
	operation := func() Operation {
		if kind == manualSuppressionNode {
			return Operation{Type: OpUpdateNode, Node: node}
		}
		return Operation{Type: OpUpdateEdge, Edge: edge}
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	write, releaseWrite, err := b.beginEntityWrite(id, kind == manualSuppressionEdge, nil)
	if err != nil {
		return err
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	var ops []Operation
	publish := func() {
		b.markManualSuppression(kind, id, suppressed)
		b.dropSuppressionEnd(kind, id)
		b.prepareCommitOperations(ops, false)
	}
	keys := func() []any {
		ops = []Operation{operation()}
		capturePublicationOperations(notification, ops)
		if kind == manualSuppressionNode {
			return []any{NodeID(id)}
		}
		return []any{EdgeID(id)}
	}
	if b.writeBehind != nil {
		// The commits its drain replays, then this one, are delivered once
		// the order is released (holdPublication).
		// Ordered after the acknowledged commits of the entity
		// (orderAfterBufferedCommits): it finds and reports the newest body.
		entities := writtenEntities{nodes: []NodeID{NodeID(id)}}
		if kind == manualSuppressionEdge {
			entities = writtenEntities{edges: []EdgeID{EdgeID(id)}}
		}
		var order orderHold
		if _, err := b.orderAfterBufferedCommits(&order, entities); err != nil {
			return err
		}
		defer order.release()
	}
	// Ordered with the end of a deleted entity's suppression
	// (endSuppressions): this write supersedes a pending end of its key.
	b.suppressionWriteMu.Lock()
	err = b.withPublishingUpdate(func(txn *badger.Txn) error {
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
	}, publish, keys, notification, write.stagePublication)
	b.suppressionWriteMu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

// forgetManualSuppression drops a deleted entity's on-demand suppression, so
// an entity created again with the same ID (a MERGEd relationship's ID is
// derived from its endpoints and properties) isn't hidden. A delete is
// reported from inside its commit; its stored key is deleted afterwards, by
// endSuppressions.
func (b *BadgerEngine) forgetManualSuppression(kind byte, id string) {
	if !b.manuallySuppressed(kind, id) {
		return
	}
	key := string(kind) + id
	b.manualSuppressionMu.Lock()
	delete(b.manualSuppressed, key)
	b.manualSuppressionCount.Store(int64(len(b.manualSuppressed)))
	if b.suppressionEnds == nil {
		b.suppressionEnds = make(map[string]struct{})
	}
	b.suppressionEnds[key] = struct{}{}
	start := !b.suppressionEnding
	b.suppressionEnding = true
	b.manualSuppressionMu.Unlock()
	if start {
		_, err := b.beginHelperTxn()
		if err != nil {
			b.manualSuppressionMu.Lock()
			b.suppressionEnding = false
			b.manualSuppressionMu.Unlock()
			return // Close owns the pending ends after helper admission closes.
		}
		go func() {
			defer b.work.Leave()
			if err := b.endSuppressions(nil); err != nil && !util.ErrorIsOnly(err, ErrStorageClosed) {
				b.work.RecordError("", fmt.Errorf("manual-suppression retirement: %w", err))
			}
		}()
	}
}

// endSuppressions deletes the stored keys of the suppressions that deletes
// ended (forgetManualSuppression). It writes under suppressionWriteMu, as
// every suppression write does: a suppression written for an entity created
// again with the same ID drops the pending end of its key, so it is never
// deleted, and a lifecycle write discards the ends it supersedes. Close runs
// it after its final drain.
func (b *BadgerEngine) endSuppressions(inherited *entityWriteContext) error {
	for {
		b.manualSuppressionMu.Lock()
		if len(b.suppressionEnds) == 0 {
			b.suppressionEnding = false
			b.manualSuppressionMu.Unlock()
			return nil
		}
		ends := make(map[string]struct{}, len(b.suppressionEnds))
		var first string
		for key := range b.suppressionEnds {
			ends[key] = struct{}{}
			if first == "" || key < first {
				first = key
			}
		}
		b.manualSuppressionMu.Unlock()
		write, releaseWrite, err := b.beginEntityWrite(first[1:], first[0] == manualSuppressionEdge, inherited, entityWritePlan{batches: suppressionEndFootprints(ends)})
		if err != nil {
			b.manualSuppressionMu.Lock()
			b.suppressionEnding = false
			b.manualSuppressionMu.Unlock()
			return err
		}
		// Never wait for entity authority while holding this mutex: an admitted
		// manual suppression owns its entity first and can cancel a pending end.
		b.suppressionWriteMu.Lock()
		for key := range ends {
			if !b.dropSuppressionEnd(key[0], key[1:]) {
				delete(ends, key)
			}
		}
		units := make([]func(*badger.Txn) error, 0, len(ends))
		for key := range ends {
			stored := manualSuppressionKey(key[0], key[1:])
			units = append(units, func(txn *badger.Txn) error { return txn.Delete(stored) })
		}
		if len(units) > 0 {
			err = b.commitEngineWrite(func(cw *commitWriter) error {
				for key := range ends {
					if key[0] == manualSuppressionEdge {
						cw.publishKeys = append(cw.publishKeys, EdgeID(key[1:]))
					} else {
						cw.publishKeys = append(cw.publishKeys, NodeID(key[1:]))
					}
				}
				if err := write.stagePublication(cw); err != nil {
					return err
				}
				for _, unit := range units {
					if err := cw.write(unit); err != nil {
						return err
					}
				}
				return nil
			})
		}
		b.suppressionWriteMu.Unlock()
		releaseWrite()
		if err != nil {
			b.manualSuppressionMu.Lock()
			b.suppressionEnding = false
			b.manualSuppressionMu.Unlock()
			return err
		}
	}
}

func (b *BadgerEngine) dropManualSuppressions(prefix string) {
	b.manualSuppressionMu.Lock()
	defer b.manualSuppressionMu.Unlock()
	for key := range b.manualSuppressed {
		if strings.HasPrefix(key[1:], prefix) {
			delete(b.manualSuppressed, key)
		}
	}
	for key := range b.suppressionEnds {
		if strings.HasPrefix(key[1:], prefix) {
			delete(b.suppressionEnds, key)
		}
	}
	b.manualSuppressionCount.Store(int64(len(b.manualSuppressed)))
}

// ManualSuppressionEngine hides and shows nodes and relationships on demand
// (kp.suppress / unsuppress, #1064).
type ManualSuppressionEngine interface {
	SetManualSuppression(kind StoredEntityKind, id string, suppressed bool) error
}

// SetManualSuppression hides or shows an entity within this namespace.
func (n *NamespacedEngine) SetManualSuppression(kind StoredEntityKind, id string, suppressed bool) error {
	if _, err := manualSuppressionKind(kind); err != nil {
		return err
	}
	inner, ok := n.inner.(ManualSuppressionEngine)
	if !ok {
		return ErrNotImplemented
	}
	return inner.SetManualSuppression(kind, qualifyEntityID(n.scope, id), suppressed)
}

// SetManualSuppression passes storage metadata through to the wrapped
// capability without adding a graph-write WAL record.
func (w *WALEngine) SetManualSuppression(kind StoredEntityKind, id string, suppressed bool) error {
	if _, err := manualSuppressionKind(kind); err != nil {
		return err
	}
	inner, ok := w.engine.(ManualSuppressionEngine)
	if !ok {
		return ErrNotImplemented
	}
	return inner.SetManualSuppression(kind, id, suppressed)
}
