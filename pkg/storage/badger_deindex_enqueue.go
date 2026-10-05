package storage

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// EnqueueDeindexIfSuppressed evaluates the entity's current decay score.
// If below the visibility threshold, it marks the entity as suppressed and
// creates a pending deindex work item. If above threshold and currently
// suppressed, it clears the suppression and deletes any tombstones.
// The returned bool is true only when the entity transitioned into the
// suppressed state during this call.
func (b *BadgerEngine) EnqueueDeindexIfSuppressed(entityID string, isEdge bool) (bool, error) {
	if !b.decayEnabled {
		return false, nil
	}

	becameSuppressed := false
	nodeChanged := false
	err := b.withUpdate(func(txn *badger.Txn) error {
		if isEdge {
			changed, err := b.evaluateEdgeSuppressionInTxn(txn, EdgeID(entityID))
			if changed {
				becameSuppressed = true
			}
			return err
		}
		changed, mutation, err := b.evaluateNodeSuppressionInTxn(txn, NodeID(entityID))
		nodeChanged = mutation
		if changed {
			becameSuppressed = true
		}
		return err
	})
	if err == nil && nodeChanged {
		b.graphMutationVersions.changed(namespaceForNodeID(NodeID(entityID)))
	}
	return becameSuppressed, err
}

func (b *BadgerEngine) evaluateNodeSuppressionInTxn(txn *badger.Txn, nodeID NodeID) (bool, bool, error) {
	item, err := txn.Get(nodeKey(nodeID))
	if err != nil {
		return false, false, nil
	}

	var node *Node
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
		return decodeErr
	}); err != nil {
		return false, false, nil
	}

	nowNanos := DecayScoringTime()
	wasSuppressed := node.VisibilitySuppressed
	node.VisibilitySuppressed = false
	suppress := b.filterNodeByDecay(node, nowNanos)

	if suppress && !wasSuppressed {
		node.VisibilitySuppressed = true
		data, _, err := b.encodeNodeInTxn(txn, namespaceForNodeID(nodeID), node)
		if err != nil {
			return false, false, err
		}
		if err := txn.Set(nodeKey(nodeID), data); err != nil {
			return false, false, err
		}
		if err := enqueueWorkItemInTxn(txn, string(nodeID), "NODE"); err != nil {
			return false, false, err
		}
		return true, true, nil
	}

	if !suppress && wasSuppressed {
		data, _, err := b.encodeNodeInTxn(txn, namespaceForNodeID(nodeID), node)
		if err != nil {
			return false, false, err
		}
		if err := txn.Set(nodeKey(nodeID), data); err != nil {
			return false, false, err
		}
		return false, true, clearTombstonesForEntityInTxn(txn, string(nodeID))
	}

	return false, false, nil
}

// rescoreSuppressionAfterLabelChangeInTxn re-evaluates a node's decay
// suppression under its NEW labels and rewrites the persisted body so the
// VisibilitySuppressed flag tracks the current binding. Called only from
// the UpdateNode path when labels changed, before writeNodeMVCCHeadInTxn.
//
// Cypher SET/REMOVE-label semantics: a node that moves out of a suppressing
// label binding must become visible on the next read; a node that moves
// into one must become suppressed. The persisted flag is still the cheap
// fallback when no scorer exists, so keep it aligned with the current
// binding.
//
// When decay is disabled at the engine level, this is a no-op so the
// codepath cost stays zero on operators who don't use the feature.
func (b *BadgerEngine) rescoreSuppressionAfterLabelChangeInTxn(txn *badger.Txn, namespace string, node *Node) error {
	if !b.decayEnabled || node == nil {
		return nil
	}
	wasSuppressed := node.VisibilitySuppressed
	// Score against the new labels and then persist the current visibility
	// decision with the updated body.
	node.VisibilitySuppressed = false
	suppressNow := b.filterNodeByDecay(node, DecayScoringTime())
	node.VisibilitySuppressed = suppressNow

	if suppressNow == wasSuppressed {
		// No state change — the body the caller is about to commit already
		// has the correct flag set (we restored it above).
		return nil
	}

	data, _, err := b.encodeNodeInTxn(txn, namespace, node)
	if err != nil {
		return err
	}
	if err := txn.Set(nodeKey(node.ID), data); err != nil {
		return err
	}

	if suppressNow {
		// Newly suppressed — enqueue deindex work.
		return enqueueWorkItemInTxn(txn, string(node.ID), "NODE")
	}
	// Newly visible — drop any tombstones from a prior suppression cycle.
	return clearTombstonesForEntityInTxn(txn, string(node.ID))
}

func (b *BadgerEngine) evaluateEdgeSuppressionInTxn(txn *badger.Txn, edgeID EdgeID) (bool, error) {
	item, err := txn.Get(edgeKey(edgeID))
	if err != nil {
		return false, nil
	}

	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
		return decodeErr
	}); err != nil {
		return false, nil
	}

	nowNanos := DecayScoringTime()
	wasSuppressed := edge.VisibilitySuppressed
	edge.VisibilitySuppressed = false
	suppress := b.filterEdgeByDecay(edge, nowNanos)

	if suppress && !wasSuppressed {
		edge.VisibilitySuppressed = true
		data, err := b.encodeEdgeInTxn(txn, namespaceForEdgeID(edgeID), edge)
		if err != nil {
			return false, err
		}
		if err := txn.Set(edgeKey(edgeID), data); err != nil {
			return false, err
		}
		if err := enqueueWorkItemInTxn(txn, string(edgeID), "EDGE"); err != nil {
			return false, err
		}
		return true, nil
	}

	if !suppress && wasSuppressed {
		data, err := b.encodeEdgeInTxn(txn, namespaceForEdgeID(edgeID), edge)
		if err != nil {
			return false, err
		}
		if err := txn.Set(edgeKey(edgeID), data); err != nil {
			return false, err
		}
		return false, clearTombstonesForEntityInTxn(txn, string(edgeID))
	}

	return false, nil
}

func enqueueWorkItemInTxn(txn *badger.Txn, entityID, scope string) error {
	workItem := &DeindexWorkItem{
		WorkItemID:  fmt.Sprintf("deindex:%s", entityID),
		TargetID:    entityID,
		TargetScope: scope,
		EnqueuedAt:  time.Now().UnixNano(),
		Status:      "pending",
	}
	return putDeindexWorkItemInTxn(txn, workItem)
}

func clearTombstonesForEntityInTxn(txn *badger.Txn, entityID string) error {
	catItem, err := txn.Get(indexEntryCatalogKey(entityID))
	if err != nil {
		return nil
	}

	var cat IndexEntryCatalog
	if err := catItem.Value(func(val []byte) error {
		return msgpack.Unmarshal(val, &cat)
	}); err != nil {
		return nil
	}

	for _, k := range cat.IndexKeys {
		txn.Delete(indexTombstoneKey(k))
	}

	if cat.Deindexed {
		cat.Deindexed = false
		if err := putIndexEntryCatalogInTxn(txn, entityID, &cat); err != nil {
			return err
		}
	}

	txn.Delete(deindexWorkItemKey(fmt.Sprintf("deindex:%s", entityID)))
	return nil
}
