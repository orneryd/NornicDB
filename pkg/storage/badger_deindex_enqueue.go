package storage

import (
	"context"
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
	return b.enqueueDeindexIfSuppressed(context.Background(), entityID, isEdge)
}

// enqueueDeindexIfSuppressed is EnqueueDeindexIfSuppressed with the write's
// context (see beginEntityWrite).
func (b *BadgerEngine) enqueueDeindexIfSuppressed(ctx context.Context, entityID string, isEdge bool) (bool, error) {
	if !b.decayEnabled {
		return false, nil
	}

	write, releaseWrite, err := b.beginEntityWrite(ctx, entityID, isEdge, nil)
	if err != nil {
		return false, err
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()

	if b.writeBehind != nil {
		// Its notifications are delivered once the order is released.
		// The rewrite is ordered after the acknowledged commits of the
		// entity (orderAfterBufferedCommits): it rescores the newest body.
		entities := writtenEntities{nodes: []NodeID{NodeID(entityID)}}
		if isEdge {
			entities = writtenEntities{edges: []EdgeID{EdgeID(entityID)}}
		}
		var order orderHold
		_, err := b.orderAfterBufferedCommits(write.ctx, &order, entities)
		if err != nil {
			return false, err
		}
		defer order.release()
	}
	becameSuppressed := false
	var nodeChanged *Node
	var edgeChanged *Edge
	var ops []Operation
	publish := func(images *commitCacheImages) {
		if len(ops) != 0 {
			images.acceptOperation(ops[0])
		}
		if edgeChanged != nil {
			b.cacheOnEdgeUpdated(edgeChanged.Type, edgeChanged)
		}
		b.prepareCommitOperations(ops, false)
	}
	keys := func() []any {
		if nodeChanged != nil {
			ops = []Operation{{Type: OpUpdateNode, Node: nodeChanged}}
		}
		if edgeChanged != nil {
			ops = []Operation{{Type: OpUpdateEdge, Edge: edgeChanged}}
		}
		capturePublicationOperations(notification, ops)
		return appendOperationPublicationKeys(nil, nil, ops)
	}
	err = b.withPublishingUpdate(write.ctx, func(txn *badger.Txn) error {
		mutation, err := b.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: entityID, isEdge: isEdge})
		nodeChanged, edgeChanged = mutation.node, mutation.edge
		if mutation.suppressed {
			becameSuppressed = true
		}
		return err
	}, publish, keys, notification, write.stagePublication)
	return becameSuppressed, err
}

// suppressionEvaluation selects either the current stored body or the node
// image a label update has just written in this same transaction.
type suppressionEvaluation struct {
	id     string
	isEdge bool
	staged bool
	node   *Node
}
type suppressionMutation struct {
	suppressed bool
	node       *Node
	edge       *Edge
	written    []byte
}

// evaluateSuppressionInTxn is the sole entity suppression transition, including
// label rebinding. It owns flag scoring, body/adjacency writes and work/tombstone
// transitions. A staged image returns its rewritten bytes for MVCC publication.
func (b *BadgerEngine) evaluateSuppressionInTxn(txn *badger.Txn, input suppressionEvaluation) (suppressionMutation, error) {
	var node *Node
	var edge *Edge
	id, isEdge := input.id, input.isEdge
	if input.staged {
		if !b.decayEnabled || input.node == nil {
			return suppressionMutation{}, nil
		}
		node, id, isEdge = input.node, string(input.node.ID), false
	}
	var key []byte
	scope := "NODE"
	if isEdge {
		key, scope = edgeKey(EdgeID(id)), "EDGE"
	} else {
		key = nodeKey(NodeID(id))
	}
	if !input.staged {
		item, err := txn.Get(key)
		if err != nil {
			return suppressionMutation{}, nil
		}
		err = item.Value(func(val []byte) error {
			var err error
			if isEdge {
				edge, err = b.decodeEdgeBodyByID(val, EdgeID(id))
			} else {
				node, err = b.decodeNodeWithEmbeddings(txn, val, NodeID(id))
			}
			return err
		})
		if err != nil {
			return suppressionMutation{}, nil
		}
	}
	var entity any = node
	var wasSuppressed bool
	metadata := NodeRuntimeMetadata{}
	if isEdge {
		entity = edge
		wasSuppressed = edge.VisibilitySuppressed
		edge.VisibilitySuppressed = false
	} else {
		metadata = node.RuntimeMetadata()
		wasSuppressed = metadata.VisibilitySuppressed
		metadata.VisibilitySuppressed = false
		node.SetRuntimeMetadata(metadata)
	}
	suppress := b.filterEntityByDecay(visibleRead, entity, DecayScoringTime())
	if isEdge {
		edge.VisibilitySuppressed = suppress
	} else {
		metadata.VisibilitySuppressed = suppress
		node.SetRuntimeMetadata(metadata)
	}
	if suppress == wasSuppressed {
		return suppressionMutation{}, nil
	}
	var data []byte
	var err error
	if isEdge {
		data, err = b.encodeEdgeInTxn(txn, namespaceForEdgeID(EdgeID(id)), edge)
	} else {
		data, _, err = b.encodeNodeInTxn(txn, namespaceForNodeID(NodeID(id)), node)
	}
	if err != nil {
		return suppressionMutation{}, err
	}
	if err := txn.Set(key, data); err != nil {
		return suppressionMutation{}, err
	}
	if isEdge {
		if err := b.setAdjacencyValuesInTxn(txn, edge, data); err != nil {
			return suppressionMutation{}, err
		}
	}
	mutation := suppressionMutation{suppressed: suppress, node: node, edge: edge, written: data}
	if suppress {
		if err := enqueueWorkItemInTxn(txn, id, scope); err != nil {
			if input.staged {
				return suppressionMutation{written: data}, err
			}
			return suppressionMutation{}, err
		}
		return mutation, nil
	}
	return mutation, clearTombstonesForEntityInTxn(txn, id)
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
