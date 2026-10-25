package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"time"
)

// nodeDeletionResult is internal command state, not a persisted operation format.
type nodeDeletionResult struct {
	node  *Node
	edges []*Edge
	ids   []EdgeID
}

func (r nodeDeletionResult) operation(id NodeID) Operation {
	return Operation{Type: OpDeleteNode, NodeID: id, OldNode: r.node, DeletedEdgeIDs: r.ids, EdgesDeleted: int64(len(r.ids))}
}

// executeNodeDeletion owns record cleanup and cascade traversal for both native
// and staged writes. The existing target selects only snapshot, history and
// bookkeeping phases; no transaction is constructed for a native write.
func (b *BadgerEngine) executeNodeDeletion(ctx context.Context, w kvWriter, id NodeID, old *Node) (r nodeDeletionResult, err error) {
	stagedTarget, isStaged := w.(*stagedKV)
	tx := (*BadgerTransaction)(stagedTarget)
	raw := kvWriterTxn(w)
	var version MVCCVersion
	if !isStaged {
		version, err = b.allocateMVCCVersion(raw, namespaceForNodeID(id), time.Now())
		if err != nil {
			return r, err
		}
	}
	if isStaged {
		err = tx.scanCommittedEmbeddingKeysLocked(id, w.Delete)
	} else {
		err = visitEmbeddingKeys(raw, id, w.Delete)
	}
	if err != nil {
		if !isStaged {
			err = fmt.Errorf("failed to delete embedding chunk: %w", err)
		}
		return r, err
	}
	r.node = old
	if r.node == nil {
		item, e := raw.Get(nodeKey(id))
		if errors.Is(e, badger.ErrKeyNotFound) {
			_ = w.Delete(pendingEmbedKey(id))
			return r, ErrNotFound
		}
		if e != nil {
			return r, e
		}
		if e = item.Value(func(data []byte) error { var e error; r.node, e = b.decodeNodeWithEmbeddings(raw, data, id); return e }); e != nil {
			return r, e
		}
	}
	if !isStaged {
		head, headErr := b.loadNodeMVCCHeadInTxn(raw, id)
		if err = archiveAtLiveHead(head, headErr, func(at MVCCVersion) error { return b.archiveNodeBodyInTxn(raw, id, r.node, at) }); err != nil {
			return r, err
		}
	} else {
		tx.bufferAdjustNodeLabelCounts(tx.namespace, r.node.Labels, nil)
	}
	var incident []*Edge
	if isStaged {
		incident, err = tx.incidentEdgesLocked(ctx, id)
	} else {
		incident, err = b.nodeDeletionEdges(raw, id)
	}
	if err != nil {
		return r, err
	}
	r.edges = incident[:0]
	for _, edge := range incident {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		if isStaged {
			err = tx.bufferRemoveEdgeBookkeepingBuffered(edge, id, r.node.Labels)
		} else {
			err = b.deleteEdgeInTxn(raw, edge.ID)
			if errors.Is(err, ErrNotFound) {
				continue
			}
		}
		if err != nil {
			return r, err
		}
		r.edges = append(r.edges, edge)
		r.ids = append(r.ids, edge.ID)
		if !isStaged {
			if err = b.writeEdgeAdjacencyDeltaInTxn(raw, edge, nil, version); err != nil {
				return r, err
			}
			if err = b.writeEdgeMVCCTombstoneInTxn(raw, edge.ID, version); err != nil {
				return r, err
			}
			if err = b.writeEdgeMVCCHeadInTxn(raw, edge.ID, version, true); err != nil {
				return r, err
			}
			if edge.Type != "" {
				ns := namespaceForEdgeID(edge.ID)
				if err = b.adjustEdgeTypeCountInTxn(raw, ns, edge.Type, -1); err != nil {
					return r, err
				}
				start, end := r.node.Labels, r.node.Labels
				if edge.StartNode == id && edge.EndNode != id {
					end, err = b.readNodeLabelsIfPresentInTxn(raw, edge.EndNode)
				} else if edge.EndNode == id && edge.StartNode != id {
					start, err = b.readNodeLabelsIfPresentInTxn(raw, edge.StartNode)
				}
				if err != nil {
					return r, err
				}
				if err = b.adjustEdgeTypeLabelCountsForEdgeInTxn(raw, ns, edge.Type, start, end, -1); err != nil {
					return r, err
				}
			}
		}
	}
	// Preserve the existing native delete's best-effort catalog cleanup outcome.
	_ = deleteIndexEntryCatalogInTxn(w, string(id))
	if err = b.removeNodeRecordKeys(w, r.node); err != nil {
		return r, err
	}
	if !isStaged {
		ns, _, ok := ParseDatabasePrefix(string(id))
		if !ok {
			return r, nil
		}
		if err = b.applyTemporalIndexesForNodeChangeInTxn(raw, ns, b.GetSchemaForNamespace(ns), r.node, nil); err != nil {
			return r, err
		}
		if err = b.adjustNodeLabelCountsInTxn(raw, ns, r.node.Labels, nil); err != nil {
			return r, err
		}
		if err = b.writeNodeMVCCTombstoneInTxn(raw, id, version); err != nil {
			return r, err
		}
		if err = b.writeNodeMVCCHeadInTxn(raw, id, version, true); err != nil {
			return r, err
		}
	}
	return r, nil
}

// nodeDeletionEdges reads the native transaction's incident population once.
// A self-loop occurs in both indexes but is one cascade operation.
func (b *BadgerEngine) nodeDeletionEdges(raw *badger.Txn, id NodeID) ([]*Edge, error) {
	var ids []EdgeID
	for _, prefix := range [...][]byte{b.outgoingIndexPrefixString(id), b.incomingIndexPrefixString(id)} {
		if prefix == nil {
			continue
		}
		it := raw.NewIterator(badgerPrefixIteratorOptions(prefix))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n, ok := extractEdgeNumIDFromOutgoingKey(it.Item().Key())
			if !ok {
				continue
			}
			if edgeID, ok := b.idDict.lookupEdgeIDByNum(n); ok {
				ids = append(ids, edgeID)
			}
		}
		it.Close()
	}
	var edges []*Edge
	var seen map[EdgeID]bool
	for _, edgeID := range ids {
		if seen[edgeID] {
			continue
		}
		edge, err := b.loadEdgeForAdjacencyTombstoneInTxn(raw, edgeID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if seen == nil {
			seen = make(map[EdgeID]bool)
		}
		seen[edgeID] = true
		edges = append(edges, edge)
	}
	return edges, nil
}
