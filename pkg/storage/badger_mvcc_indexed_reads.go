package storage

import (
	"bytes"
	"context"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	return b.getNodeVisibleAtWithView(id, version, b.withView)
}

func (b *BadgerEngine) getNodeVisibleAtWithView(id NodeID, version MVCCVersion, view func(func(*badger.Txn) error) error) (*Node, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var node *Node
	err = view(func(txn *badger.Txn) error {
		var loadErr error
		node, loadErr = b.getNodeVisibleAtInTxn(txn, id, version)
		return loadErr
	})
	return node, err
}

// getNodeVisibleAtInTxn resolves one logical MVCC version using the supplied
// physical Badger snapshot. Callers that scan an index in the same snapshot
// use this helper to avoid opening nested views or scanning unrelated nodes.
func (b *BadgerEngine) getNodeVisibleAtInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) (*Node, error) {
	head, err := b.loadNodeMVCCHeadInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	if version.Compare(head.FloorVersion) < 0 {
		return nil, ErrNotVisibleAtSnapshot
	}

	if version.Compare(head.Version) >= 0 && !head.Tombstoned {
		item, getErr := txn.Get(nodeKey(id))
		if getErr == nil {
			itemVersion := item.Version()
			if cached, ok := b.cacheLoadNodeBody(id, itemVersion); ok {
				cached, loadErr := b.loadNodeEmbeddings(txn, cached, id)
				if loadErr != nil {
					return nil, loadErr
				}
				if b.filterNodeByDecay(cached, DecayScoringTime()) {
					return nil, ErrNotFound
				}
				return cached, nil
			}
			var node *Node
			if err := item.Value(func(val []byte) error {
				namespace := namespaceForNodeID(id)
				decoded, decodeErr := b.decodeNode(namespace, val)
				if decodeErr != nil {
					return decodeErr
				}
				if decoded == nil {
					return ErrNotFound
				}
				b.cacheStoreNodeBody(id, itemVersion, decoded)
				node, decodeErr = b.loadNodeEmbeddings(txn, decoded, id)
				return decodeErr
			}); err != nil {
				return nil, err
			}
			if b.filterNodeByDecay(node, DecayScoringTime()) {
				return nil, ErrNotFound
			}
			return node, nil
		}
		if getErr != badger.ErrKeyNotFound {
			return nil, getErr
		}
	}
	if head.Tombstoned && version.Compare(head.Version) >= 0 {
		return nil, ErrNotFound
	}

	var record mvccNodeRecord
	switch {
	case version.Compare(head.Version) >= 0:
		record, err = b.loadNodeMVCCRecordExactInTxn(txn, id, head.Version)
	case version.Compare(head.FloorVersion) == 0:
		record, err = b.loadNodeMVCCRecordExactInTxn(txn, id, head.FloorVersion)
	default:
		record, _, err = b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, version)
	}
	if err != nil {
		if err == ErrNotFound && version.Compare(head.Version) >= 0 {
			record, _, err = b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
		}
		if err != nil {
			return nil, err
		}
	}
	if record.Tombstoned || record.Node == nil {
		return nil, ErrNotFound
	}
	node := copyNode(record.Node)
	if b.filterNodeByDecay(node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return node, nil
}

func (b *BadgerEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	return b.getNodesByLabelVisibleAtWithView("", label, version, b.withView)
}

// GetNodesByLabelVisibleAtInScope is GetNodesByLabelVisibleAt within one
// database (ScopedLabelNodeReader).
func (b *BadgerEngine) GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error) {
	return b.getNodesByLabelVisibleAtWithView(scope, label, version, b.withView)
}

func (b *BadgerEngine) getNodesByLabelVisibleAtWithView(scope, label string, version MVCCVersion, view func(func(*badger.Txn) error) error) ([]*Node, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var nodes []*Node
	err = view(func(txn *badger.Txn) error {
		return b.iterateNodesVisibleAtInScopeInTxn(txn, scope, version, func(node *Node) error {
			if node == nil {
				return nil
			}
			if label != "" {
				matched := false
				for _, existing := range node.Labels {
					if existing == label {
						matched = true
						break
					}
				}
				if !matched {
					return nil
				}
			}
			nodes = append(nodes, node)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// getNodesByLabelVisibleAtSnapshotWithView resolves label candidates from the
// same physical Badger snapshot used by an explicit transaction. The label
// index in that snapshot already represents membership at BEGIN, so work is
// proportional to the matching label rather than every stored node.
func (b *BadgerEngine) getNodesByLabelVisibleAtSnapshotWithView(scope, label string, version MVCCVersion, view func(func(*badger.Txn) error) error) ([]*Node, error) {
	nodes := make([]*Node, 0)
	err := b.streamNodesByLabelVisibleAtSnapshotWithView(scope, label, version, view, nil, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// streamNodesByLabelVisibleAtSnapshotWithView visits snapshot-visible label
// matches directly from the label index. Unlike the slice-returning adapter,
// it preserves early termination and never materialises nodes the caller does
// not consume.
func (b *BadgerEngine) streamNodesByLabelVisibleAtSnapshotWithView(
	scope string,
	label string,
	version MVCCVersion,
	view func(func(*badger.Txn) error) error,
	properties []string,
	visit func(*Node) error,
) error {
	if visit == nil {
		return ErrInvalidData
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return err
	}
	defer deregister()

	return view(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			nodeNum, ok := extractNodeNumIDFromLabelIndex(key, len(label))
			if !ok {
				continue
			}
			nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
			if !ok || nodeID == "" || !nodeIDInScope(nodeID, scope) {
				continue
			}
			node, getErr := b.getNodeVisibleAtInTxn(txn, nodeID, version)
			if getErr == ErrNotFound || getErr == ErrNotVisibleAtSnapshot {
				continue
			}
			if getErr != nil {
				return getErr
			}
			matched := false
			for _, existing := range node.Labels {
				if existing == label {
					matched = true
					break
				}
			}
			if matched {
				if err := visit(projectCachedNodeForRead(node, properties)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// streamNodesByLabelFromPhysicalSnapshotAfter visits the node bodies of a
// label represented by a pinned Badger read transaction, in label-index
// order after afterNodeID ("" from the start), within scope ("" every
// database). Because both the label index and node key are read from the same
// immutable physical snapshot, no per-candidate logical MVCC-head lookup is
// required.
func (b *BadgerEngine) streamNodesByLabelFromPhysicalSnapshotAfter(
	scope string,
	label string,
	view func(func(*badger.Txn) error) error,
	properties []string,
	afterNodeID NodeID,
	visit func(*Node) error,
) error {
	if visit == nil {
		return ErrInvalidData
	}
	include := propertyProjectionSet(properties)
	nowNanos := DecayScoringTime()
	covers := b.labelCoversScope(scope, label)
	return view(func(txn *badger.Txn) error {
		// readItem reads one node from its stored record; item.Version keys
		// the decoded-body cache.
		readItem := func(nodeID NodeID, item *badger.Item) (*Node, error) {
			itemVersion := item.Version()
			if cached, ok := b.cacheLoadNodeBody(nodeID, itemVersion); ok {
				if properties == nil {
					return b.loadNodeEmbeddings(txn, cached, nodeID)
				}
				return projectCachedNodeForRead(cached, properties), nil
			}
			var node *Node
			err := item.Value(func(value []byte) error {
				if properties == nil {
					decoded, decodeErr := b.decodeNode(namespaceForNodeID(nodeID), value)
					if decodeErr != nil {
						return decodeErr
					}
					b.cacheStoreNodeBody(nodeID, itemVersion, decoded)
					node, decodeErr = b.loadNodeEmbeddings(txn, decoded, nodeID)
					return decodeErr
				}
				var decodeErr error
				node, decodeErr = b.decodeNodeProjected(namespaceForNodeID(nodeID), value, include)
				return decodeErr
			})
			return node, err
		}
		emit := func(node *Node) error {
			if node == nil || b.filterNodeByDecay(node, nowNanos) || !hasLabel(node.Labels, label) {
				return nil
			}
			return visit(node)
		}
		readPoint := func(nodeID NodeID) error {
			item, err := txn.Get(nodeKey(nodeID))
			if err == badger.ErrKeyNotFound {
				return nil
			}
			if err != nil {
				return err
			}
			node, err := readItem(nodeID, item)
			if err != nil {
				return err
			}
			return emit(node)
		}

		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		if afterNodeID == "" {
			it.Rewind()
		} else {
			nodeNum, ok := b.idDict.lookupNodeNumID(afterNodeID)
			if !ok {
				return ErrNotFound
			}
			cursor := labelIndexKey(label, nodeNum)
			it.Seek(cursor)
			if it.ValidForPrefix(prefix) && bytes.Equal(it.Item().Key(), cursor) {
				it.Next()
			}
		}
		var pending []NodeID
		lookups := 0
		for ; it.ValidForPrefix(prefix); it.Next() {
			indexKey := it.Item().Key()
			nodeNum, ok := extractNodeNumIDFromLabelIndex(indexKey, len(label))
			if !ok {
				continue
			}
			nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
			if !ok || nodeID == "" || !nodeIDInScope(nodeID, scope) || (b.decayEnabled && !b.revealAll.Load() && hasIndexTombstone(txn, indexKey)) {
				continue
			}
			if pending != nil || (covers && lookups >= labelScanPointLookups) {
				if len(pending) < labelScanMaxBuffered {
					pending = append(pending, nodeID)
					continue
				}
				for _, pendingID := range pending {
					if err := readPoint(pendingID); err != nil {
						return err
					}
				}
				pending = nil
				covers = false
			}
			lookups++
			if err := readPoint(nodeID); err != nil {
				return err
			}
		}
		if len(pending) == 0 {
			return nil
		}

		// The rest are read in one pass over the node records, then visited
		// in label-index order.
		nodes := make(map[NodeID]*Node, len(pending))
		readErrors := make(map[NodeID]error)
		if err := readNodeRecordsInOnePass(txn, scope, pending, func(nodeID NodeID, item *badger.Item) error {
			node, err := readItem(nodeID, item)
			nodes[nodeID] = node
			if err != nil {
				readErrors[nodeID] = err
			}
			return nil
		}); err != nil {
			return err
		}
		for _, nodeID := range pending {
			// Record order differs from visitation order; a later corrupt
			// record must not override an earlier callback's stop error.
			if err := readErrors[nodeID]; err != nil {
				return err
			}
			if err := emit(nodes[nodeID]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = b.withView(func(txn *badger.Txn) error {
		return b.iterateEdgesVisibleAtInTxn(txn, version, func(edge *Edge) error {
			if edge == nil {
				return nil
			}
			if edgeType != "" && edge.Type != edgeType {
				return nil
			}
			edges = append(edges, edge)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

// getEdgesByTypeVisibleAtSnapshotWithView resolves type candidates from the
// same physical Badger snapshot used by an explicit transaction. The type
// index in that snapshot represents membership at BEGIN, so unrelated edge
// bodies are never decoded.
func (b *BadgerEngine) getEdgesByTypeVisibleAtSnapshotWithView(edgeType string, version MVCCVersion, view func(func(*badger.Txn) error) error) ([]*Edge, error) {
	edges := make([]*Edge, 0)
	err := b.streamEdgesByTypeVisibleAtSnapshotWithView(context.Background(), "", edgeType, version, view, func(edge *Edge) error {
		edges = append(edges, edge)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

func (b *BadgerEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = b.withView(func(txn *badger.Txn) error {
		return b.iterateEdgesVisibleAtInTxn(txn, version, func(edge *Edge) error {
			if edge == nil {
				return nil
			}
			if edge.StartNode == startID && edge.EndNode == endID {
				edges = append(edges, edge)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

func (b *BadgerEngine) IterateLatestVisibleNodes(yield func(*Node) error) error {
	nodes, err := b.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := yield(node); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) IterateLatestVisibleEdges(yield func(*Edge) error) error {
	edges, err := b.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if err := yield(edge); err != nil {
			return err
		}
	}
	return nil
}
