package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	return b.getNodeVisibleAtWithView(id, version, true, b.withView)
}

func (b *BadgerEngine) getNodeVisibleAtWithView(id NodeID, version MVCCVersion, vectors bool, view func(func(*badger.Txn) error) error) (*Node, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var node *Node
	err = view(func(txn *badger.Txn) error {
		var loadErr error
		node, loadErr = b.getNodeVisibleAtInTxn(txn, id, version, vectors)
		return loadErr
	})
	return node, err
}

// getNodeVisibleAtInTxn resolves one logical MVCC version using the supplied
// physical Badger snapshot. Callers that scan an index in the same snapshot
// use this helper to avoid opening nested views or scanning unrelated nodes.
//
// With vectors false the node reads as GetNodeWithoutEmbeddings returns it:
// its embedding metadata is settled, but no separately stored chunk vector
// is read (loadNodeEmbeddingState).
func (b *BadgerEngine) getNodeVisibleAtInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, vectors bool) (*Node, error) {
	return b.getNodeAtInTxn(txn, id, version, vectors, true)
}

func (b *BadgerEngine) getNodeAtInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, vectors, applyDecayFilter bool) (*Node, error) {
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
				cached, loadErr := b.loadNodeEmbeddingState(txn, cached, id, vectors)
				if loadErr != nil {
					return nil, loadErr
				}
				if applyDecayFilter && b.filterNodeByDecay(cached, DecayScoringTime()) {
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
				node, decodeErr = b.loadNodeEmbeddingState(txn, decoded, id, vectors)
				return decodeErr
			}); err != nil {
				return nil, err
			}
			if applyDecayFilter && b.filterNodeByDecay(node, DecayScoringTime()) {
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
	var node *Node
	if vectors {
		node = copyNode(record.Node)
	} else {
		node = copyNodeForCaller(record.Node)
	}
	if applyDecayFilter && b.filterNodeByDecay(node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return node, nil
}

// getNodeProjectedVisibleAtWithView is getNodeVisibleAtWithView for a
// projected read: it decodes only the requested user properties (none when
// properties is an empty non-nil slice) and never hydrates embedding vectors
// or touches the body cache, so a traversal that needs only a node's labels
// pays no property-decode or copy cost (#1110).
func (b *BadgerEngine) getNodeProjectedVisibleAtWithView(id NodeID, version MVCCVersion, properties []string, view func(func(*badger.Txn) error) error) (*Node, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var node *Node
	err = view(func(txn *badger.Txn) error {
		var loadErr error
		node, loadErr = b.getNodeProjectedVisibleAtInTxn(txn, id, version, properties)
		return loadErr
	})
	return node, err
}

// getNodeProjectedVisibleAtInTxn is getNodeVisibleAtInTxn for a projected
// read. The visibility resolution (MVCC head) is identical; only the body
// decode and embedding hydration are skipped.
func (b *BadgerEngine) getNodeProjectedVisibleAtInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, properties []string) (*Node, error) {
	include := propertyProjectionSet(properties)
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
			var node *Node
			if err := item.Value(func(val []byte) error {
				namespace := namespaceForNodeID(id)
				decoded, decodeErr := b.decodeNodeProjected(namespace, val, include)
				if decodeErr != nil {
					return decodeErr
				}
				if decoded == nil {
					return ErrNotFound
				}
				node = decoded
				return nil
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
	node := copyNodeProjectedWithoutEmbeddings(record.Node, properties)
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
	readOptions ...StreamNodesOptions,
) error {
	opts := nodeScanOptions(readOptions)
	queryNodes := opts.QueryNodes
	if visit == nil {
		return ErrInvalidData
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return err
	}
	defer deregister()

	return view(func(txn *badger.Txn) error {
		it := b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), "")
		defer it.Close()
		for it.Next() {
			nodeID := it.NodeID()
			node, getErr := b.getNodeAtInTxn(txn, nodeID, version, !queryNodes, opts.ApplyDecayFilter)
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
				if err := visit(projectCachedNodeForRead(node, properties, queryNodes)); err != nil {
					return err
				}
			}
		}
		return it.Err()
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
	readOptions ...StreamNodesOptions,
) error {
	opts := nodeScanOptions(readOptions)
	queryNodes := opts.QueryNodes
	if visit == nil {
		return ErrInvalidData
	}
	include := propertyProjectionSet(properties)
	nowNanos := DecayScoringTime()
	// covers is computed lazily: the label-vs-node count reads that decide
	// the one-pass switch only matter once a scan has actually walked
	// labelScanPointLookups candidates. Computing it eagerly paid two Badger
	// count reads on every small or LIMIT-stopped scan (PR #1043 write-path
	// measurement: labelCoversScope was ~29% of the match-create-delete
	// benchmark).
	var covers bool
	coversDecided := false
	return view(func(txn *badger.Txn) error {
		// readItem reads one node from its stored record; item.Version keys
		// the decoded-body cache.
		readItem := func(nodeID NodeID, item *badger.Item) (*Node, error) {
			if queryNodes {
				return b.readQueryNodeItemInTxn(txn, item, nodeID)
			}
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
			if node == nil || (opts.ApplyDecayFilter && b.filterNodeByDecay(node, nowNanos)) || !hasLabel(node.Labels, label) {
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

		it := b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), afterNodeID)
		defer it.Close()
		var pending []NodeID
		lookups := 0
		for it.Next() {
			indexKey := it.Item().Key()
			nodeID := it.NodeID()
			if opts.ApplyDecayFilter && b.decayEnabled && !b.revealAll.Load() && hasIndexTombstone(txn, indexKey) {
				continue
			}
			if !coversDecided && lookups >= labelScanPointLookups {
				covers = b.labelCoversScope(scope, label)
				coversDecided = true
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
		if err := it.Err(); err != nil {
			return err
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
