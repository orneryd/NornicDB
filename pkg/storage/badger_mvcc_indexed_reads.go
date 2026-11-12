package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// Keep the optional bulk enumeration contract bound to its native implementation.
var _ MVCCEnumerationEngine = (*BadgerEngine)(nil)

func (b *BadgerEngine) GetNodeVisibleAt(ctx context.Context, id NodeID, version MVCCVersion) (*Node, error) {
	return readEntityAtWithView(ctx, b, id, version, mvccEntityRead{vectors: true, visibility: visibleRead}, nil, (*BadgerEngine).getNodeAtInTxn)
}

// getNodeAtInTxn resolves logical visibility and hydration against the supplied
// physical snapshot. Index callers retain that same pinned transaction.
func (b *BadgerEngine) getNodeAtInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, policy mvccEntityRead) (*Node, error) {
	vectors, v := policy.vectors, policy.visibility
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
			hydration := nodeItemQueryMetadata
			if vectors {
				hydration = nodeItemFullEmbeddings
			}
			node, err := b.readNodeItemInTxn(txn, item, id, hydration)
			if err != nil {
				return nil, err
			}
			if b.filterEntityByDecay(v, node, DecayScoringTime()) {
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
	if b.filterEntityByDecay(v, node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return node, nil
}

// getNodeProjectedVisibleAtInTxn is the MVCC node kernel for a projected
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
				decoded, decodeErr := b.decodeNodeProjected(txn, namespace, val, include)
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
			if b.filterEntityByDecay(visibleRead, node, DecayScoringTime()) {
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
	node := projectCachedNodeForRead(record.Node, properties, true)
	if b.filterEntityByDecay(visibleRead, node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return node, nil
}

func (b *BadgerEngine) GetNodesByLabelVisibleAt(ctx context.Context, label string, version MVCCVersion) ([]*Node, error) {
	return b.getNodesByLabelVisibleAtWithView(ctx, "", label, version, nil)
}

// GetNodesByLabelVisibleAtInScope is GetNodesByLabelVisibleAt within one
// database (ScopedLabelNodeReader).
func (b *BadgerEngine) GetNodesByLabelVisibleAtInScope(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error) {
	return b.getNodesByLabelVisibleAtWithView(ctx, scope, label, version, nil)
}

func (b *BadgerEngine) getNodesByLabelVisibleAtWithView(ctx context.Context, scope, label string, version MVCCVersion, view func(func(*badger.Txn) error) error) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.pendingPrefixConflict(scope); err != nil {
		return nil, err
	}

	deregister, view, err := b.beginMVCCHistoryRead(ctx, version, view)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var nodes []*Node
	err = view(func(txn *badger.Txn) error {
		return b.iterateNodesVisibleAtInScopeInTxn(ctx, txn, scope, version, func(node *Node) error {
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
func (b *BadgerEngine) getNodesByLabelVisibleAtSnapshotWithView(ctx context.Context, scope, label string, version MVCCVersion, view func(func(*badger.Txn) error) error) ([]*Node, error) {
	nodes := make([]*Node, 0)
	err := b.streamNodesByLabelAtSnapshotWithView(ctx, scope, label, version, view, nil, visibleRead, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (b *BadgerEngine) streamNodesByLabelAtSnapshotWithView(
	ctx context.Context,
	scope, label string,
	version MVCCVersion,
	view func(func(*badger.Txn) error) error,
	properties []string,
	v readVisibility,
	visit func(*Node) error,
	queryRead ...bool,
) error {
	if err := b.pendingPrefixConflict(scope); err != nil {
		return err
	}

	queryNodes := len(queryRead) > 0 && queryRead[0]
	if visit == nil {
		return ErrInvalidData
	}
	deregister, view, err := b.beginMVCCHistoryRead(ctx, version, view)
	if err != nil {
		return err
	}
	defer deregister()

	return view(func(txn *badger.Txn) error {
		it, err := b.newLabelIndexReader(ctx, txn, scope, label, "", v, true)
		if err != nil {
			return err
		}
		defer it.Close()
		var cancel bulkReadCancellation
		for it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			nodeID := it.NodeID()
			node, getErr := b.getNodeAtInTxn(txn, nodeID, version, mvccEntityRead{vectors: !queryNodes, visibility: v})
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
				if err := visit(projectCachedNodeForRead(node, properties, false)); err != nil {
					return err
				}
			}
		}
		return it.Err()
	})
}

// streamNodesByLabelFromPhysicalSnapshotAfterRead visits the node bodies of a
// label represented by a pinned Badger read transaction, in label-index
// order after afterNodeID ("" from the start), within scope ("" every
// database). Because both the label index and node key are read from the same
// immutable physical snapshot, no per-candidate logical MVCC-head lookup is
// required.
func (b *BadgerEngine) streamNodesByLabelFromPhysicalSnapshotAfterRead(
	ctx context.Context,
	scope, label string,
	view func(func(*badger.Txn) error) error,
	properties []string,
	afterNodeID NodeID,
	v readVisibility,
	visit func(*Node) error,
	queryRead ...bool,
) error {
	if err := b.pendingPrefixConflict(scope); err != nil {
		return err
	}

	queryNodes := len(queryRead) > 0 && queryRead[0]
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
				return b.readNodeItemInTxn(txn, item, nodeID, nodeItemQueryMetadata)
			}
			return b.readProjectedNodeItemInTxn(txn, item, nodeID, properties, include, nodeBodyDetached)
		}
		emit := func(node *Node) error {
			if node == nil || b.filterEntityByDecay(v, node, nowNanos) || !hasLabel(node.Labels, label) {
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

		it, err := b.newLabelIndexReader(ctx, txn, scope, label, afterNodeID, v, false)
		if err != nil {
			return err
		}
		defer it.Close()
		var pending []NodeID
		lookups := 0
		// The pinned snapshot walk visits its candidates under the caller's own
		// context: a canceled read retires with that error instead of finishing
		// the population nobody asked for any more.
		var cancel bulkReadCancellation
		for it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			indexKey := it.IndexKey()
			nodeID := it.NodeID()
			if b.checksIndexTombstones(v) && hasIndexTombstone(txn, indexKey) {
				continue
			}
			if !coversDecided && lookups >= labelScanPointLookups {
				// Author: storage-format-owners lane (Codex), 1 November 2026.
				var err error
				covers, _, err = b.labelCoversScope(ctx, scope, label)
				if err != nil {
					return err
				}
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
		if err := readNodeRecordsInOnePass(ctx, txn, scope, pending, func(nodeID NodeID, item *badger.Item) error {
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
			if err := cancel.check(ctx); err != nil {
				return err
			}
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

func (b *BadgerEngine) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, version MVCCVersion) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	deregister, err := b.beginMVCCSnapshotRead(ctx, version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = b.withView(func(txn *badger.Txn) error {
		return b.iterateEdgesAtInTxn(ctx, txn, version, visibleRead, func(edge *Edge) error {
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

// yieldLatestVisible is the one owner of the latest-visible enumeration
// algorithm the MVCCEnumerationEngine node and edge callers present: load the
// population under the caller's own lifetime, then hand every record to yield.
// The caller's cancellation is checked on the first record, once every
// bulkReadCancellationInterval records (the same stride the scans inside the
// load use) and once more after the last yield, so a request already canceled
// on admission or canceled by the visitor itself retires with the context's
// own error instead of reporting a successful enumeration. A load or visitor
// error surfaces unchanged and outranks that final context check.
func yieldLatestVisible[T any](ctx context.Context, load func(context.Context) ([]T, error), yield func(T) error) error {
	records, err := load(ctx)
	if err != nil {
		return err
	}
	var cancel bulkReadCancellation
	for _, record := range records {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		if err := yield(record); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (b *BadgerEngine) IterateLatestVisibleNodes(ctx context.Context, yield func(*Node) error) error {
	if err := b.pendingNamespaceConflict(""); err != nil {
		return err
	}

	// The caller's own context is the read's lifetime: a canceled statement
	// must not keep walking every visible node, and a visitor that cancels it
	// must not be answered with a successful enumeration.
	return yieldLatestVisible(ctx, b.AllNodes, yield)
}

func (b *BadgerEngine) IterateLatestVisibleEdges(yield func(*Edge) error) error {
	if err := b.pendingNamespaceConflict(""); err != nil {
		return err
	}

	// Edge iteration has no caller context of its own: context.Background is
	// the true lifetime at this context-free boundary.
	return yieldLatestVisible(context.Background(), b.AllEdges, yield)
}
