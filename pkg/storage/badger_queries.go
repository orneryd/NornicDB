// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const edgeBetweenSelfHealMaxEdges = 64

// Query Operations
// ============================================================================

// GetFirstNodeByLabel returns the first node with the specified label.
// This is optimized for MATCH...LIMIT 1 patterns - stops after first match.
func (b *BadgerEngine) GetFirstNodeByLabel(ctx context.Context, label string) (*Node, error) {
	return b.GetFirstNodeByLabelInScope(ctx, "", label)
}

// GetFirstNodeByLabelInScope is GetFirstNodeByLabel within one database
// (ScopedLabelNodeReader).
func (b *BadgerEngine) GetFirstNodeByLabelInScope(ctx context.Context, scope, label string) (*Node, error) {
	if err := b.pendingPrefixConflict(scope); err != nil {
		return nil, err
	}

	var node *Node
	if b.writeBehind != nil {
		// Share the overlay's shadowing and current-label decisions with the
		// ordinary stream, while stopping after its first visible node. The
		// caller's context ends the first-row wait; the stream stops on its
		// first row anyway.
		err := b.StreamNodesByLabelProjectedInScope(ctx, scope, label, nil, func(first *Node) error {
			node = first
			return ErrIterationStopped
		})
		if err == ErrIterationStopped {
			err = nil
		}
		return node, err
	}
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		it, err := b.newLabelIndexReader(ctx, txn, scope, label, "", visibleRead, false)
		if err != nil {
			return err
		}
		defer it.Close()

		for it.Next() {
			indexKey := it.IndexKey()
			nodeID := it.NodeID()

			if b.decayEnabled && !b.revealAll.Load() && hasIndexTombstone(txn, indexKey) {
				continue
			}

			item, err := txn.Get(nodeKey(nodeID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				return err
			}
			if b.filterEntityByDecay(visibleRead, node, nowNanos) {
				node = nil
				continue
			}

			return nil // Found first node, stop
		}
		return it.Err()
	})

	return node, err
}

// ForEachNodeIDByLabel streams node IDs for a label without decoding nodes.
// Stops early when visit returns false.
func (b *BadgerEngine) ForEachNodeIDByLabel(ctx context.Context, label string, visit func(NodeID) bool) error {
	return b.ForEachNodeIDByLabelInScope(ctx, "", label, visit)
}

// ForEachNodeIDByLabelInScope streams only this database's label IDs. Empty
// scope preserves the unscoped interface's behavior.
func (b *BadgerEngine) ForEachNodeIDByLabelInScope(ctx context.Context, scope, label string, visit func(NodeID) bool) error {
	if err := b.pendingPrefixConflict(scope); err != nil {
		return err
	}

	if visit == nil {
		return nil
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	if b.writeBehind != nil {
		err := b.StreamNodesByLabelProjectedInScope(ctx, scope, label, []string{}, func(node *Node) error {
			if !visit(node.ID) {
				return ErrIterationStopped
			}
			return nil
		})
		if err == ErrIterationStopped {
			return nil
		}
		return err
	}

	cachedID, cachedOK := b.labelCacheGetFirst(label)
	// The first-ID cache is global. Scoped reads keep their own ordered
	// candidate stream instead of borrowing or replacing another scope's hint.
	if scope != "" {
		cachedID, cachedOK = "", false
	}
	cachedValid := false

	labelGen := b.labelFirstCacheGen.current()
	err := b.withView(func(txn *badger.Txn) error {
		ready, err := b.labelIndexReadyInTxn(txn)
		if err != nil {
			return err
		}
		if ready && cachedOK && cachedID != "" {
			cachedKey := b.labelIndexKeyStringLookup(label, cachedID)
			if cachedKey == nil {
				labelGen = b.labelCacheDropStaleFirst(labelGen, label, cachedID)
			} else {
				_, err := txn.Get(cachedKey)
				switch err {
				case nil:
					cachedValid = true
					if !visit(cachedID) {
						return ErrIterationStopped
					}
				case badger.ErrKeyNotFound:
					labelGen = b.labelCacheDropStaleFirst(labelGen, label, cachedID)
				default:
					return err
				}
			}
		}

		it, err := b.newLabelIndexReader(ctx, txn, scope, label, "", visibleRead, false)
		if err != nil {
			return err
		}
		defer it.Close()

		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		for it.Next() {
			indexKey := it.IndexKey()
			nodeID := it.NodeID()
			if cachedValid && nodeID == cachedID {
				continue
			}
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}
			if !cachedValid {
				if scope == "" {
					b.labelCacheSetFirst(labelGen, label, nodeID, txn.ReadTs())
				}
				cachedValid = true
			}
			if !visit(nodeID) {
				return ErrIterationStopped
			}
		}

		return it.Err()
	})
	if err == ErrIterationStopped {
		return nil
	}
	return err
}

// GetNodesByLabel returns all nodes with the specified label.
func (b *BadgerEngine) GetNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	return b.GetNodesByLabelInScope(ctx, "", label)
}

// GetNodesByLabelInScope is GetNodesByLabel within one database
// (ScopedLabelNodeReader).
func (b *BadgerEngine) GetNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error) {
	return b.getNodesByLabelInScope(ctx, scope, label, visibleRead)
}

// GetStoredNodesByLabel is GetNodesByLabel for integrity work: it also
// returns the nodes the visibility layer hides (StoredEntityReader).
func (b *BadgerEngine) GetStoredNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	return b.getNodesByLabelInScope(ctx, "", label, storedRead)
}

func (b *BadgerEngine) getNodesByLabelInScope(ctx context.Context, scope, label string, v readVisibility) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var sourceTs uint64
	// Single-pass: iterate label index and fetch nodes in same transaction
	// This reduces transaction overhead compared to two-phase approach
	var overlay []*Node
	var touched map[NodeID]bool
	var current bool
	var nodes []*Node
	var loaded []*Node
	nowNanos := DecayScoringTime()
	gen := b.nodeCacheGen.current()
	checkTombstones := b.checksIndexTombstones(v)
	var cancel bulkReadCancellation
	var captureErr error
	err := b.withReadView(ctx, func(w *WriteBehindBuffer) {
		pendingNodes, err := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedLabel, label: label, prefix: scope})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingNodes.nodes, pendingNodes.nodesTouched
	}, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan
		}
		sourceTs = txn.ReadTs()
		nodes = nil
		loaded = nil
		it, err := b.newLabelIndexReader(ctx, txn, scope, label, "", v, false)
		if err != nil {
			return err
		}
		defer it.Close()

		for it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			indexKey := it.IndexKey()
			nodeID := it.NodeID()

			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}

			if touched[nodeID] {
				continue
			}

			// Cache-first: avoid Badger reads/decodes for hot label scans.
			b.nodeCacheMu.RLock()
			if cached, ok := b.nodeCache[nodeID]; ok && current {
				b.nodeCacheMu.RUnlock()
				cn := copyNode(cached)
				if b.filterEntityByDecay(v, cn, nowNanos) {
					continue
				}
				nodes = append(nodes, cn)
				continue
			}
			b.nodeCacheMu.RUnlock()

			// Fetch node data in same transaction
			item, err := txn.Get(nodeKey(nodeID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue // Skip if node was deleted
			}
			if err != nil {
				return err
			}

			var node *Node
			var readErr error
			node, readErr = b.readNodeItemInTxn(txn, item, nodeID, nodeItemFullEmbeddings)
			if readErr != nil {
				return readErr
			}

			if b.filterEntityByDecay(v, node, nowNanos) {
				continue
			}

			loaded = append(loaded, node)
			nodes = append(nodes, node)
		}
		return it.Err()
	}, readViewOptions{scope: scope, scopeKind: readPrefixScope, current: &current})

	if err != nil {
		return nil, err
	}

	// Cache loaded nodes for future label scans.
	for _, n := range loaded {
		if n != nil {
			b.cacheFillNode(gen, n, nodeReadCacheFull, sourceTs)
		}
	}

	if len(touched) != 0 {
		nodes = mergeWriteBehindEntities(nodes, overlay, touched, func(n *Node) NodeID { return n.ID }, func(n *Node) *Node {
			if !nodeIDInScope(n.ID, scope) {
				return nil
			}
			return copyNode(n)
		})
	}
	return nodes, nil
}

// StreamNodesByLabelProjected iterates label-matching nodes in a single read
// transaction while decoding only the requested user properties.
func (b *BadgerEngine) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return b.StreamNodesByLabelProjectedInScope(ctx, "", label, properties, visit)
}

// StreamNodesByLabelProjectedInScope is StreamNodesByLabelProjected within
// one database (ScopedLabelNodeReader).
//
// Nodes are visited in label-index order. Scans that continue past
// labelScanPointLookups switch to one pass over the node records when the
// label is on at least half of the scope's nodes.
func (b *BadgerEngine) StreamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScope(ctx, scope, label, properties, visibleRead, visit)
}

// StreamStoredNodesByLabelProjected is StreamNodesByLabelProjected for
// integrity work: it also visits the nodes the visibility layer hides
// (StoredEntityReader).
func (b *BadgerEngine) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScope(ctx, "", label, properties, storedRead, visit)
}

func (b *BadgerEngine) streamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, v readVisibility, visit func(*Node) error, queryRead ...bool) error {
	queryNodes := len(queryRead) > 0 && queryRead[0]
	if visit == nil {
		return ErrInvalidData
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	if err := b.ensureOpen(false); err != nil {
		return err
	}

	// Write-behind overlay: acknowledged-but-unflushed writes replace or
	// shadow committed rows for their IDs and are visited after the scan.
	var overlay []*Node
	var touched map[NodeID]bool
	var captureErr error
	capture := func(w *WriteBehindBuffer) {
		pendingNodes, err := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedLabel, label: label, prefix: scope})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingNodes.nodes, pendingNodes.nodesTouched
	}

	include := propertyProjectionSet(properties)
	nowNanos := DecayScoringTime()
	checkTombstones := b.checksIndexTombstones(v)
	// covers is computed lazily (see streamNodesByLabelFromPhysicalSnapshotAfter):
	// the one-pass decision only matters after labelScanPointLookups lookups,
	// and an eager decision pays two Badger count reads on every small or
	// LIMIT-stopped scan.
	var covers bool
	coversDecided := false
	err := b.withReadView(ctx, capture, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan
		}
		readItem := func(nodeID NodeID, item *badger.Item) (*Node, error) {
			if queryNodes {
				return b.readNodeItemInTxn(txn, item, nodeID, nodeItemQueryMetadata)
			}
			return b.readProjectedNodeItemInTxn(txn, item, nodeID, properties, include, nodeBodyDetached)
		}
		cachedNode := func(nodeID NodeID) (*Node, bool) {
			b.nodeCacheMu.RLock()
			cached, ok := b.nodeCache[nodeID]
			b.nodeCacheMu.RUnlock()
			if !ok || b.writeBehind != nil {
				return nil, false
			}
			return projectCachedNodeForRead(cached, properties, false, queryNodes), true
		}
		emit := func(node *Node) error {
			if node == nil || b.filterEntityByDecay(v, node, nowNanos) {
				return nil
			}
			if len(touched) > 0 && touched[node.ID] {
				return nil // the overlay shadows committed rows for this ID
			}
			return visit(node)
		}
		readPoint := func(nodeID NodeID) error {
			node, cached := cachedNode(nodeID)
			if !cached {
				item, err := txn.Get(nodeKey(nodeID))
				if errors.Is(err, badger.ErrKeyNotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				node, err = readItem(nodeID, item)
				if err != nil {
					return err
				}
			}
			return emit(node)
		}
		type pendingNode struct {
			id   NodeID
			node *Node
			err  error
		}
		var pending []pendingNode
		it, err := b.newLabelIndexReader(ctx, txn, scope, label, "", v, false)
		if err != nil {
			return err
		}
		defer it.Close()
		lookups := 0
		var cancel bulkReadCancellation
		for it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			indexKey := it.IndexKey()
			nodeID := it.NodeID()
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}

			if !coversDecided && lookups >= labelScanPointLookups {
				var err error
				covers, err = b.labelCoversScope(ctx, scope, label)
				if err != nil {
					return err
				}
				coversDecided = true
			}
			if pending != nil || (covers && lookups >= labelScanPointLookups) {
				if len(pending) < labelScanMaxBuffered {
					pending = append(pending, pendingNode{id: nodeID})
					continue
				}
				for _, entry := range pending {
					if err := readPoint(entry.id); err != nil {
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

		ids := make([]NodeID, 0, len(pending))
		positions := make(map[NodeID]int, len(pending))
		for index, entry := range pending {
			if node, cached := cachedNode(entry.id); cached {
				pending[index].node = node
				continue
			}
			ids = append(ids, entry.id)
			positions[entry.id] = index
		}
		if err := readNodeRecordsInOnePass(ctx, txn, scope, ids, func(nodeID NodeID, item *badger.Item) error {
			index := positions[nodeID]
			pending[index].node, pending[index].err = readItem(nodeID, item)
			// Report record errors in visit order, so an early stop does not
			// surface corruption in a record the caller never reaches.
			return nil
		}); err != nil {
			return err
		}
		for _, entry := range pending {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			if entry.err != nil {
				return entry.err
			}
			if err := emit(entry.node); err != nil {
				return err
			}
		}
		return nil
	}, readViewOptions{scope: scope, scopeKind: readPrefixScope})
	if err != nil {
		return err
	}
	var overlayCancel bulkReadCancellation
	for _, n := range overlay {
		if err := overlayCancel.check(ctx); err != nil {
			return err
		}
		if !nodeIDInScope(n.ID, scope) {
			continue
		}
		if err := visit(projectCachedNodeForRead(n, properties, false, queryNodes)); err != nil {
			return err
		}
	}
	return nil
}

// mergeWriteBehindEntities folds the write-behind overlay over
// committed rows: rows whose IDs the buffer touched are dropped and the
// buffered versions (copied, or projected for the caller) take their place.
func mergeWriteBehindEntities[T any, K comparable](committed, overlay []*T, touched map[K]bool, id func(*T) K, read func(*T) *T) []*T {
	if len(touched) == 0 {
		return committed
	}
	out := committed[:0]
	for _, entity := range committed {
		if !touched[id(entity)] {
			out = append(out, entity)
		}
	}
	for _, entity := range overlay {
		if copy := read(entity); copy != nil {
			out = append(out, copy)
		}
	}
	return out
}

func projectCachedNodeForRead(node *Node, properties []string, stripEmbeddings bool, queryRead ...bool) *Node {
	if node == nil {
		return nil
	}
	if properties == nil {
		if len(queryRead) > 0 && queryRead[0] && !stripEmbeddings {
			return copyNodeForCaller(node)
		}
		if stripEmbeddings {
			return copyNodeWithoutEmbeddings(node)
		}
		return copyNode(node)
	}
	// Copy only returned fields, once. Labels and embedding metadata remain
	// part of a stripped result; vector payloads and omitted properties do not.
	source := *node
	source.Properties = nil
	var projected *Node
	if stripEmbeddings {
		projected = copyNodeWithoutEmbeddings(&source)
	} else {
		projected = copyNode(&source)
	}

	projected.Properties = make(map[string]any, len(properties))
	for _, property := range properties {
		if value, ok := node.Properties[property]; ok {
			projected.Properties[property] = canonicalPropertyValue(value)
		}
	}
	return projected
}

// AllNodes returns all nodes (implements Engine interface).
func (b *BadgerEngine) AllNodes(ctx context.Context) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var sourceTs uint64
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	var current bool
	var nodes []*Node
	var loaded []*Node
	var overlay []*Node
	var touched map[NodeID]bool
	var captureErr error
	capture := func(w *WriteBehindBuffer) {
		pendingNodes, err := b.readBufferedEntities(ctx, w, visibleRead, bufferedEntityRead{kind: bufferedNodes})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingNodes.nodes, pendingNodes.nodesTouched
	}
	nowNanos := DecayScoringTime()
	gen := b.nodeCacheGen.current()
	err := b.withReadView(ctx, capture, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan and its caches
		}
		sourceTs = txn.ReadTs()
		nodes = nil
		loaded = nil
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		var cancel bulkReadCancellation
		for it.Rewind(); it.Valid(); it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			// Extract nodeID from key (skip prefix byte)
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			if len(touched) > 0 && touched[nodeID] {
				continue // the write-behind overlay shadows committed rows
			}

			b.nodeCacheMu.RLock()
			if cached, ok := b.nodeCache[nodeID]; ok && current {
				b.nodeCacheMu.RUnlock()
				nodeCopy := copyNode(cached)
				if b.filterEntityByDecay(visibleRead, nodeCopy, nowNanos) {
					continue
				}
				nodes = append(nodes, nodeCopy)
				continue
			}
			b.nodeCacheMu.RUnlock()

			var node *Node
			var readErr error
			node, readErr = b.readNodeItemInTxn(txn, it.Item(), nodeID, nodeItemFullEmbeddings)
			if readErr != nil {
				return readErr
			}

			if b.filterEntityByDecay(visibleRead, node, nowNanos) {
				continue
			}

			loaded = append(loaded, node)
			nodes = append(nodes, node)
		}

		return nil
	}, readViewOptions{current: &current})

	if err != nil {
		return nil, err
	}
	for _, node := range loaded {
		if node != nil {
			b.cacheFillNode(gen, node, nodeReadCacheFull, sourceTs)
		}
	}
	for _, node := range overlay {
		nodes = append(nodes, copyNode(node))
	}
	return nodes, err
}

// AllEdges returns all edges (implements Engine interface).
func (b *BadgerEngine) AllEdges(ctx context.Context) ([]*Edge, error) {
	return b.allEdges(ctx, visibleRead)
}

func (b *BadgerEngine) allEdges(ctx context.Context, v readVisibility) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	var edges []*Edge
	var overlay []*Edge
	var touched map[EdgeID]bool
	var captureErr error
	capture := func(w *WriteBehindBuffer) {
		pendingEdges, err := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedEdges})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingEdges.edges, pendingEdges.edgesTouched
	}
	nowNanos := DecayScoringTime()
	err := b.withReadView(ctx, capture, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan
		}
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		var cancel bulkReadCancellation
		for it.Rewind(); it.Valid(); it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			key := it.Item().Key()
			var edgeID EdgeID
			if len(key) > 1 {
				edgeID = EdgeID(key[1:])
			}
			if len(touched) > 0 && touched[edgeID] {
				continue // the write-behind overlay shadows committed rows
			}
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = b.decodeEdgeBodyWithID(txn, namespaceForEdgeID(edgeID), val, edgeID)
				return decodeErr
			}); err != nil {
				return err
			}

			if b.filterEntityByDecay(v, edge, nowNanos) {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	for _, edge := range overlay {
		edges = append(edges, copyEdge(edge))
	}
	return edges, err
}

// GetEdgesByType returns all edges of a specific type using the edge type index.
// This is MUCH faster than AllEdges() for queries like mutual follows.
// Relationship types match exactly, as in Neo4j (#862).
// Results are cached per type to speed up repeated queries.
func (b *BadgerEngine) GetEdgesByType(ctx context.Context, edgeType string) ([]*Edge, error) {
	return b.getEdgesByType(ctx, edgeType, visibleRead)
}

// GetStoredEdgesByType is GetEdgesByType for integrity work: it also returns
// the relationships the visibility layer hides (StoredEntityReader). It never
// reads or fills the per-type cache, which holds visible results.
func (b *BadgerEngine) GetStoredEdgesByType(ctx context.Context, edgeType string) ([]*Edge, error) {
	return b.getEdgesByType(ctx, edgeType, storedRead)
}

func (b *BadgerEngine) getEdgesByType(ctx context.Context, edgeType string, v readVisibility) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if edgeType == "" {
		if v == storedRead {
			return b.allEdges(ctx, storedRead)
		}
		return b.AllEdges(ctx) // No type filter = all edges
	}

	// With write-behind active, buffered edges are not in the whole-type
	// cache, so a cached result would miss them; bypass it.
	useCache := b.writeBehind == nil && v == visibleRead
	if useCache {
		// Check cache first
		b.edgeTypeCacheMu.RLock()
		if cached, ok := b.edgeTypeCache[edgeType]; ok {
			b.edgeTypeCacheMu.RUnlock()
			return cached, nil
		}
		b.edgeTypeCacheMu.RUnlock()
	}

	var sourceTs uint64
	var overlay []*Edge
	var touched map[EdgeID]bool
	var edges []*Edge
	nowNanos := DecayScoringTime()
	gen := b.edgeTypeCacheGen.current()
	var captureErr error
	err := b.withReadView(ctx, func(w *WriteBehindBuffer) {
		pendingEdges, err := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedType, edgeType: edgeType})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingEdges.edges, pendingEdges.edgesTouched
	}, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan and its cache
		}
		sourceTs = txn.ReadTs()
		prefix := edgeTypeIndexPrefix(edgeType)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		// Collect edge IDs from index. Keys now carry an 8-byte edge
		// numID in the suffix — resolve via the reverse dict.
		checkTombstones := b.checksIndexTombstones(v)
		var edgeIDs []EdgeID
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if checkTombstones && hasIndexTombstone(txn, key) {
				continue
			}
			edgeNum, ok := extractEdgeNumIDFromEdgeTypeKey(key)
			if !ok {
				continue
			}
			edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok {
				continue
			}
			edgeIDs = append(edgeIDs, edgeID)
		}

		// Batch fetch edges
		edges = make([]*Edge, 0, len(edgeIDs))
		for _, edgeID := range edgeIDs {
			item, err := txn.Get(edgeKey(edgeID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = b.decodeEdgeBodyWithID(txn, namespaceForEdgeID(edgeID), val, edgeID)
				return decodeErr
			}); err != nil {
				return err
			}

			if b.filterEntityByDecay(v, edge, nowNanos) {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if b.writeBehind != nil {
		edges = mergeWriteBehindEntities(edges, overlay, touched, func(e *Edge) EdgeID { return e.ID }, copyEdge)
		return edges, nil
	}

	if useCache {
		b.edgeTypeCacheFill(gen, edgeType, edges, sourceTs)
	}
	return edges, nil
}

// edgeTypeCacheFill caches the edges of edgeType a read captured gen before
// reading (simple LRU-style: clear if too many types), unless the type cache
// was written since (cacheGeneration).
func (b *BadgerEngine) edgeTypeCacheFill(gen uint64, edgeType string, edges []*Edge, source uint64) {
	b.durableCaches.mu.Lock()
	defer b.durableCaches.mu.Unlock()
	if source < max(b.durableCaches.all, b.durableCaches.types[edgeType]) {
		return
	}
	b.edgeTypeCacheMu.Lock()
	defer b.edgeTypeCacheMu.Unlock()
	if b.edgeTypeCacheGen.current() != gen {
		return
	}
	if b.edgeTypeCacheMaxTypes > 0 && len(b.edgeTypeCache) > b.edgeTypeCacheMaxTypes {
		b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	}
	b.edgeTypeCache[edgeType] = edges
}

// InvalidateEdgeTypeCache removes the selected types, or clears the entire
// type cache when called with no arguments. Explicit empty types are no-ops.
// For example, InvalidateEdgeTypeCache("KNOWS") invalidates only KNOWS.
func (b *BadgerEngine) InvalidateEdgeTypeCache(edgeTypes ...string) {
	if len(edgeTypes) > 0 {
		nonempty := false
		for _, edgeType := range edgeTypes {
			if edgeType != "" {
				nonempty = true
				break
			}
		}
		if !nonempty {
			return
		}
	}
	b.edgeTypeCacheGen.lockInvalidation(&b.edgeTypeCacheMu)
	if len(edgeTypes) == 0 {
		b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	} else {
		for _, edgeType := range edgeTypes {
			if edgeType != "" {
				delete(b.edgeTypeCache, edgeType)
			}
		}
	}
	b.edgeTypeCacheMu.Unlock()
}

// BatchGetNodes materializes a batch through one capture, with no overlay map
// or transaction when every requested body is cached.
func (b *BadgerEngine) BatchGetNodes(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	return b.readNodeBatch(ctx, ids, true)
}

func (b *BadgerEngine) BatchGetNodesWithoutEmbeddings(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	return b.readNodeBatch(ctx, ids, false)
}

func (b *BadgerEngine) readNodeBatch(ctx context.Context, ids []NodeID, embeddings bool) (map[NodeID]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var sourceTs uint64
	result := make(map[NodeID]*Node, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	copyForRead := copyNodeWithoutEmbeddings
	if embeddings {
		copyForRead = copyNode
	}
	now := DecayScoringTime()
	// Author: storage-format-owners lane (Codex), 1 November 2026.
	// One bounded poll follows the batch through selection and caller copying.
	var cancel bulkReadCancellation
	cached := func(w *WriteBehindBuffer) (bool, error) {
		clear(result)
		b.nodeCacheMu.RLock()
		defer b.nodeCacheMu.RUnlock()
		for _, id := range ids {
			if err := cancel.check(ctx); err != nil {
				return false, err
			}
			if id == "" {
				continue
			}
			var node *Node
			if w != nil {
				state := w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: visibleRead})
				if state.deleted {
					continue
				}
				node = state.node
			}
			if node == nil {
				node = b.nodeCache[id]
			}
			if node == nil {
				return false, nil
			}
			if !b.filterEntityByDecay(visibleRead, node, now) {
				result[id] = node
			}
		}
		return true, nil
	}
	var states map[NodeID]bufferedEntityState
	gen := b.nodeCacheGen.current()
	var loaded []*Node
	var current bool
	var captureErr error
	err := b.withReadView(ctx, func(w *WriteBehindBuffer) {
		for _, id := range ids {
			if err := cancel.check(ctx); err != nil {
				captureErr = err
				return
			}
			if state := w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: visibleRead}); state.found {
				if states == nil {
					states = make(map[NodeID]bufferedEntityState)
				}
				states[id] = state
			}
		}
	}, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr
		}
		sourceTs = txn.ReadTs()
		clear(result)
		loaded = nil
		for _, id := range ids {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			if id == "" {
				continue
			}
			state := states[id]
			if state.deleted {
				continue
			}
			node := state.node
			if !state.found && current {
				b.nodeCacheMu.RLock()
				node = b.nodeCache[id]
				b.nodeCacheMu.RUnlock()
			}
			if !state.found && node == nil {
				item, err := txn.Get(nodeKey(id))
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				hydrate := nodeItemBodyOnly
				if embeddings {
					hydrate = nodeItemFullEmbeddings
				}
				node, err = b.readNodeItemInTxn(txn, item, id, hydrate)
				if err != nil {
					return err
				}
				if embeddings {
					loaded = append(loaded, node)
				}
			}
			if !b.filterEntityByDecay(visibleRead, node, now) {
				result[id] = node
			}
		}
		return nil
	}, readViewOptions{scopeKind: readNodeIDsScope, nodeIDs: ids, cached: cached, current: &current})
	if err != nil {
		return nil, err
	}
	// Selection completes at the captured view. Copy its immutable bodies
	// afterwards, once, outside the buffer and cache locks.
	for id, node := range result {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		result[id] = copyForRead(node)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, node := range loaded {
		b.cacheFillNode(gen, node, nodeReadCacheFull, sourceTs)
	}
	return result, nil
}

// HasLabelBatch checks label membership for a batch of node IDs using the label index.
// This avoids decoding node records and is significantly faster for large batches.
func (b *BadgerEngine) HasLabelBatch(ids []NodeID, label string) (map[NodeID]bool, error) {
	// This context-free owner has no request cancellation; its read capture
	// shares the synchronous operation's lifetime.

	if len(ids) == 0 {
		return make(map[NodeID]bool), nil
	}

	var states map[NodeID]bufferedEntityState
	result := make(map[NodeID]bool, len(ids))
	err := b.withReadView(context.Background(), func(w *WriteBehindBuffer) {
		for _, id := range ids {
			if state := w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead}); state.found {
				if states == nil {
					states = make(map[NodeID]bufferedEntityState)
				}
				states[id] = state
			}
		}
	}, func(txn *badger.Txn) error {
		for _, id := range ids {
			if state, found := states[id]; found {
				if !state.deleted && hasLabel(state.node.Labels, label) {
					result[id] = true
				}
				continue
			}
			if id == "" {
				continue
			}
			key := b.labelIndexKeyStringLookup(label, id)
			if key == nil {
				continue
			}
			_, err := txn.Get(key)
			if err == nil {
				result[id] = true
				continue
			}
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return err
		}
		return nil
	}, readViewOptions{scopeKind: readNodeIDsScope, nodeIDs: ids})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetOutgoingEdges returns all edges where the given node is the source.
func (b *BadgerEngine) GetOutgoingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	out, _, err := readAdjacency(ctx, b, adjacencyOutgoing, nodeID, "", nil, visibleRead)
	return out, err
}

// GetStoredOutgoingEdges includes hidden bodies through the shared read view.
func (b *BadgerEngine) GetStoredOutgoingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	out, _, err := readAdjacency(ctx, b, adjacencyOutgoing, nodeID, "", nil, storedRead)
	return out, err
}

// GetAdjacentEdges fetches both outgoing and incoming edges for nodeID. On a
// hot path the per-node adjacency cache short-circuits the Badger iterator
// entirely, falling back to a single view transaction when either direction
// misses.
func (b *BadgerEngine) GetAdjacentEdges(ctx context.Context, nodeID NodeID) ([]*Edge, []*Edge, error) {
	return readAdjacency(ctx, b, adjacencyBoth, nodeID, "", nil, visibleRead)
}

// materializeAdjEdges resolves a list of EdgeIDs to live *Edge bodies by
// hitting the edge body cache first, then falling back to a one-shot view
// transaction for any IDs that miss. It is the body-materialization kernel of
// the adjacency-cache fast path, so it visits the IDs under the caller's own
// context: a canceled read retires before it decodes the rest, returns the
// caller's error and no edges, and never fills the edge cache afterwards.
func (b *BadgerEngine) materializeAdjEdges(ctx context.Context, ids []EdgeID, v readVisibility) ([]*Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nowNanos := DecayScoringTime()
	out := make([]*Edge, 0, len(ids))
	var miss []EdgeID
	var cancel bulkReadCancellation
	for _, id := range ids {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		if cached, ok := b.cacheLoadEdge(id); ok {
			if b.filterEntityByDecay(v, cached, nowNanos) {
				continue
			}
			out = append(out, cached)
			continue
		}
		miss = append(miss, id)
	}
	if len(miss) == 0 {
		return out, nil
	}
	gen := b.edgeCacheGen.current()
	err := b.withView(func(txn *badger.Txn) error {
		for _, id := range miss {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			item, err := txn.Get(edgeKey(id))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = b.decodeEdgeBodyWithID(txn, namespaceForEdgeID(id), val, id)
				return decodeErr
			}); err != nil {
				return err
			}
			b.cacheFillEdgeAtVersion(gen, edge, item.Version(), txn.ReadTs())
			if b.filterEntityByDecay(v, edge, nowNanos) {
				continue
			}
			out = append(out, edge)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// collectEdgesByIndexPrefix iterates the outgoing/incoming edge index under
// prefix. Returns the live (non-decayed) edges AND the ordered EdgeID list
// the caller should hand to the adjacency cache. The ID list is the
// authoritative cache value — using it on subsequent reads lets us skip the
// Badger iterator entirely.
//
// Edge bodies are looked up in the per-engine edge cache before falling
// back to a Badger Txn.Get. The cache turns BFS-style traversals (which
// revisit a small set of edges thousands of times per request) into
// memory-bound work after the first encounter. edgeGen is the edge cache's
// generation captured before txn was opened (cacheGeneration).
func (b *BadgerEngine) collectEdgesByIndexPrefix(txn *badger.Txn, prefix []byte, nowNanos int64, edgeGen uint64, v readVisibility) ([]*Edge, []EdgeID, error) {
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()

	var edges []*Edge
	var ids []EdgeID
	for it.Rewind(); it.Valid(); it.Next() {
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok {
			continue
		}
		ids = append(ids, edgeID)

		edge, err := b.readIndexedEdgeInTxn(txn, edgeID, edgeGen)
		if err != nil {
			return nil, nil, err
		}
		if edge == nil || b.filterEntityByDecay(v, edge, nowNanos) {
			continue
		}
		edges = append(edges, edge)
	}
	return edges, ids, nil
}

// OutgoingEdgeHeaders implements EdgeHeaderReader from the outgoing adjacency
// entries' values, each a copy of the relationship record's compact header
// (adjacencyValueFromRecord). An entry without one, such as one written
// before the values existed, is answered from the relationship record.
func (b *BadgerEngine) OutgoingEdgeHeaders(ctx context.Context, nodeID NodeID) ([]*Edge, bool, error) {
	return b.edgeHeaders(ctx, nodeID, true)
}

// IncomingEdgeHeaders is the incoming-side OutgoingEdgeHeaders.
func (b *BadgerEngine) IncomingEdgeHeaders(ctx context.Context, nodeID NodeID) ([]*Edge, bool, error) {
	return b.edgeHeaders(ctx, nodeID, false)
}

func (b *BadgerEngine) edgeHeaders(ctx context.Context, nodeID NodeID, outgoing bool) ([]*Edge, bool, error) {
	// ctx is the caller's read context: it owns admission and the read capture,
	// so a canceled traversal never answers with a header set.
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if err := b.pendingNamespaceConflict(namespaceForNodeID(nodeID)); err != nil {
		return nil, true, err
	}

	if nodeID == "" {
		return nil, true, ErrInvalidID
	}
	if b.visibilityFiltered() {
		return nil, false, nil
	}
	if b.writeBehind != nil {
		// Fold the write-behind overlay into the header list so a node's
		// unnamed-relationship expansion sees acknowledged-but-unflushed edges
		// (PR #1043 read-overlay parity; monster retest part 1).
		var live []*Edge
		var suppressed map[EdgeID]struct{}
		var nodeDeleted bool
		var captureErr error
		view, err := b.captureReadView(ctx, func(w *WriteBehindBuffer) {
			// Header reads keep their existing public API (no caller context):
			// context.Background is the true lifetime at this context-free
			// boundary.
			pendingAdjacency, err := b.readBufferedEntities(ctx, w, visibleRead, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: outgoing})
			if err != nil {
				captureErr = err
				return
			}
			live, suppressed, nodeDeleted = pendingAdjacency.edges, pendingAdjacency.suppressed, pendingAdjacency.sourceDeleted
		}, readViewOptions{scope: namespaceForNodeID(nodeID)})
		if err != nil {
			return nil, true, err
		}
		if captureErr != nil {
			view.Close()
			return nil, true, captureErr
		}
		defer view.Close()
		if nodeDeleted {
			return nil, true, nil
		}
		committed, answered, err := b.edgeHeadersCommitted(ctx, nodeID, outgoing, view.withView)
		if err != nil || !answered {
			return committed, answered, err
		}
		if len(suppressed) > 0 {
			filtered := committed[:0]
			for _, e := range committed {
				if _, dead := suppressed[e.ID]; dead {
					continue
				}
				filtered = append(filtered, e)
			}
			committed = filtered
		}
		for _, e := range live {
			copyEdge := *e
			committed = append(committed, &copyEdge)
		}
		return committed, true, nil
	}
	return b.edgeHeadersCommitted(ctx, nodeID, outgoing)
}

func (b *BadgerEngine) edgeHeadersCommitted(ctx context.Context, nodeID NodeID, outgoing bool, views ...func(context.Context, func(*badger.Txn) error) error) ([]*Edge, bool, error) {
	if nodeID == "" {
		return nil, true, ErrInvalidID
	}
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil, true, nil
	}
	prefix := incomingIndexPrefix(nodeNum)
	if outgoing {
		prefix = outgoingIndexPrefix(nodeNum)
	}
	var edges []*Edge
	edgeGen := b.edgeCacheGen.current()
	read := func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			edgeNum, ok := extractEdgeNumIDFromOutgoingKey(item.Key())
			if !ok {
				continue
			}
			edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok {
				continue
			}
			// A value that can't be read is treated as absent: the record
			// answers instead.
			var header *Edge
			_ = item.Value(func(value []byte) error {
				header = b.decodeAdjacencyHeader(edgeID, value)
				return nil
			})
			if header != nil {
				edges = append(edges, header)
				continue
			}
			edge, err := b.readIndexedEdgeInTxn(txn, edgeID, edgeGen)
			if err != nil {
				return err
			}
			if edge != nil {
				edges = append(edges, edge)
			}
		}
		return nil
	}
	var err error
	if len(views) > 0 {
		err = views[0](ctx, read)
	} else {
		err = b.withView(read)
	}
	return edges, true, err
}

// decodeAdjacencyHeader returns the relationship, without properties, that an
// adjacency entry value (adjacencyValueFromRecord) describes, or nil when the
// value is empty, can't be decoded, or names an endpoint the ID dictionary
// doesn't know.
func (b *BadgerEngine) decodeAdjacencyHeader(edgeID EdgeID, value []byte) *Edge {
	edge := new(Edge)
	startNum, endNum, _, err := decodeEdgeCompactHeaderInto(value, edgeFormatCompactV2, edge)
	if err != nil {
		return nil
	}
	start, startKnown := b.idDict.lookupNodeIDByNum(startNum)
	end, endKnown := b.idDict.lookupNodeIDByNum(endNum)
	if !startKnown || !endKnown {
		return nil
	}
	edge.ID, edge.StartNode, edge.EndNode = edgeID, start, end
	return edge
}

// adjacencyHeadersInTxn reads nodeID's adjacency entries in direction from
// txn and returns the relationships their values describe
// (decodeAdjacencyHeader), keyed by ID. Entries without a usable value, or
// naming a relationship the dictionary doesn't know, are left out for the
// caller to read.
func (b *BadgerEngine) adjacencyHeadersInTxn(txn *badger.Txn, nodeID NodeID, direction EdgeDirection) map[EdgeID]*Edge {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	prefix := outgoingIndexPrefix(nodeNum)
	if direction == Incoming {
		prefix = incomingIndexPrefix(nodeNum)
	}
	headers := make(map[EdgeID]*Edge)
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(item.Key())
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok {
			continue
		}
		var header *Edge
		_ = item.Value(func(value []byte) error {
			header = b.decodeAdjacencyHeader(edgeID, value)
			return nil
		})
		if header != nil {
			headers[edgeID] = header
		}
	}
	return headers
}

// readIndexedEdgeInTxn returns the relationship an adjacency entry names: the
// cached one, else its stored record, which is then cached unless the edge
// cache was written since gen, captured before txn was opened. A missing
// record returns nil; other read or decode errors are returned.
func (b *BadgerEngine) readIndexedEdgeInTxn(txn *badger.Txn, edgeID EdgeID, gen uint64) (*Edge, error) {
	if cached, ok := b.cacheLoadEdgeForRead(edgeID, txn.ReadTs()); ok && b.writeBehind == nil {
		return cached, nil
	}
	item, err := txn.Get(edgeKey(edgeID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if cached, ok := b.cacheLoadEdgeAtVersion(edgeID, item.Version()); ok {
		return cached, nil
	}
	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyWithID(txn, namespaceForEdgeID(edgeID), val, edgeID)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	b.cacheFillEdgeAtVersion(gen, edge, item.Version(), txn.ReadTs())
	return edge, nil
}

// GetIncomingEdges returns all edges where the given node is the target.
func (b *BadgerEngine) GetIncomingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	_, in, err := readAdjacency(ctx, b, adjacencyIncoming, nodeID, "", nil, visibleRead)
	return in, err
}

// GetStoredIncomingEdges includes hidden bodies through the shared read view.
func (b *BadgerEngine) GetStoredIncomingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	_, in, err := readAdjacency(ctx, b, adjacencyIncoming, nodeID, "", nil, storedRead)
	return in, err
}

// GetEdgesBetween returns all edges between two nodes.
func (b *BadgerEngine) GetEdgesBetween(ctx context.Context, startID, endID NodeID) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.pendingNamespaceConflict(namespaceForNodeID(startID)); err != nil {
		return nil, err
	}

	if err := b.pendingNamespaceConflict(namespaceForNodeID(endID)); err != nil {
		return nil, err
	}

	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}

	var result []*Edge
	if b.writeBehind != nil {
		// A buffered endpoint delete hides all committed between rows; a
		// buffered delete of a between edge hides that row; buffered live
		// edges between the pair are appended (PR #1043 read-overlay
		// parity, MERGE identity reads).
		var live []*Edge
		var suppressed map[EdgeID]struct{}
		var sourceDeleted, targetDeleted bool
		var captureErr error
		view, err := b.captureReadView(ctx, func(w *WriteBehindBuffer) {
			pendingBetween, err := b.readBufferedEntities(ctx, w, visibleRead, bufferedEntityRead{kind: bufferedBetween, source: startID, target: endID})
			if err != nil {
				captureErr = err
				return
			}
			live, suppressed, sourceDeleted, targetDeleted = pendingBetween.edges, pendingBetween.suppressed, pendingBetween.sourceDeleted, pendingBetween.targetDeleted
		}, readViewOptions{scope: namespaceForNodeID(startID)})
		if err != nil {
			return nil, err
		}
		if captureErr != nil {
			view.Close()
			return nil, captureErr
		}
		defer view.Close()
		if sourceDeleted || targetDeleted {
			return nil, nil
		}
		committed, err := b.committedEdgesBetween(ctx, startID, endID, "", view.withView)
		if err != nil {
			return nil, err
		}
		var cancel bulkReadCancellation
		if len(suppressed) > 0 {
			for _, e := range committed {
				if err := cancel.check(ctx); err != nil {
					return nil, err
				}
				if _, dead := suppressed[e.ID]; dead {
					continue
				}
				result = append(result, e)
			}
		} else {
			result = committed
		}
		for _, e := range live {
			if err := cancel.check(ctx); err != nil {
				return nil, err
			}
			copyEdge := *e
			result = append(result, &copyEdge)
		}
		return result, nil
	}
	return b.committedEdgesBetween(ctx, startID, endID, "")
}

// committedEdgesBetween is the committed between-index read, set index first
// with the legacy outgoing fallback and self-heal.
func (b *BadgerEngine) committedEdgesBetween(ctx context.Context, startID, endID NodeID, edgeType string, views ...func(context.Context, func(*badger.Txn) error) error) ([]*Edge, error) {
	result, err := b.edgesBetweenFromSetIndex(ctx, startID, endID, edgeType, views...)
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		return result, nil
	}

	result, err = b.edgesBetweenFromLegacyOutgoingIndex(ctx, startID, endID, edgeType, views...)
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(ctx, result)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// GetEdgeBetween returns an edge between two nodes with the given type. A
// missing relationship is (nil, nil); the caller's context owns admission and
// the point read's materialization.
func (b *BadgerEngine) GetEdgeBetween(ctx context.Context, source, target NodeID, edgeType string) (*Edge, error) {
	// The caller's request owns this read: a cancelled or expired request
	// resolves no relationship, whichever index answers it.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.pendingNamespaceConflict(namespaceForNodeID(source)); err != nil {
		return nil, err
	}

	if err := b.pendingNamespaceConflict(namespaceForNodeID(target)); err != nil {
		return nil, err
	}

	if source == "" || target == "" {
		return nil, nil
	}
	if b.writeBehind == nil {
		return b.committedEdgeBetween(ctx, source, target, edgeType)
	}
	// A buffered endpoint delete hides the committed row; a buffered delete
	// of the edge hides it; a buffered live edge of the type wins.
	var live []*Edge
	var suppressed map[EdgeID]struct{}
	var sourceDeleted, targetDeleted bool
	var captureErr error
	view, err := b.captureReadView(ctx, func(w *WriteBehindBuffer) {
		pendingBetween, err := b.readBufferedEntities(ctx, w, visibleRead, bufferedEntityRead{kind: bufferedBetween, source: source, target: target})
		if err != nil {
			captureErr = err
			return
		}
		live, suppressed, sourceDeleted, targetDeleted = pendingBetween.edges, pendingBetween.suppressed, pendingBetween.sourceDeleted, pendingBetween.targetDeleted
	}, readViewOptions{scope: namespaceForNodeID(source)})
	if err != nil {
		return nil, err
	}
	if captureErr != nil {
		view.Close()
		return nil, captureErr
	}
	defer view.Close()
	if sourceDeleted || targetDeleted {
		return nil, nil
	}
	committed, err := b.committedEdgeBetween(ctx, source, target, edgeType, view.withView)
	if err != nil {
		return nil, err
	}
	if committed != nil {
		if _, dead := suppressed[committed.ID]; !dead {
			return committed, nil
		}
	}
	for _, e := range live {
		if edgeType != "" && e.Type != edgeType {
			continue
		}
		copyEdge := *e
		return &copyEdge, nil
	}
	return nil, nil
}

// committedEdgeBetween is the committed between-index read for one edge. The
// fallback scans keep their existing order; a lookup failure is reported only
// when no lookup answers, so a canceled or closed read is never mistaken for a
// missing relationship.
func (b *BadgerEngine) committedEdgeBetween(ctx context.Context, source, target NodeID, edgeType string, views ...func(context.Context, func(*badger.Txn) error) error) (*Edge, error) {
	var firstErr error
	if edgeType != "" {
		edge, err := b.edgeBetweenFromHeadIndex(ctx, source, target, edgeType, views...)
		if err != nil {
			b.log.Warn("edge-between head lookup failed; falling back to set/legacy scan",
				"subsystem", "edge_between_index",
			)
			firstErr = err
		}
		if edge != nil {
			return edge, nil
		}
	}

	indexed, err := b.edgesBetweenFromSetIndex(ctx, source, target, edgeType, views...)
	if err == nil && len(indexed) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(ctx, indexed[:1])
		return indexed[0], nil
	}
	if err != nil && firstErr == nil {
		firstErr = err
	}

	legacy, err := b.edgesBetweenFromLegacyOutgoingIndex(ctx, source, target, edgeType, views...)
	if err == nil && len(legacy) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(ctx, legacy)
		return legacy[0], nil
	}
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return nil, firstErr
}

// edgeBetweenFromHeadIndex loads the typed common-case lookup without scanning.
func (b *BadgerEngine) edgeBetweenFromHeadIndex(ctx context.Context, source, target NodeID, edgeType string, views ...func(context.Context, func(*badger.Txn) error) error) (*Edge, error) {
	sourceNum, sourceOK := b.idDict.lookupNodeNumID(source)
	targetNum, targetOK := b.idDict.lookupNodeNumID(target)
	if !sourceOK || !targetOK {
		return nil, nil // no numID ever allocated — no index entry can exist
	}
	var result *Edge
	read := func(txn *badger.Txn) error {
		headKey := edgeBetweenHeadKey(sourceNum, targetNum, edgeType)
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		if checkTombstones && hasIndexTombstone(txn, headKey) {
			return nil
		}
		item, err := txn.Get(headKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var edgeID EdgeID
		if err := item.Value(func(val []byte) error {
			edgeID = EdgeID(append([]byte(nil), val...))
			return nil
		}); err != nil {
			return err
		}
		edgeNum, edgeOK := b.idDict.lookupEdgeNumID(edgeID)
		if edgeOK && checkTombstones && hasIndexTombstone(txn, edgeBetweenIndexKey(sourceNum, targetNum, edgeType, edgeNum)) {
			return nil
		}
		edge, err := b.edgeFromTxn(txn, edgeID)
		if err != nil {
			return fmt.Errorf("load edge-between head edge %q: %w", edgeID, err)
		}
		if b.filterEntityByDecay(visibleRead, edge, DecayScoringTime()) {
			return nil
		}
		if edgeMatchesBetween(edge, source, target, edgeType) {
			result = edge
		}
		return nil
	}
	var err error
	if len(views) > 0 {
		err = views[0](ctx, read)
	} else {
		err = b.withView(read)
	}
	return result, err
}

// edgesBetweenFromSetIndex scans the exact relationship set index.
// Keys use 8-byte numeric IDs so we resolve start/end via the id dict
// before scanning. Each entry's VALUE carries the string edge ID, which
// saves a reverse-dict lookup for the scan payload.
func (b *BadgerEngine) edgesBetweenFromSetIndex(ctx context.Context, startID, endID NodeID, edgeType string, views ...func(context.Context, func(*badger.Txn) error) error) ([]*Edge, error) {
	startNum, sOK := b.idDict.lookupNodeNumID(startID)
	endNum, eOK := b.idDict.lookupNodeNumID(endID)
	if !sOK || !eOK {
		return nil, nil
	}
	var result []*Edge
	read := func(txn *badger.Txn) error {
		prefix := edgeBetweenIndexPrefix(startNum, endNum)
		if edgeType != "" {
			prefix = typedEdgeBetweenIndexPrefix(startNum, endNum, edgeType)
		}
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		nowNanos := DecayScoringTime()
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		var cancel bulkReadCancellation
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			indexKey := it.Item().Key()
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}
			var edgeID EdgeID
			if err := it.Item().Value(func(val []byte) error {
				edgeID = EdgeID(append([]byte(nil), val...))
				return nil
			}); err != nil {
				return err
			}
			if edgeID == "" {
				continue
			}
			edge, err := b.edgeFromTxn(txn, edgeID)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if !edgeMatchesBetween(edge, startID, endID, edgeType) || b.filterEntityByDecay(visibleRead, edge, nowNanos) {
				continue
			}
			result = append(result, edge)
		}
		return nil
	}
	var err error
	if len(views) > 0 {
		err = views[0](ctx, read)
	} else {
		err = b.withView(read)
	}
	return result, err
}

// edgesBetweenFromLegacyOutgoingIndex finds the edges from startID to endID
// through the adjacency indexes, for pairs the edge-between index doesn't list
// (stores that predate it, or a drifted ready marker). Every such edge is in
// both the start node's outgoing and the end node's incoming adjacency, so the
// two are walked in lockstep and the one exhausted first holds every candidate:
// the cost is the smaller endpoint degree, not the start node's. A MERGE of a
// new relationship from a hub with thousands of edges to a fresh node misses
// the between index every time, and walking the hub's outgoing edges made a
// bulk load quadratic in the hub's degree.
func (b *BadgerEngine) edgesBetweenFromLegacyOutgoingIndex(ctx context.Context, startID, endID NodeID, edgeType string, views ...func(context.Context, func(*badger.Txn) error) error) ([]*Edge, error) {
	outPrefix := b.outgoingIndexPrefixString(startID)
	inPrefix := b.incomingIndexPrefixString(endID)
	if outPrefix == nil || inPrefix == nil {
		return nil, nil
	}
	var result []*Edge
	read := func(txn *badger.Txn) error {
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		nowNanos := DecayScoringTime()
		outIt := txn.NewIterator(badgerPrefixIteratorOptions(outPrefix))
		defer outIt.Close()
		inIt := txn.NewIterator(badgerPrefixIteratorOptions(inPrefix))
		defer inIt.Close()
		var outKeys, inKeys [][]byte
		outIt.Rewind()
		inIt.Rewind()
		var candidates [][]byte
		var walkCancel bulkReadCancellation
		for candidates == nil {
			if err := walkCancel.check(ctx); err != nil {
				return err
			}
			if !outIt.ValidForPrefix(outPrefix) {
				candidates = outKeys
				break
			}
			if !inIt.ValidForPrefix(inPrefix) {
				candidates = inKeys
				break
			}
			outKeys = append(outKeys, outIt.Item().KeyCopy(nil))
			inKeys = append(inKeys, inIt.Item().KeyCopy(nil))
			outIt.Next()
			inIt.Next()
		}
		var cancel bulkReadCancellation
		for _, indexKey := range candidates {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}
			edgeNum, ok := extractEdgeNumIDFromOutgoingKey(indexKey)
			if !ok {
				continue
			}
			edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok {
				continue
			}
			edge, err := b.edgeFromTxn(txn, edgeID)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if !edgeMatchesBetween(edge, startID, endID, edgeType) || b.filterEntityByDecay(visibleRead, edge, nowNanos) {
				continue
			}
			result = append(result, edge)
		}
		return nil
	}
	var err error
	if len(views) > 0 {
		err = views[0](ctx, read)
	} else {
		err = b.withView(read)
	}
	return result, err
}

// selfHealEdgeBetweenIndexes repairs lookup indexes discovered through a
// fallback read, preventing ready-marker drift from becoming a correctness bug.
// betweenSelfHealHook lets storage tests interpose between a fallback read
// and the self-heal it starts; production leaves it nil.
var (
	betweenSelfHealHook   func()
	betweenSelfHealHookMu sync.RWMutex
)

func setBetweenSelfHealHook(hook func()) (restore func()) {
	betweenSelfHealHookMu.Lock()
	previous := betweenSelfHealHook
	betweenSelfHealHook = hook
	betweenSelfHealHookMu.Unlock()
	return func() {
		betweenSelfHealHookMu.Lock()
		betweenSelfHealHook = previous
		betweenSelfHealHookMu.Unlock()
	}
}

// Author: storage-format-owners lane (Codex), 1 November 2026.
// The synchronous repair belongs to its discovering read, through write admission.
func (b *BadgerEngine) selfHealEdgeBetweenIndexes(ctx context.Context, edges []*Edge) error {
	if len(edges) > edgeBetweenSelfHealMaxEdges {
		edges = edges[:edgeBetweenSelfHealMaxEdges]
	}
	betweenSelfHealHookMu.RLock()
	hook := betweenSelfHealHook
	betweenSelfHealHookMu.RUnlock()
	if hook != nil {
		hook()
	}
	// The fallback read ran before: a relationship may have been deleted or
	// moved since. The heal is ordered after the relationships' acknowledged
	// write-behind commits and writes only what is stored now, read again in
	// its own transaction.
	var e writtenEntities
	for _, edge := range edges {
		e.edge(edge)
	}
	err := b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			current, err := b.edgeFromTxn(txn, edge.ID)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := b.writeEdgeBetweenIndexesInTxn(txn, current); err != nil {
				return err
			}
		}
		return nil
	}, e)
	if errors.Is(err, badger.ErrConflict) {
		b.log.Debug("edge-between index self-heal skipped after badger conflict",
			"subsystem", "edge_between_index",
		)
		return nil
	}
	return err
}

// edgeMatchesBetween rejects stale secondary-index entries before returning an
// edge from either the head, set, or legacy fallback path.
func edgeMatchesBetween(edge *Edge, startID, endID NodeID, edgeType string) bool {
	if edge == nil || edge.StartNode != startID || edge.EndNode != endID {
		return false
	}
	return edgeType == "" || edge.Type == edgeType
}

// edgeFromTxn loads an edge record while callers iterate a secondary index.
// Now a method on BadgerEngine so it can dispatch to the compact-aware
// decoder via b.decodeEdgeBody.
func (b *BadgerEngine) edgeFromTxn(txn *badger.Txn, edgeID EdgeID) (*Edge, error) {
	item, err := txn.Get(edgeKey(edgeID))
	if err != nil {
		return nil, err
	}
	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyWithID(txn, namespaceForEdgeID(edgeID), val, edgeID)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	return edge, nil
}

// ============================================================================
