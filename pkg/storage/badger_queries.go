// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const edgeBetweenSelfHealMaxEdges = 64

// Query Operations
// ============================================================================

// GetFirstNodeByLabel returns the first node with the specified label.
// This is optimized for MATCH...LIMIT 1 patterns - stops after first match.
func (b *BadgerEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	return b.GetFirstNodeByLabelInScope("", label)
}

// GetFirstNodeByLabelInScope is GetFirstNodeByLabel within one database
// (ScopedLabelNodeReader).
func (b *BadgerEngine) GetFirstNodeByLabelInScope(scope, label string) (*Node, error) {
	var node *Node
	if b.writeBehind != nil {
		// Share the overlay's shadowing and current-label decisions with the
		// ordinary stream, while stopping after its first visible node.
		err := b.StreamNodesByLabelProjectedInScope(scope, label, nil, func(first *Node) error {
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
		it := b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), "")
		defer it.Close()

		for it.Next() {
			indexKey := it.Item().KeyCopy(nil)
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
			if b.filterNodeByDecay(node, nowNanos) {
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
func (b *BadgerEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	return b.ForEachNodeIDByLabelInScope("", label, visit)
}

// ForEachNodeIDByLabelInScope streams only this database's label IDs. Empty
// scope preserves the unscoped interface's behavior.
func (b *BadgerEngine) ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if b.writeBehind != nil {
		err := b.StreamNodesByLabelProjectedInScope(scope, label, []string{}, func(node *Node) error {
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
		if cachedOK && cachedID != "" {
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

		it := b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), "")
		defer it.Close()

		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		for it.Next() {
			indexKey := it.Item().KeyCopy(nil)
			nodeID := it.NodeID()
			if cachedValid && nodeID == cachedID {
				continue
			}
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}
			if !cachedValid {
				if scope == "" {
					b.labelCacheSetFirst(labelGen, label, nodeID)
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
func (b *BadgerEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return b.GetNodesByLabelInScope("", label)
}

// GetNodesByLabelInScope is GetNodesByLabel within one database
// (ScopedLabelNodeReader).
func (b *BadgerEngine) GetNodesByLabelInScope(scope, label string) ([]*Node, error) {
	return b.getNodesByLabelInScope(scope, label, visibleRead)
}

// GetStoredNodesByLabel is GetNodesByLabel for integrity work: it also
// returns the nodes the visibility layer hides (StoredEntityReader).
func (b *BadgerEngine) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return b.getNodesByLabelInScope("", label, storedRead)
}

func (b *BadgerEngine) getNodesByLabelInScope(scope, label string, v readVisibility) ([]*Node, error) {
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
	err := b.withReadView(func(w *WriteBehindBuffer) { overlay, touched = w.labelOverlayLocked(label) }, func(txn *badger.Txn) error {
		nodes = nil
		loaded = nil
		it := b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), "")
		defer it.Close()

		for it.Next() {
			indexKey := it.Item().KeyCopy(nil)
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
				if b.hidesNode(v, cn, nowNanos) {
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
			node, readErr = b.readNodeItemInTxn(txn, item, nodeID, b.loadNodeEmbeddings)
			if readErr != nil {
				return readErr
			}

			if b.hidesNode(v, node, nowNanos) {
				continue
			}

			loaded = append(loaded, node)
			nodes = append(nodes, node)
		}
		return it.Err()
	}, readViewOptions{scope: scope, current: &current})

	if err != nil {
		return nil, err
	}

	// Cache loaded nodes for future label scans.
	for _, n := range loaded {
		if n != nil {
			b.cacheFillNode(gen, n)
		}
	}

	if len(touched) != 0 {
		nodes = b.mergeWriteBehindNodes(nodes, overlay, touched, scope, nil, v)
	}
	return nodes, nil
}

// StreamNodesByLabelProjected iterates label-matching nodes in a single read
// transaction while decoding only the requested user properties.
func (b *BadgerEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return b.StreamNodesByLabelProjectedInScope("", label, properties, visit)
}

// StreamNodesByLabelProjectedInScope is StreamNodesByLabelProjected within
// one database (ScopedLabelNodeReader).
//
// Nodes are visited in label-index order. Scans that continue past
// labelScanPointLookups switch to one pass over the node records when the
// label is on at least half of the scope's nodes.
func (b *BadgerEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScope(scope, label, properties, visibleRead, visit)
}

// StreamStoredNodesByLabelProjected is StreamNodesByLabelProjected for
// integrity work: it also visits the nodes the visibility layer hides
// (StoredEntityReader).
func (b *BadgerEngine) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScope("", label, properties, storedRead, visit)
}

func (b *BadgerEngine) streamNodesByLabelProjectedInScope(scope, label string, properties []string, v readVisibility, visit func(*Node) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Write-behind overlay: acknowledged-but-unflushed writes replace or
	// shadow committed rows for their IDs and are visited after the scan.
	var overlay []*Node
	var touched map[NodeID]bool
	capture := func(w *WriteBehindBuffer) {
		overlay, touched = w.labelOverlayLocked(label)
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
	err := b.withReadView(capture, func(txn *badger.Txn) error {
		readItem := func(nodeID NodeID, item *badger.Item) (*Node, error) {
			return b.readProjectedNodeItemInTxn(txn, item, nodeID, properties, include)
		}
		cachedNode := func(nodeID NodeID) (*Node, bool) {
			b.nodeCacheMu.RLock()
			cached, ok := b.nodeCache[nodeID]
			b.nodeCacheMu.RUnlock()
			if !ok || b.writeBehind != nil {
				return nil, false
			}
			return projectCachedNodeForRead(cached, properties, false), true
		}
		emit := func(node *Node) error {
			if node == nil || b.hidesNode(v, node, nowNanos) {
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
		it := b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), "")
		defer it.Close()
		lookups := 0
		for it.Next() {
			indexKey := it.Item().KeyCopy(nil)
			nodeID := it.NodeID()
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}

			if !coversDecided && lookups >= labelScanPointLookups {
				covers = b.labelCoversScope(scope, label)
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
		if err := readNodeRecordsInOnePass(txn, scope, ids, func(nodeID NodeID, item *badger.Item) error {
			index := positions[nodeID]
			pending[index].node, pending[index].err = readItem(nodeID, item)
			// Report record errors in visit order, so an early stop does not
			// surface corruption in a record the caller never reaches.
			return nil
		}); err != nil {
			return err
		}
		for _, entry := range pending {
			if entry.err != nil {
				return entry.err
			}
			if err := emit(entry.node); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, n := range overlay {
		if !nodeIDInScope(n.ID, scope) || b.hidesNode(v, n, nowNanos) {
			continue
		}
		if err := visit(projectCachedNodeForRead(n, properties, false)); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// mergeWriteBehindNodes folds the write-behind overlay for label over
// committed rows: rows whose IDs the buffer touched are dropped and the
// buffered versions (copied, or projected for the caller) take their place.
func (b *BadgerEngine) mergeWriteBehindNodes(nodes, overlay []*Node, touched map[NodeID]bool, scope string, projected []string, v readVisibility) []*Node {
	if len(touched) == 0 {
		return nodes
	}
	nowNanos := DecayScoringTime()
	out := nodes[:0]
	for _, n := range nodes {
		if touched[n.ID] {
			continue
		}
		out = append(out, n)
	}
	for _, n := range overlay {
		if !nodeIDInScope(n.ID, scope) || b.hidesNode(v, n, nowNanos) {
			continue
		}
		out = append(out, projectCachedNodeForRead(n, projected, false))
	}
	return out
}

func projectCachedNodeForRead(node *Node, properties []string, stripEmbeddings bool) *Node {
	if node == nil {
		return nil
	}
	if properties == nil {
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
			projected.Properties[property] = value
		}
	}
	return projected
}

// GetAllNodes returns all nodes in the storage.
func (b *BadgerEngine) GetAllNodes() []*Node {
	nodes, _ := b.AllNodes()
	return nodes
}

// AllNodes returns all nodes (implements Engine interface).
func (b *BadgerEngine) AllNodes() ([]*Node, error) {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	var current bool
	var nodes []*Node
	var loaded []*Node
	var overlay []*Node
	var touched map[NodeID]bool
	capture := func(w *WriteBehindBuffer) {
		overlay, touched = w.allNodesOverlayLocked()
	}
	nowNanos := DecayScoringTime()
	gen := b.nodeCacheGen.current()
	err := b.withReadView(capture, func(txn *badger.Txn) error {
		nodes = nil
		loaded = nil
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
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
				if b.filterNodeByDecay(nodeCopy, nowNanos) {
					continue
				}
				nodes = append(nodes, nodeCopy)
				continue
			}
			b.nodeCacheMu.RUnlock()

			var node *Node
			var readErr error
			node, readErr = b.readNodeItemInTxn(txn, it.Item(), nodeID, b.loadNodeEmbeddings)
			if readErr != nil {
				return readErr
			}

			if b.filterNodeByDecay(node, nowNanos) {
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
			b.cacheFillNode(gen, node)
		}
	}
	for _, node := range overlay {
		if b.filterNodeByDecay(node, nowNanos) {
			continue
		}
		nodes = append(nodes, copyNode(node))
	}
	return nodes, err
}

// AllEdges returns all edges (implements Engine interface).
func (b *BadgerEngine) AllEdges() ([]*Edge, error) {
	return b.allEdges(visibleRead)
}

func (b *BadgerEngine) allEdges(v readVisibility) ([]*Edge, error) {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	var edges []*Edge
	var overlay []*Edge
	var touched map[EdgeID]bool
	capture := func(w *WriteBehindBuffer) {
		overlay, touched = w.allEdgesOverlayLocked()
	}
	nowNanos := DecayScoringTime()
	err := b.withReadView(capture, func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
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
				edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
				return decodeErr
			}); err != nil {
				return err
			}

			if b.hidesEdge(v, edge, nowNanos) {
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
		if b.hidesEdge(v, edge, nowNanos) {
			continue
		}
		edges = append(edges, copyEdge(edge))
	}
	return edges, err
}

// GetEdgesByType returns all edges of a specific type using the edge type index.
// This is MUCH faster than AllEdges() for queries like mutual follows.
// Relationship types match exactly, as in Neo4j (#862).
// Results are cached per type to speed up repeated queries.
func (b *BadgerEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return b.getEdgesByType(edgeType, visibleRead)
}

// GetStoredEdgesByType is GetEdgesByType for integrity work: it also returns
// the relationships the visibility layer hides (StoredEntityReader). It never
// reads or fills the per-type cache, which holds visible results.
func (b *BadgerEngine) GetStoredEdgesByType(edgeType string) ([]*Edge, error) {
	return b.getEdgesByType(edgeType, storedRead)
}

func (b *BadgerEngine) getEdgesByType(edgeType string, v readVisibility) ([]*Edge, error) {
	if edgeType == "" {
		if v == storedRead {
			return b.allEdges(storedRead)
		}
		return b.AllEdges() // No type filter = all edges
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

	var overlay []*Edge
	var touched map[EdgeID]bool
	var edges []*Edge
	nowNanos := DecayScoringTime()
	gen := b.edgeTypeCacheGen.current()
	err := b.withReadView(func(w *WriteBehindBuffer) { overlay, touched = w.typeOverlayLocked(edgeType) }, func(txn *badger.Txn) error {
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
				edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
				return decodeErr
			}); err != nil {
				return err
			}

			if b.hidesEdge(v, edge, nowNanos) {
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
		edges = b.mergeWriteBehindEdges(edges, overlay, touched, v)
		return edges, nil
	}

	if useCache {
		b.edgeTypeCacheFill(gen, edgeType, edges)
	}
	return edges, nil
}

// mergeWriteBehindEdges folds the write-behind overlay for edgeType over
// committed rows: rows whose IDs the buffer touched are dropped and the
// buffered versions (copied for the caller) take their place.
func (b *BadgerEngine) mergeWriteBehindEdges(edges, overlay []*Edge, touched map[EdgeID]bool, v readVisibility) []*Edge {
	if len(touched) == 0 {
		return edges
	}
	nowNanos := DecayScoringTime()
	out := edges[:0]
	for _, e := range edges {
		if touched[e.ID] {
			continue
		}
		out = append(out, e)
	}
	for _, e := range overlay {
		if b.hidesEdge(v, e, nowNanos) {
			continue
		}
		out = append(out, copyEdge(e))
	}
	return out
}

// edgeTypeCacheFill caches the edges of edgeType a read captured gen before
// reading (simple LRU-style: clear if too many types), unless the type cache
// was written since (cacheGeneration).
func (b *BadgerEngine) edgeTypeCacheFill(gen uint64, edgeType string, edges []*Edge) {
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

// InvalidateEdgeTypeCache clears the entire edge type cache.
// Called after bulk edge mutations to ensure cache consistency.
func (b *BadgerEngine) InvalidateEdgeTypeCache() {
	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCacheGen.advance()
	b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	b.edgeTypeCacheMu.Unlock()
}

// InvalidateEdgeTypeCacheForType removes only the specified edge type from cache.
// Much faster than full invalidation for single-edge operations.
func (b *BadgerEngine) InvalidateEdgeTypeCacheForType(edgeType string) {
	if edgeType == "" {
		return
	}
	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCacheGen.advance()
	delete(b.edgeTypeCache, edgeType)
	b.edgeTypeCacheMu.Unlock()
}

// BatchGetNodes materializes a batch through one capture, with no overlay map
// or transaction when every requested body is cached.
func (b *BadgerEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return b.readNodeBatch(ids, true)
}

func (b *BadgerEngine) BatchGetNodesWithoutEmbeddings(ids []NodeID) (map[NodeID]*Node, error) {
	return b.readNodeBatch(ids, false)
}

func (b *BadgerEngine) readNodeBatch(ids []NodeID, embeddings bool) (map[NodeID]*Node, error) {
	result := make(map[NodeID]*Node, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}
	copyForRead := copyNodeWithoutEmbeddings
	if embeddings {
		copyForRead = copyNode
	}
	now := DecayScoringTime()
	cached := func(w *WriteBehindBuffer) (bool, error) {
		clear(result)
		b.nodeCacheMu.RLock()
		defer b.nodeCacheMu.RUnlock()
		for _, id := range ids {
			if id == "" {
				continue
			}
			var node *Node
			if w != nil {
				state := w.entityStateLocked(bufferedNodeEntity, string(id))
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
			if !b.filterNodeByDecay(node, now) {
				result[id] = node
			}
		}
		return true, nil
	}
	var states map[NodeID]bufferedEntityState
	gen := b.nodeCacheGen.current()
	var loaded []*Node
	var current bool
	err := b.withReadView(func(w *WriteBehindBuffer) {
		for _, id := range ids {
			if state := w.entityStateLocked(bufferedNodeEntity, string(id)); state.found {
				if states == nil {
					states = make(map[NodeID]bufferedEntityState)
				}
				states[id] = state
			}
		}
	}, func(txn *badger.Txn) error {
		clear(result)
		loaded = nil
		for _, id := range ids {
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
				var hydrate func(*badger.Txn, *Node, NodeID) (*Node, error)
				if embeddings {
					hydrate = b.loadNodeEmbeddings
				}
				node, err = b.readNodeItemInTxn(txn, item, id, hydrate)
				if err != nil {
					return err
				}
				if embeddings {
					loaded = append(loaded, node)
				}
			}
			if !b.filterNodeByDecay(node, now) {
				result[id] = node
			}
		}
		return nil
	}, readViewOptions{cached: cached, current: &current})
	if err != nil {
		return nil, err
	}
	// Selection completes at the captured view. Copy its immutable bodies
	// afterwards, once, outside the buffer and cache locks.
	for id, node := range result {
		result[id] = copyForRead(node)
	}
	for _, node := range loaded {
		b.cacheFillNode(gen, node)
	}
	return result, nil
}

// HasLabelBatch checks label membership for a batch of node IDs using the label index.
// This avoids decoding node records and is significantly faster for large batches.
func (b *BadgerEngine) HasLabelBatch(ids []NodeID, label string) (map[NodeID]bool, error) {
	if len(ids) == 0 {
		return make(map[NodeID]bool), nil
	}

	var states map[NodeID]bufferedEntityState
	result := make(map[NodeID]bool, len(ids))
	err := b.withReadView(func(w *WriteBehindBuffer) {
		for _, id := range ids {
			if state := w.entityStateLocked(bufferedNodeEntity, string(id)); state.found {
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
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetOutgoingEdges returns all edges where the given node is the source.
func (b *BadgerEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	out, _, err := b.readAdjacency(nodeID, true, false, visibleRead)
	return out, err
}

// GetStoredOutgoingEdges includes hidden bodies through the shared read view.
func (b *BadgerEngine) GetStoredOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	out, _, err := b.readAdjacency(nodeID, true, false, storedRead)
	return out, err
}

// GetAdjacentEdges fetches both outgoing and incoming edges for nodeID. On a
// hot path the per-node adjacency cache short-circuits the Badger iterator
// entirely, falling back to a single view transaction when either direction
// misses.
func (b *BadgerEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	return b.readAdjacency(nodeID, true, true, visibleRead)
}

// materializeAdjEdges resolves a list of EdgeIDs to live *Edge bodies by
// hitting the edge body cache first, then falling back to a one-shot view
// transaction for any IDs that miss. Used by the adjacency-cache fast path.
func (b *BadgerEngine) materializeAdjEdges(ids []EdgeID, v readVisibility) ([]*Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	nowNanos := DecayScoringTime()
	out := make([]*Edge, 0, len(ids))
	var miss []EdgeID
	for _, id := range ids {
		if cached, ok := b.cacheLoadEdge(id); ok {
			if b.hidesEdge(v, cached, nowNanos) {
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
				edge, decodeErr = b.decodeEdgeBodyByID(val, id)
				return decodeErr
			}); err != nil {
				return err
			}
			b.cacheFillEdge(gen, edge)
			if b.hidesEdge(v, edge, nowNanos) {
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
		if edge == nil || b.hidesEdge(v, edge, nowNanos) {
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
func (b *BadgerEngine) OutgoingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return b.edgeHeaders(nodeID, true)
}

// IncomingEdgeHeaders is the incoming-side OutgoingEdgeHeaders.
func (b *BadgerEngine) IncomingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return b.edgeHeaders(nodeID, false)
}

func (b *BadgerEngine) edgeHeaders(nodeID NodeID, outgoing bool) ([]*Edge, bool, error) {
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
		view, err := b.captureReadView(func(w *WriteBehindBuffer) {
			live, suppressed, nodeDeleted = w.edgeAdjacencyOverlayLocked(nodeID, outgoing)
		})
		if err != nil {
			return nil, true, err
		}
		defer view.Close()
		if nodeDeleted {
			return nil, true, nil
		}
		committed, answered, err := b.edgeHeadersCommitted(nodeID, outgoing, view.withView)
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
	return b.edgeHeadersCommitted(nodeID, outgoing)
}

func (b *BadgerEngine) edgeHeadersCommitted(nodeID NodeID, outgoing bool, views ...func(func(*badger.Txn) error) error) ([]*Edge, bool, error) {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
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
	err := withView(func(txn *badger.Txn) error {
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
	})
	return edges, true, err
}

// decodeAdjacencyHeader returns the relationship, without properties, that an
// adjacency entry value (adjacencyValueFromRecord) describes, or nil when the
// value is empty, can't be decoded, or names an endpoint the ID dictionary
// doesn't know.
func (b *BadgerEngine) decodeAdjacencyHeader(edgeID EdgeID, value []byte) *Edge {
	edge, startNum, endNum, _, err := decodeEdgeCompactHeader(value, edgeFormatCompactV2)
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
	if cached, ok := b.cacheLoadEdge(edgeID); ok && b.writeBehind == nil {
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
		edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	b.cacheFillEdgeAtVersion(gen, edge, item.Version())
	return edge, nil
}

// GetIncomingEdges returns all edges where the given node is the target.
func (b *BadgerEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	_, in, err := b.readAdjacency(nodeID, false, true, visibleRead)
	return in, err
}

// GetStoredIncomingEdges includes hidden bodies through the shared read view.
func (b *BadgerEngine) GetStoredIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	_, in, err := b.readAdjacency(nodeID, false, true, storedRead)
	return in, err
}

// GetEdgesBetween returns all edges between two nodes.
func (b *BadgerEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
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
		view, err := b.captureReadView(func(w *WriteBehindBuffer) {
			live, suppressed, sourceDeleted, targetDeleted = w.edgesBetweenOverlayLocked(startID, endID)
		})
		if err != nil {
			return nil, err
		}
		defer view.Close()
		if sourceDeleted || targetDeleted {
			return nil, nil
		}
		committed, err := b.committedEdgesBetween(startID, endID, "", view.withView)
		if err != nil {
			return nil, err
		}
		if len(suppressed) > 0 {
			for _, e := range committed {
				if _, dead := suppressed[e.ID]; dead {
					continue
				}
				result = append(result, e)
			}
		} else {
			result = committed
		}
		for _, e := range live {
			copyEdge := *e
			result = append(result, &copyEdge)
		}
		return result, nil
	}
	return b.committedEdgesBetween(startID, endID, "")
}

// committedEdgesBetween is the committed between-index read, set index first
// with the legacy outgoing fallback and self-heal.
func (b *BadgerEngine) committedEdgesBetween(startID, endID NodeID, edgeType string, views ...func(func(*badger.Txn) error) error) ([]*Edge, error) {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
	result, err := b.edgesBetweenFromSetIndex(startID, endID, edgeType, withView)
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		return result, nil
	}

	result, err = b.edgesBetweenFromLegacyOutgoingIndex(startID, endID, edgeType, withView)
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(result)
	}

	return result, nil
}

// GetEdgeBetween returns an edge between two nodes with the given type.
func (b *BadgerEngine) GetEdgeBetween(source, target NodeID, edgeType string) *Edge {
	if source == "" || target == "" {
		return nil
	}
	if b.writeBehind == nil {
		return b.committedEdgeBetween(source, target, edgeType)
	}
	// A buffered endpoint delete hides the committed row; a buffered delete
	// of the edge hides it; a buffered live edge of the type wins.
	var live []*Edge
	var suppressed map[EdgeID]struct{}
	var sourceDeleted, targetDeleted bool
	view, err := b.captureReadView(func(w *WriteBehindBuffer) {
		live, suppressed, sourceDeleted, targetDeleted = w.edgesBetweenOverlayLocked(source, target)
	})
	if err != nil {
		return nil
	}
	defer view.Close()
	if sourceDeleted || targetDeleted {
		return nil
	}
	committed := b.committedEdgeBetween(source, target, edgeType, view.withView)
	if committed != nil {
		if _, dead := suppressed[committed.ID]; !dead {
			return committed
		}
	}
	for _, e := range live {
		if edgeType != "" && e.Type != edgeType {
			continue
		}
		copyEdge := *e
		return &copyEdge
	}
	return nil
}

// committedEdgeBetween is the committed between-index read for one edge.
func (b *BadgerEngine) committedEdgeBetween(source, target NodeID, edgeType string, views ...func(func(*badger.Txn) error) error) *Edge {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
	if edgeType != "" {
		edge, err := b.edgeBetweenFromHeadIndex(source, target, edgeType, withView)
		if err != nil {
			b.log.Warn("edge-between head lookup failed; falling back to set/legacy scan",
				"subsystem", "edge_between_index",
			)
		}
		if edge != nil {
			return edge
		}
	}

	indexed, err := b.edgesBetweenFromSetIndex(source, target, edgeType, withView)
	if err == nil && len(indexed) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(indexed[:1])
		return indexed[0]
	}

	legacy, err := b.edgesBetweenFromLegacyOutgoingIndex(source, target, edgeType, withView)
	if err == nil && len(legacy) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(legacy)
		return legacy[0]
	}

	return nil
}

// edgeBetweenFromHeadIndex loads the typed common-case lookup without scanning.
func (b *BadgerEngine) edgeBetweenFromHeadIndex(source, target NodeID, edgeType string, views ...func(func(*badger.Txn) error) error) (*Edge, error) {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
	sourceNum, sourceOK := b.idDict.lookupNodeNumID(source)
	targetNum, targetOK := b.idDict.lookupNodeNumID(target)
	if !sourceOK || !targetOK {
		return nil, nil // no numID ever allocated — no index entry can exist
	}
	var result *Edge
	err := withView(func(txn *badger.Txn) error {
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
		if b.filterEdgeByDecay(edge, DecayScoringTime()) {
			return nil
		}
		if edgeMatchesBetween(edge, source, target, edgeType) {
			result = edge
		}
		return nil
	})
	return result, err
}

// edgesBetweenFromSetIndex scans the exact relationship set index.
// Keys use 8-byte numeric IDs so we resolve start/end via the id dict
// before scanning. Each entry's VALUE carries the string edge ID, which
// saves a reverse-dict lookup for the scan payload.
func (b *BadgerEngine) edgesBetweenFromSetIndex(startID, endID NodeID, edgeType string, views ...func(func(*badger.Txn) error) error) ([]*Edge, error) {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
	startNum, sOK := b.idDict.lookupNodeNumID(startID)
	endNum, eOK := b.idDict.lookupNodeNumID(endID)
	if !sOK || !eOK {
		return nil, nil
	}
	var result []*Edge
	err := withView(func(txn *badger.Txn) error {
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

		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
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
			if !edgeMatchesBetween(edge, startID, endID, edgeType) || b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}
			result = append(result, edge)
		}
		return nil
	})
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
func (b *BadgerEngine) edgesBetweenFromLegacyOutgoingIndex(startID, endID NodeID, edgeType string, views ...func(func(*badger.Txn) error) error) ([]*Edge, error) {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
	outPrefix := b.outgoingIndexPrefixString(startID)
	inPrefix := b.incomingIndexPrefixString(endID)
	if outPrefix == nil || inPrefix == nil {
		return nil, nil
	}
	var result []*Edge
	err := withView(func(txn *badger.Txn) error {
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
		for candidates == nil {
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
		for _, indexKey := range candidates {
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
			if !edgeMatchesBetween(edge, startID, endID, edgeType) || b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}
			result = append(result, edge)
		}
		return nil
	})
	return result, err
}

// selfHealEdgeBetweenIndexes repairs lookup indexes discovered through a
// fallback read, preventing ready-marker drift from becoming a correctness bug.
func (b *BadgerEngine) selfHealEdgeBetweenIndexes(edges []*Edge) error {
	if len(edges) > edgeBetweenSelfHealMaxEdges {
		edges = edges[:edgeBetweenSelfHealMaxEdges]
	}
	scope := make([]string, 0, len(edges))
	for _, edge := range edges {
		if edge != nil {
			scope = append(scope, namespaceForEdgeID(edge.ID))
		}
	}
	release, err := b.beginNamespaceMutation(context.Background(), scope, nil, false, namespacePublicationFresh)
	if err != nil {
		return err
	}
	defer release()

	err = b.withUpdate(func(txn *badger.Txn) error {
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			current, err := b.edgeFromTxn(txn, edge.ID)
			if errors.Is(err, ErrNotFound) || errors.Is(err, badger.ErrKeyNotFound) {
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
	})
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
		edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	return edge, nil
}

// ============================================================================
