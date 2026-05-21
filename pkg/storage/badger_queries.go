// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const edgeBetweenSelfHealMaxEdges = 64

// Query Operations
// ============================================================================

// GetFirstNodeByLabel returns the first node with the specified label.
// This is optimized for MATCH...LIMIT 1 patterns - stops after first match.
func (b *BadgerEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	var node *Node
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			indexKey := it.Item().KeyCopy(nil)
			nodeNum, ok := extractNodeNumIDFromLabelIndex(indexKey, len(normalizeLabel(label)))
			if !ok {
				continue
			}
			nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
			if !ok || nodeID == "" {
				continue
			}

			if b.decayEnabled && !b.revealAll.Load() && hasIndexTombstone(txn, indexKey) {
				continue
			}

			item, err := txn.Get(nodeKey(nodeID))
			if err != nil {
				continue
			}

			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				continue
			}
			if b.filterNodeByDecay(node, nowNanos) {
				node = nil
				continue
			}

			return nil // Found first node, stop
		}
		return nil
	})

	return node, err
}

// ForEachNodeIDByLabel streams node IDs for a label without decoding nodes.
// Stops early when visit returns false.
func (b *BadgerEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if err := b.ensureOpen(); err != nil {
		return err
	}

	cachedID, cachedOK := b.labelCacheGetFirst(label)
	cachedValid := false

	err := b.withView(func(txn *badger.Txn) error {
		if cachedOK && cachedID != "" {
			cachedKey := b.labelIndexKeyStringLookup(label, cachedID)
			if cachedKey == nil {
				b.labelCacheInvalidateForNodeLabels([]string{label}, cachedID)
			} else {
				_, err := txn.Get(cachedKey)
				switch err {
				case nil:
					cachedValid = true
					if !visit(cachedID) {
						return ErrIterationStopped
					}
				case badger.ErrKeyNotFound:
					b.labelCacheInvalidateForNodeLabels([]string{label}, cachedID)
				default:
					return err
				}
			}
		}

		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		labelLen := len(normalizeLabel(label))
		for it.Rewind(); it.Valid(); it.Next() {
			indexKey := it.Item().KeyCopy(nil)
			nodeNum, ok := extractNodeNumIDFromLabelIndex(indexKey, labelLen)
			if !ok {
				continue
			}
			nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
			if !ok || nodeID == "" {
				continue
			}
			if cachedValid && nodeID == cachedID {
				continue
			}
			if checkTombstones && hasIndexTombstone(txn, indexKey) {
				continue
			}
			if !cachedValid {
				b.labelCacheSetFirst(label, nodeID)
				cachedValid = true
			}
			if !visit(nodeID) {
				return ErrIterationStopped
			}
		}

		return nil
	})
	if err == ErrIterationStopped {
		return nil
	}
	return err
}

// GetNodesByLabel returns all nodes with the specified label.
func (b *BadgerEngine) GetNodesByLabel(label string) ([]*Node, error) {
	// Single-pass: iterate label index and fetch nodes in same transaction
	// This reduces transaction overhead compared to two-phase approach
	var nodes []*Node
	var loaded []*Node
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			indexKey := it.Item().KeyCopy(nil)
			nodeNum, ok := extractNodeNumIDFromLabelIndex(indexKey, len(normalizeLabel(label)))
			if !ok {
				continue
			}
			nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
			if !ok || nodeID == "" {
				continue
			}

			if b.decayEnabled && !b.revealAll.Load() && hasIndexTombstone(txn, indexKey) {
				continue
			}

			// Cache-first: avoid Badger reads/decodes for hot label scans.
			b.nodeCacheMu.RLock()
			if cached, ok := b.nodeCache[nodeID]; ok {
				b.nodeCacheMu.RUnlock()
				cn := copyNode(cached)
				if b.filterNodeByDecay(cn, nowNanos) {
					continue
				}
				nodes = append(nodes, cn)
				continue
			}
			b.nodeCacheMu.RUnlock()

			// Fetch node data in same transaction
			item, err := txn.Get(nodeKey(nodeID))
			if err != nil {
				continue // Skip if node was deleted
			}

			var node *Node
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				continue
			}

			if b.filterNodeByDecay(node, nowNanos) {
				continue
			}

			loaded = append(loaded, node)
			nodes = append(nodes, node)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Cache loaded nodes for future label scans.
	for _, n := range loaded {
		if n != nil {
			b.cacheStoreNode(n)
		}
	}

	return nodes, nil
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
	var nodes []*Node
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 0))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			// Extract nodeID from key (skip prefix byte)
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])

			var node *Node
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				continue
			}

			if b.filterNodeByDecay(node, nowNanos) {
				continue
			}

			nodes = append(nodes, node)
		}

		return nil
	})

	return nodes, err
}

// AllEdges returns all edges (implements Engine interface).
func (b *BadgerEngine) AllEdges() ([]*Edge, error) {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	var edges []*Edge
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 0))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			var edgeID EdgeID
			if len(key) > 1 {
				edgeID = EdgeID(key[1:])
			}
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
				return decodeErr
			}); err != nil {
				continue
			}

			if b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	return edges, err
}

// GetEdgesByType returns all edges of a specific type using the edge type index.
// This is MUCH faster than AllEdges() for queries like mutual follows.
// Edge types are matched case-insensitively (Neo4j compatible).
// Results are cached per type to speed up repeated queries.
func (b *BadgerEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	if edgeType == "" {
		return b.AllEdges() // No type filter = all edges
	}

	normalizedType := strings.ToLower(edgeType)

	// Check cache first
	b.edgeTypeCacheMu.RLock()
	if cached, ok := b.edgeTypeCache[normalizedType]; ok {
		b.edgeTypeCacheMu.RUnlock()
		return cached, nil
	}
	b.edgeTypeCacheMu.RUnlock()

	var edges []*Edge
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		prefix := edgeTypeIndexPrefix(edgeType)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		// Collect edge IDs from index. Keys now carry an 8-byte edge
		// numID in the suffix — resolve via the reverse dict.
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
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
			if err != nil {
				continue
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
				return decodeErr
			}); err != nil {
				continue
			}

			if b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Cache the result (simple LRU-style: clear if too many types)
	b.edgeTypeCacheMu.Lock()
	if b.edgeTypeCacheMaxTypes > 0 && len(b.edgeTypeCache) > b.edgeTypeCacheMaxTypes {
		b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	}
	b.edgeTypeCache[normalizedType] = edges
	b.edgeTypeCacheMu.Unlock()

	return edges, nil
}

// InvalidateEdgeTypeCache clears the entire edge type cache.
// Called after bulk edge mutations to ensure cache consistency.
func (b *BadgerEngine) InvalidateEdgeTypeCache() {
	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	b.edgeTypeCacheMu.Unlock()
}

// InvalidateEdgeTypeCacheForType removes only the specified edge type from cache.
// Much faster than full invalidation for single-edge operations.
func (b *BadgerEngine) InvalidateEdgeTypeCacheForType(edgeType string) {
	if edgeType == "" {
		return
	}
	normalizedType := strings.ToLower(edgeType)
	b.edgeTypeCacheMu.Lock()
	delete(b.edgeTypeCache, normalizedType)
	b.edgeTypeCacheMu.Unlock()
}

// BatchGetNodes fetches multiple nodes in a single transaction.
// Returns a map for O(1) lookup by ID. Missing nodes are not included in the result.
// This is optimized for traversal operations that need to fetch many nodes.
func (b *BadgerEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if len(ids) == 0 {
		return make(map[NodeID]*Node), nil
	}

	// Fast path: satisfy as many lookups as possible from the node cache.
	//
	// This is critical for read-heavy workloads (and benchmarks) where the same
	// node sets are repeatedly fetched (e.g., aggregation and COLLECT queries).
	result := make(map[NodeID]*Node, len(ids))
	missing := make([]NodeID, 0, len(ids))

	b.nodeCacheMu.RLock()
	for _, id := range ids {
		if id == "" {
			continue
		}
		if cached, ok := b.nodeCache[id]; ok {
			nodeCopy := copyNode(cached)
			if b.filterNodeByDecay(nodeCopy, DecayScoringTime()) {
				continue
			}
			result[id] = nodeCopy
			continue
		}
		missing = append(missing, id)
	}
	b.nodeCacheMu.RUnlock()

	if len(missing) == 0 {
		return result, nil
	}

	var loaded []*Node
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		loaded = loaded[:0]
		for _, id := range missing {
			item, err := txn.Get(nodeKey(id))
			if err != nil {
				continue // Skip missing nodes
			}

			var node *Node
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, id)
				return decodeErr
			}); err != nil {
				continue
			}
			if b.filterNodeByDecay(node, nowNanos) {
				continue
			}

			loaded = append(loaded, node)
			result[id] = node
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Cache loaded nodes for future batch lookups.
	for _, n := range loaded {
		if n != nil {
			b.cacheStoreNode(n)
		}
	}

	return result, nil
}

// HasLabelBatch checks label membership for a batch of node IDs using the label index.
// This avoids decoding node records and is significantly faster for large batches.
func (b *BadgerEngine) HasLabelBatch(ids []NodeID, label string) (map[NodeID]bool, error) {
	if len(ids) == 0 {
		return make(map[NodeID]bool), nil
	}

	result := make(map[NodeID]bool, len(ids))
	err := b.withView(func(txn *badger.Txn) error {
		for _, id := range ids {
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
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	if ids, ok := b.adjCacheLoadOutgoing(nodeID); ok {
		return b.materializeAdjEdges(ids), nil
	}

	prefix := b.outgoingIndexPrefixString(nodeID)
	if prefix == nil {
		return nil, nil
	}
	var edges []*Edge
	var ids []EdgeID
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		edges, ids = b.collectEdgesByIndexPrefix(txn, prefix, nowNanos)
		return nil
	})
	if err != nil {
		return nil, err
	}
	b.adjCacheStoreOutgoing(nodeID, ids)
	return edges, nil
}

// GetAdjacentEdges fetches both outgoing and incoming edges for nodeID. On a
// hot path the per-node adjacency cache short-circuits the Badger iterator
// entirely, falling back to a single view transaction when either direction
// misses.
func (b *BadgerEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	if nodeID == "" {
		return nil, nil, ErrInvalidID
	}

	cachedOutIDs, outHit := b.adjCacheLoadOutgoing(nodeID)
	cachedInIDs, inHit := b.adjCacheLoadIncoming(nodeID)
	if outHit && inHit {
		return b.materializeAdjEdges(cachedOutIDs), b.materializeAdjEdges(cachedInIDs), nil
	}

	var outPrefix, inPrefix []byte
	if !outHit {
		outPrefix = b.outgoingIndexPrefixString(nodeID)
	}
	if !inHit {
		inPrefix = b.incomingIndexPrefixString(nodeID)
	}
	if outPrefix == nil && inPrefix == nil && !outHit && !inHit {
		return nil, nil, nil
	}

	var outgoing, incoming []*Edge
	var outIDs, inIDs []EdgeID
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		if !outHit && outPrefix != nil {
			outgoing, outIDs = b.collectEdgesByIndexPrefix(txn, outPrefix, nowNanos)
		}
		if !inHit && inPrefix != nil {
			incoming, inIDs = b.collectEdgesByIndexPrefix(txn, inPrefix, nowNanos)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if !outHit {
		b.adjCacheStoreOutgoing(nodeID, outIDs)
	} else {
		outgoing = b.materializeAdjEdges(cachedOutIDs)
	}
	if !inHit {
		b.adjCacheStoreIncoming(nodeID, inIDs)
	} else {
		incoming = b.materializeAdjEdges(cachedInIDs)
	}
	return outgoing, incoming, nil
}

// materializeAdjEdges resolves a list of EdgeIDs to live *Edge bodies by
// hitting the edge body cache first, then falling back to a one-shot view
// transaction for any IDs that miss. Used by the adjacency-cache fast path.
func (b *BadgerEngine) materializeAdjEdges(ids []EdgeID) []*Edge {
	if len(ids) == 0 {
		return nil
	}
	nowNanos := DecayScoringTime()
	out := make([]*Edge, 0, len(ids))
	var miss []EdgeID
	for _, id := range ids {
		if cached, ok := b.cacheLoadEdge(id); ok {
			if b.filterEdgeByDecay(cached, nowNanos) {
				continue
			}
			out = append(out, cached)
			continue
		}
		miss = append(miss, id)
	}
	if len(miss) == 0 {
		return out
	}
	_ = b.withView(func(txn *badger.Txn) error {
		for _, id := range miss {
			item, err := txn.Get(edgeKey(id))
			if err != nil {
				continue
			}
			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = b.decodeEdgeBodyByID(val, id)
				return decodeErr
			}); err != nil {
				continue
			}
			b.cacheStoreEdge(edge)
			if b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}
			out = append(out, edge)
		}
		return nil
	})
	return out
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
// memory-bound work after the first encounter.
func (b *BadgerEngine) collectEdgesByIndexPrefix(txn *badger.Txn, prefix []byte, nowNanos int64) ([]*Edge, []EdgeID) {
	it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
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

		if cached, ok := b.cacheLoadEdge(edgeID); ok {
			if b.filterEdgeByDecay(cached, nowNanos) {
				continue
			}
			edges = append(edges, cached)
			continue
		}

		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			continue
		}

		var edge *Edge
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
			return decodeErr
		}); err != nil {
			continue
		}

		b.cacheStoreEdge(edge)

		if b.filterEdgeByDecay(edge, nowNanos) {
			continue
		}

		edges = append(edges, edge)
	}
	return edges, ids
}

// GetIncomingEdges returns all edges where the given node is the target.
func (b *BadgerEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	if ids, ok := b.adjCacheLoadIncoming(nodeID); ok {
		return b.materializeAdjEdges(ids), nil
	}

	prefix := b.incomingIndexPrefixString(nodeID)
	if prefix == nil {
		return nil, nil
	}
	var edges []*Edge
	var ids []EdgeID
	nowNanos := DecayScoringTime()
	err := b.withView(func(txn *badger.Txn) error {
		edges, ids = b.collectEdgesByIndexPrefix(txn, prefix, nowNanos)
		return nil
	})
	if err != nil {
		return nil, err
	}
	b.adjCacheStoreIncoming(nodeID, ids)
	return edges, nil
}

// GetEdgesBetween returns all edges between two nodes.
func (b *BadgerEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}

	result, err := b.edgesBetweenFromSetIndex(startID, endID, "")
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		return result, nil
	}

	result, err = b.edgesBetweenFromLegacyOutgoingIndex(startID, endID, "")
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
	if edgeType != "" {
		edge, err := b.edgeBetweenFromHeadIndex(source, target, edgeType)
		if err != nil {
			b.log.Warn("edge-between head lookup failed; falling back to set/legacy scan",
				"subsystem", "edge_between_index",
				"start", string(source),
				"end", string(target),
				"edge_type", edgeType,
				slog.Any("error", err),
			)
		}
		if edge != nil {
			return edge
		}
	}

	indexed, err := b.edgesBetweenFromSetIndex(source, target, edgeType)
	if err == nil && len(indexed) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(indexed[:1])
		return indexed[0]
	}

	legacy, err := b.edgesBetweenFromLegacyOutgoingIndex(source, target, edgeType)
	if err == nil && len(legacy) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(legacy)
		return legacy[0]
	}

	return nil
}

// edgeBetweenFromHeadIndex loads the typed common-case lookup without scanning.
func (b *BadgerEngine) edgeBetweenFromHeadIndex(source, target NodeID, edgeType string) (*Edge, error) {
	sourceNum, sourceOK := b.idDict.lookupNodeNumID(source)
	targetNum, targetOK := b.idDict.lookupNodeNumID(target)
	if !sourceOK || !targetOK {
		return nil, nil // no numID ever allocated — no index entry can exist
	}
	var result *Edge
	err := b.withView(func(txn *badger.Txn) error {
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
func (b *BadgerEngine) edgesBetweenFromSetIndex(startID, endID NodeID, edgeType string) ([]*Edge, error) {
	startNum, sOK := b.idDict.lookupNodeNumID(startID)
	endNum, eOK := b.idDict.lookupNodeNumID(endID)
	if !sOK || !eOK {
		return nil, nil
	}
	var result []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		prefix := edgeBetweenIndexPrefix(startNum, endNum)
		if edgeType != "" {
			prefix = typedEdgeBetweenIndexPrefix(startNum, endNum, edgeType)
		}
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		nowNanos := DecayScoringTime()
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = true
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
			if err != nil || !edgeMatchesBetween(edge, startID, endID, edgeType) || b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}
			result = append(result, edge)
		}
		return nil
	})
	return result, err
}

// edgesBetweenFromLegacyOutgoingIndex preserves compatibility with stores that
// predate the edge-between indexes or missed a self-heal/backfill window.
func (b *BadgerEngine) edgesBetweenFromLegacyOutgoingIndex(startID, endID NodeID, edgeType string) ([]*Edge, error) {
	prefix := b.outgoingIndexPrefixString(startID)
	if prefix == nil {
		return nil, nil
	}
	var result []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		nowNanos := DecayScoringTime()
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			indexKey := it.Item().KeyCopy(nil)
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
			if err != nil || !edgeMatchesBetween(edge, startID, endID, edgeType) || b.filterEdgeByDecay(edge, nowNanos) {
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
	err := b.withUpdate(func(txn *badger.Txn) error {
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			if err := b.writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
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
	return edgeType == "" || strings.EqualFold(edge.Type, edgeType)
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
