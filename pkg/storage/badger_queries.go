// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

const edgeBetweenSelfHealMaxEdges = 64

// Query Operations
// ============================================================================

// GetFirstNodeByLabel returns the first node with the specified label.
// This is optimized for MATCH...LIMIT 1 patterns - stops after first match.
func (b *BadgerEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			nodeID := extractNodeIDFromLabelIndex(it.Item().Key(), len(label))
			if nodeID == "" {
				continue
			}

			item, err := txn.Get(nodeKey(nodeID))
			if err != nil {
				continue
			}

			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
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
			_, err := txn.Get(labelIndexKey(label, cachedID))
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

		prefix := labelIndexPrefix(label)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		labelLen := len(normalizeLabel(label))
		for it.Rewind(); it.Valid(); it.Next() {
			nodeID := extractNodeIDFromLabelIndex(it.Item().Key(), labelLen)
			if nodeID == "" {
				continue
			}
			if cachedValid && nodeID == cachedID {
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
			nodeID := extractNodeIDFromLabelIndex(it.Item().Key(), len(label))
			if nodeID == "" {
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
				node, decodeErr = decodeNodeWithEmbeddings(txn, val, nodeID)
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
				node, decodeErr = decodeNodeWithEmbeddings(txn, val, nodeID)
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
	var edges []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 0))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
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
	err := b.withView(func(txn *badger.Txn) error {
		prefix := edgeTypeIndexPrefix(edgeType)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		// Collect edge IDs from index
		var edgeIDs []EdgeID
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			// Extract edgeID from key: prefix + type + 0x00 + edgeID
			sepIdx := bytes.LastIndexByte(key, 0x00)
			if sepIdx >= 0 && sepIdx < len(key)-1 {
				edgeIDs = append(edgeIDs, EdgeID(key[sepIdx+1:]))
			}
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
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
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
			result[id] = copyNode(cached)
			continue
		}
		missing = append(missing, id)
	}
	b.nodeCacheMu.RUnlock()

	if len(missing) == 0 {
		return result, nil
	}

	var loaded []*Node
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
				node, decodeErr = decodeNodeWithEmbeddings(txn, val, id)
				return decodeErr
			}); err != nil {
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
			_, err := txn.Get(labelIndexKey(label, id))
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

	var edges []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		prefix := outgoingIndexPrefix(nodeID)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
			if edgeID == "" {
				continue
			}

			// Get the edge
			item, err := txn.Get(edgeKey(edgeID))
			if err != nil {
				continue
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return edges, nil
}

// GetIncomingEdges returns all edges where the given node is the target.
func (b *BadgerEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	var edges []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		prefix := incomingIndexPrefix(nodeID)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
			if edgeID == "" {
				continue
			}

			// Get the edge
			item, err := txn.Get(edgeKey(edgeID))
			if err != nil {
				continue
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

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
			log.Printf("edge-between head lookup failed; falling back to set/legacy scan: start=%q end=%q type=%q err=%v", source, target, edgeType, err)
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
	var result *Edge
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeBetweenHeadKey(source, target, edgeType))
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
		edge, err := edgeFromTxn(txn, edgeID)
		if err != nil {
			return fmt.Errorf("load edge-between head edge %q: %w", edgeID, err)
		}
		if edgeMatchesBetween(edge, source, target, edgeType) {
			result = edge
		}
		return nil
	})
	return result, err
}

// edgesBetweenFromSetIndex scans the exact relationship set index.
func (b *BadgerEngine) edgesBetweenFromSetIndex(startID, endID NodeID, edgeType string) ([]*Edge, error) {
	var result []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		prefix := edgeBetweenIndexPrefix(startID, endID)
		if edgeType != "" {
			prefix = typedEdgeBetweenIndexPrefix(startID, endID, edgeType)
		}
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			edgeID := extractEdgeIDFromEdgeBetweenIndexKey(it.Item().Key())
			if edgeID == "" {
				continue
			}
			edge, err := edgeFromTxn(txn, edgeID)
			if err != nil || !edgeMatchesBetween(edge, startID, endID, edgeType) {
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
	var result []*Edge
	err := b.withView(func(txn *badger.Txn) error {
		prefix := outgoingIndexPrefix(startID)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
			if edgeID == "" {
				continue
			}
			edge, err := edgeFromTxn(txn, edgeID)
			if err != nil || !edgeMatchesBetween(edge, startID, endID, edgeType) {
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
			if err := writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		log.Printf("edge-between index self-heal skipped after Badger conflict")
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
func edgeFromTxn(txn *badger.Txn, edgeID EdgeID) (*Edge, error) {
	item, err := txn.Get(edgeKey(edgeID))
	if err != nil {
		return nil, err
	}
	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = decodeEdge(val)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	return edge, nil
}

// ============================================================================
