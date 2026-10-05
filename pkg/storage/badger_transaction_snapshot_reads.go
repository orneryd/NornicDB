package storage

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	maxSnapshotLabelPrefixStreams   = 8
	maxSnapshotLabelPrefixNodes     = 64
	maxSnapshotLabelPrefixNodeBytes = 4 << 10
	maxSnapshotLabelPrefixBytes     = 256 << 10
	maxSnapshotPrefixNodeCacheNodes = 128
	maxSnapshotPrefixNodeCacheBytes = 256 << 10
	maxSnapshotEdgeCacheEntries     = 128
	maxSnapshotEdgeCacheBytes       = 256 << 10
)

func (tx *BadgerTransaction) storeSnapshotLabelPrefixLocked(txn *badger.Txn, key string, nodes []*Node, indexNodesByID bool) {
	if len(nodes) == 0 || len(nodes) > maxSnapshotLabelPrefixNodes {
		return
	}
	previous, exists := tx.snapshotLabelPrefixNodes[key]
	if exists && len(previous) >= len(nodes) {
		return
	}
	if !exists && len(tx.snapshotLabelPrefixNodes) >= maxSnapshotLabelPrefixStreams {
		return
	}

	bytes := 0
	for _, node := range nodes {
		nodeBytes, ok := snapshotLabelPrefixNodeBytes(node)
		if !ok || nodeBytes > maxSnapshotLabelPrefixBytes-bytes {
			return
		}
		// A node whose embedding state lives in the worker sidecar key space
		// has a clean body but must be excluded from the snapshot caches
		// like any other embedded node: cached entries are body-only and
		// would be served without the sidecar embeddings.
		if tx.nodeHasEmbeddingSidecar(txn, node.ID) {
			return
		}
		bytes += nodeBytes
	}
	previousBytes := tx.snapshotLabelPrefixNodeBytes[key]
	if tx.snapshotLabelPrefixBytes-previousBytes > maxSnapshotLabelPrefixBytes-bytes {
		return
	}
	if tx.snapshotLabelPrefixNodes == nil {
		tx.snapshotLabelPrefixNodes = make(map[string][]*Node, maxSnapshotLabelPrefixStreams)
		tx.snapshotLabelPrefixNodeBytes = make(map[string]int, maxSnapshotLabelPrefixStreams)
	}
	tx.snapshotLabelPrefixNodes[key] = append([]*Node(nil), nodes...)
	tx.snapshotLabelPrefixNodeBytes[key] = bytes
	tx.snapshotLabelPrefixBytes += bytes - previousBytes
	if indexNodesByID {
		if tx.snapshotPrefixNodeByID == nil {
			tx.snapshotPrefixNodeByID = make(map[NodeID]*Node, len(nodes))
			tx.snapshotPrefixNodeBytesByID = make(map[NodeID]int, len(nodes))
		}
		for _, node := range nodes {
			tx.cacheSnapshotPrefixNodeByIDLocked(txn, node)
		}
	}
}

func (tx *BadgerTransaction) committedNodeLabelsLocked(identifier NodeID) ([]string, error) {
	if labels, cached := tx.snapshotNodeLabels[identifier]; cached {
		return labels, nil
	}
	node, err := tx.getCommittedNodeLocked(identifier)
	if err != nil {
		return nil, err
	}
	return tx.cacheCommittedNodeLabelsLocked(node), nil
}

func (tx *BadgerTransaction) cacheCommittedNodeLabelsLocked(node *Node) []string {
	if labels, cached := tx.snapshotNodeLabels[node.ID]; cached {
		return labels
	}
	available := maxSnapshotPrefixNodeCacheBytes - tx.snapshotNodeLabelBytes
	for _, label := range node.Labels {
		if len(label) > available {
			return node.Labels
		}
		available -= len(label)
	}
	if len(tx.snapshotNodeLabels) < maxSnapshotPrefixNodeCacheNodes {
		if tx.snapshotNodeLabels == nil {
			tx.snapshotNodeLabels = make(map[NodeID][]string)
		}
		labels := append([]string(nil), node.Labels...)
		tx.snapshotNodeLabels[node.ID] = labels
		tx.snapshotNodeLabelBytes = maxSnapshotPrefixNodeCacheBytes - available
		return labels
	}
	return node.Labels
}

// nodeHasEmbeddingSidecar reports whether nodeID has a worker sidecar
// metadata record visible in txn. A nil txn (test call sites) skips the probe.
func (tx *BadgerTransaction) nodeHasEmbeddingSidecar(txn *badger.Txn, nodeID NodeID) bool {
	if txn == nil {
		return false
	}
	_, err := txn.Get(embeddingMetaKey(nodeID))
	return err == nil
}

func (tx *BadgerTransaction) cacheSnapshotPrefixNodeByIDLocked(txn *badger.Txn, node *Node) {
	nodeBytes, ok := snapshotLabelPrefixNodeBytes(node)
	if !ok || nodeBytes > maxSnapshotPrefixNodeCacheBytes {
		return
	}
	// Sidecar-bearing nodes must be re-read so embedding hydration runs.
	if tx.nodeHasEmbeddingSidecar(txn, node.ID) {
		return
	}
	if _, exists := tx.snapshotPrefixNodeByID[node.ID]; exists {
		return
	}
	if tx.snapshotPrefixNodeByID == nil {
		tx.snapshotPrefixNodeByID = make(map[NodeID]*Node, maxSnapshotPrefixNodeCacheNodes)
		tx.snapshotPrefixNodeBytesByID = make(map[NodeID]int, maxSnapshotPrefixNodeCacheNodes)
	}
	for len(tx.snapshotPrefixNodeOrder) >= maxSnapshotPrefixNodeCacheNodes ||
		tx.snapshotPrefixNodeBytes > maxSnapshotPrefixNodeCacheBytes-nodeBytes {
		oldest := tx.snapshotPrefixNodeOrder[0]
		tx.snapshotPrefixNodeOrder = tx.snapshotPrefixNodeOrder[1:]
		tx.snapshotPrefixNodeBytes -= tx.snapshotPrefixNodeBytesByID[oldest]
		delete(tx.snapshotPrefixNodeBytesByID, oldest)
		delete(tx.snapshotPrefixNodeByID, oldest)
	}
	tx.snapshotPrefixNodeOrder = append(tx.snapshotPrefixNodeOrder, node.ID)
	tx.snapshotPrefixNodeBytesByID[node.ID] = nodeBytes
	tx.snapshotPrefixNodeBytes += nodeBytes
	tx.snapshotPrefixNodeByID[node.ID] = node
}

func (tx *BadgerTransaction) clearSnapshotLabelPrefixLocked(key string) {
	if bytes, exists := tx.snapshotLabelPrefixNodeBytes[key]; exists {
		tx.snapshotLabelPrefixBytes -= bytes
		delete(tx.snapshotLabelPrefixNodeBytes, key)
		delete(tx.snapshotLabelPrefixNodes, key)
	}
	if len(tx.snapshotLabelPrefixNodes) == 0 {
		tx.snapshotLabelPrefixNodes = nil
		tx.snapshotLabelPrefixNodeBytes = nil
		tx.snapshotLabelPrefixBytes = 0
	}
}

func snapshotLabelPrefixNodeBytes(node *Node) (int, bool) {
	if node == nil || node.EmbeddingsStoredSeparately || len(node.NamedEmbeddings) > 0 || len(node.ChunkEmbeddings) > 0 || len(node.EmbedMeta) > 0 {
		return 0, false
	}
	bytes := 64 + len(node.ID)
	for _, label := range node.Labels {
		bytes += len(label)
		if bytes > maxSnapshotLabelPrefixNodeBytes {
			return 0, false
		}
	}
	for key, value := range node.Properties {
		valueBytes, ok := snapshotLabelPrefixValueBytes(value, 0)
		if !ok || len(key) > maxSnapshotLabelPrefixNodeBytes-bytes || valueBytes > maxSnapshotLabelPrefixNodeBytes-bytes-len(key) {
			return 0, false
		}
		bytes += len(key) + valueBytes
	}
	return bytes, true
}

func snapshotLabelPrefixValueBytes(value interface{}, depth int) (int, bool) {
	if depth > 8 {
		return 0, false
	}
	switch typed := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return 8, true
	case time.Time:
		return 24, true
	case string:
		return len(typed), true
	case []byte:
		return len(typed), true
	case []string:
		total := 16
		for _, item := range typed {
			if len(item) > maxSnapshotLabelPrefixNodeBytes-total {
				return 0, false
			}
			total += len(item)
		}
		return total, true
	case []int:
		return 16 + len(typed)*8, len(typed) <= maxSnapshotLabelPrefixNodeBytes/8
	case []int32:
		return 16 + len(typed)*4, len(typed) <= maxSnapshotLabelPrefixNodeBytes/4
	case []int64:
		return 16 + len(typed)*8, len(typed) <= maxSnapshotLabelPrefixNodeBytes/8
	case []float64:
		return 16 + len(typed)*8, len(typed) <= maxSnapshotLabelPrefixNodeBytes/8
	case []float32:
		return 16 + len(typed)*4, len(typed) <= maxSnapshotLabelPrefixNodeBytes/4
	case []bool:
		return 16 + len(typed), len(typed) <= maxSnapshotLabelPrefixNodeBytes
	case []interface{}:
		total := 16
		for _, item := range typed {
			itemBytes, ok := snapshotLabelPrefixValueBytes(item, depth+1)
			if !ok || itemBytes > maxSnapshotLabelPrefixNodeBytes-total {
				return 0, false
			}
			total += itemBytes
		}
		return total, true
	case map[string]interface{}:
		total := 32
		for key, item := range typed {
			itemBytes, ok := snapshotLabelPrefixValueBytes(item, depth+1)
			if !ok || len(key) > maxSnapshotLabelPrefixNodeBytes-total || itemBytes > maxSnapshotLabelPrefixNodeBytes-total-len(key) {
				return 0, false
			}
			total += len(key) + itemBytes
		}
		return total, true
	default:
		return 0, false
	}
}

func (tx *BadgerTransaction) cacheSnapshotEdgeByIDLocked(edge *Edge) {
	edgeBytes, ok := snapshotEdgeBytes(edge)
	if !ok || edgeBytes > maxSnapshotEdgeCacheBytes {
		return
	}
	if _, exists := tx.snapshotEdgeByID[edge.ID]; exists {
		return
	}
	if tx.snapshotEdgeByID == nil {
		tx.snapshotEdgeByID = make(map[EdgeID]*Edge, maxSnapshotEdgeCacheEntries)
		tx.snapshotEdgeBytesByID = make(map[EdgeID]int, maxSnapshotEdgeCacheEntries)
	}
	for len(tx.snapshotEdgeOrder) >= maxSnapshotEdgeCacheEntries || tx.snapshotEdgeBytes > maxSnapshotEdgeCacheBytes-edgeBytes {
		oldest := tx.snapshotEdgeOrder[0]
		tx.snapshotEdgeOrder = tx.snapshotEdgeOrder[1:]
		tx.snapshotEdgeBytes -= tx.snapshotEdgeBytesByID[oldest]
		delete(tx.snapshotEdgeBytesByID, oldest)
		delete(tx.snapshotEdgeByID, oldest)
	}
	tx.snapshotEdgeOrder = append(tx.snapshotEdgeOrder, edge.ID)
	tx.snapshotEdgeBytesByID[edge.ID] = edgeBytes
	tx.snapshotEdgeBytes += edgeBytes
	tx.snapshotEdgeByID[edge.ID] = copyEdge(edge)
}

func snapshotEdgeBytes(edge *Edge) (int, bool) {
	if edge == nil || edge.ID == "" || edge.StartNode == "" || edge.EndNode == "" {
		return 0, false
	}
	bytes := 96 + len(edge.ID) + len(edge.StartNode) + len(edge.EndNode) + len(edge.Type)
	for key, value := range edge.Properties {
		valueBytes, ok := snapshotLabelPrefixValueBytes(value, 0)
		if !ok || len(key) > maxSnapshotLabelPrefixNodeBytes-bytes || valueBytes > maxSnapshotLabelPrefixNodeBytes-bytes-len(key) {
			return 0, false
		}
		bytes += len(key) + valueBytes
	}
	if bytes > maxSnapshotLabelPrefixNodeBytes {
		return 0, false
	}
	return bytes, true
}

func (tx *BadgerTransaction) getAllCommittedNodesLocked() ([]*Node, error) {
	if tx.readTS.IsZero() {
		return tx.engine.AllNodes()
	}
	return tx.engine.getNodesByLabelVisibleAtWithView("", "", tx.readTS, tx.withSnapshotViewLocked)
}

// EdgeDirection selects which adjacency side an adjacency read resolves. It is
// a typed enum rather than a raw string so the hot bound-relationship-delete
// path compares an integer tag instead of magic strings.
type EdgeDirection uint8

const (
	// Outgoing selects edges whose start node is the queried node.
	Outgoing EdgeDirection = iota
	// Incoming selects edges whose end node is the queried node.
	Incoming
)

const (
	snapshotAdjacencyCacheMaxNodes   = 128
	snapshotAdjacencyCacheMaxEdgeIDs = 64
)

type snapshotAdjacencyCache struct {
	edgeIDsByNode map[NodeID][]EdgeID
	nodeOrder     []NodeID
}

func (cache *snapshotAdjacencyCache) load(nodeID NodeID) ([]EdgeID, bool) {
	edgeIDs, ok := cache.edgeIDsByNode[nodeID]
	return edgeIDs, ok
}

func (cache *snapshotAdjacencyCache) store(nodeID NodeID, edgeIDs []EdgeID) {
	if len(edgeIDs) > snapshotAdjacencyCacheMaxEdgeIDs {
		return
	}
	if cache.edgeIDsByNode == nil {
		cache.edgeIDsByNode = make(map[NodeID][]EdgeID, snapshotAdjacencyCacheMaxNodes)
	}
	if _, exists := cache.edgeIDsByNode[nodeID]; exists {
		return
	}
	if len(cache.nodeOrder) == snapshotAdjacencyCacheMaxNodes {
		delete(cache.edgeIDsByNode, cache.nodeOrder[0])
		copy(cache.nodeOrder, cache.nodeOrder[1:])
		cache.nodeOrder[len(cache.nodeOrder)-1] = nodeID
	} else {
		cache.nodeOrder = append(cache.nodeOrder, nodeID)
	}
	cache.edgeIDsByNode[nodeID] = edgeIDs
}

func (cache *snapshotAdjacencyCache) clear() {
	cache.edgeIDsByNode = nil
	cache.nodeOrder = nil
}

// getCommittedAdjacentEdgesLocked returns the committed edges adjacent to
// nodeID in the requested direction. Under an active snapshot (non-zero read
// timestamp) it resolves against the directional visible-at adjacency index so
// the cost is O(deg(nodeID)) rather than O(E): the previous implementation
// scanned every visible edge in the graph and filtered by endpoint in memory,
// which degraded linearly with the total edge count on large graphs. Pending
// transaction writes are merged by the caller.
func (tx *BadgerTransaction) getCommittedAdjacentEdgesLocked(nodeID NodeID, direction EdgeDirection) ([]*Edge, error) {
	if tx.readTS.IsZero() {
		switch direction {
		case Outgoing:
			return tx.engine.GetOutgoingEdges(nodeID)
		case Incoming:
			return tx.engine.GetIncomingEdges(nodeID)
		default:
			return nil, ErrInvalidData
		}
	}
	if tx.snapshotDeregister != nil {
		return tx.readCachedSnapshotAdjacentEdgesLocked(nodeID, direction, false)
	}
	switch direction {
	case Outgoing:
		if tx.snapshotDeregister != nil {
			return tx.engine.getOutgoingEdgesVisibleAtWithPinnedSnapshot(nodeID, tx.readTS, tx.withSnapshotViewLocked)
		}
		return tx.engine.getOutgoingEdgesVisibleAtWithView(nodeID, tx.readTS, tx.withSnapshotViewLocked)
	case Incoming:
		if tx.snapshotDeregister != nil {
			return tx.engine.getIncomingEdgesVisibleAtWithPinnedSnapshot(nodeID, tx.readTS, tx.withSnapshotViewLocked)
		}
		return tx.engine.getIncomingEdgesVisibleAtWithView(nodeID, tx.readTS, tx.withSnapshotViewLocked)
	default:
		return nil, ErrInvalidData
	}
}

// readCachedSnapshotAdjacentEdgesLocked reads nodeID's relationships at the
// pinned snapshot, reusing and filling the per-transaction adjacency cache.
func (tx *BadgerTransaction) readCachedSnapshotAdjacentEdgesLocked(nodeID NodeID, direction EdgeDirection, headersOnly bool) ([]*Edge, error) {
	cache := &tx.snapshotOutgoingAdjacency
	if direction == Incoming {
		cache = &tx.snapshotIncomingAdjacency
	}
	edgeIDs, cached := cache.load(nodeID)
	edges, snapshotEdgeIDs, err := tx.readSnapshotAdjacentEdgesLocked(nodeID, direction, edgeIDs, cached, headersOnly)
	if err != nil {
		return nil, err
	}
	if !cached {
		cache.store(nodeID, snapshotEdgeIDs)
	}
	return edges, nil
}

// readSnapshotAdjacentEdgesLocked returns nodeID's relationships visible at
// the snapshot. With headersOnly, a relationship whose adjacency entry value
// carries its type and other end is listed from that value when its MVCC head
// in the snapshot view is live and not newer than the read version, so no
// later change can differ from it; otherwise its snapshot record is read.
func (tx *BadgerTransaction) readSnapshotAdjacentEdgesLocked(nodeID NodeID, direction EdgeDirection, edgeIDs []EdgeID, cached, headersOnly bool) ([]*Edge, []EdgeID, error) {
	edges := make([]*Edge, 0, len(edgeIDs))
	err := tx.withSnapshotViewLocked(func(snapshot *badger.Txn) error {
		var err error
		if !cached {
			var prefix []byte
			switch direction {
			case Outgoing:
				prefix = tx.engine.mvccOutgoingAdjacencyPrefixString(nodeID)
			case Incoming:
				prefix = tx.engine.mvccIncomingAdjacencyPrefixString(nodeID)
			default:
				return ErrInvalidData
			}
			edgeIDs, err = tx.engine.collectVisibleAdjacencyEdgeIDsInTxn(snapshot, prefix, tx.readTS)
			if err != nil {
				return err
			}
			edges = make([]*Edge, 0, len(edgeIDs))
		}
		var headers map[EdgeID]*Edge
		if headersOnly {
			headers = tx.engine.adjacencyHeadersInTxn(snapshot, nodeID, direction)
		}
		for _, edgeID := range edgeIDs {
			var edge *Edge
			decayStable := !tx.engine.decayEnabled || tx.engine.revealAll.Load()
			if cachedEdge, ok := tx.snapshotEdgeByID[edgeID]; ok && decayStable {
				edge = copyEdge(cachedEdge)
			} else if header := tx.snapshotHeaderLocked(snapshot, headers, edgeID); header != nil {
				edge = header
			} else {
				var edgeErr error
				edge, edgeErr = tx.engine.getEdgeVisibleAtInTxn(snapshot, edgeID, tx.readTS)
				if edgeErr == ErrNotFound || edgeErr == ErrNotVisibleAtSnapshot {
					continue
				}
				if edgeErr != nil {
					return edgeErr
				}
				if decayStable {
					tx.cacheSnapshotEdgeByIDLocked(edge)
				}
			}
			if edge == nil {
				continue
			}
			if direction == Outgoing && edge.StartNode == nodeID || direction == Incoming && edge.EndNode == nodeID {
				edges = append(edges, edge)
			}
		}
		return nil
	})
	return edges, edgeIDs, err
}

// OutgoingEdgeHeaders implements EdgeHeaderReader at the transaction's
// snapshot, with its own writes merged as GetOutgoingEdges merges them.
func (tx *BadgerTransaction) OutgoingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return tx.edgeHeaders(nodeID, Outgoing)
}

// IncomingEdgeHeaders implements EdgeHeaderReader at the transaction's
// snapshot, with its own writes merged as GetIncomingEdges merges them.
func (tx *BadgerTransaction) IncomingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return tx.edgeHeaders(nodeID, Incoming)
}

// edgeHeaders lists the snapshot's relationships of nodeID as
// getCommittedAdjacentEdgesLocked does, taking each one's type and other end
// from its adjacency entry value where it can. It declines without a pinned
// snapshot and under decay filtering.
func (tx *BadgerTransaction) edgeHeaders(nodeID NodeID, direction EdgeDirection) ([]*Edge, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, true, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(nodeID)); err != nil {
		return nil, true, err
	}
	if tx.readTS.IsZero() || tx.snapshotDeregister == nil || tx.engine.decayEnabled && !tx.engine.revealAll.Load() {
		return nil, false, nil
	}
	committed, err := tx.readCachedSnapshotAdjacentEdgesLocked(nodeID, direction, true)
	if err != nil {
		return nil, true, err
	}
	return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool {
		if direction == Outgoing {
			return edge.StartNode == nodeID
		}
		return edge.EndNode == nodeID
	}), true, nil
}

// snapshotHeaderLocked returns edgeID's header from headers when its MVCC head
// in snapshot is live and not newer than the read version, else nil.
func (tx *BadgerTransaction) snapshotHeaderLocked(snapshot *badger.Txn, headers map[EdgeID]*Edge, edgeID EdgeID) *Edge {
	header, ok := headers[edgeID]
	if !ok {
		return nil
	}
	head, err := tx.engine.loadEdgeMVCCHeadInTxn(snapshot, edgeID)
	if err != nil || head.Tombstoned || tx.readTS.Compare(head.Version) < 0 {
		return nil
	}
	return header
}

// withSnapshotViewLocked keeps every snapshot read on the same physical Badger
// version. The MVCC namespace sequence alone can include an uncommitted peer's
// reservation. Fresh Views would admit that peer halfway through this reader.
// The separate read-only transaction does not enlarge the writer's SSI read set.
func (tx *BadgerTransaction) withSnapshotViewLocked(read func(*badger.Txn) error) error {
	if tx.snapshotTx != nil {
		return read(tx.snapshotTx)
	}
	// Legacy manually constructed transactions lack a lifetime-pinned reader.
	return tx.engine.withView(read)
}

// snapshotHeadConflict also compares physical publication versions: namespace
// MVCC reservations can predate this reader even when their data commits later.
// A separate read transaction preserves the existing consumer conflict shape
// without adding planning reads to the writer's Badger SSI set.
func (tx *BadgerTransaction) snapshotHeadConflict(key []byte, version MVCCVersion) (bool, error) {
	if tx.snapshotIsolationConflict(version) {
		return true, nil
	}
	if tx.snapshotTx == nil || key == nil {
		return false, nil
	}
	var changed bool
	err := tx.engine.withView(func(view *badger.Txn) error {
		var err error
		changed, err = tx.snapshotHeadConflictInView(view, key, version)
		return err
	})
	return changed, err
}

func (tx *BadgerTransaction) snapshotHeadConflictInView(view *badger.Txn, key []byte, version MVCCVersion) (bool, error) {
	if tx.snapshotIsolationConflict(version) {
		return true, nil
	}
	if tx.snapshotTx == nil || key == nil {
		return false, nil
	}
	item, err := view.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return item.Version() > tx.snapshotTx.ReadTs(), nil
}

func (tx *BadgerTransaction) snapshotHeadVersionConflict(version MVCCVersion, physicalVersion uint64) bool {
	if tx.snapshotIsolationConflict(version) {
		return true
	}
	return tx.snapshotTx != nil && physicalVersion > tx.snapshotTx.ReadTs()
}
