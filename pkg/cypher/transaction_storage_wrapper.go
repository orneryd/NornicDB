package cypher

import (
	"fmt"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// transactionStorageWrapper wraps a BadgerTransaction to implement storage.Engine
// for use in implicit transaction execution. It routes writes through the transaction
// (for atomicity/rollback) and reads through the underlying engine (for performance).
type transactionStorageWrapper struct {
	tx               *storage.BadgerTransaction
	underlying       storage.Engine // For read operations not supported by transaction
	namespace        string
	separator        string
	mutatedNodeIDs   map[string]struct{}
	mutatedNodeIDsMu sync.Mutex

	// txNodeLookupCache scopes the executor's MERGE/MATCH lookup cache to
	// this single transaction. Concurrent transactions get distinct
	// wrappers — and therefore distinct caches — so a peer's uncommitted
	// node-ID mapping cannot leak into this transaction's read set via
	// store.GetNode(...) inside tx.badgerTx (which would otherwise put
	// the peer's node key into Badger's SSI read set and convert a
	// constraint violation into a generic Transaction Conflict). Within
	// a single transaction the cache is shared across re-entries that
	// reuse the same wrapper, so multi-clause queries still benefit
	// from the cross-clause speedup.
	txNodeLookupCache   map[string]*storage.Node
	txNodeLookupCacheMu *sync.RWMutex
}

func (w *transactionStorageWrapper) Namespace() string {
	if w == nil {
		return ""
	}
	return w.namespace
}

// ensureNodeLookupCacheLocked lazily initializes the wrapper's MERGE/MATCH
// lookup cache, seeding it once from the parent executor's committed
// entries. The cache exists for the lifetime of the transaction; on
// commit, executor code drains it back into the parent executor via
// promoteNodeLookupCacheTo, on rollback the wrapper is discarded and the
// cache with it. Subsequent calls (e.g. recursive Execute re-entry on
// the same wrapper) are no-ops, so the in-tx state survives across
// multi-clause queries.
func (w *transactionStorageWrapper) ensureNodeLookupCacheLocked(seedFrom *StorageExecutor) {
	if w.txNodeLookupCacheMu != nil && w.txNodeLookupCache != nil {
		return
	}
	w.txNodeLookupCacheMu = &sync.RWMutex{}
	w.txNodeLookupCache = make(map[string]*storage.Node, 1000)
	if seedFrom == nil {
		return
	}
	srcMu := seedFrom.nodeLookupCacheLock()
	srcMu.RLock()
	for k, v := range seedFrom.nodeLookupCache {
		w.txNodeLookupCache[k] = v
	}
	srcMu.RUnlock()
}

func (w *transactionStorageWrapper) GetEngine() storage.Engine {
	if w == nil {
		return nil
	}
	return w.underlying
}

func (w *transactionStorageWrapper) markMutatedNodeID(id storage.NodeID) {
	if id == "" || w.mutatedNodeIDs == nil {
		return
	}
	w.mutatedNodeIDsMu.Lock()
	w.mutatedNodeIDs[string(id)] = struct{}{}
	w.mutatedNodeIDsMu.Unlock()
}

func (w *transactionStorageWrapper) snapshotMutatedNodeIDs() map[string]struct{} {
	if len(w.mutatedNodeIDs) == 0 {
		return nil
	}
	w.mutatedNodeIDsMu.Lock()
	defer w.mutatedNodeIDsMu.Unlock()
	out := make(map[string]struct{}, len(w.mutatedNodeIDs))
	for id := range w.mutatedNodeIDs {
		out[id] = struct{}{}
	}
	return out
}

// Write operations - go through transaction for atomicity
func (w *transactionStorageWrapper) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if w.namespace == "" {
		id, err := w.tx.CreateNode(node)
		if err == nil {
			w.markMutatedNodeID(id)
		}
		return id, err
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	actualID, err := w.tx.CreateNode(namespaced)
	if err != nil {
		return "", err
	}
	userID := w.unprefixNodeID(actualID)
	w.markMutatedNodeID(userID)
	return userID, nil
}

func (w *transactionStorageWrapper) UpdateNode(node *storage.Node) error {
	if w.namespace == "" {
		err := w.tx.UpdateNode(node)
		if err == nil {
			w.markMutatedNodeID(node.ID)
		}
		return err
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	err := w.tx.UpdateNode(namespaced)
	if err == nil {
		w.markMutatedNodeID(node.ID)
	}
	return err
}

func (w *transactionStorageWrapper) DeleteNode(id storage.NodeID) error {
	return w.tx.DeleteNode(w.prefixNodeID(id))
}

func (w *transactionStorageWrapper) CreateEdge(edge *storage.Edge) error {
	if w.namespace == "" {
		return w.tx.CreateEdge(edge)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	return w.tx.CreateEdge(namespaced)
}

func (w *transactionStorageWrapper) DeleteEdge(id storage.EdgeID) error {
	return w.tx.DeleteEdge(w.prefixEdgeID(id))
}

// Read operations - transaction supports GetNode, forward others to underlying
func (w *transactionStorageWrapper) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetNode(w.prefixNodeID(id))
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdge(id)
	}
	edge, err := w.tx.GetEdge(w.prefixEdgeID(id))
	if err != nil {
		return nil, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) UpdateEdge(edge *storage.Edge) error {
	if w.namespace == "" {
		return w.tx.UpdateEdge(edge)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	return w.tx.UpdateEdge(namespaced)
}

func (w *transactionStorageWrapper) GetNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := w.tx.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNodes(nodes), nil
}

func (w *transactionStorageWrapper) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	node, err := w.tx.GetFirstNodeByLabel(label)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if !w.tx.HasPendingNodeMutations() {
		if lookup, ok := w.underlying.(storage.LabelNodeIDLookupEngine); ok {
			return lookup.ForEachNodeIDByLabel(label, visit)
		}
	}

	nodes, err := w.GetNodesByLabel(label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !visit(node.ID) {
			return nil
		}
	}
	return nil
}

func (w *transactionStorageWrapper) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetOutgoingEdges(nodeID)
	}
	edges, err := w.tx.GetOutgoingEdges(w.prefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetIncomingEdges(nodeID)
	}
	edges, err := w.tx.GetIncomingEdges(w.prefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdgesBetween(startID, endID)
	}
	edges, err := w.tx.GetEdgesBetween(w.prefixNodeID(startID), w.prefixNodeID(endID))
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	if w.namespace == "" {
		return w.tx.GetEdgeBetween(startID, endID, edgeType)
	}
	edge := w.tx.GetEdgeBetween(w.prefixNodeID(startID), w.prefixNodeID(endID), edgeType)
	if edge == nil {
		return nil
	}
	return w.toUserEdge(edge)
}

func (w *transactionStorageWrapper) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdgesByType(edgeType)
	}
	edges, err := w.tx.GetEdgesByType(edgeType)
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetNodesByLabelVisibleAt(label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	edges, err := provider.GetOutgoingEdgesVisibleAt(w.prefixNodeID(nodeID), version)
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserNamespacedEdges(edges), nil
}

func (w *transactionStorageWrapper) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	edges, err := provider.GetIncomingEdgesVisibleAt(w.prefixNodeID(nodeID), version)
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserNamespacedEdges(edges), nil
}

func (w *transactionStorageWrapper) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) AllNodes() ([]*storage.Node, error) {
	nodes, err := w.tx.AllNodes()
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNodes(nodes), nil
}

func (w *transactionStorageWrapper) AllEdges() ([]*storage.Edge, error) {
	return w.underlying.AllEdges()
}

func (w *transactionStorageWrapper) GetAllNodes() []*storage.Node {
	nodes := w.tx.GetAllNodes()
	if w.namespace == "" {
		return nodes
	}
	return w.toUserNodes(nodes)
}

func (w *transactionStorageWrapper) GetInDegree(nodeID storage.NodeID) int {
	return w.underlying.GetInDegree(nodeID)
}

func (w *transactionStorageWrapper) GetOutDegree(nodeID storage.NodeID) int {
	return w.underlying.GetOutDegree(nodeID)
}

func (w *transactionStorageWrapper) GetSchema() *storage.SchemaManager {
	return w.underlying.GetSchema()
}

func (w *transactionStorageWrapper) BulkCreateNodes(nodes []*storage.Node) error {
	// For bulk operations within transaction, create one by one
	for _, node := range nodes {
		if w.namespace == "" {
			if _, err := w.tx.CreateNode(node); err != nil {
				return err
			}
			continue
		}
		namespaced := storage.CopyNode(node)
		namespaced.ID = w.prefixNodeID(node.ID)
		if _, err := w.tx.CreateNode(namespaced); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(edges []*storage.Edge) error {
	if len(edges) == 0 {
		return nil
	}
	if w.namespace == "" {
		return w.tx.BulkCreateEdges(edges)
	}
	namespaced := make([]*storage.Edge, len(edges))
	for i, edge := range edges {
		cp := storage.CopyEdge(edge)
		cp.ID = w.prefixEdgeID(edge.ID)
		cp.StartNode = w.prefixNodeID(edge.StartNode)
		cp.EndNode = w.prefixNodeID(edge.EndNode)
		namespaced[i] = cp
	}
	return w.tx.BulkCreateEdges(namespaced)
}

func (w *transactionStorageWrapper) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteNode(w.prefixNodeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteEdge(w.prefixEdgeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) prefixNodeID(id storage.NodeID) storage.NodeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	if strings.HasPrefix(string(id), prefix) {
		return id
	}
	return storage.NodeID(w.namespace + w.separator + string(id))
}

func (w *transactionStorageWrapper) unprefixNodeID(id storage.NodeID) storage.NodeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return storage.NodeID(s[len(prefix):])
	}
	return id
}

func (w *transactionStorageWrapper) prefixEdgeID(id storage.EdgeID) storage.EdgeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	if strings.HasPrefix(string(id), prefix) {
		return id
	}
	return storage.EdgeID(w.namespace + w.separator + string(id))
}

func (w *transactionStorageWrapper) unprefixEdgeID(id storage.EdgeID) storage.EdgeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return storage.EdgeID(s[len(prefix):])
	}
	return id
}

func (w *transactionStorageWrapper) toUserNode(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	out := storage.CopyNode(node)
	out.ID = w.unprefixNodeID(out.ID)
	return out
}

func (w *transactionStorageWrapper) toUserEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	out := storage.CopyEdge(edge)
	out.ID = w.unprefixEdgeID(out.ID)
	out.StartNode = w.unprefixNodeID(out.StartNode)
	out.EndNode = w.unprefixNodeID(out.EndNode)
	return out
}

func (w *transactionStorageWrapper) toUserEdges(edges []*storage.Edge) []*storage.Edge {
	out := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, w.toUserEdge(edge))
	}
	return out
}

func (w *transactionStorageWrapper) toUserNamespacedEdges(edges []*storage.Edge) []*storage.Edge {
	prefix := w.namespace + w.separator
	out := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		if edge == nil || !strings.HasPrefix(string(edge.ID), prefix) {
			continue
		}
		out = append(out, w.toUserEdge(edge))
	}
	return out
}

func (w *transactionStorageWrapper) toUserNodes(nodes []*storage.Node) []*storage.Node {
	out := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, w.toUserNode(node))
	}
	return out
}

func (w *transactionStorageWrapper) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return w.underlying.BatchGetNodes(ids)
}

func (w *transactionStorageWrapper) Close() error {
	// Don't close underlying engine
	return nil
}

func (w *transactionStorageWrapper) NodeCount() (int64, error) {
	return w.underlying.NodeCount()
}

func (w *transactionStorageWrapper) EdgeCount() (int64, error) {
	return w.underlying.EdgeCount()
}

func (w *transactionStorageWrapper) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// DeleteByPrefix is not supported within a transaction context.
	// This operation should be performed outside of a transaction.
	return 0, 0, fmt.Errorf("DeleteByPrefix not supported within transaction context")
}
