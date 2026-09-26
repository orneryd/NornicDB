package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
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

func (w *transactionStorageWrapper) GetInnerEngine() storage.Engine {
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

func (w *transactionStorageWrapper) clearMutatedNodeIDs(processed map[string]struct{}) {
	if len(processed) == 0 || len(w.mutatedNodeIDs) == 0 {
		return
	}
	w.mutatedNodeIDsMu.Lock()
	for id := range processed {
		delete(w.mutatedNodeIDs, id)
	}
	w.mutatedNodeIDsMu.Unlock()
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
	return w.toUserNamespacedNodes(nodes), nil
}

// StreamNodesByLabelProjected keeps labelled MATCH reads on the transaction's
// pinned snapshot while preserving the storage iterator's early-stop signal.
// Namespace conversion happens per visited node, so unconsumed nodes are never
// copied into a transaction-local result slice.
func (w *transactionStorageWrapper) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	return w.tx.StreamNodesByLabelProjected(label, properties, func(node *storage.Node) error {
		if node == nil {
			return nil
		}
		if w.namespace == "" {
			return visit(node)
		}
		if !strings.HasPrefix(string(node.ID), w.namespace+w.separator) {
			return nil
		}
		// Streaming readers treat nodes as immutable, matching
		// NamespacedEngine.StreamNodesByLabelProjected. Strip only the ID
		// prefix here; a second deep property copy would make every explicit
		// transaction scan allocate once more per visited node.
		out := *node
		out.ID = w.unprefixNodeID(out.ID)
		return visit(&out)
	})
}

// StreamNodesWithOptions satisfies the storage.Engine streaming contract on the
// transaction view. Reads route through the transaction's merged node view
// (pending writes included), with prefix scope and projection applied per node.
func (w *transactionStorageWrapper) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	if fn == nil {
		return storage.ErrInvalidData
	}
	nodes, err := w.GetNodesByLabel("")
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if node == nil {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(string(node.ID), opts.Prefix) {
			continue
		}
		out := node
		switch {
		case opts.Projection != nil:
			out = copyNodeProjected(node, opts.Projection)
		case opts.StripEmbeddings || (!opts.WithEmbeddings && !opts.ApplyDecayFilter):
			out = copyNodeWithoutEmbeddingVectors(node)
		}
		if err := fn(out); err != nil {
			if err == storage.ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// copyNodeProjected returns a node carrying only the requested user properties.
func copyNodeProjected(node *storage.Node, properties []string) *storage.Node {
	out := *node
	out.Properties = make(map[string]interface{}, len(properties))
	for _, property := range properties {
		if value, ok := node.Properties[property]; ok {
			out.Properties[property] = value
		}
	}
	return &out
}

// copyNodeWithoutEmbeddingVectors returns a node with embedding vector payloads
// removed while retaining metadata and user properties.
func copyNodeWithoutEmbeddingVectors(node *storage.Node) *storage.Node {
	out := *node
	out.ChunkEmbeddings = nil
	out.NamedEmbeddings = nil
	return &out
}

func (w *transactionStorageWrapper) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	nodes, err := w.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
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
	return w.toUserNamespacedEdges(edges), nil
}

func (w *transactionStorageWrapper) GetNodesByLabelVisibleAt(label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		nodes, err := provider.GetNodesByLabelVisibleAt(label, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return nodes, err
		}
		return w.toUserNamespacedNodes(nodes), nil
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	if w.underlyingIsNamespaced() {
		return provider.GetOutgoingEdgesVisibleAt(nodeID, version)
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
	if w.underlyingIsNamespaced() {
		return provider.GetIncomingEdgesVisibleAt(nodeID, version)
	}
	edges, err := provider.GetIncomingEdgesVisibleAt(w.prefixNodeID(nodeID), version)
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserNamespacedEdges(edges), nil
}

func (w *transactionStorageWrapper) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		edges, err := provider.GetEdgesByTypeVisibleAt(edgeType, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return edges, err
		}
		return w.toUserNamespacedEdges(edges), nil
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	if w.underlyingIsNamespaced() {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}
	edges, err := provider.GetEdgesBetweenVisibleAt(w.prefixNodeID(startID), w.prefixNodeID(endID), version)
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserNamespacedEdges(edges), nil
}

func (w *transactionStorageWrapper) AllNodes() ([]*storage.Node, error) {
	nodes, err := w.tx.AllNodes()
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNamespacedNodes(nodes), nil
}

func (w *transactionStorageWrapper) AllEdges() ([]*storage.Edge, error) {
	edges, err := w.tx.AllEdges()
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserNamespacedEdges(edges), nil
}

func (w *transactionStorageWrapper) GetAllNodes() []*storage.Node {
	nodes := w.tx.GetAllNodes()
	if w.namespace == "" {
		return nodes
	}
	return w.toUserNamespacedNodes(nodes)
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

// BulkCreateNodes creates the nodes one by one through CreateNode, so a bulk
// create inside a transaction is namespaced and recorded as mutated exactly
// like a single create.
func (w *transactionStorageWrapper) BulkCreateNodes(nodes []*storage.Node) error {
	for _, node := range nodes {
		if _, err := w.CreateNode(node); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(edges []*storage.Edge) error {
	if w.namespace == "" {
		return w.tx.BulkCreateEdges(edges)
	}
	namespaced := make([]*storage.Edge, len(edges))
	for i, edge := range edges {
		if edge == nil {
			// The transaction rejects it, as CreateEdge does.
			namespaced[i] = nil
			continue
		}
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

func (w *transactionStorageWrapper) toUserNamespacedNodes(nodes []*storage.Node) []*storage.Node {
	prefix := w.namespace + w.separator
	out := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if node != nil && strings.HasPrefix(string(node.ID), prefix) {
			out = append(out, w.toUserNode(node))
		}
	}
	return out
}

func (w *transactionStorageWrapper) underlyingIsNamespaced() bool {
	scoped, ok := w.underlying.(interface{ Namespace() string })
	return ok && w.namespace != "" && scoped.Namespace() == w.namespace
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

// NodeCountByLabel keeps the count-only MATCH fast path available inside an
// explicit transaction. A mutation-free snapshot can use the storage label
// counter directly; once the transaction stages node changes, count its
// transaction-visible label result so uncommitted creates and deletes retain
// Neo4j-compatible visibility.
func (w *transactionStorageWrapper) NodeCountByLabel(label string) (int64, error) {
	if (w.namespace == "" || w.underlyingIsNamespaced()) && !w.tx.HasPendingNodeMutations() {
		if counter, ok := w.underlying.(interface {
			NodeCountByLabel(string) (int64, error)
		}); ok {
			return counter.NodeCountByLabel(label)
		}
	}
	nodes, err := w.GetNodesByLabel(label)
	if err != nil {
		return 0, err
	}
	return int64(len(nodes)), nil
}

func (w *transactionStorageWrapper) EdgeCount() (int64, error) {
	return w.underlying.EdgeCount()
}

// EdgeCountByType keeps the typed relationship count inside an explicit
// transaction. A mutation-free snapshot uses the storage per-type counter
// directly (issue #638); once the transaction stages edge changes, it counts
// the transaction-visible typed edges so uncommitted creates and deletes
// retain Neo4j-compatible visibility (same shape as NodeCountByLabel).
func (w *transactionStorageWrapper) EdgeCountByType(edgeType string) (int64, error) {
	if (w.namespace == "" || w.underlyingIsNamespaced()) && !w.tx.HasPendingEdgeMutations() {
		return w.underlying.EdgeCountByType(edgeType)
	}
	edges, err := w.GetEdgesByType(edgeType)
	if err != nil {
		return 0, err
	}
	return int64(len(edges)), nil
}

// EdgeCountByStartLabel / EdgeCountByEndLabel keep the one-labeled-endpoint
// count shapes available inside an explicit transaction. A mutation-free
// snapshot uses the storage positional counters; a transaction with staged
// edge changes counts its transaction-visible typed edges by endpoint label.
func (w *transactionStorageWrapper) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return w.edgeCountByEndpointLabel(label, edgeType, true)
}

func (w *transactionStorageWrapper) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return w.edgeCountByEndpointLabel(label, edgeType, false)
}

func (w *transactionStorageWrapper) edgeCountByEndpointLabel(label, edgeType string, start bool) (int64, error) {
	if (w.namespace == "" || w.underlyingIsNamespaced()) && !w.tx.HasPendingEdgeMutations() {
		if start {
			return w.underlying.EdgeCountByStartLabel(label, edgeType)
		}
		return w.underlying.EdgeCountByEndLabel(label, edgeType)
	}
	edges, err := w.GetEdgesByType(edgeType)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		nodeID := edge.EndNode
		if start {
			nodeID = edge.StartNode
		}
		node, err := w.GetNode(nodeID)
		if err != nil || node == nil {
			continue
		}
		for _, nodeLabel := range node.Labels {
			if strings.EqualFold(nodeLabel, label) {
				count++
				break
			}
		}
	}
	return count, nil
}

func (w *transactionStorageWrapper) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// DeleteByPrefix is not supported within a transaction context.
	// This operation should be performed outside of a transaction.
	return 0, 0, localizedError(localization.CypherInvariantsDeleteByPrefixTransactionUnsupported(), nil)
}
