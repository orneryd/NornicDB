package cypher

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// propertyIndexLookup is the one lookup of label's property = value in a
// property index, as engine sees it. The index describes committed state, so
// inside a transaction the transaction's own node writes are merged in
// (storage.BadgerTransaction.MergePendingPropertyMatches): nodes it created
// or changed are listed by their current values, nodes it changed or deleted
// lose their committed entries (#809). The lookup stays an index lookup: a
// transaction that reads by an indexed property doesn't scan the label.
func propertyIndexLookup(ctx context.Context, engine storage.Engine, schema *storage.SchemaManager, label, property string, value interface{}) ([]storage.NodeID, error) {
	ids, err := schema.PropertyIndexLookup(ctx, label, property, value)
	if err != nil {
		return nil, err
	}
	if wrapper, transactional := engine.(*transactionStorageWrapper); transactional {
		return wrapper.tx.MergePendingPropertyMatches(ctx, ids, label, property, value)
	}
	return ids, nil
}

// compositeIndexLookup is the composite-index counterpart of
// propertyIndexLookup: a full or prefix lookup against committed state, with
// the transaction's own pending node writes merged in. full selects LookupFull
// (all index properties equal) over LookupPrefix (leading properties equal).
func compositeIndexLookup(ctx context.Context, engine storage.Engine, schema *storage.SchemaManager, idx *storage.CompositeIndex, values []interface{}, full bool) ([]storage.NodeID, error) {
	ids, err := schema.CompositeIndexLookup(ctx, idx, values, full)
	if err != nil {
		return nil, err
	}
	if wrapper, transactional := engine.(*transactionStorageWrapper); transactional {
		return wrapper.tx.MergePendingCompositeMatches(ctx, ids, idx.Label, idx.Properties, values)
	}
	return ids, nil
}

// labellessPropertyIndexUsable reports whether a pattern without a label may
// take its candidates from the property indexes read through engine. The
// indexes are per label, so their union for a property lists only the nodes
// that carry an indexed label. A committed lookup is complete only when an
// indexed label covers the namespace; transaction reads use the merged scan.
func labellessPropertyIndexUsable(ctx context.Context, engine storage.Engine, labels ...string) bool {
	if ctx.Err() != nil {
		return false
	}
	if _, transactional := engine.(*transactionStorageWrapper); transactional {
		return false
	}
	stats, supported := engine.(storage.LabelStatsEngine)
	if !supported || len(labels) == 0 {
		return false
	}
	total, err := engine.NodeCount(ctx)
	if err != nil {
		return false
	}
	for _, label := range labels {
		count, err := stats.NodeCountByLabel(ctx, label)
		if err == nil && count == total {
			return true
		}
	}
	return false
}

// orderedPropertyIndexSchema returns the schema whose property indexes can
// answer an ordered or not-null index scan through engine, or nil when they
// can't: inside a transaction that has written nodes, the order of the
// committed index isn't the transaction's order, so those scans read the
// transaction's merged label scan instead. A transaction that has written no
// nodes reads the index, as an auto-commit statement does.
func orderedPropertyIndexSchema(engine storage.Engine) *storage.SchemaManager {
	if wrapper, transactional := engine.(*transactionStorageWrapper); transactional && wrapper.tx.HasPendingNodeMutations() {
		return nil
	}
	return engine.GetSchema()
}

// transactionStorageWrapper wraps a BadgerTransaction to implement storage.Engine
// for use in implicit transaction execution. It routes writes through the transaction
// (for atomicity/rollback) and reads through the underlying engine (for performance).
type transactionStorageWrapper struct {
	statement           atomic.Pointer[compositeStatement]
	resolvedMode        atomic.Pointer[resolvedStatementMode]
	readScope           atomic.Pointer[executionReadScope]
	owner               *TransactionContext
	tx                  *storage.BadgerTransaction
	underlying          storage.Engine // For read operations not supported by transaction
	schema              *storage.SchemaManager
	commitActions       []func(context.Context) error
	namespace           string
	separator           string
	namespacePrefix     string
	namespacePrefixOnce sync.Once
	mutatedNodeIDs      map[string]struct{}
	mutatedNodeIDsMu    sync.Mutex

	// searchNodeRefreshes and searchEdgeRefreshes are what the transaction
	// changed for search. Search reads them back once the commit makes them
	// visible (refreshSearchAfterCommit), and never after a rollback.
	searchRefreshMu     sync.Mutex
	searchNodeRefreshes map[storage.NodeID]struct{}
	searchEdgeRefreshes map[storage.EdgeID]struct{}

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

// PropertyKeyKnown reports whether a write in the transaction's database has
// stored a value under name (storage.PropertyKeyLookup), from the engine the
// transaction reads through.
func (w *transactionStorageWrapper) PropertyKeyKnown(name string) bool {
	if lookup, ok := w.underlying.(storage.PropertyKeyLookup); ok {
		return lookup.PropertyKeyKnown(name)
	}
	registry, ok := w.underlying.(storage.PropertyKeyRegistry)
	return ok && registry.PropertyKeyKnownInNamespace(w.namespace, name)
}

func (w *transactionStorageWrapper) Namespace() string {
	if w == nil {
		return ""
	}
	return w.namespace
}

func (w *transactionStorageWrapper) initNamespacePrefix() {
	w.namespacePrefixOnce.Do(func() {
		if w.namespace != "" {
			w.namespacePrefix = w.namespace + w.separator
		}
	})
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
	// Lazy-sized map: autocommit transactions rarely touch more than a
	// handful of nodes, and an eager 1000-bucket table was ~5% of TCK
	// allocation space per statement (#48).
	w.txNodeLookupCache = make(map[string]*storage.Node)
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

func (w *transactionStorageWrapper) deferSearchRefresh(node storage.NodeID, edge storage.EdgeID) {
	w.searchRefreshMu.Lock()
	defer w.searchRefreshMu.Unlock()
	if node != "" {
		if w.searchNodeRefreshes == nil {
			w.searchNodeRefreshes = make(map[storage.NodeID]struct{})
		}
		w.searchNodeRefreshes[node] = struct{}{}
	}
	if edge != "" {
		if w.searchEdgeRefreshes == nil {
			w.searchEdgeRefreshes = make(map[storage.EdgeID]struct{})
		}
		w.searchEdgeRefreshes[edge] = struct{}{}
	}
}

func (w *transactionStorageWrapper) takeSearchRefreshes() (map[storage.NodeID]struct{}, map[storage.EdgeID]struct{}) {
	w.searchRefreshMu.Lock()
	defer w.searchRefreshMu.Unlock()
	nodes, edges := w.searchNodeRefreshes, w.searchEdgeRefreshes
	w.searchNodeRefreshes, w.searchEdgeRefreshes = nil, nil
	return nodes, edges
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
func (w *transactionStorageWrapper) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	return w.createNodes(ctx, []*storage.Node{node}, false)
}

// CreateMintedNode is CreateNode for a node whose ID the executor minted
// (generateID); see storage.BadgerTransaction.CreateMintedNode.
func (w *transactionStorageWrapper) CreateMintedNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	return w.createNodes(ctx, []*storage.Node{node}, true)
}

func (w *transactionStorageWrapper) UpdateNode(ctx context.Context, updateRequest storage.NodeUpdateRequest) error {
	node := updateRequest.After
	if node == nil {
		return storage.ErrInvalidData
	}
	if err := w.GetSchema().RegisterTokens(ctx, node.Labels, nil); err != nil {
		return err
	}
	if w.namespace == "" {
		err := w.tx.UpdateGraphNode(ctx, updateRequest)
		if err == nil {
			w.markMutatedNodeID(node.ID)
		}
		return err
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	updateRequest.After = namespaced
	err := w.tx.UpdateGraphNode(ctx, updateRequest)
	if err == nil {
		w.markMutatedNodeID(node.ID)
	}
	return err
}

func (w *transactionStorageWrapper) DeleteNode(ctx context.Context, id storage.NodeID) error {
	return w.tx.DeleteGraphNode(ctx, w.prefixNodeID(id), storage.CascadeEdges)
}

// DeleteGraphNode applies a transactional deletion policy in this namespace.
func (w *transactionStorageWrapper) DeleteGraphNode(ctx context.Context, id storage.NodeID, mode storage.NodeDeletionMode) error {
	return w.tx.DeleteGraphNode(ctx, w.prefixNodeID(id), mode)
}

// IncidentEdges is BadgerTransaction.IncidentEdges for this wrapper's
// namespace (storage.IncidentEdgeLister). It is a bulk read, so ctx is the
// caller's: it reaches the native incidence read and this wrapper's projection,
// and a canceled request returns its own error with no partial list.
func (w *transactionStorageWrapper) IncidentEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	edges, err := w.tx.IncidentEdges(ctx, w.prefixNodeID(nodeID))
	return projectTransactionRead(ctx, w, edges, err, readOwnAdjacency)
}

func (w *transactionStorageWrapper) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	return w.createEdges(ctx, []*storage.Edge{edge}, false)
}

// CreateMintedEdge is CreateEdge for a relationship whose ID the executor
// minted (generateID).
func (w *transactionStorageWrapper) CreateMintedEdge(ctx context.Context, edge *storage.Edge) error {
	return w.createEdges(ctx, []*storage.Edge{edge}, true)
}

// EdgeCreateWasNoOp exposes the transaction's read-only hidden-ID candidate
// disposition in user-ID space. It does not unwrap or route a mutation around
// any engine layer; the Badger commit remains the sole identity owner.
func (w *transactionStorageWrapper) EdgeCreateWasNoOp(id storage.EdgeID) bool {
	if w == nil || w.tx == nil {
		return false
	}
	return w.tx.EdgeCreateWasNoOp(w.prefixEdgeID(id))
}

func (w *transactionStorageWrapper) DeleteEdge(ctx context.Context, id storage.EdgeID) error {
	return w.tx.DeleteEdge(ctx, w.prefixEdgeID(id))
}

// Read operations - transaction supports GetNode, forward others to underlying
func (w *transactionStorageWrapper) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetNode(ctx, w.prefixNodeID(id))
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

// GetQueryNode implements storage.QueryNodeReader in the wrapper's
// namespace (BadgerTransaction.GetQueryNode): the node with its embedding
// metadata, without separately stored vectors.
func (w *transactionStorageWrapper) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetQueryNode(ctx, w.prefixNodeID(id))
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

// GetNodeProjected implements storage.ProjectedNodeReader in the wrapper's
// namespace, decoding only the requested properties (see
// BadgerTransaction.GetNodeProjected). Traversals use it with an empty
// property list to read an endpoint's labels without its properties (#1110).
func (w *transactionStorageWrapper) GetNodeProjected(ctx context.Context, id storage.NodeID, properties []string) (*storage.Node, error) {
	node, err := w.tx.GetNodeProjected(ctx, w.prefixNodeID(id), properties)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

// RelationshipEndpointVisible answers for the transaction, in its namespace.
func (w *transactionStorageWrapper) RelationshipEndpointVisible(id storage.NodeID) (visible, answered bool) {
	return w.tx.RelationshipEndpointVisible(w.prefixNodeID(id))
}

func (w *transactionStorageWrapper) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdge(ctx, id)
	}
	edge, err := w.tx.GetEdge(ctx, w.prefixEdgeID(id))
	if err != nil {
		return nil, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) UpdateEdge(ctx context.Context, updateRequest storage.EdgeUpdateRequest) error {
	edge := updateRequest.After
	if edge == nil {
		return storage.ErrInvalidData
	}
	if err := w.GetSchema().RegisterTokens(ctx, nil, []string{edge.Type}); err != nil {
		return err
	}
	if w.namespace == "" {
		return w.tx.UpdateEdge(ctx, updateRequest)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	updateRequest.After = namespaced
	return w.tx.UpdateEdge(ctx, updateRequest)
}

func (w *transactionStorageWrapper) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := w.tx.GetNodesByLabel(ctx, label)
	return projectTransactionRead(ctx, w, nodes, err, readScanned)
}

// StreamNodesByLabelProjected keeps labelled MATCH reads on the transaction's
// pinned snapshot while preserving the storage iterator's early-stop signal.
// Namespace conversion happens per visited node, so unconsumed nodes are never
// copied into a transaction-local result slice.
func (w *transactionStorageWrapper) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjected(ctx, label, properties, false, visit)
}

func (w *transactionStorageWrapper) streamNodesByLabelProjected(ctx context.Context, label string, properties []string, stored bool, visit func(*storage.Node) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	stream := w.tx.StreamNodesByLabelProjected
	if stored {
		stream = w.tx.StreamStoredNodesByLabelProjected
	}
	return stream(ctx, label, properties, func(node *storage.Node) error {
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
		{
			metadata := out.RuntimeMetadata()
			metadata.Database = w.namespace
			out.SetRuntimeMetadata(metadata)
		}
		return visit(&out)
	})
}

// StreamNodesWithOptions satisfies the storage.Engine streaming contract on the
// transaction view: the transaction's snapshot overlaid with its pending
// writes, streamed by BadgerTransaction.StreamNodesWithOptions with the
// caller's projection, PropertyFilter and prefix (within this wrapper's
// namespace). Materialising AllNodes here decoded every node in full for a
// label-less match inside a transaction (#824).
func (w *transactionStorageWrapper) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	if fn == nil {
		return storage.ErrInvalidData
	}
	if w.tx != nil {
		physical := opts
		if w.namespace != "" {
			w.initNamespacePrefix()
			physical.Prefix = w.namespacePrefix
			if opts.Prefix != "" {
				physical.Prefix += opts.Prefix
			}
		}
		return w.tx.StreamNodesWithOptions(ctx, physical, func(node *storage.Node) error {
			if w.namespace == "" {
				return fn(node)
			}
			// Streamed nodes are read-only for the caller, as in
			// StreamNodesByLabelProjected: strip the ID prefix on a shallow copy.
			out := *node
			out.ID = w.unprefixNodeID(out.ID)
			{
				metadata := out.RuntimeMetadata()
				metadata.Database = w.namespace
				out.SetRuntimeMetadata(metadata)
			}
			return fn(&out)
		})
	}
	nodes, err := w.AllNodes(ctx)
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

func (w *transactionStorageWrapper) GetFirstNodeByLabel(ctx context.Context, label string) (*storage.Node, error) {
	nodes, err := w.GetNodesByLabel(ctx, label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}

func (w *transactionStorageWrapper) ForEachNodeIDByLabel(ctx context.Context, label string, visit func(storage.NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if !w.tx.HasPendingNodeMutations() {
		if lookup, ok := w.underlying.(storage.LabelNodeIDLookupEngine); ok {
			return lookup.ForEachNodeIDByLabel(ctx, label, visit)
		}
	}

	nodes, err := w.GetNodesByLabel(ctx, label)
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

func (w *transactionStorageWrapper) GetOutgoingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.adjacencyEdges(ctx, adjacencyRead{kind: storage.TransactionReadOutgoing, node: nodeID})
}

// OutgoingEdgeHeaders lists the transaction's relationship headers in its
// namespace.
func (w *transactionStorageWrapper) OutgoingEdgeHeaders(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, bool, error) {
	edges, answered, err := w.tx.OutgoingEdgeHeaders(ctx, w.prefixNodeID(nodeID))
	return w.userEdgeHeaders(ctx, edges, answered, err)
}

// IncomingEdgeHeaders lists the transaction's relationship headers in its
// namespace.
func (w *transactionStorageWrapper) IncomingEdgeHeaders(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, bool, error) {
	edges, answered, err := w.tx.IncomingEdgeHeaders(ctx, w.prefixNodeID(nodeID))
	return w.userEdgeHeaders(ctx, edges, answered, err)
}

// userEdgeHeaders keeps the header read's answered/error contract around the
// wrapper's read exit under the same caller lifetime.
func (w *transactionStorageWrapper) userEdgeHeaders(ctx context.Context, edges []*storage.Edge, answered bool, err error) ([]*storage.Edge, bool, error) {
	if err != nil || !answered {
		return edges, answered, err
	}
	projected, err := projectTransactionRead(ctx, w, edges, nil, readOwnAdjacency)
	if err != nil {
		return nil, true, err
	}
	return projected, true, nil
}

func (w *transactionStorageWrapper) GetIncomingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.adjacencyEdges(ctx, adjacencyRead{kind: storage.TransactionReadIncoming, node: nodeID})
}

func (w *transactionStorageWrapper) GetEdgesBetween(ctx context.Context, startID, endID storage.NodeID) ([]*storage.Edge, error) {
	return w.adjacencyEdges(ctx, adjacencyRead{kind: storage.TransactionReadBetween, node: startID, other: endID})
}

// MatchEdgesBetween matches the transaction's edges of one type between two
// nodes, reading only the listed properties to match
// (storage.EdgesBetweenMatcher).
func (w *transactionStorageWrapper) MatchEdgesBetween(ctx context.Context, startID, endID storage.NodeID, edgeType string, properties []string, match func(*storage.Edge) bool) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.MatchEdgesBetween(ctx, startID, endID, edgeType, properties, match)
	}
	var candidate storage.Edge
	edges, err := w.tx.MatchEdgesBetween(ctx, w.prefixNodeID(startID), w.prefixNodeID(endID), edgeType, properties, func(edge *storage.Edge) bool {
		candidate = *edge
		candidate.ID = w.unprefixEdgeID(edge.ID)
		candidate.StartNode, candidate.EndNode = startID, endID
		return match(&candidate)
	})
	// Matching and projection share the caller lifetime.
	return projectTransactionRead(ctx, w, edges, err, readOwnAdjacency)
}

func (w *transactionStorageWrapper) GetEdgeBetween(ctx context.Context, startID, endID storage.NodeID, edgeType string) (*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdgeBetween(ctx, startID, endID, edgeType)
	}
	edge, err := w.tx.GetEdgeBetween(ctx, w.prefixNodeID(startID), w.prefixNodeID(endID), edgeType)
	if err != nil || edge == nil {
		return nil, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) GetEdgesByType(ctx context.Context, edgeType string) ([]*storage.Edge, error) {
	edges, err := w.tx.GetEdgesByType(ctx, edgeType)
	return projectTransactionRead(ctx, w, edges, err, readScanned)
}

func (w *transactionStorageWrapper) GetOutgoingEdgesVisibleAt(ctx context.Context, nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return w.visibleAtEdges(ctx, visibleAtEdgeRequest{kind: visibleAtOutgoing, node: nodeID, version: version})
}

func (w *transactionStorageWrapper) GetIncomingEdgesVisibleAt(ctx context.Context, nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return w.visibleAtEdges(ctx, visibleAtEdgeRequest{kind: visibleAtIncoming, node: nodeID, version: version})
}

func (w *transactionStorageWrapper) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return w.visibleAtEdges(ctx, visibleAtEdgeRequest{kind: visibleAtType, label: edgeType, version: version})
}

func (w *transactionStorageWrapper) GetEdgesBetweenVisibleAt(ctx context.Context, startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return w.visibleAtEdges(ctx, visibleAtEdgeRequest{kind: visibleAtBetween, node: startID, other: endID, version: version})
}

func (w *transactionStorageWrapper) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	nodes, err := w.tx.AllNodes(ctx)
	return projectTransactionRead(ctx, w, nodes, err, readScanned)
}

func (w *transactionStorageWrapper) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	edges, err := w.tx.AllEdges(ctx)
	return projectTransactionRead(ctx, w, edges, err, readScanned)
}

func (w *transactionStorageWrapper) GetInDegree(ctx context.Context, nodeID storage.NodeID) (int, error) {
	return w.underlying.GetInDegree(ctx, nodeID)
}

func (w *transactionStorageWrapper) GetOutDegree(ctx context.Context, nodeID storage.NodeID) (int, error) {
	return w.underlying.GetOutDegree(ctx, nodeID)
}

func (w *transactionStorageWrapper) GetSchema() *storage.SchemaManager {
	if w.schema != nil {
		return w.schema
	}
	return w.underlying.GetSchema()
}

// BulkCreateNodes shares node admission, namespace conversion and mutation
// tracking with single creates.
func (w *transactionStorageWrapper) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	_, err := w.createNodes(ctx, nodes, false)
	return err
}

// BulkCreateMintedNodes is BulkCreateNodes for nodes whose IDs the executor
// minted (CreateMintedNode).
func (w *transactionStorageWrapper) BulkCreateMintedNodes(ctx context.Context, nodes []*storage.Node) error {
	_, err := w.createNodes(ctx, nodes, true)
	return err
}

func (w *transactionStorageWrapper) createNodes(ctx context.Context, nodes []*storage.Node, minted bool) (storage.NodeID, error) {
	var logicalID storage.NodeID
	create := w.tx.CreateNode
	if minted {
		create = w.tx.CreateMintedNode
	}
	for _, node := range nodes {
		if node == nil {
			return "", storage.ErrInvalidData
		}
		if err := w.GetSchema().RegisterTokens(ctx, node.Labels, nil); err != nil {
			return "", err
		}
		body := node
		if w.namespace != "" {
			body = storage.CopyNode(node)
			body.ID = w.prefixNodeID(node.ID)
		}
		id, err := create(ctx, body)
		if err != nil {
			return "", err
		}
		logicalID = w.unprefixNodeID(id)
		w.markMutatedNodeID(logicalID)
	}
	return logicalID, nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(ctx context.Context, edges []*storage.Edge) error {
	return w.createEdges(ctx, edges, false)
}

// BulkCreateMintedEdges is BulkCreateEdges for relationships whose IDs the
// executor minted (CreateMintedEdge).
func (w *transactionStorageWrapper) BulkCreateMintedEdges(ctx context.Context, edges []*storage.Edge) error {
	return w.createEdges(ctx, edges, true)
}

func (w *transactionStorageWrapper) createEdges(ctx context.Context, edges []*storage.Edge, minted bool) error {
	if err := w.GetSchema().RegisterEdgeTokens(ctx, edges); err != nil {
		return err
	}
	var singleEdge [1]*storage.Edge
	namespaced := edges
	if w.namespace != "" {
		if len(edges) == 1 {
			namespaced = singleEdge[:]
		} else {
			namespaced = make([]*storage.Edge, len(edges))
		}
	}
	if w.namespace != "" {
		for i, edge := range edges {
			cp := storage.CopyEdge(edge)
			cp.ID = w.prefixEdgeID(edge.ID)
			cp.StartNode = w.prefixNodeID(edge.StartNode)
			cp.EndNode = w.prefixNodeID(edge.EndNode)
			namespaced[i] = cp
		}
	}
	if len(namespaced) == 1 {
		if minted {
			return w.tx.CreateMintedEdge(ctx, namespaced[0])
		}
		return w.tx.CreateEdge(ctx, namespaced[0])
	}
	if minted {
		return w.tx.BulkCreateMintedEdges(ctx, namespaced)
	}
	return w.tx.BulkCreateEdges(ctx, namespaced)
}

func (w *transactionStorageWrapper) BulkDeleteNodes(ctx context.Context, ids []storage.NodeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteGraphNode(ctx, w.prefixNodeID(id), storage.CascadeEdges); err != nil {
			return err
		}
	}
	return nil
}

// BulkDeleteEdges deletes ids in the transaction. Like the engines'
// BulkDeleteEdges, it skips IDs that do not exist, those the transaction
// already deleted included (#827).
func (w *transactionStorageWrapper) BulkDeleteEdges(ctx context.Context, ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteEdge(ctx, w.prefixEdgeID(id)); err != nil && !errors.Is(err, storage.ErrNotFound) {
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
	// The storage layer already returned a private copy (copyNode / a freshly
	// decoded node), so the Properties map and Labels slice are owned by this
	// caller. A shallow struct copy that rewrites the ID prefix is enough; the
	// previous storage.CopyNode deep-copied every property a second time just
	// to change the ID (#1110).
	out := *node
	out.ID = w.unprefixNodeID(node.ID)
	{
		metadata := out.RuntimeMetadata()
		metadata.Database = w.namespace
		out.SetRuntimeMetadata(metadata)
	}
	return &out
}

func (w *transactionStorageWrapper) toUserEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	out := storage.CopyEdge(edge)
	out.ID = w.unprefixEdgeID(out.ID)
	out.Database = w.namespace
	out.StartNode = w.unprefixNodeID(out.StartNode)
	out.EndNode = w.unprefixNodeID(out.EndNode)
	return out
}

func (w *transactionStorageWrapper) underlyingIsNamespaced() bool {
	scoped, ok := w.underlying.(interface{ Namespace() string })
	return ok && w.namespace != "" && scoped.Namespace() == w.namespace
}

func (w *transactionStorageWrapper) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return w.underlying.BatchGetNodes(ctx, ids)
}

func (w *transactionStorageWrapper) Close() error {
	// Don't close underlying engine
	return nil
}

// NodeCount is the transaction-visible number of nodes, as the derived
// counters below are: the committed count while the transaction has staged
// no node change, otherwise a count of the nodes it sees (#1076).
func (w *transactionStorageWrapper) NodeCount(ctx context.Context) (int64, error) {
	if w.countersDescribeTransaction() && !w.tx.HasPendingNodeMutations() {
		return w.underlying.NodeCount(ctx)
	}
	return countOfRead(w.AllNodes(ctx))
}

// The derived counters (node label counts; relationship type and positional
// (label, type) counts, issue #638) keep the count-only fast paths available
// inside an explicit transaction. What the transaction sees is the committed
// counter plus the delta its own staged writes will apply at commit
// (storage.BadgerTransaction.Pending*CountDelta), Neo4j's read-committed
// view, so a count stays O(1) after the transaction writes (#683). When the
// counters don't describe the transaction's namespace, a transaction with
// staged changes counts by scanning its visible nodes or relationships
// (the *Scan methods, which the fast paths are tested against).

// countersDescribeTransaction reports whether the underlying counters count
// the transaction's namespace.
func (w *transactionStorageWrapper) countersDescribeTransaction() bool {
	return w.namespace == "" || w.underlyingIsNamespaced()
}

// NodeCountByLabel is the transaction-visible count of nodes with label.
func (w *transactionStorageWrapper) NodeCountByLabel(ctx context.Context, label string) (int64, error) {
	if w.countersDescribeTransaction() {
		if counter, ok := w.underlying.(storage.LabelStatsEngine); ok {
			delta, tracked := w.tx.PendingNodeLabelCountDelta(w.namespace, label)
			if tracked || !w.tx.HasPendingNodeMutations() {
				count, err := counter.NodeCountByLabel(ctx, label)
				if err != nil {
					return 0, err
				}
				return count + delta, nil
			}
		}
	}
	return w.nodeCountByLabelScan(ctx, label)
}

func (w *transactionStorageWrapper) nodeCountByLabelScan(ctx context.Context, label string) (int64, error) {
	return countOfRead(w.GetNodesByLabel(ctx, label))
}

// EdgeCount is the transaction-visible number of relationships, as
// NodeCount is (#1076).
func (w *transactionStorageWrapper) EdgeCount(ctx context.Context) (int64, error) {
	if w.countersDescribeTransaction() && !w.tx.HasPendingEdgeMutations() {
		return w.underlying.EdgeCount(ctx)
	}
	return countOfRead(w.AllEdges(ctx))
}

// EdgeCountByType is the transaction-visible count of relationships of
// edgeType.
func (w *transactionStorageWrapper) EdgeCountByType(ctx context.Context, edgeType string) (int64, error) {
	if w.countersDescribeTransaction() {
		delta, tracked := w.tx.PendingEdgeTypeCountDelta(w.namespace, edgeType)
		if tracked || !w.tx.HasPendingEdgeMutations() {
			count, err := w.underlying.EdgeCountByType(ctx, edgeType)
			return count + delta, err
		}
	}
	return w.edgeCountByTypeScan(ctx, edgeType)
}

func (w *transactionStorageWrapper) edgeCountByTypeScan(ctx context.Context, edgeType string) (int64, error) {
	return countOfRead(w.GetEdgesByType(ctx, edgeType))
}

// EdgeCountByStartLabel / EdgeCountByEndLabel are the transaction-visible
// counts of relationships of edgeType whose start / end node has label (the
// one-labeled-endpoint count shapes). Relabelling a node changes them too,
// so without the staged deltas they need a transaction with no staged node
// or relationship changes.
func (w *transactionStorageWrapper) EdgeCountByStartLabel(ctx context.Context, label, edgeType string) (int64, error) {
	return w.edgeCountByEndpointLabel(ctx, label, edgeType, true)
}

func (w *transactionStorageWrapper) EdgeCountByEndLabel(ctx context.Context, label, edgeType string) (int64, error) {
	return w.edgeCountByEndpointLabel(ctx, label, edgeType, false)
}

func (w *transactionStorageWrapper) edgeCountByEndpointLabel(ctx context.Context, label, edgeType string, start bool) (int64, error) {
	// The staged positional deltas are not used: DETACH DELETE of a node whose
	// relationship the same transaction created stages them wrong (#741).
	if w.countersDescribeTransaction() && !w.tx.HasPendingEdgeMutations() && !w.tx.HasPendingNodeMutations() {
		if start {
			return w.underlying.EdgeCountByStartLabel(ctx, label, edgeType)
		}
		return w.underlying.EdgeCountByEndLabel(ctx, label, edgeType)
	}
	return w.edgeCountByEndpointLabelScan(ctx, label, edgeType, start)
}

func (w *transactionStorageWrapper) edgeCountByEndpointLabelScan(ctx context.Context, label, edgeType string, start bool) (int64, error) {
	edges, err := w.GetEdgesByType(ctx, edgeType)
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
		node, err := w.GetNode(ctx, nodeID)
		if err != nil || node == nil {
			continue
		}
		for _, nodeLabel := range node.Labels {
			if nodeLabel == label {
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

// WithStoreTransaction joins system-store mutations to this existing transaction.
func (w *transactionStorageWrapper) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return mutate(w)
}

// completeCommitActions is the sole completion owner for work admitted with the
// native transaction. Clear the queue first: a terminal path cannot replay it.
// ctx is the admitted commit's lifetime; it is the executing lifetime of every
// queued side effect, never the statement that queued it.
func (w *transactionStorageWrapper) completeCommitActions(ctx context.Context) error {
	actions := w.commitActions
	w.commitActions = nil
	failures := make([]error, len(actions))
	for i, action := range actions {
		failures[i] = action(ctx)
	}
	return nornicerrors.CombineTerminalOutcomes(nil, failures...)
}

// completeInlineEmbedding consumes this native owner's recorded mutations once.
// It does not end the transaction; its caller retains commit and rollback.
func (e *StorageExecutor) completeInlineEmbedding(ctx context.Context, wrapper *transactionStorageWrapper) error {
	mutated := wrapper.snapshotMutatedNodeIDs()
	if err := e.applyInlineEmbeddingMutations(ctx, mutated); err != nil {
		return err
	}
	wrapper.clearMutatedNodeIDs(mutated)
	return nil
}

// RecordMaterializedAccess preserves the underlying read-side access recorder.
// Access observations are not mutations owned by the entity transaction.
func (w *transactionStorageWrapper) RecordMaterializedAccess(entityID string) {
	if recorder, ok := w.underlying.(materializedAccessRecorder); ok {
		recorder.RecordMaterializedAccess(entityID)
	}
}

// hasPendingCommitEffects retains the original four-way implicit decision:
// entity operations, knowledge-policy schema changes, deferred validation, or
// completion actions. Ordinary schema DDL registers its onSchemaCommit action;
// no new schema heuristic changes when a transaction must commit.
func (w *transactionStorageWrapper) hasPendingCommitEffects() bool {
	return w.tx.OperationCount() != 0 || w.tx.HasKnowledgePolicyChanges() || w.tx.RequiresCommitValidation() || len(w.commitActions) != 0
}
