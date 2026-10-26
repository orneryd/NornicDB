package cypher

import (
	"context"
	"errors"
	"strings"
	"sync"

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
	member, _ := transactionMemberForEngine(engine)
	return lookupPropertyIndex(ctx, member, schema, label, property, value)
}

func lookupPropertyIndex(ctx context.Context, wrapper *transactionStorageWrapper, schema *storage.SchemaManager, label, property string, value interface{}) ([]storage.NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids, err := schema.PropertyIndexLookupWithContext(ctx, label, property, value)
	if err != nil {
		return nil, err
	}
	if wrapper != nil {
		return wrapper.tx.MergePendingPropertyMatchesWithContext(ctx, ids, label, property, value)
	}
	return ids, ctx.Err()
}

// compositeIndexLookup is the composite-index counterpart of
// propertyIndexLookup: a full or prefix lookup against committed state, with
// the transaction's own pending node writes merged in. full selects LookupFull
// (all index properties equal) over LookupPrefix (leading properties equal).
func compositeIndexLookup(ctx context.Context, engine storage.Engine, idx *storage.CompositeIndex, values []interface{}, full bool) ([]storage.NodeID, error) {
	member, _ := transactionMemberForEngine(engine)
	return lookupCompositeIndex(ctx, member, idx, values, full)
}

func lookupCompositeIndex(ctx context.Context, wrapper *transactionStorageWrapper, idx *storage.CompositeIndex, values []interface{}, full bool) ([]storage.NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var ids []storage.NodeID
	var err error
	if full {
		ids, err = idx.LookupFullWithContext(ctx, values...)
	} else {
		ids, err = idx.LookupPrefixWithContext(ctx, values...)
	}
	if err != nil {
		return nil, err
	}
	if wrapper != nil {
		return wrapper.tx.MergePendingCompositeMatchesWithContext(ctx, ids, idx.Label, idx.Properties, values)
	}
	return ids, ctx.Err()
}

// labellessPropertyIndexUsable reports whether a pattern without a label may
// take its candidates from the property indexes read through engine. The
// indexes are per label, so their union for a property lists only the nodes
// that carry an indexed label. A committed lookup is complete only when an
// indexed label covers the namespace; transaction reads use the merged scan.
func (e *StorageExecutor) labellessPropertyIndexUsable(ctx context.Context, engine storage.Engine, labels ...string) bool {
	if _, transactional := e.nativeMemberFor(engine); transactional {
		return false
	}
	supported := storage.HasReadCapability[storage.LabelStatsEngine](engine)
	if !supported || len(labels) == 0 {
		return false
	}
	total, err := engine.NodeCount()
	if err != nil {
		return false
	}
	for _, label := range labels {
		count, err := storage.ReadWithCapability(engine, func(stats storage.LabelStatsEngine) (int64, error) {
			return storage.NodeCountByLabelWithContext(ctx, stats, label)
		}, nil)
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
func (e *StorageExecutor) orderedPropertyIndexSchema(engine storage.Engine) *storage.SchemaManager {
	if wrapper, transactional := e.nativeMemberFor(engine); transactional && wrapper.tx.HasPendingNodeMutations() {
		return nil
	}
	return engine.GetSchema()
}

// transactionStorageWrapper wraps a BadgerTransaction to implement storage.Engine
// for use in implicit transaction execution. It routes writes through the transaction
// (for atomicity/rollback) and reads through the underlying engine (for performance).
type transactionStorageWrapper struct {
	lifecycleOwner any
	membershipOnce sync.Once
	membershipKind func() storage.MembershipKind
	controller     *StorageExecutor
	ownerOnce      sync.Once
	commitOwner    storage.TransactionCommitter
	leaseMu        sync.Mutex
	leaseContext   context.Context
	statementMode  *resolvedStatementMode

	tx                  *storage.BadgerTransaction
	underlying          storage.Engine // For read operations not supported by transaction
	schema              *storage.SchemaManager
	schemaCommitActions []func()
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

// transactionStorageWrapper declares every optional method; admission is derived from this policy.
var transactionStorageWrapperReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "BatchGetNodesWithContext", Disposition: storage.CapabilityOwned, Scope: "Uses the native transaction batch point-view owner and applies this issuance namespace; pending writes, pinned bodies and ended lifetime cannot delegate to committed storage."},
	{Methods: "BulkCreateEdgesWithContext", Disposition: storage.CapabilityRefused, Scope: "Calls createEdgesWithContext in batch mode, retaining schema token registration, namespace input copies and the same native transaction mutation."},
	{Methods: "BulkCreateMintedEdgesWithContext", Disposition: storage.CapabilityRefused, Scope: "Selects minted creation through createEdgesWithContext; exact native transaction admission and rollback remain on this issued mutation adapter."},
	{Methods: "CreateEdgeWithContext", Disposition: storage.CapabilityRefused, Scope: "Selects single-edge creation through createEdgesWithContext with schema token registration and namespace endpoint translation; not a data-read operation."},
	{Methods: "CreateMintedEdgeWithContext", Disposition: storage.CapabilityRefused, Scope: "Selects minted single-edge creation through the same input/schema/transaction mutation owner, never optional read discovery."},
	{Methods: "DeleteGraphNodeWithContext", Disposition: storage.CapabilityRefused, Scope: "Prefixes the exact requested node and invokes the native transaction cascade/reject policy owner; optional read selection cannot authorize deletion."},
	{Methods: "DeleteNodeWithContext", Disposition: storage.CapabilityRefused, Scope: "Chooses CascadeEdges via the issued DeleteGraphNodeWithContext adapter, retaining native transaction rollback and namespace."},
	{Methods: "IncidentEdgesWithContext", Disposition: storage.CapabilityOwned, Scope: "Reads the native pending/snapshot incident set without ordinary fallback and converts its node/edge namespace before returning."},
	{Methods: "BindQueryNodeRead BindEdgeHeaderRead", Disposition: storage.CapabilityRefused, Scope: "Typed operation issuance protocol, not an optional data read."},
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityOwned, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "BulkCreateMintedNodesWithContext", Disposition: storage.CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "BulkCreateNodesWithContext", Disposition: storage.CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "CreateMintedNodeWithContext", Disposition: storage.CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "CreateNodeWithContext", Disposition: storage.CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "UpdateNodeWithContext", Disposition: storage.CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "AllNodesWithContext ForEachNodeIDByLabelWithContext GetFirstNodeByLabelWithContext GetNodesByLabelVisibleAtWithContext GetNodesByLabelWithContext GetStoredNodesByLabelWithContext NodeCountByLabelWithContext StreamNodesByLabelProjectedWithContext StreamStoredNodesByLabelProjectedWithContext", Disposition: storage.CapabilityOwned, Scope: "Context-bearing reads preserve this view and the original caller."},
	{Methods: "ForEachNodeIDByLabel GetEdgesBetweenVisibleAt GetEdgesByTypeVisibleAt GetIncomingEdgesVisibleAt GetNodeProjected " +
		"GetNodesByLabelVisibleAt GetOutgoingEdgesVisibleAt GetQueryNode GetStoredEdge GetStoredIncomingEdges " +
		"GetStoredNode GetStoredNodesByLabel GetStoredOutgoingEdges HasManualSuppressions IncidentEdges " +
		"IncomingEdgeHeaders IsDecayEnabled MatchEdgesBetween Namespace " +
		"NodeCountByLabel OutgoingEdgeHeaders PropertyKeyKnown StreamEdgesByType StreamNodesByLabelProjected " +
		"StreamStoredEdgesByType StreamStoredNodesByLabelProjected",
		Disposition: storage.CapabilityOwned, Scope: "Pinned transaction and pending writes; namespace conversion and historical dependency checks."},
	{Methods: "SetManualSuppression BindReadCapabilityGuard CommitTransaction BulkCreateMintedEdges BulkCreateMintedNodes CreateMintedEdge CreateMintedNode DeleteGraphNode " +
		"EdgeCreateWasNoOp GetInnerEngine PrepareMergeNode PrepareMergeNodeForWrite RelationshipEndpointVisible " +
		"WithStoreTransaction",
		Disposition: storage.CapabilityRefused, Scope: "No scoped optional read contract on this view; configuration, mutation and maintenance require their exact operation owner."},
}

func (*transactionStorageWrapper) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return transactionStorageWrapperReadPolicy.Copy()
}

// PropertyKeyKnown reports whether a write in the transaction's database has
// stored a value under name (storage.PropertyKeyLookup), from the engine the
// transaction reads through.
func (w *transactionStorageWrapper) PropertyKeyKnown(name string) bool {
	if lookup, ok := storage.ViewMetadataForEngine[storage.PropertyKeyLookup](w.underlying); ok {
		return lookup.PropertyKeyKnown(name)
	}
	registry, ok := storage.ViewMetadataForEngine[storage.NamespacePropertyKeyLookup](w.underlying)
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
func (w *transactionStorageWrapper) CreateNode(node *storage.Node) (storage.NodeID, error) {
	return w.CreateNodeWithContext(context.Background(), node)
}

// CreateNodeWithContext carries the caller through mutation validation.
func (w *transactionStorageWrapper) CreateNodeWithContext(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	return w.createNodesWithContext(ctx, []*storage.Node{node}, false)
}

// CreateMintedNode is CreateNode for a node whose ID the executor minted
// (generateID); see storage.BadgerTransaction.CreateMintedNode.
func (w *transactionStorageWrapper) CreateMintedNode(node *storage.Node) (storage.NodeID, error) {
	return w.CreateMintedNodeWithContext(context.Background(), node)
}

// CreateMintedNodeWithContext carries the caller through mutation validation.
func (w *transactionStorageWrapper) CreateMintedNodeWithContext(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	return w.createNodesWithContext(ctx, []*storage.Node{node}, true)
}

func (w *transactionStorageWrapper) UpdateNode(node *storage.Node) error {
	return w.UpdateNodeWithContext(context.Background(), node)
}

// UpdateNodeWithContext carries the caller through mutation validation.
func (w *transactionStorageWrapper) UpdateNodeWithContext(ctx context.Context, node *storage.Node) error {
	if node == nil {
		return storage.ErrInvalidData
	}
	if err := w.GetSchema().RegisterTokens(node.Labels, nil); err != nil {
		return err
	}
	if w.namespace == "" {
		err := w.tx.UpdateGraphNodeWithContext(ctx, node, storage.ClientNodeEdits)
		if err == nil {
			w.markMutatedNodeID(node.ID)
		}
		return err
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	err := w.tx.UpdateGraphNodeWithContext(ctx, namespaced, storage.ClientNodeEdits)
	if err == nil {
		w.markMutatedNodeID(node.ID)
	}
	return err
}

func (w *transactionStorageWrapper) DeleteNode(id storage.NodeID) error {
	return w.DeleteNodeWithContext(context.Background(), id)
}

func (w *transactionStorageWrapper) DeleteNodeWithContext(ctx context.Context, id storage.NodeID) error {
	return w.DeleteGraphNodeWithContext(ctx, id, storage.CascadeEdges)
}

// DeleteGraphNode applies a transactional deletion policy in this namespace.
func (w *transactionStorageWrapper) DeleteGraphNode(id storage.NodeID, mode storage.NodeDeletionMode) error {
	return w.DeleteGraphNodeWithContext(context.Background(), id, mode)
}

// DeleteGraphNodeWithContext uses the native transaction's one cascade owner.
func (w *transactionStorageWrapper) DeleteGraphNodeWithContext(ctx context.Context, id storage.NodeID, mode storage.NodeDeletionMode) error {
	return w.tx.DeleteGraphNodeWithContext(ctx, w.prefixNodeID(id), mode)
}

// IncidentEdges is BadgerTransaction.IncidentEdges for this wrapper's
// namespace (storage.IncidentEdgeLister).
func (w *transactionStorageWrapper) IncidentEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.IncidentEdgesWithContext(context.Background(), nodeID)
}

func (w *transactionStorageWrapper) IncidentEdgesWithContext(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return storage.ReadWithCapability(w.tx, func(reader storage.IncidentEdgeLister) ([]*storage.Edge, error) {
			return w.tx.IncidentEdgesWithContext(ctx, nodeID)
		}, nil)
	}
	edges, err := storage.ReadWithCapability(w.tx, func(reader storage.IncidentEdgeLister) ([]*storage.Edge, error) {
		return w.tx.IncidentEdgesWithContext(ctx, w.prefixNodeID(nodeID))
	}, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out = append(out, w.toUserEdge(edge))
	}
	return out, nil
}

func (w *transactionStorageWrapper) CreateEdge(edge *storage.Edge) error {
	return w.createEdges([]*storage.Edge{edge}, false)
}

// CreateMintedEdge is CreateEdge for a relationship whose ID the executor
// minted (generateID).
func (w *transactionStorageWrapper) CreateMintedEdge(edge *storage.Edge) error {
	return w.createEdges([]*storage.Edge{edge}, true)
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

// GetQueryNode implements storage.QueryNodeReader in the wrapper's
// namespace (BadgerTransaction.GetQueryNode): the node with its embedding
// metadata, without separately stored vectors.
func (w *transactionStorageWrapper) GetQueryNode(id storage.NodeID) (*storage.Node, error) {
	return w.BindQueryNodeRead()(id)
}

// GetNodeProjected implements storage.ProjectedNodeReader in the wrapper's
// namespace, decoding only the requested properties (see
// BadgerTransaction.GetNodeProjected). Traversals use it with an empty
// property list to read an endpoint's labels without its properties (#1110).
func (w *transactionStorageWrapper) GetNodeProjected(id storage.NodeID, properties []string) (*storage.Node, error) {
	node, err := storage.ReadWithCapability(w.tx, func(reader storage.ProjectedNodeReader) (*storage.Node, error) {
		return reader.GetNodeProjected(w.prefixNodeID(id), properties)
	}, nil)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

// RelationshipEndpointVisible answers for the transaction, in its namespace.
func (w *transactionStorageWrapper) RelationshipEndpointVisible(id storage.NodeID) (bool, bool) {
	return storage.RelationshipEndpointVisible(w.tx, w.prefixNodeID(id))
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
	if edge == nil {
		return storage.ErrInvalidData
	}
	if err := w.GetSchema().RegisterTokens(nil, []string{edge.Type}); err != nil {
		return err
	}
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
	return w.GetNodesByLabelWithContext(context.Background(), label)
}

// GetNodesByLabelWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) GetNodesByLabelWithContext(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := storage.GetNodesByLabelWithContext(ctx, w.tx, label)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNamespacedNodesWithContext(ctx, nodes)
}

// StreamNodesByLabelProjected keeps labelled MATCH reads on the transaction's
// pinned snapshot while preserving the storage iterator's early-stop signal.
// Namespace conversion happens per visited node, so unconsumed nodes are never
// copied into a transaction-local result slice.
func (w *transactionStorageWrapper) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	return w.StreamNodesByLabelProjectedWithContext(context.Background(), label, properties, visit)
}

// StreamNodesByLabelProjectedWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) StreamNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjectedWithContext(ctx, label, properties, false, visit)
}

func (w *transactionStorageWrapper) streamNodesByLabelProjected(label string, properties []string, stored bool, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjectedWithContext(context.Background(), label, properties, stored, visit)
}

func (w *transactionStorageWrapper) streamNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, stored bool, visit func(*storage.Node) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	stream := func(label string, properties []string, visit func(*storage.Node) error) error {
		if stored {
			return storage.VisitWithCapability(w.tx, func(reader storage.StoredEntityReader, deliver func(*storage.Node) error) error {
				return storage.StreamStoredNodesByLabelProjectedWithContext(ctx, reader, label, properties, deliver)
			}, nil, visit)
		}
		return storage.VisitWithCapability(w.tx, func(reader storage.ProjectedLabelNodeReader, deliver func(*storage.Node) error) error {
			return storage.StreamNodesByLabelProjectedWithContext(ctx, reader, label, properties, deliver)
		}, nil, visit)
	}
	return stream(label, properties, func(node *storage.Node) error {
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
		out.Database = w.namespace
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
			out.Database = w.namespace
			return fn(&out)
		})
	}
	return storage.ErrNotImplemented
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
	return w.GetFirstNodeByLabelWithContext(context.Background(), label)
}

// GetFirstNodeByLabelWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) GetFirstNodeByLabelWithContext(ctx context.Context, label string) (*storage.Node, error) {
	nodes, err := storage.GetNodesByLabelWithContext(ctx, w, label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}

func (w *transactionStorageWrapper) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	return w.ForEachNodeIDByLabelWithContext(context.Background(), label, visit)
}

// ForEachNodeIDByLabelWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) ForEachNodeIDByLabelWithContext(ctx context.Context, label string, visit func(storage.NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if !w.tx.HasPendingNodeMutations() {
		return storage.VisitNodeIDsByLabel(w.underlying, label, visit)
	}
	nodes, err := w.GetNodesByLabelWithContext(ctx, label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node != nil && !visit(node.ID) {
			break
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

// OutgoingEdgeHeaders lists the transaction's relationship headers in its
// namespace.
func (w *transactionStorageWrapper) OutgoingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	return w.BindEdgeHeaderRead(true)(id)
}

// IncomingEdgeHeaders lists the transaction's relationship headers in its
// namespace.
func (w *transactionStorageWrapper) IncomingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	return w.BindEdgeHeaderRead(false)(id)
}

func (w *transactionStorageWrapper) userEdgeHeaders(edges []*storage.Edge, answered bool, err error) ([]*storage.Edge, bool, error) {
	if err != nil || !answered || w.namespace == "" {
		return edges, answered, err
	}
	return w.toUserEdges(edges), true, nil
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

// MatchEdgesBetween matches the transaction's edges of one type between two
// nodes, reading only the listed properties to match
// (storage.EdgesBetweenMatcher).
func (w *transactionStorageWrapper) MatchEdgesBetween(startID, endID storage.NodeID, edgeType string, properties []string, match func(*storage.Edge) bool) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return storage.MatchEdgesBetween(w.tx, startID, endID, edgeType, properties, match)
	}
	var candidate storage.Edge
	edges, err := storage.MatchEdgesBetween(w.tx, w.prefixNodeID(startID), w.prefixNodeID(endID), edgeType, properties, func(edge *storage.Edge) bool {
		candidate = *edge
		candidate.ID = w.unprefixEdgeID(edge.ID)
		candidate.StartNode, candidate.EndNode = startID, endID
		return match(&candidate)
	})
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

//
// Deprecated: Use GetEdgesBetween and filter by type to receive errors. Failed lookups may appear as nil.
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
	return w.GetNodesByLabelVisibleAtWithContext(context.Background(), label, version)
}

// GetNodesByLabelVisibleAtWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) GetNodesByLabelVisibleAtWithContext(ctx context.Context, label string, version storage.MVCCVersion) ([]*storage.Node, error) {

	return storage.ReadWithCapability(w.underlying, func(reader storage.MVCCIndexedVisibilityEngine) ([]*storage.Node, error) {
		values, err := storage.GetNodesByLabelVisibleAtWithContext(ctx, reader, label, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return values, err
		}
		return w.toUserNamespacedNodesWithContext(ctx, values)
	}, nil)
}

func (w *transactionStorageWrapper) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if !w.underlyingIsNamespaced() {
		nodeID = w.prefixNodeID(nodeID)
	}
	return storage.ReadWithCapability(w.underlying, func(reader storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		values, err := reader.GetOutgoingEdgesVisibleAt(nodeID, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return values, err
		}
		return w.toUserNamespacedEdges(values), nil
	}, nil)
}

func (w *transactionStorageWrapper) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if !w.underlyingIsNamespaced() {
		nodeID = w.prefixNodeID(nodeID)
	}
	return storage.ReadWithCapability(w.underlying, func(reader storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		values, err := reader.GetIncomingEdgesVisibleAt(nodeID, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return values, err
		}
		return w.toUserNamespacedEdges(values), nil
	}, nil)
}

func (w *transactionStorageWrapper) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {

	return storage.ReadWithCapability(w.underlying, func(reader storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		values, err := reader.GetEdgesByTypeVisibleAt(edgeType, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return values, err
		}
		return w.toUserNamespacedEdges(values), nil
	}, nil)
}

func (w *transactionStorageWrapper) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if !w.underlyingIsNamespaced() {
		startID, endID = w.prefixNodeID(startID), w.prefixNodeID(endID)
	}
	return storage.ReadWithCapability(w.underlying, func(reader storage.MVCCIndexedVisibilityEngine) ([]*storage.Edge, error) {
		values, err := reader.GetEdgesBetweenVisibleAt(startID, endID, version)
		if err != nil || w.namespace == "" || w.underlyingIsNamespaced() {
			return values, err
		}
		return w.toUserNamespacedEdges(values), nil
	}, nil)
}

func (w *transactionStorageWrapper) AllNodes() ([]*storage.Node, error) {
	return w.AllNodesWithContext(context.Background())
}

// AllNodesWithContext carries the caller through full materialization.
func (w *transactionStorageWrapper) AllNodesWithContext(ctx context.Context) ([]*storage.Node, error) {
	nodes, err := storage.AllNodesWithContext(ctx, w.tx)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNamespacedNodesWithContext(ctx, nodes)
}

func (w *transactionStorageWrapper) AllEdges() ([]*storage.Edge, error) {
	edges, err := w.tx.AllEdges()
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserNamespacedEdges(edges), nil
}

//
// Deprecated: Use AllNodes to receive errors. Failed reads are omitted and may appear as an empty collection.
func (w *transactionStorageWrapper) GetAllNodes() []*storage.Node {
	nodes := w.tx.GetAllNodes()
	if w.namespace == "" {
		return nodes
	}
	return w.toUserNamespacedNodes(nodes)
}

//
// Deprecated: Use GetIncomingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (w *transactionStorageWrapper) GetInDegree(nodeID storage.NodeID) int {
	edges, _ := w.GetIncomingEdges(nodeID)
	return len(edges)
}

//
// Deprecated: Use GetOutgoingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (w *transactionStorageWrapper) GetOutDegree(nodeID storage.NodeID) int {
	edges, _ := w.GetOutgoingEdges(nodeID)
	return len(edges)
}

func (w *transactionStorageWrapper) GetSchema() *storage.SchemaManager {
	if w.schema != nil {
		return w.schema
	}
	return w.underlying.GetSchema()
}

// BulkCreateNodes shares node admission, namespace conversion and mutation
// tracking with single creates.
func (w *transactionStorageWrapper) BulkCreateNodes(nodes []*storage.Node) error {
	return w.BulkCreateNodesWithContext(context.Background(), nodes)
}

// BulkCreateNodesWithContext carries the caller through mutation validation.
func (w *transactionStorageWrapper) BulkCreateNodesWithContext(ctx context.Context, nodes []*storage.Node) error {
	_, err := w.createNodesWithContext(ctx, nodes, false)
	return err
}

// BulkCreateMintedNodes is BulkCreateNodes for nodes whose IDs the executor
// minted (CreateMintedNode).
func (w *transactionStorageWrapper) BulkCreateMintedNodes(nodes []*storage.Node) error {
	return w.BulkCreateMintedNodesWithContext(context.Background(), nodes)
}

// BulkCreateMintedNodesWithContext carries the caller through mutation validation.
func (w *transactionStorageWrapper) BulkCreateMintedNodesWithContext(ctx context.Context, nodes []*storage.Node) error {
	_, err := w.createNodesWithContext(ctx, nodes, true)
	return err
}

func (w *transactionStorageWrapper) createNodes(nodes []*storage.Node, minted bool) (storage.NodeID, error) {
	return w.createNodesWithContext(context.Background(), nodes, minted)
}

// createNodesWithContext carries the caller through mutation validation.
func (w *transactionStorageWrapper) createNodesWithContext(ctx context.Context, nodes []*storage.Node, minted bool) (storage.NodeID, error) {
	var logicalID storage.NodeID
	create := func(n *storage.Node) (storage.NodeID, error) { return w.tx.CreateNodeWithContext(ctx, n) }
	if minted {
		create = func(n *storage.Node) (storage.NodeID, error) { return w.tx.CreateMintedNodeWithContext(ctx, n) }
	}
	for _, node := range nodes {
		if node == nil {
			return "", storage.ErrInvalidData
		}
		if err := w.GetSchema().RegisterTokens(node.Labels, nil); err != nil {
			return "", err
		}
		body := node
		if w.namespace != "" {
			body = storage.CopyNode(node)
			body.ID = w.prefixNodeID(node.ID)
		}
		id, err := create(body)
		if err != nil {
			return "", err
		}
		logicalID = w.unprefixNodeID(id)
		w.markMutatedNodeID(logicalID)
	}
	return logicalID, nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(edges []*storage.Edge) error {
	return w.createEdges(edges, false)
}

// BulkCreateMintedEdges is BulkCreateEdges for relationships whose IDs the
// executor minted (CreateMintedEdge).
func (w *transactionStorageWrapper) BulkCreateMintedEdges(edges []*storage.Edge) error {
	return w.createEdges(edges, true)
}

func (w *transactionStorageWrapper) createEdges(edges []*storage.Edge, minted bool) error {
	return w.createEdgesWithContext(context.Background(), edges, minted)
}

func (w *transactionStorageWrapper) CreateEdgeWithContext(ctx context.Context, edge *storage.Edge) error {
	return w.createEdgesWithContext(ctx, []*storage.Edge{edge}, false)
}
func (w *transactionStorageWrapper) CreateMintedEdgeWithContext(ctx context.Context, edge *storage.Edge) error {
	return w.createEdgesWithContext(ctx, []*storage.Edge{edge}, true)
}
func (w *transactionStorageWrapper) BulkCreateEdgesWithContext(ctx context.Context, edges []*storage.Edge) error {
	return w.createEdgesWithContext(ctx, edges, false)
}
func (w *transactionStorageWrapper) BulkCreateMintedEdgesWithContext(ctx context.Context, edges []*storage.Edge) error {
	return w.createEdgesWithContext(ctx, edges, true)
}

func (w *transactionStorageWrapper) createEdgesWithContext(ctx context.Context, edges []*storage.Edge, minted bool) error {
	var singleType [1]string
	relationshipTypes := singleType[:0]
	if len(edges) > 1 {
		relationshipTypes = make([]string, 0, len(edges))
	}
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return err
		}
		if edge == nil {
			return storage.ErrInvalidData
		}
		relationshipTypes = append(relationshipTypes, edge.Type)
	}
	if err := w.GetSchema().RegisterTokens(nil, relationshipTypes); err != nil {
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
			if err := ctx.Err(); err != nil {
				return err
			}
			cp := storage.CopyEdge(edge)
			cp.ID = w.prefixEdgeID(edge.ID)
			cp.StartNode = w.prefixNodeID(edge.StartNode)
			cp.EndNode = w.prefixNodeID(edge.EndNode)
			namespaced[i] = cp
		}
	}
	if len(namespaced) == 1 {
		if minted {
			return w.tx.CreateMintedEdgeWithContext(ctx, namespaced[0])
		}
		return w.tx.CreateEdgeWithContext(ctx, namespaced[0])
	}
	if minted {
		return w.tx.BulkCreateMintedEdgesWithContext(ctx, namespaced)
	}
	return w.tx.BulkCreateEdgesWithContext(ctx, namespaced)
}

func (w *transactionStorageWrapper) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteGraphNode(w.prefixNodeID(id), storage.CascadeEdges); err != nil {
			return err
		}
	}
	return nil
}

// BulkDeleteEdges deletes ids in the transaction. Like the engines'
// BulkDeleteEdges, it skips IDs that do not exist, those the transaction
// already deleted included (#827).
func (w *transactionStorageWrapper) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteEdge(w.prefixEdgeID(id)); err != nil && !errors.Is(err, storage.ErrNotFound) {
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
	out.Database = w.namespace
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
	nodes, _ = w.toUserNamespacedNodesWithContext(context.Background(), nodes)
	return nodes
}

func (w *transactionStorageWrapper) toUserNamespacedNodesWithContext(ctx context.Context, nodes []*storage.Node) ([]*storage.Node, error) {
	prefix := w.namespace + w.separator
	out := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if node != nil && strings.HasPrefix(string(node.ID), prefix) {
			out = append(out, w.toUserNode(node))
		}
	}
	return out, nil
}

func (w *transactionStorageWrapper) underlyingIsNamespaced() bool {
	scoped, ok := storage.ViewMetadataForEngine[interface{ Namespace() string }](w.underlying)
	return ok && w.namespace != "" && scoped.Namespace() == w.namespace
}

func (w *transactionStorageWrapper) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return w.BatchGetNodesWithContext(context.Background(), ids)
}

// BatchGetNodesWithContext scopes the transaction's existing pinned batch
// owner, including pending mutations and the ended-owner boundary.
func (w *transactionStorageWrapper) BatchGetNodesWithContext(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	requests := make([]storage.NodeID, len(ids))
	for i, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		requests[i] = w.prefixNodeID(id)
	}
	nodes, err := w.tx.BatchGetNodesWithContext(ctx, requests)
	if err != nil || w.namespace == "" {
		return nodes, err
	}
	result := make(map[storage.NodeID]*storage.Node, len(nodes))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if node := nodes[w.prefixNodeID(id)]; node != nil {
			result[id] = w.toUserNode(node)
		}
	}
	return result, nil
}

func (w *transactionStorageWrapper) Close() error {
	// Don't close underlying engine
	return nil
}

// NodeCount is the transaction-visible number of nodes, as the derived
// counters below are: the committed count while the transaction has staged
// no node change, otherwise a count of the nodes it sees (#1076).
func (w *transactionStorageWrapper) NodeCount() (int64, error) {
	if w.countersDescribeTransaction() && !w.tx.HasPendingNodeMutations() {
		return w.underlying.NodeCount()
	}
	nodes, err := w.AllNodes()
	return int64(len(nodes)), err
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
func (w *transactionStorageWrapper) NodeCountByLabel(label string) (int64, error) {
	return w.NodeCountByLabelWithContext(context.Background(), label)
}

// NodeCountByLabelWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) NodeCountByLabelWithContext(ctx context.Context, label string) (int64, error) {
	if w.countersDescribeTransaction() {
		delta, tracked := w.tx.PendingNodeLabelCountDelta(w.namespace, label)
		if tracked || !w.tx.HasPendingNodeMutations() {
			return storage.ReadWithCapability(w.underlying, func(reader storage.LabelStatsEngine) (int64, error) {
				count, err := storage.NodeCountByLabelWithContext(ctx, reader, label)
				return count + delta, err
			}, func() (int64, error) { return w.nodeCountByLabelScan(label) })
		}
	}
	return w.nodeCountByLabelScan(label)
}

func (w *transactionStorageWrapper) nodeCountByLabelScan(label string) (int64, error) {
	return w.nodeCountByLabelScanWithContext(context.Background(), label)
}

// nodeCountByLabelScanWithContext carries the caller through this read and its materialization.
func (w *transactionStorageWrapper) nodeCountByLabelScanWithContext(ctx context.Context, label string) (int64, error) {
	nodes, err := storage.GetNodesByLabelWithContext(ctx, w, label)
	if err != nil {
		return 0, err
	}
	return int64(len(nodes)), nil
}

// EdgeCount is the transaction-visible number of relationships, as
// NodeCount is (#1076).
func (w *transactionStorageWrapper) EdgeCount() (int64, error) {
	if w.countersDescribeTransaction() && !w.tx.HasPendingEdgeMutations() {
		return w.underlying.EdgeCount()
	}
	edges, err := w.AllEdges()
	return int64(len(edges)), err
}

// EdgeCountByType is the transaction-visible count of relationships of
// edgeType.
func (w *transactionStorageWrapper) EdgeCountByType(edgeType string) (int64, error) {
	if w.countersDescribeTransaction() {
		delta, tracked := w.tx.PendingEdgeTypeCountDelta(w.namespace, edgeType)
		if tracked || !w.tx.HasPendingEdgeMutations() {
			count, err := w.underlying.EdgeCountByType(edgeType)
			return count + delta, err
		}
	}
	return w.edgeCountByTypeScan(edgeType)
}

func (w *transactionStorageWrapper) edgeCountByTypeScan(edgeType string) (int64, error) {
	edges, err := w.GetEdgesByType(edgeType)
	if err != nil {
		return 0, err
	}
	return int64(len(edges)), nil
}

// EdgeCountByStartLabel / EdgeCountByEndLabel are the transaction-visible
// counts of relationships of edgeType whose start / end node has label (the
// one-labeled-endpoint count shapes). Relabelling a node changes them too,
// so without the staged deltas they need a transaction with no staged node
// or relationship changes.
func (w *transactionStorageWrapper) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return w.edgeCountByEndpointLabel(label, edgeType, true)
}

func (w *transactionStorageWrapper) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return w.edgeCountByEndpointLabel(label, edgeType, false)
}

func (w *transactionStorageWrapper) edgeCountByEndpointLabel(label, edgeType string, start bool) (int64, error) {
	// The staged positional deltas are not used: DETACH DELETE of a node whose
	// relationship the same transaction created stages them wrong (#741).
	if w.countersDescribeTransaction() && !w.tx.HasPendingEdgeMutations() && !w.tx.HasPendingNodeMutations() {
		if start {
			return w.underlying.EdgeCountByStartLabel(label, edgeType)
		}
		return w.underlying.EdgeCountByEndLabel(label, edgeType)
	}
	return w.edgeCountByEndpointLabelScan(label, edgeType, start)
}

func (w *transactionStorageWrapper) edgeCountByEndpointLabelScan(label, edgeType string, start bool) (int64, error) {
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
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return 0, err
		}
		if node == nil {
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

func (*transactionStorageWrapper) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineView, ReadState: storage.BindingOwned, Membership: storage.BindingOwned, Controller: storage.BindingOwned, PendingActions: storage.BindingOwned, SchemaPublication: storage.BindingOwned, ReadLease: storage.BindingOwned, StatementMode: storage.BindingOwned, Identity: storage.BindingOwned, Enforcement: storage.BindingOwned, CompoundDependencies: storage.BindingOwned, SameVersionFallback: storage.BindingOwned, Delivery: storage.BindingOwned, ExactEffects: storage.BindingOwned, FullRead: storage.BindingOwned, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

func (e *StorageExecutor) propertyIndexLookup(ctx context.Context, engine storage.Engine, schema *storage.SchemaManager, label, property string, value interface{}) ([]storage.NodeID, error) {
	member, _ := e.nativeMemberFor(engine)
	return lookupPropertyIndex(ctx, member, schema, label, property, value)
}
func (e *StorageExecutor) compositeIndexLookup(ctx context.Context, engine storage.Engine, index *storage.CompositeIndex, values []interface{}, full bool) ([]storage.NodeID, error) {
	member, _ := e.nativeMemberFor(engine)
	return lookupCompositeIndex(ctx, member, index, values, full)
}

// Compile the original native member once while this view retains scope and
// delivery transforms. Row execution calls only the retained typed operation.
func (w *transactionStorageWrapper) BindQueryNodeRead() func(storage.NodeID) (*storage.Node, error) {
	if w.tx == nil {
		return func(storage.NodeID) (*storage.Node, error) { return nil, storage.ErrTransactionClosed }
	}
	read := storage.BindQueryNodeRead(w.tx)
	return func(id storage.NodeID) (*storage.Node, error) {
		node, err := read(w.prefixNodeID(id))
		if err != nil || w.namespace == "" {
			return node, err
		}
		return w.toUserNode(node), nil
	}
}
func (w *transactionStorageWrapper) BindEdgeHeaderRead(outgoing bool) func(storage.NodeID) ([]*storage.Edge, bool, error) {
	if w.tx == nil {
		return func(storage.NodeID) ([]*storage.Edge, bool, error) { return nil, false, storage.ErrTransactionClosed }
	}
	read := storage.BindEdgeHeaderReads(w.tx)
	return func(id storage.NodeID) ([]*storage.Edge, bool, error) {
		return w.userEdgeHeaders(read(w.prefixNodeID(id), outgoing))
	}
}
