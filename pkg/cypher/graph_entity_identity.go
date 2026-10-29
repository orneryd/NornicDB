package cypher

import (
	"context"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// graphEntityReference is the sole reader of existing native graph node inputs.
// Its caller supplies the projection/engine ID domain for unscoped text IDs.
func graphEntityReference(value interface{}) (storage.EntityIdentity, bool) {
	switch node := value.(type) {
	case *storage.Node:
		if node != nil {
			return node.Identity(), true
		}
	case string:
		return storage.EntityIdentity{LocalID: node}, true
	default:
		if isIntegerProcedureValue(value) {
			return storage.EntityIdentity{LocalID: fmt.Sprint(value)}, true
		}
	}
	return storage.EntityIdentity{}, false
}

// graphEntityDisplayID renders the public value, before any graph-domain
// canonicalization. Internal keys must never be formatted in client messages.
func graphEntityDisplayID(value interface{}) (storage.NodeID, bool) {
	reference, ok := graphEntityReference(value)
	if !ok {
		return "", false
	}
	return storage.NodeID(reference.InDatabase("")), true
}

// identityGraphEngine preserves nil storage so existing graph builders return
// their established error. Wrapping nil would hide that contract from them.
func identityGraphEngine(engine storage.Engine, scopes ...*graphIdentityScope) storage.Engine {
	if engine == nil {
		return nil
	}
	return graphIdentityView(engine, scopes...)
}

// graphIdentityStorage is a read view for algorithms whose maps accept NodeID.
// Inside that view every node, edge and endpoint uses the canonical identity
// key. Native values are restored at the result boundary; no identity cache or
// graph-sized cross-stream seen set is retained by this adapter.
type graphIdentityStorage struct {
	namespace carriedNamespace
	identity  carriedIdentityLookup
	storage.Engine
	scope *graphIdentityScope
}

// graphIdentityStorage declares every optional method; admission is derived from this policy.
var graphIdentityStorageReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "BatchGetNodesWithContext", Disposition: storage.CapabilityOwned, Scope: "Maps requested graph identities through its bound identity lookup, then maps result keys and nodes back before delivery; passing this view would lose identity."},
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityRefused, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "AllNodesWithContext", Disposition: storage.CapabilityOwned, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetFirstNodeByLabelWithContext", Disposition: storage.CapabilityOwned, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetNodesByLabelWithContext", Disposition: storage.CapabilityOwned, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "Namespace", Disposition: storage.CapabilityOwned, Scope: "Carried namespace identity; the filtered data view stays owned."},
	{Methods: "StreamEdgesByType SubscribeEvents",
		Disposition: storage.CapabilityRefused, Scope: "No scoped optional read contract on this view; configuration, mutation and maintenance require their exact operation owner."},
}

func (*graphIdentityStorage) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return graphIdentityStorageReadPolicy.Copy()
}

func graphIdentityView(engine storage.Engine, scopes ...*graphIdentityScope) *graphIdentityStorage {
	if view, ok := engine.(*graphIdentityStorage); ok {
		return view
	}
	var scope *graphIdentityScope
	if len(scopes) > 0 {
		scope = scopes[0]
	}
	return &graphIdentityStorage{Engine: engine, scope: scope}
}

func (v *graphIdentityStorage) node(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	copy := *node
	copy.ID = storage.NodeID(node.IdentityKey())
	copy.Database = ""
	return &copy
}

func graphIdentityNodeValue(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	identity := storage.EntityIdentityFromKey(string(node.ID))
	copy := *node
	copy.ID, copy.Database = storage.NodeID(identity.LocalID), identity.Database
	return &copy
}

func graphIdentityEdgeValue(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	identity := storage.EntityIdentityFromKey(string(edge.ID))
	start := storage.EntityIdentityFromKey(string(edge.StartNode))
	end := storage.EntityIdentityFromKey(string(edge.EndNode))
	copy := *edge
	copy.ID, copy.Database = storage.EdgeID(identity.LocalID), identity.Database
	copy.StartNode, copy.EndNode = storage.NodeID(start.LocalID), storage.NodeID(end.LocalID)
	return &copy
}

func graphIdentityPathValue(path PathResult) PathResult {
	copy := path
	copy.Nodes = make([]*storage.Node, len(path.Nodes))
	copy.Relationships = make([]*storage.Edge, len(path.Relationships))
	for i, node := range path.Nodes {
		copy.Nodes[i] = graphIdentityNodeValue(node)
	}
	for i, edge := range path.Relationships {
		copy.Relationships[i] = graphIdentityEdgeValue(edge)
	}
	return copy
}

func (e *StorageExecutor) graphIdentityExecutor(ctx context.Context) *StorageExecutor {
	engine := identityGraphEngine(e.getStorage(ctx), e.graphIdentityScope)
	if engine == nil {
		return e
	}
	return e.cloneForStorage(engine)
}

func (e *StorageExecutor) graphReadArguments(ctx context.Context, args []interface{}, nodeCount int) (*StorageExecutor, []interface{}, error) {
	graph := e.graphIdentityExecutor(ctx)
	view := graph.storage.(*graphIdentityStorage)
	input := append([]interface{}(nil), args...)
	for i := 0; i < nodeCount && i < len(input); i++ {
		switch value := input[i].(type) {
		case *storage.Node:
			input[i] = view.node(value)
		case string:
			key, ok, err := view.inputKey(value)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				input[i] = string(key)
			}
		}
	}
	return graph, input, nil
}

func (v *graphIdentityStorage) outputPath(path []storage.NodeID) []storage.NodeID {
	result := make([]storage.NodeID, len(path))
	for i, id := range path {
		result[i] = storage.NodeID(v.outputID(id))
	}
	return result
}

func (v *graphIdentityStorage) edge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	copy := *edge
	copy.ID = storage.EdgeID(edge.IdentityKey())
	copy.StartNode = storage.NodeID(edge.StartIdentity().Key())
	copy.EndNode = storage.NodeID(edge.EndIdentity().Key())
	copy.Database = ""
	return &copy
}

func (v *graphIdentityStorage) nodes(nodes []*storage.Node) []*storage.Node {
	result := make([]*storage.Node, len(nodes))
	for i, node := range nodes {
		result[i] = v.node(node)
	}
	return result
}

func (v *graphIdentityStorage) edges(edges []*storage.Edge, err error) ([]*storage.Edge, error) {
	if err != nil {
		return nil, err
	}
	result := make([]*storage.Edge, len(edges))
	for i, edge := range edges {
		result[i] = v.edge(edge)
	}
	return result, nil
}

func (v *graphIdentityStorage) GetNode(key storage.NodeID) (*storage.Node, error) {
	id, ok := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(key)))
	if !ok {
		return nil, storage.ErrNotFound
	}
	node, err := v.Engine.GetNode(id)
	return v.node(node), err
}

func (v *graphIdentityStorage) BatchGetNodes(keys []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	ctx := context.Background()
	requests := make([]storage.NodeID, 0, len(keys))
	originals := make(map[storage.NodeID][]storage.NodeID, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if id, ok := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(key))); ok {
			if _, requested := originals[id]; !requested {
				requests = append(requests, id)
			}
			originals[id] = append(originals[id], key)
		}
	}
	nodes, err := v.Engine.BatchGetNodes(requests)
	if err != nil {
		return nil, err
	}
	result := make(map[storage.NodeID]*storage.Node, len(nodes))
	for id, node := range nodes {
		for _, key := range originals[id] {
			result[key] = v.node(node)
		}
	}
	return result, nil
}

// Deprecated: Use AllNodes to receive errors. Failed reads are omitted and may appear as an empty collection.
func (v *graphIdentityStorage) GetAllNodes() []*storage.Node { return v.nodes(v.Engine.GetAllNodes()) }
func (v *graphIdentityStorage) AllNodes() ([]*storage.Node, error) {
	nodes, err := v.Engine.AllNodes()
	if err != nil {
		return nil, err
	}
	return v.nodes(nodes), nil
}
func (v *graphIdentityStorage) AllEdges() ([]*storage.Edge, error) {
	return v.edges(v.Engine.AllEdges())
}
func (v *graphIdentityStorage) GetNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := v.Engine.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	return v.nodes(nodes), nil
}
func (v *graphIdentityStorage) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	node, err := v.Engine.GetFirstNodeByLabel(label)
	return v.node(node), err
}
func (v *graphIdentityStorage) GetOutgoingEdges(key storage.NodeID) ([]*storage.Edge, error) {
	id, ok := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(key)))
	if !ok {
		return nil, nil
	}
	return v.edges(v.Engine.GetOutgoingEdges(id))
}
func (v *graphIdentityStorage) GetIncomingEdges(key storage.NodeID) ([]*storage.Edge, error) {
	id, ok := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(key)))
	if !ok {
		return nil, nil
	}
	return v.edges(v.Engine.GetIncomingEdges(id))
}
func (v *graphIdentityStorage) GetEdgesByType(kind string) ([]*storage.Edge, error) {
	return v.edges(v.Engine.GetEdgesByType(kind))
}

func (v *graphIdentityStorage) GetEdgesBetween(start, end storage.NodeID) ([]*storage.Edge, error) {
	left, leftOK := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(start)))
	right, rightOK := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(end)))
	if !leftOK || !rightOK {
		return nil, nil
	}
	return v.edges(v.Engine.GetEdgesBetween(left, right))
}

// Deprecated: Use GetEdgesBetween and filter by type to receive errors. Failed lookups may appear as nil.
func (v *graphIdentityStorage) GetEdgeBetween(start, end storage.NodeID, kind string) *storage.Edge {
	left, leftOK := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(start)))
	right, rightOK := v.identity.For(v.Engine).NodeLookupID(storage.EntityIdentityFromKey(string(end)))
	if !leftOK || !rightOK {
		return nil
	}
	return v.edge(v.Engine.GetEdgeBetween(left, right, kind))
}

// Deprecated: Use GetIncomingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (v *graphIdentityStorage) GetInDegree(key storage.NodeID) int {
	edges, _ := v.GetIncomingEdges(key)
	return len(edges)
}

// Deprecated: Use GetOutgoingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (v *graphIdentityStorage) GetOutDegree(key storage.NodeID) int {
	edges, _ := v.GetOutgoingEdges(key)
	return len(edges)
}
func (v *graphIdentityStorage) StreamNodesWithOptions(ctx context.Context, options storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return v.Engine.StreamNodesWithOptions(ctx, options, func(node *storage.Node) error { return visit(v.node(node)) })
}
func (v *graphIdentityStorage) StreamEdgesByType(ctx context.Context, kind string, visit func(*storage.Edge) error) error {
	return storage.StreamEdgesByType(ctx, v.Engine, kind, func(edge *storage.Edge) error { return visit(v.edge(edge)) })
}

func (v *graphIdentityStorage) inputKey(value interface{}) (storage.NodeID, bool, error) {
	reference, ok := graphEntityReference(value)
	if !ok {
		return "", false, nil
	}
	if reference.Database == "" {
		if kind, database, local, qualified := storage.ParseElementID(reference.LocalID); qualified {
			if kind != "4" {
				return "", false, nil
			}
			domain := graphIdentityDomain{engine: v.Engine, scope: v.scope}
			if storage.CompositeDatabaseOf(v.Engine) == "" && !domain.foreignQualifier(database) {
				database = storage.DatabaseOf(v.Engine)
			}
			reference.Database, reference.LocalID = database, local
		} else if storage.CompositeDatabaseOf(v.Engine) == "" {
			reference.Database = storage.DatabaseOf(v.Engine)
		} else {
			node, err := v.Engine.GetNode(storage.NodeID(reference.LocalID))
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return "", false, err
			}
			if node != nil {
				reference = node.Identity()
			}
		}
	}
	return storage.NodeID(reference.Key()), true, nil
}

// algorithmIdentityInput gives all whole-graph algorithms one key domain for
// native arguments, graph scans, adjacency and their computed maps.
func (e *StorageExecutor) algorithmIdentityInput(ctx context.Context, procedure string, args []interface{}) (*StorageExecutor, []*storage.Node, error) {
	view := graphIdentityView(e.storage, e.graphIdentityScope)
	graph := e.cloneForStorage(view)
	input := append([]interface{}(nil), args...)
	if len(input) > 0 {
		if items, ok := cypherListValue(input[0]); ok && len(items) > 0 {
			if _, native := items[0].(*storage.Node); native {
				values := append([]interface{}(nil), items...)
				for i, value := range values {
					if node, ok := value.(*storage.Node); ok {
						values[i] = view.node(node)
					}
				}
				input[0] = values
			}
		}
	}
	nodes, err := graph.algorithmNodes(ctx, procedure, input)
	return graph, nodes, err
}

func graphAlgorithmRows[T any](nodes []*storage.Node, values map[storage.NodeID]T) [][]interface{} {
	rows := algorithmRows(nodes, values)
	for _, row := range rows {
		if node, ok := row[0].(*storage.Node); ok {
			row[0] = graphIdentityNodeValue(node)
		}
	}
	return rows
}

// bindIdentity converts native/config references only after the graph's key
// domain is known. Projection IDs retain their public catalog domain; stored
// graphs use the same canonical read view as scalar and APOC algorithms.
func (c *linkPredictionConfig) bindIdentity(engine storage.Engine, projection *GraphProjection) error {
	c.identityStore = engine
	var read func(interface{}) (storage.NodeID, bool, error)
	if projection != nil {
		read = func(value interface{}) (storage.NodeID, bool, error) {
			id, ok := projection.nodeReference(value)
			return storage.NodeID(id), ok, nil
		}
		c.identityOutput = func(id storage.NodeID) string { return string(id) }
	} else {
		view := graphIdentityView(engine)
		read, c.identityStore, c.identityOutput = view.inputKey, identityGraphEngine(engine), view.outputID
	}
	value := c.sourceValue
	if value == nil {
		value = string(c.SourceNode)
	}
	var err error
	c.SourceNode, _, err = read(value)
	if err != nil {
		return err
	}
	if c.TargetNodes != nil {
		values := c.targetValues
		if values == nil {
			for id := range c.TargetNodes {
				values = append(values, string(id))
			}
		}
		c.TargetNodes = make(map[storage.NodeID]bool, len(values))
		for _, value := range values {
			id, ok, err := read(value)
			if err != nil {
				return err
			}
			if ok {
				c.TargetNodes[id] = true
			}
		}
	}
	return nil
}

func (c *linkPredictionConfig) identityRows(result *ExecuteResult) *ExecuteResult {
	if c.identityOutput != nil {
		for _, row := range result.Rows {
			for i := 0; i < 2 && i < len(row); i++ {
				if id, ok := row[i].(string); ok {
					row[i] = c.identityOutput(storage.NodeID(id))
				}
			}
		}
	}
	return result
}

func (v *graphIdentityStorage) outputID(key storage.NodeID) string {
	return storage.EntityIdentityFromKey(string(key)).InDatabase(storageDatabaseName(v.Engine))
}

func (c *linkPredictionConfig) sourceDisplay() string {
	value := c.sourceValue
	if value == nil {
		value = string(c.SourceNode)
	}
	id, _ := graphEntityDisplayID(value)
	return string(id)
}

// graphEntityLookupID is the storage-domain adapter for utility functions.
// Preserve their legacy string coercion without losing native provenance.
func graphEntityLookupID(engine storage.Engine, value interface{}, domains ...graphIdentityDomain) (storage.NodeID, bool) {
	reference, ok := graphEntityReference(value)
	if !ok {
		reference = storage.EntityIdentity{LocalID: fmt.Sprint(value)}
	}
	if _, native := value.(*storage.Node); !native {
		if kind, database, local, qualified := storage.ParseElementID(reference.LocalID); qualified {
			if kind != "4" {
				return "", false
			}
			domain := graphIdentityDomain{engine: engine}
			if len(domains) > 0 {
				domain = domains[0]
			}
			if storage.CompositeDatabaseOf(engine) == "" && !domain.foreignQualifier(database) {
				return storage.NodeID(local), true
			}
			reference = storage.EntityIdentity{Database: database, LocalID: local}
		}
	}
	return reference.NodeLookupID(engine)
}

// SubscribeEvents retains the underlying engine's publication owner through
// the identity read view; the adapter never publishes a second event stream.
func (v *graphIdentityStorage) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(v.Engine, handlers)
}

func (*graphIdentityStorage) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineView, ReadState: storage.BindingCarried, Membership: storage.BindingCarried, Controller: storage.BindingRefused, PendingActions: storage.BindingCarried, SchemaPublication: storage.BindingCarried, ReadLease: storage.BindingCarried, StatementMode: storage.BindingCarried, Identity: storage.BindingCarried, Enforcement: storage.BindingCarried, CompoundDependencies: storage.BindingCarried, SameVersionFallback: storage.BindingCarried, Delivery: storage.BindingCarried, ExactEffects: storage.BindingCarried, FullRead: storage.BindingCarried, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

// Stored integrity facts may not bypass this view.
func (g *graphIdentityStorage) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.RefusedStoredReadPolicy()
}
func (g *graphIdentityStorage) ReadStored(ctx context.Context, _ storage.StoredReadRequest) (storage.StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, storage.ErrNotImplemented
}
