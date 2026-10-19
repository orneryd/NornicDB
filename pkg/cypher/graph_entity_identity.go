package cypher

import (
	"context"
	"fmt"
	"sort"

	"github.com/orneryd/nornicdb/pkg/localization"
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
	storage.Engine
	scope *graphIdentityScope
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

func (e *StorageExecutor) graphReadArguments(ctx context.Context, args []interface{}, nodeCount int) (*StorageExecutor, []interface{}) {
	graph := e.graphIdentityExecutor(ctx)
	view := graph.storage.(*graphIdentityStorage)
	input := append([]interface{}(nil), args...)
	for i := 0; i < nodeCount && i < len(input); i++ {
		switch value := input[i].(type) {
		case *storage.Node:
			input[i] = view.node(value)
		case string:
			if key, ok := view.inputKey(value); ok {
				input[i] = string(key)
			}
		}
	}
	return graph, input
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
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(v.Engine)
	if !ok {
		return nil, storage.ErrNotFound
	}
	node, err := v.Engine.GetNode(id)
	return v.node(node), err
}

func (v *graphIdentityStorage) BatchGetNodes(keys []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	requests := make([]storage.NodeID, 0, len(keys))
	originals := make(map[storage.NodeID][]storage.NodeID, len(keys))
	for _, key := range keys {
		if id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(v.Engine); ok {
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
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(v.Engine)
	if !ok {
		return nil, nil
	}
	return v.edges(v.Engine.GetOutgoingEdges(id))
}
func (v *graphIdentityStorage) GetIncomingEdges(key storage.NodeID) ([]*storage.Edge, error) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(v.Engine)
	if !ok {
		return nil, nil
	}
	return v.edges(v.Engine.GetIncomingEdges(id))
}
func (v *graphIdentityStorage) GetEdgesByType(kind string) ([]*storage.Edge, error) {
	return v.edges(v.Engine.GetEdgesByType(kind))
}

func (v *graphIdentityStorage) GetEdgesBetween(start, end storage.NodeID) ([]*storage.Edge, error) {
	left, leftOK := storage.EntityIdentityFromKey(string(start)).NodeLookupID(v.Engine)
	right, rightOK := storage.EntityIdentityFromKey(string(end)).NodeLookupID(v.Engine)
	if !leftOK || !rightOK {
		return nil, nil
	}
	return v.edges(v.Engine.GetEdgesBetween(left, right))
}
func (v *graphIdentityStorage) GetEdgeBetween(start, end storage.NodeID, kind string) *storage.Edge {
	left, leftOK := storage.EntityIdentityFromKey(string(start)).NodeLookupID(v.Engine)
	right, rightOK := storage.EntityIdentityFromKey(string(end)).NodeLookupID(v.Engine)
	if !leftOK || !rightOK {
		return nil
	}
	return v.edge(v.Engine.GetEdgeBetween(left, right, kind))
}
func (v *graphIdentityStorage) GetInDegree(key storage.NodeID) int {
	edges, _ := v.GetIncomingEdges(key)
	return len(edges)
}
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

func (v *graphIdentityStorage) inputKey(value interface{}) (storage.NodeID, bool) {
	reference, ok := graphEntityReference(value)
	if !ok {
		return "", false
	}
	if reference.Database == "" {
		if kind, database, local, qualified := storage.ParseElementID(reference.LocalID); qualified {
			if kind != "4" {
				return "", false
			}
			domain := graphIdentityDomain{engine: v.Engine, scope: v.scope}
			if storage.CompositeDatabaseOf(v.Engine) == "" && !domain.foreignQualifier(database) {
				database = storage.DatabaseOf(v.Engine)
			}
			reference.Database, reference.LocalID = database, local
		} else if storage.CompositeDatabaseOf(v.Engine) == "" {
			reference.Database = storage.DatabaseOf(v.Engine)
		} else if node, err := v.Engine.GetNode(storage.NodeID(reference.LocalID)); err == nil && node != nil {
			reference = node.Identity()
		}
	}
	return storage.NodeID(reference.Key()), true
}

// algorithmIdentityInput gives all whole-graph algorithms one key domain for
// native arguments, graph scans, adjacency and their computed maps.
// Algorithm keys have a distinct type and exist only in a read-only graph API.
// Native storage values never masquerade as nodes whose physical ID is a key.
type graphEntityKey string
type algorithmGraphNode struct {
	ID    graphEntityKey
	value *storage.Node
}
type algorithmGraphEdge struct {
	ID                 graphEntityKey
	StartNode, EndNode graphEntityKey
	Type               string
	Properties         map[string]interface{}
}
type algorithmGraphStorage struct {
	source, reader storage.Engine
}
type algorithmGraphExecutor struct{ storage *algorithmGraphStorage }

func algorithmIdentity(source storage.Engine, identity storage.EntityIdentity) storage.EntityIdentity {
	if identity.Database == "" {
		identity.Database = storage.DatabaseOf(source)
	}
	return identity
}

func algorithmNativeNodes(nodes []*storage.Node, source storage.Engine) []algorithmGraphNode {
	values := make([]algorithmGraphNode, 0, len(nodes))
	for _, node := range nodes {
		if node != nil {
			values = append(values, algorithmGraphNode{ID: graphEntityKey(algorithmIdentity(source, node.Identity()).Key()), value: node})
		}
	}
	return values
}

func algorithmNodeValue(node algorithmGraphNode) *storage.Node {
	value := *node.value
	return &value
}

func (g *algorithmGraphStorage) edges(edges []*storage.Edge, err error) ([]algorithmGraphEdge, error) {
	if err != nil {
		return nil, err
	}
	values := make([]algorithmGraphEdge, 0, len(edges))
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		values = append(values, algorithmGraphEdge{
			ID:        graphEntityKey(algorithmIdentity(g.source, edge.Identity()).Key()),
			StartNode: graphEntityKey(algorithmIdentity(g.source, edge.StartIdentity()).Key()),
			EndNode:   graphEntityKey(algorithmIdentity(g.source, edge.EndIdentity()).Key()),
			Type:      edge.Type, Properties: edge.Properties,
		})
	}
	return values, nil
}

func (g *algorithmGraphStorage) GetOutgoingEdges(key graphEntityKey) ([]algorithmGraphEdge, error) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(g.source)
	if !ok {
		return nil, storage.ErrNotFound
	}
	return g.edges(g.reader.GetOutgoingEdges(id))
}

func (g *algorithmGraphStorage) GetIncomingEdges(key graphEntityKey) ([]algorithmGraphEdge, error) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(g.source)
	if !ok {
		return nil, storage.ErrNotFound
	}
	return g.edges(g.reader.GetIncomingEdges(id))
}

func (g *algorithmGraphExecutor) getNodeEdges(key graphEntityKey) []algorithmGraphEdge {
	outgoing, _ := g.storage.GetOutgoingEdges(key)
	incoming, _ := g.storage.GetIncomingEdges(key)
	return append(outgoing, incoming...)
}

func (g *algorithmGraphExecutor) filtered(types map[string]struct{}) *algorithmGraphExecutor {
	return &algorithmGraphExecutor{storage: &algorithmGraphStorage{source: g.storage.source, reader: &algorithmRelationshipReader{Engine: g.storage.reader, types: types}}}
}

func (e *StorageExecutor) algorithmIdentityInput(procedure string, args []interface{}) (*algorithmGraphExecutor, []algorithmGraphNode, error) {
	native, err := e.algorithmNodes(procedure, args)
	if err != nil {
		return nil, nil, err
	}
	graph := &algorithmGraphExecutor{storage: &algorithmGraphStorage{source: e.storage, reader: e.storage}}
	nodes := algorithmNativeNodes(native, e.storage)
	selected := false
	if items, list := cypherListValue(procedureArgument(args, 0)); list && len(items) > 0 {
		_, selected = items[0].(*storage.Node)
	}
	if !selected {
		sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	}
	return graph, nodes, nil
}

func graphAlgorithmRows[T any](nodes []algorithmGraphNode, values map[graphEntityKey]T) [][]interface{} {
	return algorithmRows(nodes, values)
}

// bindIdentity converts native/config references only after the graph's key
// domain is known. Projection IDs retain their public catalog domain; stored
// graphs use the same canonical read view as scalar and APOC algorithms.
func (c *linkPredictionConfig) bindIdentity(engine storage.Engine, projection *GraphProjection) {
	c.identityStore = engine
	var read func(interface{}) (storage.NodeID, bool)
	if projection != nil {
		read = func(value interface{}) (storage.NodeID, bool) {
			id, ok := projection.nodeReference(value)
			return storage.NodeID(id), ok
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
	c.SourceNode, _ = read(value)
	if c.TargetNodes != nil {
		values := c.targetValues
		if values == nil {
			for id := range c.TargetNodes {
				values = append(values, string(id))
			}
		}
		c.TargetNodes = make(map[storage.NodeID]bool, len(values))
		for _, value := range values {
			if id, ok := read(value); ok {
				c.TargetNodes[id] = true
			}
		}
	}
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

// nativeEntityIdentity also serves relationship consumers whose public graph
// input is not a node. It never interprets a stored local ID as an element ID.
func nativeEntityIdentity(value interface{}) (storage.EntityIdentity, bool) {
	switch entity := value.(type) {
	case *storage.Node:
		if entity != nil {
			return entity.Identity(), true
		}
	case *storage.Edge:
		if entity != nil {
			return entity.Identity(), true
		}
	}
	return storage.EntityIdentity{}, false
}

// Native values reach storage only through this adapter. Their local IDs are
// opaque; the selected source is retained until the engine boundary decides
// whether a qualified lookup is needed. No operation unwraps its transaction.
func nativeEntityStorageID(engine storage.Engine, value interface{}) (string, bool) {
	identity, ok := nativeEntityIdentity(value)
	if !ok {
		return "", false
	}
	kind := "4"
	if _, relationship := value.(*storage.Edge); relationship {
		kind = "5"
	}
	return identity.LookupID(engine, kind)
}

func nativeEdgeStorageEndpoints(engine storage.Engine, edge *storage.Edge) (storage.NodeID, storage.NodeID, bool) {
	if edge == nil {
		return "", "", false
	}
	start, startOK := edge.StartIdentity().NodeLookupID(engine)
	end, endOK := edge.EndIdentity().NodeLookupID(engine)
	return start, end, startOK && endOK
}

// A CREATE plan already carries any source selected by bound endpoints.
// Supply a source only for a fresh unscoped value; a coordinator is never
// authority to overwrite an existing physical provenance selection.
func bindNativeCreationSource(engine storage.Engine, value interface{}) error {
	switch entity := value.(type) {
	case *storage.Node:
		if entity.Database == "" {
			entity.Database = storage.DatabaseOf(engine)
		}
		if _, ok := entity.Identity().NodeLookupID(engine); !ok {
			return localizedStatusError("Neo.ClientError.Statement.EntityNotFound", "EntityNotFound", localization.CypherProceduresNodeNotFound(entity.ElementID("")))
		}
	case *storage.Edge:
		if entity.Database == "" {
			entity.Database = storage.DatabaseOf(engine)
		}
		if _, ok := entity.Identity().LookupID(engine, "5"); !ok {
			return localizedStatusError("Neo.ClientError.Statement.EntityNotFound", "EntityNotFound", localization.CypherProceduresRelationshipNotFound(entity.ElementID("")))
		}
	}
	return nil
}

// Deleted entity views preserve physical provenance and relationship endpoints
// while removing visible labels/properties. These are value copies, not keys.
func nativeDeletedNodeValue(node *storage.Node) *storage.Node {
	return &storage.Node{ID: node.ID, Database: node.Database}
}

func nativeDeletedEdgeValue(edge *storage.Edge) *storage.Edge {
	return &storage.Edge{ID: edge.ID, Database: edge.Database, Type: edge.Type, StartNode: edge.StartNode, EndNode: edge.EndNode}
}

func nativeEntityMetadataID(engine storage.Engine, identity storage.EntityIdentity) string {
	if identity.Database != "" && storage.DatabaseOf(engine) != identity.Database {
		return identity.Database + ":" + identity.LocalID
	}
	return storage.QualifiedEntityID(engine, identity.LocalID)
}

// nativeEntityMetadataTarget shares source selection with storage's identity
// owner. Metadata reads may inspect a source engine; data operations continue
// through their transaction/admission wrapper and never use this escape.
func (e *StorageExecutor) nativeEntityMetadataTarget(ctx context.Context, identity storage.EntityIdentity) (storage.Engine, string) {
	selected := e.getStorage(ctx)
	source := func(engine storage.Engine) storage.Engine {
		if identity.Database == "" || storage.DatabaseOf(engine) == identity.Database {
			return engine
		}
		for depth := 0; engine != nil && depth < 16; depth++ {
			if sources, ok := engine.(interface {
				EntitySource(storage.EntityIdentity) (storage.Engine, error)
			}); ok {
				if selected, err := sources.EntitySource(identity); err == nil {
					return selected
				}
				return nil
			}
			wrapper, ok := engine.(storage.EngineUnwrapper)
			if !ok {
				break
			}
			next := wrapper.GetInnerEngine()
			if next == engine {
				break
			}
			engine = next
		}
		return nil
	}
	if engine := source(selected); engine != nil {
		return engine, storage.QualifiedEntityID(engine, identity.LocalID)
	}
	if scoped, err := e.graphFunctionStorage("", selected); err == nil {
		if engine := source(scoped); engine != nil {
			return engine, storage.QualifiedEntityID(engine, identity.LocalID)
		}
	}
	return nil, nativeEntityMetadataID(selected, identity)
}
