package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"

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
	value              *storage.Edge
}
type algorithmGraphStorage struct {
	source, reader storage.Engine
	scope          *graphIdentityScope
}
type algorithmGraphExecutor struct{ storage *algorithmGraphStorage }

func algorithmIdentity(source storage.Engine, identity storage.EntityIdentity) storage.EntityIdentity {
	if resolved, ok := identity.Resolve(source); ok {
		identity = resolved
	}
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
			value:     edge,
			ID:        graphEntityKey(algorithmIdentity(g.source, edge.Identity()).Key()),
			StartNode: graphEntityKey(algorithmIdentity(g.source, edge.StartIdentity()).Key()),
			EndNode:   graphEntityKey(algorithmIdentity(g.source, edge.EndIdentity()).Key()),
			Type:      edge.Type, Properties: edge.Properties,
		})
	}
	return values, nil
}

func (g *algorithmGraphStorage) GetOutgoingEdges(key graphEntityKey) ([]algorithmGraphEdge, error) {
	if g.reader == nil {
		return nil, storage.ErrInvalidData
	}
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(g.source)
	if !ok {
		return nil, storage.ErrNotFound
	}
	return g.edges(g.reader.GetOutgoingEdges(id))
}

func (g *algorithmGraphStorage) GetIncomingEdges(key graphEntityKey) ([]algorithmGraphEdge, error) {
	if g.reader == nil {
		return nil, storage.ErrInvalidData
	}
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

// Projection builders receive keys and payloads together from this boundary.
// They cannot reconstruct an endpoint or property key from a native local ID.
type projectionNodeInput struct {
	key        string
	labels     []string
	properties map[string]interface{}
}

type projectionEdgeInput struct {
	key, source, target, relationshipType string
	properties                            map[string]interface{}
}

func (p *GraphProjection) nativeNodeInput(node *storage.Node) projectionNodeInput {
	return projectionNodeInput{key: node.Identity().InDatabase(p.Database), labels: node.Labels, properties: node.Properties}
}

func (p *GraphProjection) nativeEdgeInput(edge *storage.Edge) projectionEdgeInput {
	return projectionEdgeInput{
		key:    edge.Identity().ReferenceIn(p.Database, "5"),
		source: edge.StartIdentity().InDatabase(p.Database), target: edge.EndIdentity().InDatabase(p.Database),
		relationshipType: edge.Type, properties: edge.Properties,
	}
}

// bindIdentity converts native/config references only after the graph's key
// domain is known. Projection IDs retain their public catalog domain; stored
// graphs use the same typed key/payload boundary as scalar and APOC algorithms.
func graphProjectionKey(id string) graphEntityKey { return graphEntityKey(id) }

func (c *linkPredictionConfig) bindIdentity(graph *algorithmGraphStorage, projection *GraphProjection) {
	read := graph.inputKey
	c.identityOutput = graph.outputID
	if projection != nil {
		read = func(value interface{}) (graphEntityKey, bool) {
			id, ok := projection.nodeReference(value)
			return graphProjectionKey(id), ok
		}
		c.identityOutput = func(id graphEntityKey) string { return string(id) }
	}
	c.identityNode = func(key graphEntityKey) (*storage.Node, error) {
		if projection != nil {
			key, _ = graph.inputKey(string(key))
		}
		node, err := graph.node(key)
		return node.value, err
	}
	value := c.sourceValue
	if value == nil {
		value = string(c.SourceNode)
	}
	c.sourceKey, _ = read(value)
	if c.TargetNodes != nil {
		values := c.targetValues
		if values == nil {
			for id := range c.TargetNodes {
				values = append(values, string(id))
			}
		}
		c.targetKeys = make(map[graphEntityKey]bool, len(values))
		for _, value := range values {
			if id, ok := read(value); ok {
				c.targetKeys[id] = true
			}
		}
	}
}

func (c *linkPredictionConfig) identityRows(result *ExecuteResult) *ExecuteResult {
	if c.identityOutput != nil {
		for _, row := range result.Rows {
			for i := 0; i < 2 && i < len(row); i++ {
				if id, ok := row[i].(string); ok {
					row[i] = c.identityOutput(graphEntityKey(id))
				}
			}
		}
	}
	return result
}

func (g *algorithmGraphStorage) outputID(key graphEntityKey) string {
	identity, ok := storage.EntityIdentityFromKey(string(key)).Resolve(g.source)
	if !ok {
		return ""
	}
	return identity.InDatabase(storageDatabaseName(g.source))
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
	case storage.Node:
        return entity.Identity(), true
    case storage.Edge:
        return entity.Identity(), true
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
	copy := *node
	copy.Labels, copy.Properties = nil, nil
	return &copy
}

func nativeDeletedEdgeValue(edge *storage.Edge) *storage.Edge {
	copy := *edge
	copy.Properties = nil
	return &copy
}

// All native-to-native binding comparisons use physical source and entity
// kind. A local ID or a public alias alone is not an entity identity.
func nativeEntitiesEqual(left, right interface{}) bool {
	switch left := left.(type) {
	case *storage.Node:
		right, ok := right.(*storage.Node)
		return ok && left != nil && right != nil && left.Identity().Equal(right.Identity())
	case *storage.Edge:
		right, ok := right.(*storage.Edge)
		return ok && left != nil && right != nil && left.Identity().Equal(right.Identity())
	}
	return false
}

func nativeGraphEntityKey(value interface{}) graphEntityKey {
	identity, ok := nativeEntityIdentity(value)
	if !ok {
		return ""
	}
	return graphEntityKey(identity.Key())
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

	if engine, ok := identity.SourceEngine(selected); ok {
		return engine, storage.QualifiedEntityID(engine, identity.LocalID)
	}
	if scoped, err := e.graphFunctionStorage("", selected); err == nil {
		if engine, ok := identity.SourceEngine(scoped); ok {
			return engine, storage.QualifiedEntityID(engine, identity.LocalID)
		}
	}
	return nil, nativeEntityMetadataID(selected, identity)
}

func classifyDeleteTargetValue(val interface{}) deleteTargetValue {
	switch v := val.(type) {
	case map[string]interface{}:
		if id, ok := v["_edgeId"].(string); ok && id != "" {
			return deleteTargetValue{kind: deleteProjectionRelationship, identity: storage.EntityIdentity{LocalID: id}, edgeID: storage.EdgeID(id)}
		}
		if id, ok := v["_nodeId"].(string); ok && id != "" {
			return deleteTargetValue{kind: deleteProjectionNode, identity: storage.EntityIdentity{LocalID: id}, nodeID: storage.NodeID(id)}
		}
	case *storage.Node:
		if identity, ok := nativeEntityIdentity(v); ok {
			return deleteTargetValue{kind: deleteProjectionNode, identity: identity, nodeID: storage.NodeID(identity.LocalID)}
		}
	case *storage.Edge:
		if identity, ok := nativeEntityIdentity(v); ok {
			return deleteTargetValue{kind: deleteProjectionRelationship, identity: identity, edgeID: storage.EdgeID(identity.LocalID)}
		}
	case string:
		if v != "" {
			return deleteTargetValue{kind: deleteProjectionNode, identity: storage.EntityIdentity{LocalID: v}, nodeID: storage.NodeID(v)}
		}
	}
	return deleteTargetValue{}
}

func (p *GraphProjection) nodeReference(value interface{}) (string, bool) {
	identity, ok := graphEntityReference(value)
	if !ok {
		return "", false
	}
	// Text IDs already use the projection's public domain. Native values carry
	// their source explicitly and must pass through the same owner as producers.
	if _, native := value.(*storage.Node); !native {
		if _, present := p.nodeIndex[identity.LocalID]; present {
			return identity.LocalID, true
		}
		if kind, database, local, qualified := storage.ParseElementID(identity.LocalID); qualified {
			if kind != "4" {
				return "", false
			}
			identity = storage.EntityIdentity{Database: database, LocalID: local}
			return identity.InDatabase(p.Database), true
		}
		return identity.LocalID, true
	}
	return identity.InDatabase(p.Database), true
}

// The path and adjacency API has the same key/payload boundary as whole-graph
// algorithms. Only this owner translates a graph key to a storage lookup.
func (e *StorageExecutor) algorithmGraph(ctx context.Context) *algorithmGraphExecutor {
	source := e.getStorage(ctx)
	return &algorithmGraphExecutor{storage: &algorithmGraphStorage{source: source, reader: source, scope: e.graphIdentityScope}}
}

func (g *algorithmGraphStorage) inputKey(value interface{}) (graphEntityKey, bool) {
	reference, ok := graphEntityReference(value)
	if !ok {
		return "", false
	}
	if _, text := value.(string); text {
		if kind, database, local, qualified := storage.ParseElementID(reference.LocalID); qualified {
			if kind != "4" {
				return "", false
			}
			domain := graphIdentityDomain{engine: g.source, scope: g.scope}
			if storage.CompositeDatabaseOf(g.source) == "" && !domain.foreignQualifier(database) {
				database = storage.DatabaseOf(g.source)
			}
			reference.Database, reference.LocalID = database, local
		}
	}
	if reference.Database == "" && reference.Source == "" {
		if storage.CompositeDatabaseOf(g.source) != "" {
			if node, err := g.source.GetNode(storage.NodeID(reference.LocalID)); err == nil && node != nil {
				reference = node.Identity()
			}
		} else {
			reference.Database = storage.DatabaseOf(g.source)
		}
	}
	reference, ok = reference.Resolve(g.source)
	if !ok {
		return "", false
	}
	return graphEntityKey(reference.Key()), true
}

func (g *algorithmGraphStorage) node(key graphEntityKey) (algorithmGraphNode, error) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(g.source)
	if !ok || g.reader == nil {
		return algorithmGraphNode{}, storage.ErrNotFound
	}
	node, err := g.reader.GetNode(id)
	if err != nil || node == nil {
		if err == nil {
			err = storage.ErrNotFound
		}
		return algorithmGraphNode{}, err
	}
	return algorithmGraphNode{ID: key, value: node}, nil
}

func (g *algorithmGraphStorage) nodeKey(node *storage.Node) graphEntityKey {
	if node == nil {
		return ""
	}
	return graphEntityKey(algorithmIdentity(g.source, node.Identity()).Key())
}

func (g *algorithmGraphStorage) incidentEdges(ctx context.Context, key graphEntityKey) ([]algorithmGraphEdge, []algorithmGraphEdge, error) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(g.source)
	if !ok || g.reader == nil {
		return nil, nil, storage.ErrNotFound
	}
	outgoing, incoming, err := incidentEdgeListsContext(ctx, g.reader, id, "both")
	if err != nil {
		return nil, nil, err
	}
	out, err := g.edges(outgoing, nil)
	if err != nil {
		return nil, nil, err
	}
	in, err := g.edges(incoming, nil)
	return out, in, err
}

// These methods adapt the typed native payload boundary to linkpredict's one
// graph builder. No native Node.ID or Edge endpoint is replaced with a key.
func (g *algorithmGraphStorage) AllNodes() ([]algorithmGraphNode, error) {
	if g.reader == nil {
		return nil, storage.ErrInvalidData
	}
	nodes, err := g.reader.AllNodes()
	if err != nil {
		return nil, err
	}
	return algorithmNativeNodes(nodes, g.source), nil
}
func (*algorithmGraphStorage) NodeKey(node algorithmGraphNode) graphEntityKey { return node.ID }
func (g *algorithmGraphStorage) NodeDisplay(key graphEntityKey) string        { return g.outputID(key) }
func (*algorithmGraphStorage) EdgeEndpoints(edge algorithmGraphEdge) (graphEntityKey, graphEntityKey, string) {
	return edge.StartNode, edge.EndNode, edge.Type
}

func algorithmEdgeValue(edge algorithmGraphEdge) *storage.Edge {
	if edge.value == nil {
		return nil
	}
	copy := *edge.value
	return &copy
}

// Native selected-path readers still use the storage-local endpoint domain.
// Endpoint choice itself is shared with the typed graph API; provenance is
// retained by the native edge until those readers enter the lookup owner.
func otherEnd(edge *storage.Edge, from storage.NodeID) storage.NodeID {
	return otherEndpoint(edge.StartNode, edge.EndNode, from)
}

func otherEndpoint[K comparable](start, end, from K) K {
	if start == from {
		return end
	}
	return start
}

func (g *algorithmGraphStorage) outputPath(path []graphEntityKey) []storage.NodeID {
	result := make([]storage.NodeID, len(path))
	for i, key := range path {
		identity, ok := storage.EntityIdentityFromKey(string(key)).Resolve(g.source)
		if ok {
			result[i] = storage.NodeID(identity.InDatabase(storage.DatabaseOf(g.source)))
		}
	}
	return result
}

func (g *algorithmGraphExecutor) requiredNodeKey(procedure string, args []interface{}, index int, argument string) (graphEntityKey, error) {
	value := procedureArgument(args, index)
	switch value := value.(type) {
	case nil:
		return "", procedureArgumentNullError(procedure, argument)
	case *storage.Node:
		if value == nil {
			return "", procedureArgumentNullError(procedure, argument)
		}
		key, _ := g.storage.inputKey(value)
		return key, nil
	case string:
		key, _ := g.storage.inputKey(value)
		return key, nil
	default:
		return "", procedureArgumentTypeError(procedure, argument, "NODE", value)
	}
}

// Selected-path products and witnesses use graph keys. Their storage reads
// translate here, retaining the native payload's source until lookup.
func nativeEdgeGraphEndpointKeys(edge *storage.Edge) [2]graphEntityKey {
	if edge == nil {
		return [2]graphEntityKey{}
	}
	return [2]graphEntityKey{graphEntityKey(edge.StartIdentity().Key()), graphEntityKey(edge.EndIdentity().Key())}
}

func nativeOtherEndpointKey(edge *storage.Edge, from graphEntityKey) graphEntityKey {
	endpoints := nativeEdgeGraphEndpointKeys(edge)
	return otherEndpoint(endpoints[0], endpoints[1], from)
}

func nativeGraphIncidentEdges(ctx context.Context, source storage.Engine, key graphEntityKey, direction string) ([]*storage.Edge, []*storage.Edge, error) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(source)
	// A bound node from another graph has no adjacency in this graph. Read
	// failures for a selected node still come from the shared storage reader.
	if !ok {
		return nil, nil, nil
	}
	return incidentEdgeListsContext(ctx, source, id, direction)
}

func (e *StorageExecutor) loadTraversalEndpointKey(ctx *TraversalContext, key graphEntityKey) (*storage.Node, bool) {
	id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(e.storage)
	if !ok {
		return nil, false
	}
	return e.loadTraversalEndpointNode(ctx, id)
}

func nativeGraphBatchNodes(source storage.Engine, keys []graphEntityKey) (map[graphEntityKey]*storage.Node, error) {
	requests := make([]storage.NodeID, 0, len(keys))
	originals := make(map[storage.NodeID][]graphEntityKey, len(keys))
	for _, key := range keys {
		if id, ok := storage.EntityIdentityFromKey(string(key)).NodeLookupID(source); ok {
			if _, present := originals[id]; !present {
				requests = append(requests, id)
			}
			originals[id] = append(originals[id], key)
		}
	}
	loaded, err := source.BatchGetNodes(requests)
	if err != nil {
		return nil, err
	}
	result := make(map[graphEntityKey]*storage.Node, len(keys))
	for id, node := range loaded {
		for _, key := range originals[id] {
			result[key] = node
		}
	}
	return result, nil
}

// The transaction boundary owns the conversion between native source values
// and the physical namespace of its Badger participant. Every write route uses
// these same adapters; a foreign source is rejected before tokens or data change.
func nativeNodeLookupID(engine storage.Engine, node *storage.Node) (storage.NodeID, bool) {
	id, ok := nativeEntityStorageID(engine, node)
	return storage.NodeID(id), ok
}

func (w *transactionStorageWrapper) nativeStorageNode(node *storage.Node) (*storage.Node, storage.NodeID, error) {
	if node == nil {
		return nil, "", storage.ErrInvalidData
	}
	id, ok := nativeNodeLookupID(w, node)
	if !ok {
		return nil, "", storage.ErrNotFound
	}
	if w.namespace == "" {
		return node, id, nil
	}
	physical := storage.CopyNode(node)
	physical.ID = w.prefixNodeID(id)
	return physical, id, nil
}

func (w *transactionStorageWrapper) nativeStorageEdge(edge *storage.Edge) (*storage.Edge, error) {
	if edge == nil {
		return nil, storage.ErrInvalidData
	}
	id, ok := nativeEntityStorageID(w, edge)
	start, end, endpoints := nativeEdgeStorageEndpoints(w, edge)
	if !ok || !endpoints {
		return nil, storage.ErrNotFound
	}
	if w.namespace == "" {
		return edge, nil
	}
	physical := storage.CopyEdge(edge)
	physical.ID = w.prefixEdgeID(storage.EdgeID(id))
	physical.StartNode, physical.EndNode = w.prefixNodeID(start), w.prefixNodeID(end)
	return physical, nil
}

// Stored namespace membership is checked before exposing a participant value.
// It is a physical-prefix boundary, never a key for comparing graph entities.
func (w *transactionStorageWrapper) storedNodeInNamespace(node *storage.Node) bool {
	return node != nil && (w.namespace == "" || strings.HasPrefix(string(node.ID), w.namespace+w.separator))
}
func (w *transactionStorageWrapper) storedEdgeInNamespace(edge *storage.Edge) bool {
	return edge != nil && (w.namespace == "" || strings.HasPrefix(string(edge.ID), w.namespace+w.separator))
}
func (w *transactionStorageWrapper) userNodeHasPrefix(node *storage.Node, prefix string) bool {
	id, ok := nativeNodeLookupID(w, node)
	return ok && strings.HasPrefix(string(id), prefix)
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

// Streaming keeps the transaction's immutable payload on a shallow value copy.
// Point/batch readers retain their existing deep-copy contract below. Both
// restore the same endpoint IDs and database/source provenance.
func (w *transactionStorageWrapper) toUserEdgeView(edge *storage.Edge) storage.Edge {
	out := *edge
	out.ID, out.Database = w.unprefixEdgeID(out.ID), w.namespace
	out.StartNode, out.EndNode = w.unprefixNodeID(out.StartNode), w.unprefixNodeID(out.EndNode)
	return out
}
func (w *transactionStorageWrapper) toUserEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	out := w.toUserEdgeView(edge)
	return storage.CopyEdge(&out)
}

// gdsNodeID retains the accepted bare-ID contract only for the deferred row
// similarity adapter. Projection/filter consumers use GraphProjection.nodeReference.
func gdsNodeID(value interface{}) (storage.NodeID, bool) {
	switch node := value.(type) {
	case *storage.Node:
		if node != nil {
			return node.ID, true
		}
	case string:
		return storage.NodeID(node), true
	default:
		if isIntegerProcedureValue(value) {
			return storage.NodeID(fmt.Sprint(value)), true
		}
	}
	return "", false
}

// Native mutation adapters retain the selected source until the actual storage
// boundary. The existing callbacks, cache, retry and completion owners stay in
// their callers; no private graph key is used as a storage ID or client ID.
func nativeEdgeLookupID(engine storage.Engine, edge *storage.Edge) (storage.EdgeID, bool) {
	id, ok := nativeEntityStorageID(engine, edge)
	return storage.EdgeID(id), ok
}

func setNativeCreatedNodeID(node *storage.Node, id storage.NodeID)           { node.ID = id }
func setNativeRelationshipCandidateID(edge *storage.Edge, id storage.EdgeID) { edge.ID = id }

func nativeRelationshipEndpointsEqual(engine storage.Engine, left, right *storage.Edge) bool {
	leftStart, leftEnd, leftOK := nativeEdgeStorageEndpoints(engine, left)
	rightStart, rightEnd, rightOK := nativeEdgeStorageEndpoints(engine, right)
	return leftOK && rightOK && leftStart == rightStart && leftEnd == rightEnd
}

func (e *StorageExecutor) notifyNativeEntityMutated(engine storage.Engine, value interface{}) {
	id, selected := nativeEntityStorageID(engine, value)
	if !selected {
		return
	}
	switch value.(type) {
	case *storage.Node:
		e.notifyNodeMutated(id)
	case *storage.Edge:
		e.notifyEdgeMutated(id)
	}
}

func (e *StorageExecutor) notifyNativeRelationshipEndpoints(engine storage.Engine, edge *storage.Edge) {
	start, end, selected := nativeEdgeStorageEndpoints(engine, edge)
	if !selected {
		return
	}
	e.notifyNodeMutated(string(start))
	if start != end {
		e.notifyNodeMutated(string(end))
	}
}

// Native prefix checks apply only at an existing physical namespace boundary;
// they do not interpret an opaque local ID or turn it into a graph key.
func nativeStoredIDHasPrefix(value interface{}, prefix string) bool {
	identity, present := nativeEntityIdentity(value)
	return present && strings.HasPrefix(identity.LocalID, prefix)
}

// Identity ordering compares the source-qualified value directly. It does not
// serialize private keys on every sort comparison, and retains local-ID order
// within one source. No source string becomes part of an output value.
func nativeEntityOrder(left, right interface{}) int {
	a, aOK := nativeEntityIdentity(left)
	b, bOK := nativeEntityIdentity(right)
	if !aOK || !bOK {
		if aOK {
			return 1
		}
		if bOK {
			return -1
		}
		return 0
	}
	if a.Source != "" || b.Source != "" {
		if order := strings.Compare(a.Source, b.Source); order != 0 {
			return order
		}
	} else if order := strings.Compare(a.Database, b.Database); order != 0 {
		return order
	}
	return strings.Compare(a.LocalID, b.LocalID)
}

// Public local-ID contracts and storage lookup contracts are intentionally
// different adapters. Output IDs preserve their existing bytes; they are never
// graph keys and must not be used to choose a storage participant.
func nativeEntityOutputID(value interface{}) string {
    identity, ok := nativeEntityIdentity(value)
    if !ok { return "" }
    return identity.LocalID
}

func nativeRelationshipOutputEndpoints(edge *storage.Edge) (string, string) {
    if edge == nil { return "", "" }
    return string(edge.StartNode), string(edge.EndNode)
}

func nativeEdgeStartsAt(edge *storage.Edge, node *storage.Node) bool {
    return edge != nil && node != nil && edge.StartIdentity().Equal(node.Identity())
}

func nativeNodeScoringSnapshot(source *storage.Node) *storage.Node {
    snapshot := *source
    snapshot.EmbedMeta, snapshot.NamedEmbeddings, snapshot.ChunkEmbeddings = nil, nil, nil
    return storage.CopyNode(&snapshot)
}

func nativeEdgeScoringSnapshot(source *storage.Edge) *storage.Edge {
    return storage.CopyEdge(source)
}

func nativeIdentityBytes(value interface{}) int64 {
    switch value := value.(type) {
    case *storage.Node:
        if value != nil { return int64(len(value.ID)) }
    case *storage.Edge:
        if value != nil { return int64(len(value.ID)+len(value.StartNode)+len(value.EndNode)) }
    }
    return 0
}

func nativeNodeWithID(source *storage.Node, id storage.NodeID) *storage.Node {
    copy := *source
    copy.ID = id
    return &copy
}

func setNativeRelationshipEndpoints(edge *storage.Edge, start, end storage.NodeID) {
    edge.StartNode, edge.EndNode = start, end
}

func nativeRelationshipEndpointValue(e *StorageExecutor, edge *storage.Edge, end bool, store storage.Engine) interface{} {
    if edge == nil { return nil }
    id, selected := nativeEdgeLookupID(store, edge)
    start, target, endpoints := nativeEdgeStorageEndpoints(store, edge)
    if !selected || !endpoints { return nil }
    if _, err := store.GetEdge(id); err != nil { return nil }
    if end { start = target }
    node, err := e.getQueryNode(store, start)
    if err != nil { return nil }
    return node
}

func nativeNodeDegree(store storage.Engine, node *storage.Node, direction string) int64 {
    id, selected := nativeNodeLookupID(store, node)
    if !selected { return 0 }
    switch direction {
    case "incoming": return int64(store.GetInDegree(id))
    case "outgoing": return int64(store.GetOutDegree(id))
    }
    return int64(store.GetInDegree(id)+store.GetOutDegree(id))
}

func nativeRelationshipStartOutputID(edge *storage.Edge) string { start, _ := nativeRelationshipOutputEndpoints(edge); return start }
func nativeRelationshipEndOutputID(edge *storage.Edge) string { _, end := nativeRelationshipOutputEndpoints(edge); return end }

func deleteNativeCatalogNode(store storage.Engine, node *storage.Node) error {
    id, selected := nativeNodeLookupID(store, node)
    if !selected { return storage.ErrNotFound }
    return store.DeleteNode(id)
}

func nativeEntityNotFound(value interface{}) error {
    if edge, ok := value.(*storage.Edge); ok && edge != nil {
        return localizedStatusError("Neo.ClientError.Statement.EntityNotFound", "EntityNotFound", localization.CypherProceduresRelationshipNotFound(edge.ElementID("")))
    }
    id := nativeEntityOutputID(value)
    if node, ok := value.(*storage.Node); ok && node != nil { id = node.ElementID("") }
    return localizedStatusError("Neo.ClientError.Statement.EntityNotFound", "EntityNotFound", localization.CypherProceduresNodeNotFound(id))
}

func nativeSchemaRelationship(database string, id storage.EdgeID, start, end *storage.Node, kind string) *storage.Edge {
    return &storage.Edge{ID: id, Database: database, StartNode: start.ID, EndNode: end.ID, Type: kind, Properties: map[string]interface{}{"name": kind}}
}

func nativeSchemaIndexID(store storage.Engine, node *storage.Node) (storage.NodeID, bool) {
    id, ok := nativeNodeLookupID(store, node)
    return storage.EnsureNodeIDDatabasePrefixForEngine(store, id), ok
}

func nativeLookupCacheIDMatches(store storage.Engine, node *storage.Node, id storage.NodeID) bool {
    selected, ok := nativeNodeLookupID(store, node)
    return ok && selected == id
}

func nativeEndpointCacheKey(store storage.Engine, id storage.NodeID) graphEntityKey {
    reference := storage.EntityIdentity{LocalID: string(id)}
    reference = algorithmIdentity(store, reference)
    return graphEntityKey(reference.Key())
}

func nativeProjectionNodeID(node *storage.Node, database string) string { return node.Identity().InDatabase(database) }

type nativeMaterializedAccessRecorder struct { executor *StorageExecutor }
func (r nativeMaterializedAccessRecorder) RecordMaterializedAccess(id string) {
    if recorder, ok := r.executor.storage.(materializedAccessRecorder); ok { recorder.RecordMaterializedAccess(id) }
}
func (r nativeMaterializedAccessRecorder) RecordNativeEntityAccess(value interface{}) {
    identity, present := nativeEntityIdentity(value)
    if !present { return }
    engine, id := r.executor.nativeEntityMetadataTarget(context.Background(), identity)
    if recorder, ok := engine.(materializedAccessRecorder); ok { recorder.RecordMaterializedAccess(id) }
}
