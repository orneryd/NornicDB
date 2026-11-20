package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ErrFineGrainedForbidden is returned when the principal lacks a required
// fine-grained entitlement. Invisible entities use storage.ErrNotFound for
// graph reads.
var ErrFineGrainedForbidden = &FineGrainedPermissionError{}

// FineGrainedDatabaseIdentity resolves a canonical database name to its stable
// database identifier. An empty result means that the target cannot be resolved.
type FineGrainedDatabaseIdentity func(canonicalName string) string

// FineGrainedRequest binds a catalog snapshot to a principal and a catalog
// identity resolver for a request.
type FineGrainedRequest struct {
	Access                 FineGrainedAccess
	DatabaseID             FineGrainedDatabaseIdentity
	HomeDatabaseID         string
	HomeDatabaseName       string
	PasswordChangeRequired bool
	Runtime                *FineGrainedRuntime
	Fingerprint            string
	identity               RequestIdentity
	databaseOwner          func(string) string
}

type fineGrainedRequestKey struct{}

// WithFineGrainedRequest associates an admitted principal with ctx.
func WithFineGrainedRequest(ctx context.Context, request *FineGrainedRequest) context.Context {
	return context.WithValue(ctx, fineGrainedRequestKey{}, request)
}

// FineGrainedRequestFromContext returns the request's fine-grained principal,
// when enabled by the request owner.
func FineGrainedRequestFromContext(ctx context.Context) *FineGrainedRequest {
	if ctx == nil {
		return nil
	}
	request, _ := ctx.Value(fineGrainedRequestKey{}).(*FineGrainedRequest)
	return request
}

// AllowsDatabase checks a capability on a canonical database target.
func (r *FineGrainedRequest) AllowsDatabase(action, database, property string) bool {
	return r.Access.Allows(action, FineGrainedResource{
		Kind: FineGrainedDatabase, DatabaseID: r.DatabaseID(database), Property: property, HomeDatabaseID: r.HomeDatabaseID,
	})
}

// AllowsDBMS checks an operation that affects the server rather than a database.
func (r *FineGrainedRequest) AllowsDBMS(action string, target string) bool {
	return r.Access.Allows(action, FineGrainedResource{Kind: FineGrainedDBMS, Property: target})
}

// Graph binds graph operations to the same policy for both point and
// population reads. The supplied engine determines the transaction snapshot.
func (r *FineGrainedRequest) Graph(engine storage.Engine, database string) storage.Engine {
	if scoped, ok := engine.(*fineGrainedGraph); ok && scoped.request == r && scoped.database == database {
		return scoped
	}
	return &fineGrainedGraph{engine: engine, request: r, database: database, databaseID: r.DatabaseID(database)}
}

// FineGrainedEntityID identifies one stored node or relationship incarnation.
// For example, a policy editor uses the ID of the resolved node and its
// persisted creation time to target that node across property updates.
func FineGrainedEntityID(localID string, createdAt time.Time) string {
	if localID == "" {
		return ""
	}
	fingerprint := sha256.New()
	fmt.Fprintf(fingerprint, "%s\x00%d\x00%d", localID, createdAt.Unix(), createdAt.Nanosecond())
	return fmt.Sprintf("%x", fingerprint.Sum(nil))
}

type fineGrainedGraph struct {
	engine     storage.Engine
	request    *FineGrainedRequest
	database   string
	databaseID string
	// Update and relationship creation may refer to entities bound by this
	// statement's CREATE. These authorizations never widen a graph read.
	createdNodes sync.Map // storage.NodeID -> time.Time
	createdEdges sync.Map // storage.EdgeID -> time.Time
}

var _ storage.Engine = (*fineGrainedGraph)(nil)

func (g *fineGrainedGraph) Namespace() string { return g.database }

func (g *fineGrainedGraph) entityDatabaseID(database string) string {
	if database == "" || database == g.database {
		return g.databaseID
	}
	return g.request.DatabaseID(database)
}

func (g *fineGrainedGraph) nodeResource(node *storage.Node) FineGrainedResource {
	return g.request.nodeResource(g.entityDatabaseID(node.Identity().Database), node)
}

func (r *FineGrainedRequest) nodeResource(databaseID string, node *storage.Node) FineGrainedResource {
	return FineGrainedResource{
		Kind: FineGrainedNode, DatabaseID: databaseID,
		Types: node.Labels, EntityID: FineGrainedEntityID(node.Identity().LocalID, node.CreatedAt),
		EntityProperties: node.Properties, HomeDatabaseID: r.HomeDatabaseID,
	}
}

// AllowsManagedEmbedding checks the actual labels and fields a provider
// consumes, plus the target's managed-vector write privilege.
func (r *FineGrainedRequest) AllowsManagedEmbedding(database string, node *storage.Node, input storage.EmbeddingTextPolicy, allProperties bool) bool {
	if !r.CanAccessDatabase(database) {
		return false
	}
	target := r.nodeResource(r.DatabaseID(database), node)
	if !r.Access.Allows("graph.node.traverse", target) || !r.Access.Allows("graph.node.embedding.set", target) {
		return false
	}
	for property := range node.Properties {
		if !allProperties && !input.FeedsText(property) {
			continue
		}
		target.Property = property
		if !r.Access.Allows("graph.node.property.read", target) {
			return false
		}
	}
	return true
}

func (g *fineGrainedGraph) edgeResource(edge *storage.Edge) FineGrainedResource {
	return FineGrainedResource{
		Kind: FineGrainedRelationship, DatabaseID: g.entityDatabaseID(edge.Database),
		Types: []string{edge.Type}, EntityID: FineGrainedEntityID(edge.Identity().LocalID, edge.CreatedAt),
		EntityProperties: edge.Properties, HomeDatabaseID: g.request.HomeDatabaseID,
	}
}

func (g *fineGrainedGraph) projectNode(node *storage.Node) *storage.Node {
	target := g.nodeResource(node)
	if !g.request.Access.Allows("graph.node.traverse", target) {
		return nil
	}
	projected := *node
	projected.Labels = slices.Clone(node.Labels)
	projected.Properties = g.readProperties("graph.node.property.read", target)
	if !g.request.Access.Allows("graph.node.embedding.read", target) {
		projected.NamedEmbeddings, projected.ChunkEmbeddings, projected.EmbedMeta = nil, nil, nil
	}
	return &projected
}

func (g *fineGrainedGraph) projectEdge(ctx context.Context, edge *storage.Edge) (*storage.Edge, error) {
	target := g.edgeResource(edge)
	if !g.request.Access.Allows("graph.relationship.traverse", target) {
		return nil, nil
	}
	for _, endpoint := range [...]storage.EntityIdentity{edge.StartIdentity(), edge.EndIdentity()} {
		id, local := endpoint.NodeLookupID(g.engine)
		if !local {
			return nil, nil
		}
		node, err := g.engine.GetNode(ctx, id)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if !g.request.Access.Allows("graph.node.traverse", g.nodeResource(node)) {
			return nil, nil
		}
	}
	projected := *edge
	projected.Properties = g.readProperties("graph.relationship.property.read", target)
	return &projected, nil
}

func (g *fineGrainedGraph) readProperties(action string, target FineGrainedResource) map[string]any {
	properties := make(map[string]any)
	for key, value := range target.EntityProperties {
		target.Property = key
		if g.request.Access.Allows(action, target) {
			properties[key] = value
		}
	}
	return properties
}

func (g *fineGrainedGraph) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := g.engine.GetNode(ctx, id)
	if err != nil {
		return nil, err
	}
	if visible := g.projectNode(node); visible != nil {
		return visible, nil
	}
	return nil, storage.ErrNotFound
}

func (g *fineGrainedGraph) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	edge, err := g.engine.GetEdge(ctx, id)
	if err != nil {
		return nil, err
	}
	visible, err := g.projectEdge(ctx, edge)
	if err == nil && visible == nil {
		err = storage.ErrNotFound
	}
	return visible, err
}

func (g *fineGrainedGraph) visibleNodes(nodes []*storage.Node, err error) ([]*storage.Node, error) {
	if err != nil {
		return nil, err
	}
	var visible []*storage.Node
	for _, node := range nodes {
		if projected := g.projectNode(node); projected != nil {
			visible = append(visible, projected)
		}
	}
	return visible, nil
}

func (g *fineGrainedGraph) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	return g.visibleNodes(g.engine.GetNodesByLabel(ctx, label))
}

func (g *fineGrainedGraph) GetFirstNodeByLabel(ctx context.Context, label string) (*storage.Node, error) {
	return storage.FirstNodeResult(g.GetNodesByLabel(ctx, label))
}

func (g *fineGrainedGraph) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	return g.visibleNodes(g.engine.AllNodes(ctx))
}

func (g *fineGrainedGraph) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	nodes, err := g.engine.BatchGetNodes(ctx, ids)
	if err != nil {
		return nil, err
	}
	visible := make(map[storage.NodeID]*storage.Node, len(nodes))
	for id, node := range nodes {
		if projected := g.projectNode(node); projected != nil {
			visible[id] = projected
		}
	}
	return visible, nil
}

func (g *fineGrainedGraph) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	// Property selection and predicates operate on the authorized image. The
	// storage scanner still owns the namespace, label and decay rules.
	scan := opts
	scan.Projection = nil
	scan.PropertyFilter, scan.PropertyFilterProjection, scan.PropertyStringEquals = nil, nil, nil
	scan.WithEmbeddings, scan.StripEmbeddings = opts.WithEmbeddings, opts.StripEmbeddings
	return g.engine.StreamNodesWithOptions(ctx, scan, func(node *storage.Node) error {
		projected := g.projectNode(node)
		if projected == nil {
			return nil
		}
		if opts.PropertyFilter != nil && !opts.PropertyFilter(projected.Properties) {
			return nil
		}
		if opts.Projection != nil {
			maps.DeleteFunc(projected.Properties, func(key string, _ any) bool {
				return !slices.Contains(opts.Projection, key)
			})
		}
		return visit(projected)
	})
}

func (g *fineGrainedGraph) visibleEdges(ctx context.Context, edges []*storage.Edge, readErr error) ([]*storage.Edge, error) {
	if readErr != nil {
		return nil, readErr
	}
	var visible []*storage.Edge
	for _, edge := range edges {
		projected, err := g.projectEdge(ctx, edge)
		if err != nil {
			return nil, err
		}
		if projected != nil {
			visible = append(visible, projected)
		}
	}
	return visible, nil
}

func (g *fineGrainedGraph) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	headers := request.Headers
	request.Headers = false
	edges, err := g.engine.ReadAdjacentEdges(ctx, request)
	visible, err := g.visibleEdges(ctx, edges, err)
	if headers {
		for _, edge := range visible {
			edge.Properties = nil
		}
	}
	return visible, err
}

func (g *fineGrainedGraph) GetEdgeBetween(ctx context.Context, start, end storage.NodeID, relationshipType string) (*storage.Edge, error) {
	request := storage.AdjacentEdgeRead{Direction: storage.AdjacentBetween, Node: start, Other: end}
	if relationshipType != "" {
		request.Types = []string{relationshipType}
	}
	edges, err := g.ReadAdjacentEdges(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(edges) == 0 {
		return nil, storage.ErrNotFound
	}
	return edges[0], nil
}

func (g *fineGrainedGraph) GetEdgesByType(ctx context.Context, relationshipType string) ([]*storage.Edge, error) {
	edges, err := g.engine.GetEdgesByType(ctx, relationshipType)
	return g.visibleEdges(ctx, edges, err)
}

func (g *fineGrainedGraph) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	edges, err := g.engine.AllEdges(ctx)
	return g.visibleEdges(ctx, edges, err)
}

// GraphMutationVersion exports only the version needed to invalidate scoped
// continuations. Access to the engine and its optional indexes stays private.
func (g *fineGrainedGraph) GraphMutationVersion() (uint64, bool) {
	if owner, ok := g.engine.(storage.GraphMutationVersionProvider); ok {
		return owner.GraphMutationVersion()
	}
	return 0, false
}
