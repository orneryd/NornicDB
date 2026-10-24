package cypher

import (
	"reflect"
	"slices"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Read-only expressions reuse immutable projections until their public row is
// published. The scoring instant belongs to the statement, but bodies do not.
// The additional budget bounds work between published rows (e.g. filtered
// candidates). Write statements omit this memo and observe their new body.
const statementPropertyViewBytes = 1 << 20
const statementPropertyViewEntries = 1024

type statementPropertyViews struct {
	mu         sync.Mutex
	projection storage.QueryResultProjection
	nodes      map[*storage.Node]*storage.Node
	edges      map[*storage.Edge]*storage.Edge
	nodeBodies map[storage.NodeID]nodePropertyViewBody
	edgeBodies map[storage.EdgeID]edgePropertyViewBody
	bytes      int64
}
type nodePropertyViewBody struct {
	source     *storage.Node
	properties map[string]interface{}
}
type edgePropertyViewBody struct {
	source     *storage.Edge
	properties map[string]interface{}
}

func (e *StorageExecutor) withStatementPropertyViews(query string) *StorageExecutor {
	if e.statementPropertyViews != nil || !e.queryPropertyDecayEnabled() {
		return nil
	}
	requirements := QueryPermissionRequirements(query)
	if requirements.Write || requirements.Schema || requirements.Admin || !isCacheableReadQuery(query) {
		return nil
	}
	scoped := e.cloneWithStorage(e.storage)
	scoped.statementPropertyViews = &statementPropertyViews{projection: storage.NewQueryResultProjection(e.storage)}
	return scoped
}

func (v *statementPropertyViews) clear() {
	clear(v.nodes)
	clear(v.edges)
	clear(v.nodeBodies)
	clear(v.edgeBodies)
	v.bytes = 0
}

func (v *statementPropertyViews) releaseRow() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.clear()
}

// reserve runs under mu, before retaining either source pointers or detached
// snapshots. Oversized bodies may be projected for the current expression but
// never enter the memo. This is a cache estimate, not a statement memory limit.
func (v *statementPropertyViews) reserve(bytes int64) bool {
	if bytes > statementPropertyViewBytes {
		return false
	}
	if v.bytes+bytes > statementPropertyViewBytes || len(v.nodes)+len(v.edges) >= statementPropertyViewEntries {
		v.clear()
	}
	v.bytes += bytes
	return true
}

func (v *statementPropertyViews) node(source *storage.Node) *storage.Node {
	if source == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if cached := v.nodes[source]; cached != nil {
		return cached
	}
	// Include source, detached scoring body, projected map and map/key overhead.
	bytes := int64(512+len(source.ID)) + 3*util.EstimateValueBytes(source.Properties) + util.EstimateValueBytes(source.Labels) + util.EstimateValueBytes(source.EmbedMeta)
	for name, vector := range source.NamedEmbeddings {
		bytes += int64(64 + len(name) + 4*cap(vector))
	}
	for _, vector := range source.ChunkEmbeddings {
		bytes += int64(24 + 4*cap(vector))
	}
	cache := v.reserve(bytes)
	if !cache {
		out := *source
		out.Properties = v.projection.NodeProperties(source)
		return &out
	}
	if v.nodes == nil {
		v.nodes = make(map[*storage.Node]*storage.Node)
		v.nodeBodies = make(map[storage.NodeID]nodePropertyViewBody)
	}
	identity := storage.NodeID(source.IdentityKey())
	body, ok := v.nodeBodies[identity]
	// A wrapper may return a fresh pointer on every index recheck. Reuse only
	// when its complete scoring input equals our detached snapshot: timestamps
	// alone are not a body version, and an ID alone is never sufficient.
	if !ok || !source.CreatedAt.Equal(body.source.CreatedAt) || !source.UpdatedAt.Equal(body.source.UpdatedAt) || !slices.Equal(source.Labels, body.source.Labels) || !reflect.DeepEqual(source.Properties, body.source.Properties) {
		snapshot := storage.CopyNode(&storage.Node{ID: source.ID, Database: source.Database, Labels: source.Labels, Properties: source.Properties, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt})
		body = nodePropertyViewBody{snapshot, v.projection.NodeProperties(source)}
		v.nodeBodies[identity] = body
	}
	out := *source
	out.Properties = body.properties
	v.nodes[source] = &out
	v.nodes[&out] = &out
	return &out
}

func (v *statementPropertyViews) edge(source *storage.Edge) *storage.Edge {
	if source == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if cached := v.edges[source]; cached != nil {
		return cached
	}
	bytes := int64(512+len(source.ID)+len(source.StartNode)+len(source.EndNode)+len(source.Type)) + 3*util.EstimateValueBytes(source.Properties)
	if !v.reserve(bytes) {
		out := *source
		out.Properties = v.projection.EdgeProperties(source)
		return &out
	}
	if v.edges == nil {
		v.edges = make(map[*storage.Edge]*storage.Edge)
		v.edgeBodies = make(map[storage.EdgeID]edgePropertyViewBody)
	}
	identity := storage.EdgeID(source.IdentityKey())
	body, ok := v.edgeBodies[identity]
	if !ok || source.Type != body.source.Type || !source.CreatedAt.Equal(body.source.CreatedAt) || !source.UpdatedAt.Equal(body.source.UpdatedAt) || !reflect.DeepEqual(source.Properties, body.source.Properties) {
		snapshot := storage.CopyEdge(&storage.Edge{ID: source.ID, Database: source.Database, Type: source.Type, Properties: source.Properties, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt})
		body = edgePropertyViewBody{snapshot, v.projection.EdgeProperties(source)}
		v.edgeBodies[identity] = body
	}
	out := *source
	out.Properties = body.properties
	v.edges[source] = &out
	v.edges[&out] = &out
	return &out
}
