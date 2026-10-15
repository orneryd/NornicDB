package storage

import "strings"

// QueryResultProjection applies property decay to detached normal-query values.
// It never changes engine reads or stored property maps. Remote entities retain
// their source database's projection instead of being scored against this store.
// Construct it once per result owner; raw debug/history/export reads omit it.
type QueryResultProjection struct {
	engine *BadgerEngine
	prefix string
	now    int64
}

// NewQueryResultProjection resolves the database and storage wrapper once.
// The caller must already own its query reveal scope (as Cypher does). External
// query owners should use BeginQueryResultProjection instead.
// For example, visible := NewQueryResultProjection(engine).Node(node).
func NewQueryResultProjection(engine Engine) QueryResultProjection {
	be, _ := UnwrapEngine(engine).(*BadgerEngine)
	if be == nil || !be.IsDecayEnabled() {
		return QueryResultProjection{}
	}
	return QueryResultProjection{engine: be, prefix: QualifiedEntityID(engine, ""), now: DecayScoringTime()}
}

// BeginQueryResultProjection owns a normal-query reveal scope until release.
// Call it once at an external query boundary, not inside an already scoped query.
// For example: view, release := BeginQueryResultProjection(engine); defer release()
// then return view.Node(node). Independent REVEAL queries cannot bypass its filter.
func BeginQueryResultProjection(engine Engine) (QueryResultProjection, func()) {
	view := NewQueryResultProjection(engine)
	if view.engine == nil {
		return view, func() {}
	}
	release := view.engine.BeginQueryRevealScope(false)
	view.now = DecayScoringTime()
	return view, release
}

func (p QueryResultProjection) qualified(id string) string {
	if p.prefix != "" && !strings.HasPrefix(id, p.prefix) {
		return p.prefix + id
	}
	return id
}

// NodeProperties returns visible node properties without mutating their source.
// With decay disabled it returns the original map without allocating.
func (p QueryResultProjection) NodeProperties(node *Node) map[string]interface{} {
	if node == nil {
		return nil
	}
	if p.engine == nil {
		return node.Properties
	}
	created, updated := node.CreatedAt.UnixNano(), node.UpdatedAt.UnixNano()
	if node.UpdatedAt.IsZero() {
		updated = created
	}
	out := make(map[string]interface{}, len(node.Properties))
	id := NodeID(p.qualified(string(node.ID)))
	for key, value := range node.Properties {
		if !p.engine.FilterPropertyByDecay(id, node.Labels, key, created, updated, p.now) {
			out[key] = value
		}
	}
	return out
}

// EdgeProperties is NodeProperties' relationship counterpart, using the edge's
// database, type, update time and property policy. Stored properties stay intact.
func (p QueryResultProjection) EdgeProperties(edge *Edge) map[string]interface{} {
	if edge == nil {
		return nil
	}
	if p.engine == nil {
		return edge.Properties
	}
	created, updated := edge.CreatedAt.UnixNano(), edge.UpdatedAt.UnixNano()
	if edge.UpdatedAt.IsZero() {
		updated = created
	}
	out := make(map[string]interface{}, len(edge.Properties))
	id := EdgeID(p.qualified(string(edge.ID)))
	for key, value := range edge.Properties {
		if !p.engine.FilterEdgePropertyByDecay(id, edge.Type, key, created, updated, p.now) {
			out[key] = value
		}
	}
	return out
}

// Node projects a detached node for a query response, preserving typed identity.
// The original pointer is returned without allocation when decay is disabled.
func (p QueryResultProjection) Node(node *Node) *Node {
	if node == nil || p.engine == nil {
		return node
	}
	out := *node
	out.Properties = p.NodeProperties(node)
	return &out
}

// Edge projects a detached relationship for a query response. Mutation owners
// must keep the original edge, so unrelated writes retain suppressed fields.
func (p QueryResultProjection) Edge(edge *Edge) *Edge {
	if edge == nil || p.engine == nil {
		return edge
	}
	out := *edge
	out.Properties = p.EdgeProperties(edge)
	return &out
}
