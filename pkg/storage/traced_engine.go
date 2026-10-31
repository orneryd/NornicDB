package storage

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TracedEngine wraps an Engine and emits nornicdb.storage.<op> spans for each
// operation. The context used for span parenting is set via SetContext by the
// calling layer (cypher executor) which has the active span context.
type TracedEngine struct {
	Engine
	mu  sync.RWMutex
	ctx context.Context
}

// NewTracedEngine wraps inner with tracing instrumentation.
func NewTracedEngine(inner Engine) *TracedEngine {
	return &TracedEngine{Engine: inner, ctx: context.Background()}
}

// SetContext updates the context used for span parenting. Called by the cypher
// executor at the start of Execute so storage spans nest under the cypher span.
func (t *TracedEngine) SetContext(ctx context.Context) {
	t.mu.Lock()
	t.ctx = ctx
	t.mu.Unlock()
}

func (t *TracedEngine) getCtx() context.Context {
	t.mu.RLock()
	ctx := t.ctx
	t.mu.RUnlock()
	return ctx
}

// GetInnerEngine returns the underlying Engine so capability checks can peer
// through the tracing decorator via the canonical EngineUnwrapper accessor.
func (t *TracedEngine) GetInnerEngine() Engine { return t.Engine }

// startOperation is the single owner of storage span construction. It starts a
// nornicdb.storage.<op> span of kind SpanKindInternal parented by the context
// installed via SetContext, tagged with the entity kind. The caller owns the
// returned span and must End it (the facades defer End right after starting so
// the span covers the backing operation, including a panic).
func (t *TracedEngine) startOperation(name, kind string) trace.Span {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), name,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", kind)),
	)
	return span
}

func (t *TracedEngine) CreateNode(node *Node) (NodeID, error) {
	span := t.startOperation("nornicdb.storage.CreateNode", "node")
	defer span.End()
	return t.Engine.CreateNode(node)
}

func (t *TracedEngine) GetNode(id NodeID) (*Node, error) {
	span := t.startOperation("nornicdb.storage.GetNode", "node")
	defer span.End()
	return t.Engine.GetNode(id)
}

func (t *TracedEngine) UpdateNode(updateRequest NodeUpdateRequest) error {
	span := t.startOperation("nornicdb.storage.UpdateNode", "node")
	defer span.End()
	return t.Engine.UpdateNode(updateRequest)
}

func (t *TracedEngine) DeleteNode(id NodeID, origins ...uint64) error {
	span := t.startOperation("nornicdb.storage.DeleteNode", "node")
	defer span.End()
	return t.Engine.DeleteNode(id, origins...)
}

func (t *TracedEngine) CreateEdge(edge *Edge) error {
	span := t.startOperation("nornicdb.storage.CreateEdge", "edge")
	defer span.End()
	return t.Engine.CreateEdge(edge)
}

func (t *TracedEngine) GetEdge(id EdgeID) (*Edge, error) {
	span := t.startOperation("nornicdb.storage.GetEdge", "edge")
	defer span.End()
	return t.Engine.GetEdge(id)
}

func (t *TracedEngine) UpdateEdge(updateRequest EdgeUpdateRequest) error {
	span := t.startOperation("nornicdb.storage.UpdateEdge", "edge")
	defer span.End()
	return t.Engine.UpdateEdge(updateRequest)
}

func (t *TracedEngine) DeleteEdge(id EdgeID, origins ...uint64) error {
	span := t.startOperation("nornicdb.storage.DeleteEdge", "edge")
	defer span.End()
	return t.Engine.DeleteEdge(id, origins...)
}

func (t *TracedEngine) GetNodesByLabel(label string) ([]*Node, error) {
	span := t.startOperation("nornicdb.storage.GetNodesByLabel", "node")
	defer span.End()
	return t.Engine.GetNodesByLabel(label)
}

func (t *TracedEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	span := t.startOperation("nornicdb.storage.GetOutgoingEdges", "edge")
	defer span.End()
	return t.Engine.GetOutgoingEdges(nodeID)
}

func (t *TracedEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	span := t.startOperation("nornicdb.storage.GetIncomingEdges", "edge")
	defer span.End()
	return t.Engine.GetIncomingEdges(nodeID)
}

func (t *TracedEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	span := t.startOperation("nornicdb.storage.GetEdgesByType", "edge")
	defer span.End()
	return t.Engine.GetEdgesByType(edgeType)
}

func (t *TracedEngine) AllNodes() ([]*Node, error) {
	span := t.startOperation("nornicdb.storage.AllNodes", "node")
	defer span.End()
	return t.Engine.AllNodes()
}

func (t *TracedEngine) AllEdges() ([]*Edge, error) {
	span := t.startOperation("nornicdb.storage.AllEdges", "edge")
	defer span.End()
	return t.Engine.AllEdges()
}

func (t *TracedEngine) BulkCreateNodes(nodes []*Node) error {
	span := t.startOperation("nornicdb.storage.BulkCreateNodes", "node")
	span.SetAttributes(attribute.Int("batch_size", len(nodes)))
	defer span.End()
	return t.Engine.BulkCreateNodes(nodes)
}

func (t *TracedEngine) BulkCreateEdges(edges []*Edge) error {
	span := t.startOperation("nornicdb.storage.BulkCreateEdges", "edge")
	span.SetAttributes(attribute.Int("batch_size", len(edges)))
	defer span.End()
	return t.Engine.BulkCreateEdges(edges)
}

func (t *TracedEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	span := t.startOperation("nornicdb.storage.BatchGetNodes", "node")
	span.SetAttributes(attribute.Int("batch_size", len(ids)))
	defer span.End()
	return t.Engine.BatchGetNodes(ids)
}
