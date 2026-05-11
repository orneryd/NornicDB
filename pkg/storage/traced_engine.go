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

// Unwrap returns the underlying Engine (for type assertions by callers that
// need access to the concrete engine, e.g. AsyncEngine).
func (t *TracedEngine) Unwrap() Engine { return t.Engine }

func (t *TracedEngine) CreateNode(node *Node) (NodeID, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.CreateNode",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return t.Engine.CreateNode(node)
}

func (t *TracedEngine) GetNode(id NodeID) (*Node, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetNode",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return t.Engine.GetNode(id)
}

func (t *TracedEngine) UpdateNode(node *Node) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.UpdateNode",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return t.Engine.UpdateNode(node)
}

func (t *TracedEngine) DeleteNode(id NodeID) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.DeleteNode",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return t.Engine.DeleteNode(id)
}

func (t *TracedEngine) CreateEdge(edge *Edge) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.CreateEdge",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.CreateEdge(edge)
}

func (t *TracedEngine) GetEdge(id EdgeID) (*Edge, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetEdge",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.GetEdge(id)
}

func (t *TracedEngine) UpdateEdge(edge *Edge) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.UpdateEdge",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.UpdateEdge(edge)
}

func (t *TracedEngine) DeleteEdge(id EdgeID) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.DeleteEdge",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.DeleteEdge(id)
}

func (t *TracedEngine) GetNodesByLabel(label string) ([]*Node, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetNodesByLabel",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return t.Engine.GetNodesByLabel(label)
}

func (t *TracedEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetOutgoingEdges",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.GetOutgoingEdges(nodeID)
}

func (t *TracedEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetIncomingEdges",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.GetIncomingEdges(nodeID)
}

func (t *TracedEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetEdgesByType",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.GetEdgesByType(edgeType)
}

func (t *TracedEngine) AllNodes() ([]*Node, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.AllNodes",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return t.Engine.AllNodes()
}

func (t *TracedEngine) AllEdges() ([]*Edge, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.AllEdges",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "edge")),
	)
	defer span.End()
	return t.Engine.AllEdges()
}

func (t *TracedEngine) BulkCreateNodes(nodes []*Node) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.BulkCreateNodes",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("kind", "node"),
			attribute.Int("batch_size", len(nodes)),
		),
	)
	defer span.End()
	return t.Engine.BulkCreateNodes(nodes)
}

func (t *TracedEngine) BulkCreateEdges(edges []*Edge) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.BulkCreateEdges",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("kind", "edge"),
			attribute.Int("batch_size", len(edges)),
		),
	)
	defer span.End()
	return t.Engine.BulkCreateEdges(edges)
}

func (t *TracedEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.BatchGetNodes",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("kind", "node"),
			attribute.Int("batch_size", len(ids)),
		),
	)
	defer span.End()
	return t.Engine.BatchGetNodes(ids)
}
