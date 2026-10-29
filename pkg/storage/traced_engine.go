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

func (t *TracedEngine) CreateNode(node *Node) (NodeID, error) {
	return t.CreateNodeWithContext(context.Background(), node)
}

// CreateNodeWithContext carries the caller through mutation validation.
func (t *TracedEngine) CreateNodeWithContext(ctx context.Context, node *Node) (NodeID, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.CreateNode",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return CreateNodeWithContext(ctx, t.Engine, node)
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
	return t.UpdateNodeWithContext(context.Background(), node)
}

// UpdateNodeWithContext carries the caller through mutation validation.
func (t *TracedEngine) UpdateNodeWithContext(ctx context.Context, node *Node) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.UpdateNode",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return UpdateNodeWithContext(ctx, t.Engine, node)
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
	return t.GetNodesByLabelWithContext(context.Background(), label)
}

// GetNodesByLabelWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) GetNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.GetNodesByLabel",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return GetNodesByLabelWithContext(ctx, t.Engine, label)
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
	return t.AllNodesWithContext(context.Background())
}

// AllNodesWithContext carries the caller through full materialization.
func (t *TracedEngine) AllNodesWithContext(ctx context.Context) ([]*Node, error) {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.AllNodes",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("kind", "node")),
	)
	defer span.End()
	return AllNodesWithContext(ctx, t.Engine)
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
	return t.BulkCreateNodesWithContext(context.Background(), nodes)
}

// BulkCreateNodesWithContext carries the caller through mutation validation.
func (t *TracedEngine) BulkCreateNodesWithContext(ctx context.Context, nodes []*Node) error {
	_, span := otel.Tracer("nornicdb/storage").Start(t.getCtx(), "nornicdb.storage.BulkCreateNodes",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("kind", "node"),
			attribute.Int("batch_size", len(nodes)),
		),
	)
	defer span.End()
	return BulkCreateNodesWithContext(ctx, t.Engine, nodes)
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
	return t.BatchGetNodesWithContext(t.getCtx(), ids)
}

// BatchGetNodesWithContext keeps the batch trace on the caller's lifetime and
// delegates preparation to the shared contextual dispatcher.
func (t *TracedEngine) BatchGetNodesWithContext(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	ctx, span := otel.Tracer("nornicdb/storage").Start(ctx, "nornicdb.storage.BatchGetNodes",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("kind", "node"),
			attribute.Int("batch_size", len(ids)),
		),
	)
	defer span.End()
	return BatchGetNodesWithContext(ctx, t.Engine, ids)
}
