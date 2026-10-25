package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

// EngineWithVectorWorkOrigin retains admission on transient entity arguments.
// Delegation always goes through the supplied Engine's original methods: an
// optional method promoted from an embedded engine must not bypass a wrapper.
func EngineWithVectorWorkOrigin(engine Engine, origin uint64) (Engine, error) {
	if engine == nil {
		return nil, fmt.Errorf("nil admitted entity engine: %w", ErrInvalidData)
	}
	if bound, ok := engine.(*vectorWorkOriginEngine); ok {
		return bound, nil
	}
	return &vectorWorkOriginEngine{Engine: engine, origin: origin}, nil
}

// vectorWorkOriginEngine transports metadata only. Every write still uses its
// original Engine methods, including wrappers, validation, CDC and bulk policy.
// Deletions carry no candidate vectors: they retain their original teardown path.
type vectorWorkOriginEngine struct {
	Engine
	origin uint64
}

func (e *vectorWorkOriginEngine) GetInnerEngine() Engine { return e.Engine }

// SubscribeEvents keeps the original publisher and its origin-bearing facts.
// Admission changes write arguments only; it creates no event stream of its own.
func (e *vectorWorkOriginEngine) SubscribeEvents(handlers EventHandlers) error {
	return SubscribeEvents(e.Engine, handlers)
}

var _ EventSubscriber = (*vectorWorkOriginEngine)(nil)

func (e *vectorWorkOriginEngine) Logger() *slog.Logger {
	if logger, ok := e.Engine.(StructuredLogger); ok {
		return logger.Logger()
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (e *vectorWorkOriginEngine) CreateNode(node *Node) (NodeID, error) {
	return e.Engine.CreateNode(admittedNodeArgument(node, e.origin))
}

func (e *vectorWorkOriginEngine) UpdateNode(node *Node) error {
	return e.Engine.UpdateNode(admittedNodeArgument(node, e.origin))
}

func (e *vectorWorkOriginEngine) DeleteNode(id NodeID) error {
	return e.Engine.DeleteNode(id)
}

func (e *vectorWorkOriginEngine) CreateEdge(edge *Edge) error {
	return e.Engine.CreateEdge(admittedEdgeArgument(edge, e.origin))
}

func (e *vectorWorkOriginEngine) UpdateEdge(edge *Edge) error {
	return e.Engine.UpdateEdge(admittedEdgeArgument(edge, e.origin))
}

func (e *vectorWorkOriginEngine) DeleteEdge(id EdgeID) error {
	return e.Engine.DeleteEdge(id)
}

func (e *vectorWorkOriginEngine) BulkCreateNodes(nodes []*Node) error {
	bound := make([]*Node, len(nodes))
	for i, node := range nodes {
		bound[i] = admittedNodeArgument(node, e.origin)
	}
	return e.Engine.BulkCreateNodes(bound)
}

func (e *vectorWorkOriginEngine) BulkCreateEdges(edges []*Edge) error {
	bound := make([]*Edge, len(edges))
	for i, edge := range edges {
		bound[i] = admittedEdgeArgument(edge, e.origin)
	}
	return e.Engine.BulkCreateEdges(bound)
}

func (e *vectorWorkOriginEngine) BulkDeleteNodes(ids []NodeID) error {
	return e.Engine.BulkDeleteNodes(ids)
}

func (e *vectorWorkOriginEngine) BulkDeleteEdges(ids []EdgeID) error {
	return e.Engine.BulkDeleteEdges(ids)
}

func admittedNodeArgument(node *Node, origin uint64) *Node {
	if node == nil || node.runtimeState().entityOriginBound {
		return node
	}
	bound := *node
	state := bound.runtimeState()
	state.entityWorkOrigin, state.entityOriginBound = origin, true
	bound.setRuntimeState(state)
	return &bound
}

func admittedEdgeArgument(edge *Edge, origin uint64) *Edge {
	if edge == nil || edge.entityOriginBound {
		return edge
	}
	bound := *edge
	bound.entityWorkOrigin, bound.entityOriginBound = origin, true
	return &bound
}

// take*Argument consumes transport metadata before the native owner retains any
// body image. Returned/stored entities therefore never relabel a later write.
func takeNodeArgument(node *Node, origins []uint64) (*Node, []uint64) {
	if node == nil || !node.runtimeState().entityOriginBound {
		return node, origins
	}
	state := node.runtimeState()
	if len(origins) == 0 {
		origins = []uint64{state.entityWorkOrigin}
	}
	clean := *node
	state.entityWorkOrigin, state.entityOriginBound = 0, false
	clean.setRuntimeState(state)
	return &clean, origins
}

func takeEdgeArgument(edge *Edge, origins []uint64) (*Edge, []uint64) {
	if edge == nil || !edge.entityOriginBound {
		return edge, origins
	}
	if len(origins) == 0 {
		origins = []uint64{edge.entityWorkOrigin}
	}
	clean := *edge
	clean.entityWorkOrigin, clean.entityOriginBound = 0, false
	return &clean, origins
}

// VectorWorkOriginFromArguments reads already-admitted local argument metadata.
// An earlier carrier wins over a later wrapper context; no schema decision is
// made here. Wire encoders omit these private fields at a process boundary.
// It reports false when neither ctx nor any argument carries an admission.
func VectorWorkOriginFromArguments(ctx context.Context, values ...any) (uint64, bool) {
	origin, admitted := VectorWorkOrigin(ctx)
	var visit func(any)
	visit = func(value any) {
		keep := func(candidate uint64, bound bool) {
			if bound && (!admitted || candidate < origin) {
				origin, admitted = candidate, true
			}
		}
		switch value := value.(type) {
		case *Node:
			if value != nil {
				state := value.runtimeState()
				keep(state.entityWorkOrigin, state.entityOriginBound)
			}
		case *Edge:
			if value != nil {
				keep(value.entityWorkOrigin, value.entityOriginBound)
			}
		case []*Node:
			for _, node := range value {
				visit(node)
			}
		case []*Edge:
			for _, edge := range value {
				visit(edge)
			}
		case Operation:
			keep(value.vectorWorkOrigin, value.vectorOriginBound)
			visit(value.Node)
			visit(value.Edge)
		case []Operation:
			for _, op := range value {
				visit(op)
			}
		}
	}
	for _, value := range values {
		visit(value)
	}
	return origin, admitted
}

// WithEntityOperationOrigins is the admission of a direct operation batch: it
// keeps an admission carried by ctx or the arguments, otherwise it admits the
// batch now, and carries that origin into the retained transaction owner
// before preparation can wait. Consuming the transient fields detaches only
// marked arguments; caller-owned images are not mutated.
func WithEntityOperationOrigins(ctx context.Context, ops []Operation) (context.Context, []Operation, uint64) {
	origin, admitted := VectorWorkOriginFromArguments(ctx, ops)
	if !admitted {
		origin = CaptureVectorWorkOrigin()
	}
	bound := append([]Operation(nil), ops...)
	for i := range bound {
		bound[i].Node, _ = takeNodeArgument(bound[i].Node, []uint64{origin})
		bound[i].Edge, _ = takeEdgeArgument(bound[i].Edge, []uint64{origin})
	}
	return WithVectorWorkOrigin(ctx, origin), bound, origin
}
