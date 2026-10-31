package storage

import (
	"context"
	"fmt"
)

// EngineWithVectorWorkOrigin retains admission on transient entity arguments.
// Delegation always goes through the supplied Engine's original methods: an
// optional method promoted from an embedded engine must not bypass a wrapper.
func EngineWithVectorWorkOrigin(engine Engine, origin uint64) (Engine, error) {
	if engine == nil {
		return nil, fmt.Errorf("nil admitted entity engine: %w", ErrInvalidData)
	}
	return AdmittedEngine(engine, origin), nil
}

// AdmittedEngine is engine carrying origin: engine itself if it already carries
// an admission, which it keeps. A nil engine stays nil.
func AdmittedEngine(engine Engine, origin uint64) Engine {
	if engine == nil {
		return nil
	}
	if carried, _, ok := vectorWorkCarrier(engine); ok {
		return carried
	}
	return newVectorWorkOriginEngine(engine, origin)
}

// vectorWorkOriginEngine transports metadata only. Every method delegates to
// the wrapped engine's own method with nothing added but the admission: it
// performs no filtering, looping, chunking or fallback of its own, so the
// wrapped owner keeps its semantics (validation, CDC, bulk transactions,
// missing-ID handling). It exposes an optional capability only when the
// wrapped engine has it (streamingVectorWorkOriginEngine).
type vectorWorkOriginEngine struct {
	Engine
	origin uint64
}

// streamingVectorWorkOriginEngine is the carrier of an engine that streams:
// streaming is a read, so it goes to the wrapped engine's own methods.
type streamingVectorWorkOriginEngine struct {
	*vectorWorkOriginEngine
	streamer StreamingEngine
}

// newVectorWorkOriginEngine is the one place that decides a carrier's
// capability set: exactly the wrapped engine's. An engine that already carries
// an admission is returned as it is by both public constructors, never
// rewrapped, so it keeps its own capability set and origin.
func newVectorWorkOriginEngine(engine Engine, origin uint64) Engine {
	carrier := &vectorWorkOriginEngine{Engine: engine, origin: origin}
	if streamer, ok := engine.(StreamingEngine); ok {
		return &streamingVectorWorkOriginEngine{vectorWorkOriginEngine: carrier, streamer: streamer}
	}
	return carrier
}

// vectorWorkCarrier is the one decision whether engine is an admission carrier.
// It returns the carrier as it is, whichever capability variant, and its origin.
func vectorWorkCarrier(engine Engine) (Engine, uint64, bool) {
	switch carrier := engine.(type) {
	case *vectorWorkOriginEngine:
		return carrier, carrier.origin, true
	case *streamingVectorWorkOriginEngine:
		return carrier, carrier.origin, true
	}
	return nil, 0, false
}

func (e *streamingVectorWorkOriginEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	return e.streamer.StreamNodes(ctx, fn)
}

func (e *streamingVectorWorkOriginEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	return e.streamer.StreamEdges(ctx, fn)
}

func (e *streamingVectorWorkOriginEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	return e.streamer.StreamNodeChunks(ctx, chunkSize, fn)
}

var _ StreamingEngine = (*streamingVectorWorkOriginEngine)(nil)

func (e *vectorWorkOriginEngine) GetInnerEngine() Engine { return e.Engine }

// SubscribeEvents keeps the original publisher and its origin-bearing facts.
// Admission changes write arguments only; it creates no event stream of its own.
func (e *vectorWorkOriginEngine) SubscribeEvents(handlers EventHandlers) error {
	return SubscribeEvents(e.Engine, handlers)
}

var _ EventSubscriber = (*vectorWorkOriginEngine)(nil)

func (e *vectorWorkOriginEngine) CreateNode(node *Node) (NodeID, error) {
	return e.Engine.CreateNode(admittedNodeArgument(node, e.origin))
}

// UpdateNode binds the admission on the request's new image; its observed
// predecessor (Before) is passed through unchanged.
func (e *vectorWorkOriginEngine) UpdateNode(updateRequest NodeUpdateRequest) error {
	updateRequest.After = admittedNodeArgument(updateRequest.After, e.origin)
	return e.Engine.UpdateNode(updateRequest)
}

func (e *vectorWorkOriginEngine) DeleteNode(id NodeID, origins ...uint64) error {
	return e.Engine.DeleteNode(id, e.admission(origins)...)
}

func (e *vectorWorkOriginEngine) CreateEdge(edge *Edge) error {
	return e.Engine.CreateEdge(admittedEdgeArgument(edge, e.origin))
}

func (e *vectorWorkOriginEngine) UpdateEdge(updateRequest EdgeUpdateRequest) error {
	updateRequest.After = admittedEdgeArgument(updateRequest.After, e.origin)
	return e.Engine.UpdateEdge(updateRequest)
}

func (e *vectorWorkOriginEngine) DeleteEdge(id EdgeID, origins ...uint64) error {
	return e.Engine.DeleteEdge(id, e.admission(origins)...)
}

func (e *vectorWorkOriginEngine) BulkCreateNodes(nodes []*Node) error {
	return e.Engine.BulkCreateNodes(admittedNodeArguments(nodes, e.origin))
}

func (e *vectorWorkOriginEngine) BulkCreateEdges(edges []*Edge) error {
	return e.Engine.BulkCreateEdges(admittedEdgeArguments(edges, e.origin))
}

// BulkDeleteNodes keeps the wrapped engine's one bulk deletion (one
// transaction; missing, repeated and empty IDs skipped as that owner decides).
func (e *vectorWorkOriginEngine) BulkDeleteNodes(ids []NodeID, origins ...uint64) error {
	return e.Engine.BulkDeleteNodes(ids, e.admission(origins)...)
}

func (e *vectorWorkOriginEngine) BulkDeleteEdges(ids []EdgeID, origins ...uint64) error {
	return e.Engine.BulkDeleteEdges(ids, e.admission(origins)...)
}

// admittedNodeArguments binds the admission on a bulk argument; caller-owned
// images are not mutated.
func admittedNodeArguments(nodes []*Node, origin uint64) []*Node {
	bound := make([]*Node, len(nodes))
	for i, node := range nodes {
		bound[i] = admittedNodeArgument(node, origin)
	}
	return bound
}

func admittedEdgeArguments(edges []*Edge, origin uint64) []*Edge {
	bound := make([]*Edge, len(edges))
	for i, edge := range edges {
		bound[i] = admittedEdgeArgument(edge, origin)
	}
	return bound
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
		case NodeUpdateRequest:
			visit(value.After)
		case EdgeUpdateRequest:
			visit(value.After)
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
		origin = fallbackVectorWorkOrigin("storage.operation-batch")
	}
	bound := append([]Operation(nil), ops...)
	for i := range bound {
		bound[i].Node, _ = takeNodeArgument(bound[i].Node, []uint64{origin})
		bound[i].Edge, _ = takeEdgeArgument(bound[i].Edge, []uint64{origin})
	}
	return WithVectorWorkOrigin(ctx, origin), bound, origin
}

// admission is what a deletion through the carrier passes on: the caller's own
// origin if it supplied one, otherwise the carrier's.
func (e *vectorWorkOriginEngine) admission(origins []uint64) []uint64 {
	if len(origins) > 0 {
		return origins
	}
	return []uint64{e.origin}
}
