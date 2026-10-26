package storage

import (
	"context"
	"errors"
	"time"
)

// HasReadCapability is a selection hint only. It never exposes a reader;
// execution must use ReadWithCapability or VisitWithCapability.
func HasReadCapability[T any](engine any) bool {
	_, ok := readCapabilityForEngine[T](engine)
	return ok
}

// RecordMaterializedAccess executes the optional ON ACCESS operation using the
// same live availability contract as the data read that materialized the entity.
func RecordMaterializedAccess(engine any, id string) {
	ReadValueWithCapability(engine, func(reader materializedAccess) struct{} { reader.RecordMaterializedAccess(id); return struct{}{} }, nil)
}

// VisitWhileWithCapability adapts a stopping predicate to the shared delivery
// owner. Even a callback returning false counts as delivery and forbids replay.
func VisitWhileWithCapability[T, Row any](engine any, read func(T, func(Row) bool) error, fallback func(func(Row) bool) error, visit func(Row) bool) error {
	if visit == nil {
		return ErrInvalidData
	}
	adapt := func(deliver func(Row) error) func(Row) bool { return func(row Row) bool { return deliver(row) == nil } }
	err := VisitWithCapability(engine, func(reader T, deliver func(Row) error) error { return read(reader, adapt(deliver)) }, func(deliver func(Row) error) error {
		if fallback == nil {
			return ErrNotImplemented
		}
		return fallback(adapt(deliver))
	}, func(row Row) error {
		if !visit(row) {
			return ErrIterationStopped
		}
		return nil
	})
	if errors.Is(err, ErrIterationStopped) {
		return nil
	}
	return err
}

// VisitNodes executes optional node iteration with a same-view ordinary scan.
func VisitNodes(engine Engine, visit func(*Node) bool) error {
	return VisitWhileWithCapability(engine, func(reader NodeIterator, deliver func(*Node) bool) error { return reader.IterateNodes(deliver) }, func(deliver func(*Node) bool) error {
		err := (nodeReadOperation{engine, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}}).visit(context.Background(), func(node *Node) error {
			if !deliver(node) {
				return ErrIterationStopped
			}
			return nil
		})
		if errors.Is(err, ErrIterationStopped) {
			return nil
		}
		return err
	}, visit)
}

// VisitNodeIDsByLabel executes ID iteration on the same view, with core label
// lookup on a refusal before the first callback.
func VisitNodeIDsByLabel(engine Engine, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScope(engine, "", label, visit)
}

// LookupNodesByEquality executes a transaction's complete equality decision.
// Refusal propagates with no replacement for the complete flag.
func LookupNodesByEquality(engine any, label string, properties map[string]any) ([]*Node, bool, error) {
	type result struct {
		nodes    []*Node
		complete bool
	}
	value, err := ReadWithCapability(engine, func(reader TransactionEqualityLookup) (result, error) {
		nodes, complete, err := reader.LookupNodesByEquality(label, properties)
		return result{nodes, complete}, err
	}, nil)
	return value.nodes, value.complete, err
}

// RelationshipEndpointVisible preserves both the visibility and answered flags
// of an infallible endpoint capability on this exact read view.
func RelationshipEndpointVisible(engine any, id NodeID) (bool, bool) {
	value := ReadValueWithCapability(engine, func(reader RelationshipEndpointChecker) [2]bool {
		visible, answered := reader.RelationshipEndpointVisible(id)
		return [2]bool{visible, answered}
	}, nil)
	return value[0], value[1]
}

// IsCurrentTemporalNode executes temporal visibility on the same read view.
// A declared provider's refusal always propagates. For a view without that
// operation, its ordinary predicate is true only when its own schema has no
// applicable temporal constraint; otherwise the missing predicate is refused.
func IsCurrentTemporalNode(engine Engine, node *Node, asOf time.Time) (bool, error) {
	if node == nil {
		return false, nil
	}
	return ReadWithCapability(engine, func(reader TemporalCurrentNodeEngine) (bool, error) {
		return reader.IsCurrentTemporalNode(node, asOf)
	}, func() (bool, error) {
		if _, declared := engineCapability[TemporalCurrentNodeEngine](engine, capabilityViewMetadata, nil); declared {
			return false, ErrNotImplemented
		}
		return currentTemporalNodeWithoutProvider(engine, node)
	})
}

// An absent predicate is vacuously true only when the same view's schema
// establishes that this node has no applicable temporal constraint. Callers
// must first preserve any declared provider's refusal.
func currentTemporalNodeWithoutProvider(engine Engine, node *Node) (bool, error) {
	if node == nil {
		return false, nil
	}
	if engine == nil {
		return false, ErrNotImplemented
	}
	schema := engine.GetSchema()
	if schema == nil || len(temporalConstraintsForLabels(schema, node.Labels)) != 0 {
		return false, ErrNotImplemented
	}
	return true, nil
}

// ReadValueWithCapability executes an infallible optional read. Its fallback
// describes absence only; fallible predicates must use ReadWithCapability so
// a refusal remains an error rather than a replacement decision.
func ReadValueWithCapability[T, R any](engine any, read func(T) R, fallback func() R) R {
	value, _ := ReadWithCapability(engine, func(reader T) (R, error) { return read(reader), nil }, func() (R, error) {
		if fallback != nil {
			return fallback(), nil
		}
		var zero R
		return zero, ErrNotImplemented
	})
	return value
}

// ReadWithCapability selects and executes one optional read as one operation.
// A refusal (ErrNotImplemented) discards the optional result and runs fallback.
// Other errors are preserved. A nil fallback explicitly refuses the operation;
// historical callers use nil, or a fallback at exactly the requested version.
// read must not deliver rows or perform externally visible work: streaming
// callers use VisitWithCapability, which tracks delivery before allowing retry.
//
// For example, a versioned node read supplies no current-data fallback:
//
//	ReadWithCapability(engine, func(r MVCCVisibilityEngine) (*Node, error) {
//	    return r.GetNodeVisibleAt(id, version)
//	}, nil)
func ReadWithCapability[T, R any](engine any, read func(T) (R, error), fallback func() (R, error)) (R, error) {
	return readWithFallback(func() (R, error) {
		if reader, ok := readCapabilityForEngine[T](engine); ok {
			return read(reader)
		}
		var zero R
		return zero, ErrNotImplemented
	}, fallback)
}

func readWithFallback[R any](read func() (R, error), fallback func() (R, error)) (R, error) {
	result, err := read()
	if !errors.Is(err, ErrNotImplemented) {
		return result, err
	}
	if fallback != nil {
		return fallback()
	}
	var zero R
	return zero, err
}

// VisitWithCapability owns optional selection, execution and refusal handling
// for one stream. Neither an error from visit nor refusal after the first row
// permits fallback: replaying the read would duplicate externally visible work.
// Fan-out owners reserve every constituent before calling the external visit,
// so a later constituent's refusal can still refuse the whole optional read.
func VisitWithCapability[T, Row any](engine any, read func(T, func(Row) error) error, fallback func(func(Row) error) error, visit func(Row) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	started := false
	var readErr error
	var visitorErr error
	deliver := func(row Row) error {
		if visitorErr != nil {
			return visitorErr
		}
		started = true
		visitorErr = visit(row)
		return visitorErr
	}
	_, err := ReadWithCapability(engine, func(reader T) (struct{}, error) {
		readErr = read(reader, deliver)
		return struct{}{}, readErr
	}, func() (struct{}, error) {
		if started {
			return struct{}{}, readErr
		}
		if fallback == nil {
			return struct{}{}, ErrNotImplemented
		}
		return struct{}{}, fallback(deliver)
	})
	if visitorErr != nil && !errors.Is(visitorErr, ErrIterationStopped) {
		return visitorErr
	}
	return err
}

// EmbeddingFailure is one failure-scan row. Its metadata is valid for the visit.
type EmbeddingFailure struct {
	NodeID NodeID
	Meta   map[string]any
}

// VisitEmbeddingFailures owns failure scan selection and delivery. A provider's
// count is preserved even when its visitor refuses a row. Fallback is permitted
// only before the first callback and cannot replay any callback.
func VisitEmbeddingFailures(engine Engine, ctx context.Context, fallback func(func(NodeID, map[string]any) error) (int, error), visit func(NodeID, map[string]any) error) (int, error) {
	if visit == nil {
		return 0, ErrInvalidData
	}
	count := 0
	err := VisitWithCapability(engine, func(reader EmbeddingFailureStreamer, deliver func(EmbeddingFailure) error) error {
		var err error
		count, err = reader.StreamParkedEmbeddingFailures(ctx, func(id NodeID, meta map[string]any) error { return deliver(EmbeddingFailure{id, meta}) })
		return err
	}, func(deliver func(EmbeddingFailure) error) error {
		if fallback == nil {
			return ErrNotImplemented
		}
		var err error
		count, err = fallback(func(id NodeID, meta map[string]any) error { return deliver(EmbeddingFailure{id, meta}) })
		return err
	}, func(row EmbeddingFailure) error { return visit(row.NodeID, row.Meta) })
	return count, err
}

// StreamNodesByLabelProjected executes a projected label read with the core
// read of the same view as its refusal-before-rows fallback. MATCH, traversal
// and validation share this owner instead of retaining an availability bool.
func StreamNodesByLabelProjected(engine Engine, label string, properties []string, visit func(*Node) error) error {
	return VisitWithCapability(engine, func(reader ProjectedLabelNodeReader, deliver func(*Node) error) error {
		return reader.StreamNodesByLabelProjected(label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return streamNodesByLabelCore(engine, label, properties, deliver)
	}, visit)
}

func streamNodesByLabelCore(engine Engine, label string, properties []string, visit func(*Node) error) error {
	return (nodeReadOperation{engine, StreamNodesOptions{Label: label, Projection: properties, WithEmbeddings: properties == nil, ApplyDecayFilter: true}}).visit(context.Background(), visit)
}

type edgeHeaderResult struct {
	edges    []*Edge
	answered bool
}

type adjacentEdgeResult struct {
	outgoing []*Edge
	incoming []*Edge
}

// GetAdjacentEdges executes a combined adjacency read, using the same view's
// two core direction reads if the optional operation refuses before results.
func GetAdjacentEdges(engine Engine, nodeID NodeID) ([]*Edge, []*Edge, error) {
	result, err := ReadWithCapability(engine, func(reader AdjacentEdgesEngine) (adjacentEdgeResult, error) {
		outgoing, incoming, err := reader.GetAdjacentEdges(nodeID)
		return adjacentEdgeResult{outgoing: outgoing, incoming: incoming}, err
	}, func() (adjacentEdgeResult, error) {
		outgoing, err := engine.GetOutgoingEdges(nodeID)
		if err != nil {
			return adjacentEdgeResult{}, err
		}
		incoming, err := engine.GetIncomingEdges(nodeID)
		return adjacentEdgeResult{outgoing: outgoing, incoming: incoming}, err
	})
	return result.outgoing, result.incoming, err
}

func readEdgeHeaders(engine any, nodeID NodeID, outgoing bool) ([]*Edge, bool, error) {
	return BindEdgeHeaderReads(engine)(nodeID, outgoing)
}

// OutgoingEdgeHeaders executes the optional header read. A refusal reports
// answered=false, retaining the caller's ordinary outgoing-edge read.
func OutgoingEdgeHeaders(engine any, nodeID NodeID) ([]*Edge, bool, error) {
	return readEdgeHeaders(engine, nodeID, true)
}

// IncomingEdgeHeaders executes the optional header read with the same refusal
// contract as OutgoingEdgeHeaders.
func IncomingEdgeHeaders(engine any, nodeID NodeID) ([]*Edge, bool, error) {
	return readEdgeHeaders(engine, nodeID, false)
}

// LookupNodesByEqualityWithContext selects the context-aware operation on the
// issued view; the legacy same-view operation is allowed only before execution.
func LookupNodesByEqualityWithContext(ctx context.Context, engine any, label string, properties map[string]any) ([]*Node, bool, error) {
	type result struct {
		nodes    []*Node
		complete bool
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	value, err := ReadWithCapability(engine, func(reader ContextTransactionEqualityLookup) (result, error) {
		nodes, complete, err := reader.LookupNodesByEqualityWithContext(ctx, label, properties)
		return result{nodes, complete}, err
	}, func() (result, error) {
		return ReadWithCapability(engine, func(reader TransactionEqualityLookup) (result, error) {
			nodes, complete, err := reader.LookupNodesByEquality(label, properties)
			if err == nil {
				err = ctx.Err()
			}
			return result{nodes, complete}, err
		}, func() (result, error) { return result{}, nil })
	})
	return value.nodes, value.complete, err
}
