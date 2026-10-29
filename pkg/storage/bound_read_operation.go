package storage

import (
	"context"
	"reflect"
	"time"
)

// ReadOperationGuard binds a structural request once. The returned predicate
// reads only live availability; it must not rediscover or re-type the topology.
type ReadOperationGuard interface {
	BindReadCapabilityGuard(reflect.Type) func() bool
}

// BoundReadOperation retains the operation adapter and compiled admission for a
// statement/traversal. It grants no access to an unscoped underlying view.
type BoundReadOperation[T any] struct {
	reader    T
	supported bool
	guards    []func() bool
}

func BindReadOperation[T any](engine any) BoundReadOperation[T] {
	request := reflect.TypeFor[T]()
	op := BoundReadOperation[T]{}
	var owner any = engine
	var layers []any
	if e, ok := engine.(Engine); ok {
		binding := BindReadView(e)
		owner = binding.owner
		layers = binding.layers
	} else {
		layers = []any{engine}
	}
	if scope, ok := owner.(ReadCapabilityScope); ok && !scope.ReadCapabilityScoped(request) {
		return op
	}
	op.reader, op.supported = owner.(T)
	if !op.supported {
		return op
	}
	for _, layer := range layers {
		if binder, ok := layer.(ReadOperationGuard); ok {
			if guard := binder.BindReadCapabilityGuard(request); guard != nil {
				op.guards = append(op.guards, guard)
			}
		} else if availability, ok := layer.(ReadCapabilityAvailability); ok {
			op.guards = append(op.guards, func() bool { return availability.ReadCapabilityAvailable(request) })
		}
		if ReadCapabilityIncludes[BatchNodeWithoutEmbeddingsReader](request) {
			if batch, ok := layer.(BatchNodeWithoutEmbeddingsCapability); ok {
				op.guards = append(op.guards, batch.BatchGetNodesWithoutEmbeddingsSupported)
			}
		}
	}
	return op
}
func (op BoundReadOperation[T]) Available() bool {
	if !op.supported {
		return false
	}
	for _, guard := range op.guards {
		if !guard() {
			return false
		}
	}
	return true
}
func ExecuteBoundRead[T, R any](op BoundReadOperation[T], read func(T) (R, error), fallback func() (R, error)) (R, error) {
	return readWithFallback(func() (R, error) {
		if op.Available() {
			return read(op.reader)
		}
		var zero R
		return zero, ErrNotImplemented
	}, fallback)
}

// QueryNodeReadBinder lets a transforming owner compile its child operation
// without repeating the child discovery in every row's forwarding method.
type QueryNodeReadBinder interface {
	BindQueryNodeRead() func(NodeID) (*Node, error)
}

func BindQueryNodeRead(engine any) func(NodeID) (*Node, error) {
	op := BindReadOperation[QueryNodeReader](engine)
	ordinary, _ := OwnedCapability[interface{ GetNode(NodeID) (*Node, error) }](engine)
	var read func(NodeID) (*Node, error)
	if binder, ok := OwnedCapability[QueryNodeReadBinder](op.reader); ok {
		read = binder.BindQueryNodeRead()
	}
	return func(id NodeID) (*Node, error) {
		return ExecuteBoundRead(op, func(reader QueryNodeReader) (*Node, error) {
			if read != nil {
				return read(id)
			}
			return reader.GetQueryNode(id)
		}, func() (*Node, error) {
			if ordinary == nil {
				return nil, ErrNotImplemented
			}
			return ordinary.GetNode(id)
		})
	}
}
func (n *NamespacedEngine) BindQueryNodeRead() func(NodeID) (*Node, error) {
	read := BindQueryNodeRead(n.inner)
	return func(id NodeID) (*Node, error) {
		return materializedRead(func() (*Node, error) { return read(n.PrefixNodeID(id)) }, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, false)
	}
}

type outgoingHeaderRead interface {
	OutgoingEdgeHeaders(NodeID) ([]*Edge, bool, error)
}
type incomingHeaderRead interface {
	IncomingEdgeHeaders(NodeID) ([]*Edge, bool, error)
}
type EdgeHeaderReadBinder interface {
	BindEdgeHeaderRead(bool) func(NodeID) ([]*Edge, bool, error)
}

// BindEdgeHeaderReads compiles each direction separately and retains both for a
// traversal. Before-answer refusal retains the same-view ordinary adjacency.
func BindEdgeHeaderReads(engine any) func(NodeID, bool) ([]*Edge, bool, error) {
	out := BindReadOperation[outgoingHeaderRead](engine)
	in := BindReadOperation[incomingHeaderRead](engine)
	var outgoing, incoming func(NodeID) ([]*Edge, bool, error)
	if binder, ok := OwnedCapability[EdgeHeaderReadBinder](out.reader); ok {
		outgoing = binder.BindEdgeHeaderRead(true)
	}
	if binder, ok := OwnedCapability[EdgeHeaderReadBinder](in.reader); ok {
		incoming = binder.BindEdgeHeaderRead(false)
	}
	return func(id NodeID, direction bool) ([]*Edge, bool, error) {
		read := func(read func(NodeID) ([]*Edge, bool, error)) (edgeHeaderResult, error) {
			edges, answered, err := read(id)
			return edgeHeaderResult{edges: edges, answered: answered}, err
		}
		fallback := func() (edgeHeaderResult, error) { return edgeHeaderResult{}, nil }
		var result edgeHeaderResult
		var err error
		if direction {
			result, err = ExecuteBoundRead(out, func(reader outgoingHeaderRead) (edgeHeaderResult, error) {
				if outgoing != nil {
					return read(outgoing)
				}
				edges, answered, err := reader.OutgoingEdgeHeaders(id)
				return edgeHeaderResult{edges: edges, answered: answered}, err
			}, fallback)
		} else {
			result, err = ExecuteBoundRead(in, func(reader incomingHeaderRead) (edgeHeaderResult, error) {
				if incoming != nil {
					return read(incoming)
				}
				edges, answered, err := reader.IncomingEdgeHeaders(id)
				return edgeHeaderResult{edges: edges, answered: answered}, err
			}, fallback)
		}
		return result.edges, result.answered, err
	}
}
func (n *NamespacedEngine) BindEdgeHeaderRead(outgoing bool) func(NodeID) ([]*Edge, bool, error) {
	read := BindEdgeHeaderReads(n.inner)
	return func(id NodeID) ([]*Edge, bool, error) {
		edges, answered, err := read(n.PrefixNodeID(id), outgoing)
		if !answered || err != nil {
			return nil, answered, err
		}
		return n.toUserEdgesInNamespace(edges, false, true), true, nil
	}
}

func allReadGuards(guards []func() bool) func() bool {
	if len(guards) == 0 {
		return nil
	}
	return func() bool {
		for _, guard := range guards {
			if !guard() {
				return false
			}
		}
		return true
	}
}
func appendReadGuard[T any](guards *[]func() bool, engine any) {
	op := BindReadOperation[T](engine)
	*guards = append(*guards, op.Available)
}
func (n *NamespacedEngine) BindReadCapabilityGuard(request reflect.Type) func() bool {
	var guards []func() bool
	if ReadCapabilityIncludes[historicalLabelNodes](request) {
		a := BindReadOperation[scopedHistoricalLabelNodes](n.inner)
		b := BindReadOperation[historicalLabelNodes](n.inner)
		guards = append(guards, func() bool { return a.Available() || b.Available() })
	}
	if ReadCapabilityIncludes[historicalOutgoingEdges](request) {
		appendReadGuard[historicalOutgoingEdges](&guards, n.inner)
	}
	if ReadCapabilityIncludes[historicalIncomingEdges](request) {
		appendReadGuard[historicalIncomingEdges](&guards, n.inner)
	}
	if ReadCapabilityIncludes[historicalTypedEdges](request) {
		appendReadGuard[historicalTypedEdges](&guards, n.inner)
	}
	if ReadCapabilityIncludes[historicalBetweenEdges](request) {
		appendReadGuard[historicalBetweenEdges](&guards, n.inner)
	}
	if ReadCapabilityIncludes[snapshotNode](request) {
		appendReadGuard[snapshotNode](&guards, n.inner)
	}
	if ReadCapabilityIncludes[snapshotEdge](request) {
		appendReadGuard[snapshotEdge](&guards, n.inner)
	}
	if ReadCapabilityIncludes[nodeHead](request) {
		appendReadGuard[nodeHead](&guards, n.inner)
	}
	if ReadCapabilityIncludes[edgeHead](request) {
		appendReadGuard[edgeHead](&guards, n.inner)
	}
	if ReadCapabilityIncludes[materializedAccess](request) {
		appendReadGuard[materializedAccess](&guards, n.inner)
	}
	if ReadCapabilityIncludes[TemporalLookupEngine](request) {
		appendReadGuard[NamespaceTemporalLookupProvider](&guards, n.inner)
	}
	if ReadCapabilityIncludes[PropertyKeyLookup](request) {
		appendReadGuard[NamespacePropertyKeyLookup](&guards, n.inner)
	}
	if ReadCapabilityIncludes[ReadSnapshotter](request) {
		appendReadGuard[ReadSnapshotter](&guards, n.inner)
	}
	if ReadCapabilityIncludes[TemporalCurrentNodeEngine](request) {
		if _, declared := engineCapability[NamespaceTemporalCurrentNodeProvider](n.inner, capabilityViewMetadata, nil); declared {
			appendReadGuard[NamespaceTemporalCurrentNodeProvider](&guards, n.inner)
		}
	}
	return allReadGuards(guards)
}
func (c *CompositeEngine) BindReadCapabilityGuard(request reflect.Type) func() bool {
	var guards []func() bool
	var historical bool
	historical = historical || ReadCapabilityIncludes[historicalLabelNodes](request)
	historical = historical || ReadCapabilityIncludes[historicalOutgoingEdges](request)
	historical = historical || ReadCapabilityIncludes[historicalIncomingEdges](request)
	historical = historical || ReadCapabilityIncludes[historicalTypedEdges](request)
	historical = historical || ReadCapabilityIncludes[historicalBetweenEdges](request)
	historical = historical || ReadCapabilityIncludes[snapshotNode](request)
	historical = historical || ReadCapabilityIncludes[snapshotEdge](request)
	historical = historical || ReadCapabilityIncludes[nodeHead](request)
	historical = historical || ReadCapabilityIncludes[edgeHead](request)
	if !historical {
		return nil
	}
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return func() bool { return false }
		}
		if ReadCapabilityIncludes[historicalLabelNodes](request) {
			appendReadGuard[historicalLabelNodes](&guards, engine)
		}
		if ReadCapabilityIncludes[historicalOutgoingEdges](request) {
			appendReadGuard[historicalOutgoingEdges](&guards, engine)
		}
		if ReadCapabilityIncludes[historicalIncomingEdges](request) {
			appendReadGuard[historicalIncomingEdges](&guards, engine)
		}
		if ReadCapabilityIncludes[historicalTypedEdges](request) {
			appendReadGuard[historicalTypedEdges](&guards, engine)
		}
		if ReadCapabilityIncludes[historicalBetweenEdges](request) {
			appendReadGuard[historicalBetweenEdges](&guards, engine)
		}
		if ReadCapabilityIncludes[snapshotNode](request) {
			appendReadGuard[snapshotNode](&guards, engine)
		}
		if ReadCapabilityIncludes[snapshotEdge](request) {
			appendReadGuard[snapshotEdge](&guards, engine)
		}
		if ReadCapabilityIncludes[nodeHead](request) {
			appendReadGuard[nodeHead](&guards, engine)
		}
		if ReadCapabilityIncludes[edgeHead](request) {
			appendReadGuard[edgeHead](&guards, engine)
		}
	}
	return allReadGuards(guards)
}

type TemporalNodeReadBinder interface {
	BindTemporalNodeRead(context.Context) func(*Node, time.Time) (bool, error)
}

func BindTemporalNodeRead(ctx context.Context, engine Engine) func(*Node, time.Time) (bool, error) {
	op := BindReadOperation[TemporalCurrentNodeEngine](engine)
	_, declared := engineCapability[TemporalCurrentNodeEngine](engine, capabilityViewMetadata, nil)
	var read func(*Node, time.Time) (bool, error)
	if binder, ok := OwnedCapability[TemporalNodeReadBinder](op.reader); ok {
		read = binder.BindTemporalNodeRead(ctx)
	} else if contextual, ok := OwnedCapability[interface {
		IsCurrentTemporalNodeWithContext(context.Context, *Node, time.Time) (bool, error)
	}](op.reader); ok {
		read = func(node *Node, asOf time.Time) (bool, error) {
			return contextual.IsCurrentTemporalNodeWithContext(ctx, node, asOf)
		}
	}
	return func(node *Node, asOf time.Time) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if node == nil {
			return false, nil
		}
		return ExecuteBoundRead(op, func(reader TemporalCurrentNodeEngine) (bool, error) {
			if read != nil {
				return read(node, asOf)
			}
			value, err := reader.IsCurrentTemporalNode(node, asOf)
			if err == nil {
				err = ctx.Err()
			}
			return value, err
		}, func() (bool, error) {
			if declared {
				return false, ErrNotImplemented
			}
			return currentTemporalNodeWithoutProvider(engine, node)
		})
	}
}
func (n *NamespacedEngine) BindTemporalNodeRead(ctx context.Context) func(*Node, time.Time) (bool, error) {
	op := BindReadOperation[NamespaceTemporalCurrentNodeProvider](n.inner)
	_, declared := engineCapability[NamespaceTemporalCurrentNodeProvider](n.inner, capabilityViewMetadata, nil)
	var read func(*Node, time.Time) (bool, error)
	if contextual, ok := OwnedCapability[interface {
		IsCurrentTemporalNodeInNamespaceWithContext(context.Context, string, *Node, time.Time) (bool, error)
	}](op.reader); ok {
		read = func(node *Node, asOf time.Time) (bool, error) {
			return contextual.IsCurrentTemporalNodeInNamespaceWithContext(ctx, n.namespace, node, asOf)
		}
	}
	return func(node *Node, asOf time.Time) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if node != nil && node.Database != "" && node.Database != n.namespace {
			return false, ErrInvalidID
		}
		return ExecuteBoundRead(op, func(reader NamespaceTemporalCurrentNodeProvider) (bool, error) {
			if read != nil {
				return read(node, asOf)
			}
			value, err := reader.IsCurrentTemporalNodeInNamespace(n.namespace, node, asOf)
			if err == nil {
				err = ctx.Err()
			}
			return value, err
		}, func() (bool, error) {
			if declared {
				return false, ErrNotImplemented
			}
			return currentTemporalNodeWithoutProvider(n, node)
		})
	}
}
