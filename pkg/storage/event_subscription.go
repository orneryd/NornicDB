package storage

import (
	"errors"
	"strings"
)

// ErrEventSubscriptionUnsupported reports an engine that cannot deliver the
// storage events: neither it nor the engines it wraps publish them.
var ErrEventSubscriptionUnsupported = errors.New("storage: engine does not deliver storage events")

// EventHandlers are the consumers of one subscription. A nil handler is not
// registered.
type EventHandlers struct {
	NodeCreated    NodeEventCallback
	NodeUpdated    NodeEventCallback
	NodeDeleted    NodeDeleteCallback
	EdgeCreated    EdgeEventCallback
	EdgeUpdated    EdgeEventCallback
	EdgeDeleted    EdgeDeleteCallback
	NamespaceReset NamespaceResetCallback
}

// EventSubscriber delivers every storage event, entity changes and namespace
// resets alike, to one set of handlers. The engine that publishes implements
// it; every wrapper forwards it to the engine it wraps (compile-time checked
// in wrapper_capabilities.go and in the wrapper's own package), so a
// subscription reaches the publisher through any configuration of wrappers.
type EventSubscriber interface {
	SubscribeEvents(handlers EventHandlers) error
}

// SubscribeEvents subscribes handlers to engine, or reports that it cannot
// deliver events. It checks engine only, never unwrapping it: a wrapper that
// does not forward the capability is an error, not a silent gap.
func SubscribeEvents(engine Engine, handlers EventHandlers) error {
	subscriber, ok := engine.(EventSubscriber)
	if !ok || subscriber == nil {
		return ErrEventSubscriptionUnsupported
	}
	return subscriber.SubscribeEvents(handlers)
}

// SubscribeEvents registers handlers with the commit-ordered callbacks.
func (b *BadgerEngine) SubscribeEvents(handlers EventHandlers) error {
	b.OnNodeCreated(handlers.NodeCreated)
	b.OnNodeUpdated(handlers.NodeUpdated)
	b.OnNodeDeleted(handlers.NodeDeleted)
	b.OnEdgeCreated(handlers.EdgeCreated)
	b.OnEdgeUpdated(handlers.EdgeUpdated)
	b.OnEdgeDeleted(handlers.EdgeDeleted)
	b.OnNamespaceReset(handlers.NamespaceReset)
	return nil
}

// SubscribeEvents forwards to the inner engine; WAL does not translate IDs.
func (w *WALEngine) SubscribeEvents(handlers EventHandlers) error {
	return SubscribeEvents(w.engine, handlers)
}

// SubscribeEvents forwards to the traced engine.
func (t *TracedEngine) SubscribeEvents(handlers EventHandlers) error {
	return SubscribeEvents(t.Engine, handlers)
}

// SubscribeEvents forwards this namespace's events with namespace-stripped
// IDs. A reset covering the whole namespace is reported with an empty prefix;
// one inside it, with the prefix stripped; one elsewhere, not at all.
func (n *NamespacedEngine) SubscribeEvents(handlers EventHandlers) error {
	var translated EventHandlers
	if h := handlers.NodeCreated; h != nil {
		translated.NodeCreated = func(node *Node) {
			if node != nil && n.hasNodePrefix(node.ID) {
				h(n.toUserNode(node))
			}
		}
	}
	if h := handlers.NodeUpdated; h != nil {
		translated.NodeUpdated = func(node *Node) {
			if node != nil && n.hasNodePrefix(node.ID) {
				h(n.toUserNode(node))
			}
		}
	}
	if h := handlers.NodeDeleted; h != nil {
		translated.NodeDeleted = func(id NodeID) {
			if n.hasNodePrefix(id) {
				h(n.unprefixNodeID(id))
			}
		}
	}
	if h := handlers.EdgeCreated; h != nil {
		translated.EdgeCreated = func(edge *Edge) {
			if edge != nil && n.hasEdgePrefix(edge.ID) {
				h(n.toUserEdge(edge))
			}
		}
	}
	if h := handlers.EdgeUpdated; h != nil {
		translated.EdgeUpdated = func(edge *Edge) {
			if edge != nil && n.hasEdgePrefix(edge.ID) {
				h(n.toUserEdge(edge))
			}
		}
	}
	if h := handlers.EdgeDeleted; h != nil {
		translated.EdgeDeleted = func(id EdgeID) {
			if n.hasEdgePrefix(id) {
				h(n.unprefixEdgeID(id))
			}
		}
	}
	if h := handlers.NamespaceReset; h != nil {
		namespace := n.Namespace() + ":"
		translated.NamespaceReset = func(prefix string) {
			switch {
			case strings.HasPrefix(namespace, prefix):
				h("")
			case strings.HasPrefix(prefix, namespace):
				h(strings.TrimPrefix(prefix, namespace))
			}
		}
	}
	return SubscribeEvents(n.inner, translated)
}
