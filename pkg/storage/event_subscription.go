package storage

import (
	"errors"
)

// ErrEventSubscriptionUnsupported reports an engine that cannot deliver the
// storage events: neither it nor the engines it wraps publish them.
var ErrEventSubscriptionUnsupported = errors.New("storage: engine does not deliver storage events")

// EventHandlers are the consumers of one subscription. A nil handler is not
// registered.
type EventHandlers struct {
	EntityCommitted EntityCommitCallback
	NodeCreated     NodeEventCallback
	NodeUpdated     NodeEventCallback
	NodeDeleted     NodeDeleteCallback
	EdgeCreated     EdgeEventCallback
	EdgeUpdated     EdgeEventCallback
	EdgeDeleted     EdgeDeleteCallback
	NamespaceReset  NamespaceResetCallback
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
	if handlers.EntityCommitted != nil {
		b.subscribeEntityCommit(handlers.EntityCommitted)
	}
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
	views := []*NamespacedEngine{n}
	if handler := handlers.EntityCommitted; handler != nil {
		translated.EntityCommitted = scopeStorageEvent(handler, views)
	}
	if handler := handlers.NodeCreated; handler != nil {
		translated.NodeCreated = scopeStorageEvent(handler, views)
	}
	if handler := handlers.NodeUpdated; handler != nil {
		translated.NodeUpdated = scopeStorageEvent(handler, views)
	}
	if handler := handlers.NodeDeleted; handler != nil {
		translated.NodeDeleted = scopeStorageEvent(handler, views)
	}
	if handler := handlers.EdgeCreated; handler != nil {
		translated.EdgeCreated = scopeStorageEvent(handler, views)
	}
	if handler := handlers.EdgeUpdated; handler != nil {
		translated.EdgeUpdated = scopeStorageEvent(handler, views)
	}
	if handler := handlers.EdgeDeleted; handler != nil {
		translated.EdgeDeleted = scopeStorageEvent(handler, views)
	}
	if handler := handlers.NamespaceReset; handler != nil {
		translated.NamespaceReset = scopeStorageEvent(handler, views)
	}

	return SubscribeEvents(n.inner, translated)
}
