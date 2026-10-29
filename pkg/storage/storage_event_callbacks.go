package storage

import "strings"

// storageEventCallback retains the notifier's concrete callback types. The
// delivery shape is selected once at registration, never per published event.
type storageEventCallback interface {
	NodeEventCallback | EdgeEventCallback | NodeDeleteCallback | EdgeDeleteCallback | NamespaceResetCallback
}

// subscribeStorageEvent registers each already-deduplicated target through the
// same projection owner used by EventHandlers subscriptions. Raw targets retain
// the exact callback, including nil; scoped targets retain the original panic
// and callback lifetime policies.
func subscribeStorageEvent[Callback storageEventCallback](targets []storageEventTarget, callback Callback, register func(StorageEventNotifier, Callback)) {
	for _, target := range targets {
		if notifier, ok := ViewMetadataForEngine[StorageEventNotifier](target.engine); ok {
			register(notifier, scopeStorageEvent(callback, target.views))
		}
	}
}

// firstStorageEventView owns entity-ID scope selection for every payload type.
func firstStorageEventView(views []*NamespacedEngine, id string) *NamespacedEngine {
	for _, view := range views {
		if strings.HasPrefix(id, view.scope) {
			return view
		}
	}
	return nil
}

// scopeStorageEvent selects a concrete delivery closure once, preserving typed
// payloads on the hot path. Scoped events use their first matching view; no
// payload type tests or interface conversions occur during delivery.
func scopeStorageEvent[Callback storageEventCallback](callback Callback, views []*NamespacedEngine) Callback {
	if len(views) == 0 {
		return callback
	}
	switch handler := any(callback).(type) {
	case NodeEventCallback:
		return any(NodeEventCallback(func(node *Node) {
			if node == nil {
				return
			}
			if view := firstStorageEventView(views, string(node.ID)); view != nil {
				handler(view.toUserNode(node, nodeUserView{}))
			}
		})).(Callback)
	case EdgeEventCallback:
		return any(EdgeEventCallback(func(edge *Edge) {
			if edge == nil {
				return
			}
			if view := firstStorageEventView(views, string(edge.ID)); view != nil {
				handler(view.toUserEdge(edge))
			}
		})).(Callback)
	case NodeDeleteCallback:
		return any(NodeDeleteCallback(func(id NodeID) {
			if view := firstStorageEventView(views, string(id)); view != nil {
				handler(NodeID(strings.TrimPrefix(string(id), view.scope)))
			}
		})).(Callback)
	case EdgeDeleteCallback:
		return any(EdgeDeleteCallback(func(id EdgeID) {
			if view := firstStorageEventView(views, string(id)); view != nil {
				handler(EdgeID(strings.TrimPrefix(string(id), view.scope)))
			}
		})).(Callback)
	case NamespaceResetCallback:
		return any(NamespaceResetCallback(func(prefix string) {
			for _, view := range views {
				switch {
				case strings.HasPrefix(view.scope, prefix):
					handler("")
					return
				case strings.HasPrefix(prefix, view.scope):
					handler(strings.TrimPrefix(prefix, view.scope))
					return
				}
			}
		})).(Callback)
	}
	panic("storage: unsupported callback type") // exhaustive constrained union above
}
