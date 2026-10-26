package storage

import "strings"

type namespacedPointMethod[C any, I ~string, T any] interface {
	func(C, I) (T, error) | func(C, I, MVCCVersion) (T, error)
}

// namespacedPointRead owns optional capability discovery on the actual view,
// ID qualification and result/error projection. Core Engine reads retain the
// issued inner engine and share materializedRead without optional discovery.
// Method expressions bind distinct
// live/query/effective/pinned operations without changing their contracts.
func namespacedPointRead[C any, I ~string, T any, M namespacedPointMethod[C, I, T]](n *NamespacedEngine, id I, method M, prefix func(I) I, view func(T) T, fallback func(I) (T, error), keepErrorValue bool, version MVCCVersion) (T, error) {
	qualified := prefix(id)
	return ReadWithCapability(n.inner, func(provider C) (T, error) {
		return materializedRead(func() (T, error) {
			switch invoke := any(method).(type) {
			case func(C, I) (T, error):
				return invoke(provider, qualified)
			case func(C, I, MVCCVersion) (T, error):
				return invoke(provider, qualified, version)
			}
			panic("unreachable namespaced point method type")
		}, func(value T) T {
			if view != nil {
				return view(value)
			}
			return value
		}, keepErrorValue)
	}, func() (T, error) {
		if fallback != nil {
			return fallback(id)
		}
		var zero T
		return zero, ErrNotImplemented
	})
}

func relationshipEndpointVisibility(engine Engine, id NodeID) (bool, bool) {
	result := ReadValueWithCapability(engine, func(provider RelationshipEndpointChecker) [2]bool {
		visible, answered := provider.RelationshipEndpointVisible(id)
		return [2]bool{visible, answered}
	}, nil)
	return result[0], result[1]
}

// updatePendingEmbedding binds the two supported work transitions while
// preserving partial optional providers: an engine may implement only one.
func (n *NamespacedEngine) updatePendingEmbedding(id NodeID, pending bool) {
	id = n.PrefixNodeID(id)
	if pending {
		if provider, ok := OwnedCapability[interface{ AddToPendingEmbeddings(NodeID) }](n.inner); ok {
			provider.AddToPendingEmbeddings(id)
		}
	} else if provider, ok := OwnedCapability[interface{ MarkNodeEmbedded(NodeID) }](n.inner); ok {
		provider.MarkNodeEmbedded(id)
	}
}

// namespacedPrefixCount owns one idempotent qualification and scalar dispatch.
// Each caller supplies its same-view streaming fallback for the entity kind.
func namespacedPrefixCount(n *NamespacedEngine, prefix string, method func(PrefixStatsEngine, string) (int64, error), fallback func(string) (int64, error)) (int64, error) {
	qualified := qualifyEntityID(n.scope, prefix)
	local := strings.TrimPrefix(qualified, n.scope)
	return ReadWithCapability(n.inner, func(provider PrefixStatsEngine) (int64, error) { return method(provider, qualified) }, func() (int64, error) { return fallback(local) })
}
