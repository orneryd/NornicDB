package storage

import "strings"

type namespacedPointMethod[C any, I ~string, T any] interface {
	func(C, I) (T, error) | func(C, I, MVCCVersion) (T, error)
}

// namespacedPointRead owns capability discovery on the actual decorator, ID
// qualification and result/error projection. Method expressions bind distinct
// live/query/effective/pinned operations without changing their contracts.
func namespacedPointRead[C any, I ~string, T any, M namespacedPointMethod[C, I, T]](n *NamespacedEngine, id I, method M, prefix func(I) I, view func(T) T, fallback func(I) (T, error), keepErrorValue bool, version MVCCVersion) (T, error) {
	provider, ok := n.inner.(C)
	if !ok {
		if fallback != nil {
			return fallback(id)
		}
		var zero T
		return zero, ErrNotImplemented
	}
	qualified := prefix(id)
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
}

func relationshipEndpointVisibility(engine Engine, id NodeID) (bool, bool) {
	if provider, ok := engine.(RelationshipEndpointChecker); ok {
		return provider.RelationshipEndpointVisible(id)
	}
	return false, false
}

type namespacedCountEntity interface{ Node | Edge }

// namespacedPrefixCount owns provider-first prefix statistics and the filtered
// materialized fallback for either entity kind, including nil entries/errors.
func namespacedPrefixCount[T namespacedCountEntity](n *NamespacedEngine, prefix string, method func(PrefixStatsEngine, string) (int64, error), all func() ([]*T, error), id func(*T) string) (int64, error) {
	return namespacedPointRead[PrefixStatsEngine, string, int64](n, prefix, method, func(value string) string { return qualifyEntityID(n.scope, value) }, nil, func(local string) (int64, error) {
		values, err := all()
		if err != nil {
			return 0, err
		}
		var count int64
		for _, value := range values {
			if value != nil && strings.HasPrefix(id(value), local) {
				count++
			}
		}
		return count, nil
	}, true, MVCCVersion{})
}
