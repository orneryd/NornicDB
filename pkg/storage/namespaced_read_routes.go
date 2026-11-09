package storage

import (
	"context"
	"strings"
)

type namespacedPointMethod[C any, I ~string, T any] interface {
	func(C, context.Context, I) (T, error) | func(C, context.Context, I, MVCCVersion) (T, error)
}

// namespacedPointRead owns capability discovery on the actual decorator, ID
// qualification and result/error projection. Method expressions bind distinct
// live/query/effective/pinned operations without changing their contracts. ctx
// is the caller's read context: the capability call and the fallback read both
// run on it.
func namespacedPointRead[C any, I ~string, T any, M namespacedPointMethod[C, I, T]](ctx context.Context, n *NamespacedEngine, id I, method M, prefix func(I) I, view func(T) T, fallback func(context.Context, I) (T, error), keepErrorValue bool, version MVCCVersion) (T, error) {
	provider, ok := n.inner.(C)
	if !ok {
		if fallback != nil {
			return fallback(ctx, id)
		}
		var zero T
		return zero, ErrNotImplemented
	}
	qualified := prefix(id)
	return materializedRead(func() (T, error) {
		switch invoke := any(method).(type) {
		case func(C, context.Context, I) (T, error):
			return invoke(provider, ctx, qualified)
		case func(C, context.Context, I, MVCCVersion) (T, error):
			return invoke(provider, ctx, qualified, version)
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

// updatePendingEmbedding binds the two supported work transitions while
// preserving partial optional providers: an engine may implement only one.
func (n *NamespacedEngine) updatePendingEmbedding(id NodeID, pending bool) {
	id = n.PrefixNodeID(id)
	if pending {
		if provider, ok := n.inner.(interface{ AddToPendingEmbeddings(NodeID) }); ok {
			provider.AddToPendingEmbeddings(id)
		}
	} else if provider, ok := n.inner.(interface{ MarkNodeEmbedded(NodeID) }); ok {
		provider.MarkNodeEmbedded(id)
	}
}

type namespacedCountEntity interface{ Node | Edge }

// namespacedPrefixCount owns provider-first prefix statistics and the filtered
// materialized fallback for either entity kind, including nil entries/errors.
// ctx is the caller's count context: the inner capability read and the fallback
// collection read both run on it.
func namespacedPrefixCount[T namespacedCountEntity](ctx context.Context, n *NamespacedEngine, prefix string, method func(PrefixStatsEngine, context.Context, string) (int64, error), all func(context.Context) ([]*T, error), id func(*T) string) (int64, error) {
	return namespacedPointRead[PrefixStatsEngine, string, int64](ctx, n, prefix, method, func(value string) string { return qualifyEntityID(n.scope, value) }, nil, func(ctx context.Context, local string) (int64, error) {
		values, err := all(ctx)
		if err != nil {
			return 0, err
		}
		var count int64
		var cancel bulkReadCancellation
		for _, value := range values {
			if err := cancel.check(ctx); err != nil {
				return 0, err
			}
			if value != nil && strings.HasPrefix(id(value), local) {
				count++
			}
		}
		return count, nil
	}, true, MVCCVersion{})
}
