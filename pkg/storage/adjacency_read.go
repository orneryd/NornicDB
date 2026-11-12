package storage

import (
	"context"
	"sync/atomic"
)

// Tests can park a captured physical read without holding a cache mutex that
// durability itself must acquire. The cache-only path does not consult it.
var adjacencyReadCaptureObserver atomic.Pointer[func(*BadgerEngine)]

// readAdjacentEdges routes one adjacent-edge request to the reader that owns
// its view: a namespace view qualifies the request's IDs and projects the
// result, an exact version goes to the MVCC provider, a stored read goes to the
// stored reader, and everything else to the engine's own current reader. Only
// an actual namespace view qualifies IDs; native physical readers are never
// found by unwrap.
func readAdjacentEdges(ctx context.Context, engine Engine, request AdjacentEdgeRead, version *MVCCVersion, visibility readVisibility) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	if namespace, ok := engine.(*NamespacedEngine); ok {
		qualified := request
		qualified.Node = namespace.PrefixNodeID(request.Node)
		if request.Direction == AdjacentBetween {
			qualified.Other = namespace.PrefixNodeID(request.Other)
		}
		edges, err := readAdjacentEdges(ctx, namespace.inner, qualified, version, visibility)
		if err != nil {
			return nil, err
		}
		pinned := version != nil
		// Physical reader leases have ended before copying namespace projections.
		// A stored read keeps the hidden bodies it asked for.
		return namespace.toUserEdgesInNamespace(ctx, edges, visibility == storedRead, pinned)
	}
	if version != nil {
		provider, ok := engine.(MVCCIndexedVisibilityEngine)
		if !ok {
			return nil, ErrNotImplemented
		}
		return provider.ReadAdjacentEdgesVisibleAt(ctx, request, *version)
	}
	if visibility == storedRead {
		if reader, ok := engine.(StoredEntityReader); ok {
			return reader.ReadStoredAdjacentEdges(ctx, request)
		}
	}
	return engine.ReadAdjacentEdges(ctx, request)
}
