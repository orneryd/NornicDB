package storage

import (
	"context"
	"sync/atomic"
)

// Tests can park a captured physical read without holding a cache mutex that
// durability itself must acquire. The cache-only path does not consult it.
var adjacencyReadCaptureObserver atomic.Pointer[func(*BadgerEngine)]

type adjacencyRead uint8

const (
	adjacencyOutgoing adjacencyRead = iota
	adjacencyIncoming
	adjacencyBetween
	adjacencyBoth
)

// readAdjacency owns request routing, direction order and result projection.
// Only an actual namespace view qualifies IDs. Optional providers are bound on
// its immediate decorator; native physical readers are never found by unwrap.
func readAdjacency(ctx context.Context, engine Engine, kind adjacencyRead, start, end NodeID, version *MVCCVersion, visibility readVisibility) (out, in []*Edge, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var namespace *NamespacedEngine
	if n, ok := engine.(*NamespacedEngine); ok {
		namespace = n
		engine = n.inner
		start = n.PrefixNodeID(start)
		if kind == adjacencyBetween {
			end = n.PrefixNodeID(end)
		}
	}
	var wantOut, wantIn bool
	switch kind {
	case adjacencyOutgoing, adjacencyBetween:
		wantOut = true
	case adjacencyIncoming:
		wantIn = true
	case adjacencyBoth:
		wantOut, wantIn = true, true
	default:
		return nil, nil, ErrInvalidData
	}
	// The dispatcher is the sole owner of ordering and partial-result policy.
	consume := func(read func(bool) ([]*Edge, error)) error {
		if wantOut {
			out, err = read(true)
			if err != nil {
				return err
			}
		}
		if wantIn {
			in, err = read(false)
		}
		return err
	}
	if version != nil {
		provider, ok := engine.(MVCCIndexedVisibilityEngine)
		if !ok {
			return nil, nil, ErrNotImplemented
		}
		if kind == adjacencyBoth {
			return nil, nil, ErrInvalidData
		}
		err = consume(func(outgoing bool) ([]*Edge, error) {
			if kind == adjacencyBetween {
				return provider.GetEdgesBetweenVisibleAt(ctx, start, end, *version)
			}
			if outgoing {
				return provider.GetOutgoingEdgesVisibleAt(ctx, start, *version)
			}
			return provider.GetIncomingEdgesVisibleAt(ctx, start, *version)
		})
	} else {
		native, isNative := engine.(*BadgerEngine)
		if isNative && kind != adjacencyBetween {
			reader, captureErr := native.captureAdjacencyReader(ctx, start, wantOut, wantIn, visibility)
			if captureErr != nil {
				err = captureErr
			} else {
				if reader.view != nil {
					defer reader.view.Close()
					if observe := adjacencyReadCaptureObserver.Load(); observe != nil {
						(*observe)(native)
					}
				}
				err = consume(func(outgoing bool) ([]*Edge, error) { return reader.read(ctx, outgoing) })
				// End physical ownership before any namespace copying/projection.
				if reader.view != nil {
					reader.view.Close()
				}
			}
		} else {
			var combined AdjacentEdgesEngine
			if kind == adjacencyBoth {
				combined, _ = engine.(AdjacentEdgesEngine)
			}
			if combined != nil {
				out, in, err = combined.GetAdjacentEdges(ctx, start)
			} else {
				err = consume(func(outgoing bool) ([]*Edge, error) {
					if kind == adjacencyBetween {
						return engine.GetEdgesBetween(ctx, start, end)
					}
					if outgoing {
						return engine.GetOutgoingEdges(ctx, start)
					}
					return engine.GetIncomingEdges(ctx, start)
				})
			}
		}
	}

	if namespace != nil {
		if err != nil {
			return nil, nil, err
		}
		pinned := version != nil
		// Physical reader leases have ended before copying namespace projections.
		if out, err = namespace.toUserEdgesInNamespace(ctx, out, false, pinned); err != nil {
			return nil, nil, err
		}
		if in, err = namespace.toUserEdgesInNamespace(ctx, in, false, pinned); err != nil {
			return nil, nil, err
		}
	}
	return out, in, err
}
