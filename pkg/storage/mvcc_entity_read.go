package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// mvccEntityRead selects hydration and graph visibility at one logical version.
// Edge bodies have no embedding hydration; vectors is used only by node loads.
type mvccEntityRead struct {
	vectors    bool
	visibility readVisibility
}

// readEntityAtWithView is the single point-read admission/reader-registration
// owner. A typed method expression selects the entity decoder; all loads keep
// the caller's physical snapshot and return its exact value/error pair.
func readEntityAtWithView[I ~string, T Node | Edge, P any](ctx context.Context, b *BadgerEngine, id I, version MVCCVersion, policy P, view func(func(*badger.Txn) error) error, load func(*BadgerEngine, *badger.Txn, I, MVCCVersion, P) (*T, error)) (*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespace, _, _ := ParseDatabasePrefix(string(id))
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return nil, err
	}
	deregister, view, err := b.beginMVCCHistoryRead(ctx, version, view)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var entity *T
	err = view(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var loadErr error
		entity, loadErr = load(b, txn, id, version, policy)
		return loadErr
	})
	return entity, err
}

// readAdjacentEdgesAtWithView owns the pinned adjacent-edge traversal. Pinned
// transactions already use their registered cached reader before reaching this
// fallback, while public/fallback requests register for this complete view.
//
// A relationship's adjacency entry carries only its tombstone bit, so a typed
// request first validates the current header against the relationship's head
// version inside this same view and uses it only while it is in effect at the
// read version; a stale header leaves the relationship to its snapshot record.
func (b *BadgerEngine) readAdjacentEdgesAtWithView(ctx context.Context, request AdjacentEdgeRead, version MVCCVersion, view func(func(*badger.Txn) error) error) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Headers && b.visibilityFiltered() {
		// The visible-at record owner must retain the properties its policy
		// reads, including when a caller only needs the returned headers.
		request.Headers = false
	}
	if request.Direction == AdjacentBoth {
		return readBothAdjacentEdges(ctx, request, func(direction AdjacentEdgeRead) ([]*Edge, error) {
			return b.readAdjacentEdgesAtWithView(ctx, direction, version, view)
		})
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	if request.Direction == AdjacentBetween {
		if err := b.pendingReadConflict(readNodePairScope, "", []NodeID{request.Other, request.Node}, ctx); err != nil {
			return nil, err
		}
	}
	if err := b.pendingNamespaceConflict(namespaceForNodeID(request.Node)); err != nil {
		return nil, err
	}
	var prefix []byte
	switch request.Direction {
	case AdjacentOutgoing, AdjacentBetween:
		prefix = b.mvccOutgoingAdjacencyPrefixString(request.Node)
	case AdjacentIncoming:
		prefix = b.mvccIncomingAdjacencyPrefixString(request.Node)
	default:
		return nil, ErrInvalidData
	}
	deregister, view, err := b.beginMVCCHistoryRead(ctx, version, view)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = view(func(txn *badger.Txn) error {
		ids, err := b.collectVisibleAdjacencyEdgeIDsInTxn(ctx, txn, prefix, version)
		if err != nil {
			return err
		}
		var headers map[EdgeID]*Edge
		var decided map[EdgeID]struct{}
		if request.Headers || len(request.Types) > 0 {
			headers, decided, err = b.adjacentSnapshotHeadersInTxn(ctx, txn, request, version)
			if err != nil {
				return err
			}
		}
		var cancel bulkReadCancellation
		for _, id := range ids {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			if _, excluded := decided[id]; excluded {
				continue
			}
			if request.Headers {
				if header := headers[id]; header != nil {
					edges = append(edges, header)
					continue
				}
			}
			edge, e := b.getEdgeAtInTxn(txn, id, version, mvccEntityRead{visibility: visibleRead})
			if e == ErrNotFound || e == ErrNotVisibleAtSnapshot {
				continue
			}
			if e != nil {
				return e
			}
			if edge == nil || !request.admits(edge) {
				continue
			}
			edges = append(edges, request.header(edge))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}
