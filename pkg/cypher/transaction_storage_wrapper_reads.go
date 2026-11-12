package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// This file owns the transaction wrapper's read exits: the one implementation
// of what a collection read through the wrapper returns to its caller. Every
// read routed to the transaction or to the engine it reads through ends here,
// so namespace filtering, ID detagging, caller cancellation and the raw
// error/result pairing of a read are decided once instead of once per Engine
// method.

// readScope says what a collection read through the wrapper returned, which
// decides how much of it belongs to the caller.
type readScope uint8

const (
	// readScanned records come from a read that sees the whole engine (or a
	// whole label or relationship type): only the wrapper's own namespace is
	// the caller's.
	readScanned readScope = iota
	// readOwnAdjacency records were read through one of the wrapper's own
	// nodes, so every record the read returned is the caller's.
	readOwnAdjacency
	// readUserSpace records were answered in the caller's ID space already, by
	// an underlying engine scoped to the wrapper's namespace.
	readUserSpace
)

// readProjectionCancellationInterval is how many projected records pass
// between two observations of the caller's context. storage.bulkReadCancellation
// owns this policy for storage's own copy loops; it is unexported, and the
// wrapper's projections are this package's copy loops, so the same amortization
// is repeated here rather than exported as new storage API.
const readProjectionCancellationInterval = 256

// projectTransactionRead is the collection read exit for both entity kinds.
// Primary read errors keep their raw result/error pairing. Successful reads
// observe cancellation even when no namespace transformation is needed. Typed
// blocks share scope, allocation and publication without a per-record callback.
func projectTransactionRead[T *storage.Node | *storage.Edge](ctx context.Context, w *transactionStorageWrapper, records []T, readErr error, scope readScope) ([]T, error) {
	if readErr != nil {
		return records, readErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if records == nil || w.namespace == "" || scope == readUserSpace {
		return records, nil
	}
	filtered := scope == readScanned
	prefix := ""
	if filtered {
		prefix = w.namespace + w.separator
	}
	out := make([]T, 0, len(records))
	for start := 0; start < len(records); start += readProjectionCancellationInterval {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+readProjectionCancellationInterval, len(records))
		switch values := any(records[start:end]).(type) {
		case []*storage.Node:
			for _, node := range values {
				if filtered && (node == nil || !strings.HasPrefix(string(node.ID), prefix)) {
					continue
				}
				out = append(out, any(w.toUserNode(node)).(T))
			}
		case []*storage.Edge:
			for _, edge := range values {
				if filtered && (edge == nil || !strings.HasPrefix(string(edge.ID), prefix)) {
					continue
				}
				out = append(out, any(w.toUserEdge(edge)).(T))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// visibleAtEdgeRequest describes one relationship-shaped
// storage.MVCCIndexedVisibilityEngine read in the caller's namespace: which
// read, the relationship type or node ID(s) it names, and the exact snapshot
// version it asks for.
// GetNodesByLabelVisibleAt answers the snapshot-visible node collection read. The
// immediate underlying engine capability answers it with the exact requested
// version: the wrapper neither unwraps further nor substitutes the latest state
// for the requested snapshot.
func (w *transactionStorageWrapper) GetNodesByLabelVisibleAt(ctx context.Context, label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	nodes, err := provider.GetNodesByLabelVisibleAt(ctx, label, version)
	return projectTransactionRead(ctx, w, nodes, err, w.visibleAtScope())
}

// visibleAtEdges answers one snapshot-visible adjacent-edge request. An
// underlying engine scoped to the wrapper's namespace names the caller's own
// node IDs and answers in the caller's ID space, so the request is neither
// prefixed twice nor detagged; any other engine names namespace-prefixed IDs.
func (w *transactionStorageWrapper) visibleAtEdges(ctx context.Context, request storage.AdjacentEdgeRead, version storage.MVCCVersion) ([]*storage.Edge, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	qualified := request
	if !w.underlyingIsNamespaced() {
		qualified.Node = w.prefixNodeID(request.Node)
		if request.Direction == storage.AdjacentBetween {
			qualified.Other = w.prefixNodeID(request.Other)
		}
	}
	edges, err := provider.ReadAdjacentEdgesVisibleAt(ctx, qualified, version)
	return projectTransactionRead(ctx, w, edges, err, w.visibleAtScope())
}

// visibleAtTypeEdges answers the snapshot-visible relationship-type population
// read, which is a different operation from an adjacent-edge read.
func (w *transactionStorageWrapper) visibleAtTypeEdges(ctx context.Context, edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, storage.ErrNotImplemented
	}
	edges, err := provider.GetEdgesByTypeVisibleAt(ctx, edgeType, version)
	return projectTransactionRead(ctx, w, edges, err, w.visibleAtScope())
}

// visibleAtScope reports how a snapshot-visible read's records map into the
// wrapper's namespace. An underlying engine scoped to this namespace answered
// in the caller's ID space; any other engine read a whole label/type or
// namespace-prefixed node, so its records are filtered into the namespace.
func (w *transactionStorageWrapper) visibleAtScope() readScope {
	if w.underlyingIsNamespaced() {
		return readUserSpace
	}
	return readScanned
}

// adjacentEdges answers one adjacent-edge read of the transaction in the
// caller's namespace. Prefixing is idempotent, so a read names its
// namespace-prefixed node exactly once whatever the caller passed. The stored
// view answers a stored request over the physically stored relationships; every
// other request reads the transaction's live, pending-inclusive view. The
// read's own error and result are preserved for the caller.
func (w *transactionStorageWrapper) adjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead, stored bool) ([]*storage.Edge, error) {
	qualified := request
	qualified.Node = w.prefixNodeID(request.Node)
	if request.Direction == storage.AdjacentBetween {
		qualified.Other = w.prefixNodeID(request.Other)
	}
	var (
		edges []*storage.Edge
		err   error
	)
	if stored {
		edges, err = w.tx.ReadStoredAdjacentEdges(ctx, qualified)
	} else {
		edges, err = w.tx.ReadEdges(ctx, qualified)
	}
	return projectTransactionRead(ctx, w, edges, err, readOwnAdjacency)
}

// countOfRead returns the size of one count's collection read, ending at the read's
// own error. A count has no caller context of its own, so it reads its
// collection on context.Background at that boundary; a failed or canceled read
// returns no collection, hence no count.
func countOfRead[T any](records []T, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return int64(len(records)), nil
}
