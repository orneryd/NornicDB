package storage

import (
	"context"
)

// nativeAdjacencyReader is one owned physical read cut. Returning the frame
// by value keeps directional state out of escaping higher-order callbacks.
type nativeAdjacencyReader struct {
	engine              *BadgerEngine
	request             AdjacentEdgeRead
	visibility          readVisibility
	adjGen, edgeGen     uint64
	out, in             adjacencyDirection
	cachedOut, cachedIn []*Edge
	view                *badgerReadView
}
type adjacencyDirection struct {
	ids        []EdgeID
	hit        bool
	live       []*Edge
	suppressed map[EdgeID]struct{}
	deleted    bool
}

// captureAdjacencyReader selects the request's native directions at one
// cache/overlay boundary. A nil view means the captured cache result is
// already complete.
//
// ctx is the adjacency read's own context, so the overlay capture and the
// cache fast path observe the caller's cancellation before they return a
// population; a canceled read closes the captured view and publishes nothing.
func (b *BadgerEngine) captureAdjacencyReader(ctx context.Context, request AdjacentEdgeRead, v readVisibility) (reader nativeAdjacencyReader, err error) {
	if err := ctx.Err(); err != nil {
		return reader, err
	}
	nodeID := request.Node
	if nodeID == "" {
		return reader, ErrInvalidID
	}
	wantOut := request.Direction != AdjacentIncoming
	wantIn := request.Direction != AdjacentOutgoing
	reader.engine, reader.request, reader.visibility = b, request, v
	reader.adjGen, reader.edgeGen = b.adjCacheGen.current(), b.edgeCacheGen.current()
	var captureErr error
	reader.view, err = b.captureReadView(ctx, func(w *WriteBehindBuffer) {
		if wantOut {
			reader.out.ids, reader.out.hit = b.adjCacheLoad(nodeID, true)
			if w != nil {
				pendingAdjacency, readErr := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: true})
				if readErr != nil {
					captureErr = readErr
					return
				}
				reader.out.live, reader.out.suppressed, reader.out.deleted = pendingAdjacency.edges, pendingAdjacency.suppressed, pendingAdjacency.sourceDeleted
			}
		}
		if wantIn {
			reader.in.ids, reader.in.hit = b.adjCacheLoad(nodeID, false)
			if w != nil {
				pendingAdjacency, readErr := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: false})
				if readErr != nil {
					captureErr = readErr
					return
				}
				reader.in.live, reader.in.suppressed, reader.in.deleted = pendingAdjacency.edges, pendingAdjacency.suppressed, pendingAdjacency.sourceDeleted
			}
		}
	}, readViewOptions{scope: namespaceForNodeID(nodeID), cached: func(w *WriteBehindBuffer) (bool, error) {
		// A header read answers from the adjacency entries' own values, so the
		// in-memory body cache cannot stand in for it.
		if request.Headers {
			return false, nil
		}
		cachedOut, cachedIn, complete, cacheErr := b.cachedAdjacency(ctx, w, request, v)
		if cacheErr != nil {
			return false, cacheErr
		}
		reader.cachedOut, reader.cachedIn = cachedOut, cachedIn
		return complete, nil
	}})
	if err != nil {
		return nativeAdjacencyReader{}, err
	}
	if captureErr != nil {
		// The caller is gone: release the captured view and publish nothing.
		reader.view.Close()
		return nativeAdjacencyReader{}, captureErr
	}
	return reader, nil
}

// read hydrates one captured direction; it never selects a later view or cache.
//
// A full request rejects an entry of another type before its relationship is
// read, both from the adjacency value and from a cached body. A header request
// answers from the entries' values, reading a relationship record only for an
// entry written before those values existed.
func (r *nativeAdjacencyReader) read(ctx context.Context, outgoing bool) (edges []*Edge, err error) {
	if r.view == nil {
		if outgoing {
			return r.cachedOut, nil
		}
		return r.cachedIn, nil
	}
	b, nodeID, v := r.engine, r.request.Node, r.visibility
	request := r.request
	adjGen, edgeGen, txn := r.adjGen, r.edgeGen, r.view.txn
	d := &r.in
	if outgoing {
		d = &r.out
	}
	err = recoverBadgerClosedPanic(func() error {
		if d.deleted {
			return nil
		}
		if request.Headers {
			edges, err = b.readAdjacentEdgeHeadersInTxn(ctx, txn, request, outgoing, edgeGen)
			if err != nil {
				return err
			}
		} else if !d.hit {
			prefix := b.incomingIndexPrefixString(nodeID)
			if outgoing {
				prefix = b.outgoingIndexPrefixString(nodeID)
			}
			if prefix != nil {
				var err error
				edges, d.ids, err = b.collectEdgesByIndexPrefix(txn, prefix, DecayScoringTime(), edgeGen, v, request.Types)
				if err != nil {
					return err
				}
			}
			b.adjCacheStore(adjGen, nodeID, d.ids, txn.ReadTs(), outgoing)
		} else {
			// Read cached IDs from this transaction, never a later body cache.
			var cancel bulkReadCancellation
			for _, id := range d.ids {
				if err := cancel.check(ctx); err != nil {
					return err
				}
				if _, hidden := d.suppressed[id]; hidden {
					continue
				}
				edge, err := b.readIndexedEdgeInTxn(txn, id, edgeGen)
				if err != nil {
					return err
				}
				if edge != nil && adjacentTypesAdmit(request.Types, edge.Type) && !b.filterEntityByDecay(v, edge, DecayScoringTime()) {
					edges = append(edges, edge)
				}
			}
		}
		if len(d.suppressed) != 0 {
			visible := edges[:0]
			var cancel bulkReadCancellation
			for _, edge := range edges {
				if err := cancel.check(ctx); err != nil {
					return err
				}
				if _, hidden := d.suppressed[edge.ID]; !hidden {
					visible = append(visible, edge)
				}
			}
			edges = visible
		}
		if !request.Headers {
			attached := edges[:0]
			var attachCancel bulkReadCancellation
			for _, edge := range edges {
				if err := attachCancel.check(ctx); err != nil {
					return err
				}
				if attachedAt(edge, nodeID, outgoing) {
					attached = append(attached, edge)
				}
			}
			edges = attached
		}
		var liveCancel bulkReadCancellation
		for _, edge := range d.live {
			if err := liveCancel.check(ctx); err != nil {
				return err
			}
			if !adjacentTypesAdmit(request.Types, edge.Type) {
				continue
			}
			edges = append(edges, request.header(copyEdge(edge)))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

// attachedAt reports whether edge has nodeID at the end an adjacency read in
// that direction lists. An index or cache entry left at an endpoint the
// relationship moved away from (a buffered or applied update) is not
// adjacency: a node delete's cascade or a traversal there must not meet it.
func attachedAt(edge *Edge, nodeID NodeID, outgoing bool) bool {
	if outgoing {
		return edge.StartNode == nodeID
	}
	return edge.EndNode == nodeID
}

// cachedAdjacency resolves a traversal as one captured operation. The edge cache
// is locked once for both directions; only pending adjacency entries are copied.
//
// The cached population is copied under the read's own context: a caller that
// was canceled before or during the copy gets its own error and no edges,
// instead of a complete-looking adjacency nobody asked for any more.
func (b *BadgerEngine) cachedAdjacency(ctx context.Context, w *WriteBehindBuffer, request AdjacentEdgeRead, v readVisibility) ([]*Edge, []*Edge, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	id := request.Node
	wantOut := request.Direction != AdjacentIncoming
	wantIn := request.Direction != AdjacentOutgoing
	var outIDs, inIDs []EdgeID
	var outHit, inHit = !wantOut, !wantIn
	if wantOut {
		outIDs, outHit = b.adjCacheLoad(id, true)
	}
	if wantIn {
		inIDs, inHit = b.adjCacheLoad(id, false)
	}
	if !outHit || !inHit {
		return nil, nil, false, nil
	}
	if w != nil && w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead}).deleted {
		return nil, nil, true, nil
	}
	b.edgeCacheMu.RLock()
	defer b.edgeCacheMu.RUnlock()
	now := DecayScoringTime()
	read := func(ids []EdgeID, outgoing bool) ([]*Edge, bool, error) {
		var live []*Edge
		if w != nil {
			pending, err := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedLiveAdjacency, source: id, outgoing: outgoing})
			if err != nil {
				return nil, false, err
			}
			live = pending.edges
		}
		if len(ids) == 0 && len(live) == 0 {
			return nil, true, nil
		}
		result := make([]*Edge, 0, len(ids)+len(live))
		var cancel bulkReadCancellation
		for _, edgeID := range ids {
			if err := cancel.check(ctx); err != nil {
				return nil, false, err
			}
			if w != nil && w.entityStateLocked(bufferedEdgeEntity, string(edgeID), bufferedReadVisibility{engine: b, visibility: storedRead}).found {
				continue
			}
			edge := b.edgeCache[edgeID]
			if edge == nil {
				return nil, false, nil
			}
			if attachedAt(edge, id, outgoing) && adjacentTypesAdmit(request.Types, edge.Type) && !b.filterEntityByDecay(v, edge, now) {
				result = append(result, edge)
			}
		}
		for _, edge := range live {
			if err := cancel.check(ctx); err != nil {
				return nil, false, err
			}
			if !adjacentTypesAdmit(request.Types, edge.Type) {
				continue
			}
			result = append(result, copyEdge(edge))
		}
		return result, true, nil
	}
	var out, in []*Edge
	if wantOut {
		edges, ok, readErr := read(outIDs, true)
		if readErr != nil {
			return nil, nil, false, readErr
		}
		if !ok {
			return nil, nil, false, nil
		}
		out = edges
	}
	if wantIn {
		edges, ok, readErr := read(inIDs, false)
		if readErr != nil {
			return nil, nil, false, readErr
		}
		if !ok {
			return nil, nil, false, nil
		}
		in = edges
	}
	return out, in, true, nil
}

// liveAdjacencyLocked enumerates the overlapping adjacency index, not the
// buffer's global tombstones. Cached committed IDs resolve tombstones directly.
// ctx is the read's own lifetime: the copy polls it per visited edge.
func (w *WriteBehindBuffer) liveAdjacencyLocked(ctx context.Context, id NodeID, outgoing bool) ([]*Edge, error) {
	var cancel bulkReadCancellation
	var cancelErr error
	var result []*Edge
	var seen map[EdgeID]bool
	visit := func(buf *CommitBuffer) {
		ids := buf.incomingByNode[id]
		if outgoing {
			ids = buf.outgoingByNode[id]
		}
		for _, edgeID := range ids {
			if err := cancel.check(ctx); err != nil {
				cancelErr = err
				return
			}
			if seen[edgeID] {
				continue
			}
			if seen == nil {
				seen = make(map[EdgeID]bool)
			}
			seen[edgeID] = true
			state := w.entityStateLocked(bufferedEdgeEntity, string(edgeID))
			if state.deleted || state.edge == nil {
				continue
			}
			edge := state.edge
			if outgoing && edge.StartNode == id || !outgoing && edge.EndNode == id {
				result = append(result, edge)
			}
		}
	}
	visit(w.active)
	for i := len(w.draining) - 1; i >= 0 && cancelErr == nil; i-- {
		visit(w.draining[i])
	}
	if cancelErr != nil {
		return nil, cancelErr
	}
	return result, nil
}
