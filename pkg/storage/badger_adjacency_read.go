package storage

import "context"

// nativeAdjacencyReader is one owned physical read cut. Returning the frame
// by value keeps directional state out of escaping higher-order callbacks.
type nativeAdjacencyReader struct {
	ctx                 context.Context
	engine              *BadgerEngine
	nodeID              NodeID
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

// captureAdjacencyReader selects both native directions at one cache/overlay
// boundary. A nil view means the captured cache result is already complete.
func (b *BadgerEngine) captureAdjacencyReader(ctx context.Context, nodeID NodeID, wantOut, wantIn bool, v readVisibility) (reader nativeAdjacencyReader, err error) {
	if err := ctx.Err(); err != nil {
		return reader, err
	}
	reader.ctx = ctx
	if nodeID == "" {
		return reader, ErrInvalidID
	}
	reader.engine, reader.nodeID, reader.visibility = b, nodeID, v
	reader.adjGen, reader.edgeGen = b.adjCacheGen.current(), b.edgeCacheGen.current()
	var captureErr error
	reader.view, err = b.captureReadView(func(w *WriteBehindBuffer) {
		if wantOut {
			reader.out.ids, reader.out.hit = b.adjCacheLoadOutgoing(nodeID)
			if w != nil {
				pending, readErr := b.readBufferedEntitiesWithContext(ctx, w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: true})
				reader.out.live, reader.out.suppressed, reader.out.deleted, captureErr = pending.edges, pending.suppressed, pending.sourceDeleted, readErr
			}
		}
		if wantIn {
			if captureErr != nil {
				return
			}
			reader.in.ids, reader.in.hit = b.adjCacheLoadIncoming(nodeID)
			if w != nil {
				pending, readErr := b.readBufferedEntitiesWithContext(ctx, w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: false})
				reader.in.live, reader.in.suppressed, reader.in.deleted, captureErr = pending.edges, pending.suppressed, pending.sourceDeleted, readErr
			}
		}
	}, readViewOptions{ctx: ctx, scope: namespaceForNodeID(nodeID), cached: func(w *WriteBehindBuffer) (bool, error) {
		var complete bool
		var err error
		reader.cachedOut, reader.cachedIn, complete, err = b.cachedAdjacency(ctx, w, nodeID, wantOut, wantIn, v)
		return complete, err
	}})
	if err == nil && captureErr != nil {
		if reader.view != nil {
			reader.view.Close()
			reader.view = nil
		}
		err = captureErr
	}
	return reader, err
}

// read hydrates one captured direction; it never selects a later view or cache.
func (r *nativeAdjacencyReader) read(outgoing bool) (edges []*Edge, err error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.view == nil {
		if outgoing {
			return r.cachedOut, nil
		}
		return r.cachedIn, nil
	}
	b, nodeID, v := r.engine, r.nodeID, r.visibility
	adjGen, edgeGen, txn := r.adjGen, r.edgeGen, r.view.txn
	d := &r.in
	if outgoing {
		d = &r.out
	}
	err = recoverBadgerClosedPanic(func() error {
		if d.deleted {
			return nil
		}
		if !d.hit {
			prefix := b.incomingIndexPrefixString(nodeID)
			if outgoing {
				prefix = b.outgoingIndexPrefixString(nodeID)
			}
			if prefix != nil {
				var err error
				edges, d.ids, err = b.collectEdgesByIndexPrefix(r.ctx, txn, prefix, DecayScoringTime(), edgeGen, v)
				if err != nil {
					return err
				}
			}
			if outgoing {
				b.adjCacheStoreOutgoing(adjGen, nodeID, d.ids)
			} else {
				b.adjCacheStoreIncoming(adjGen, nodeID, d.ids)
			}
		}
		// Read cached IDs from this transaction, never a later body cache.
		if edges == nil {
			for _, id := range d.ids {
				if err := r.ctx.Err(); err != nil {
					return err
				}
				if _, hidden := d.suppressed[id]; hidden {
					continue
				}
				edge, err := b.readIndexedEdgeInTxn(txn, id, edgeGen)
				if err != nil {
					return err
				}
				if edge != nil && !b.filterEntityByDecay(v, edge, DecayScoringTime()) {
					edges = append(edges, edge)
				}
			}
		} else if len(d.suppressed) != 0 {
			visible := edges[:0]
			for _, edge := range edges {
				if err := r.ctx.Err(); err != nil {
					return err
				}
				if _, hidden := d.suppressed[edge.ID]; !hidden {
					visible = append(visible, edge)
				}
			}
			edges = visible
		}
		attached := edges[:0]
		for _, edge := range edges {
			if err := r.ctx.Err(); err != nil {
				return err
			}
			if attachedAt(edge, nodeID, outgoing) {
				attached = append(attached, edge)
			}
		}
		edges = attached
		for _, edge := range d.live {
			if err := r.ctx.Err(); err != nil {
				return err
			}
			edges = append(edges, copyEdge(edge))
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
func (b *BadgerEngine) cachedAdjacency(ctx context.Context, w *WriteBehindBuffer, id NodeID, wantOut, wantIn bool, v readVisibility) ([]*Edge, []*Edge, bool, error) {
	var outIDs, inIDs []EdgeID
	var outHit, inHit = !wantOut, !wantIn
	if wantOut {
		outIDs, outHit = b.adjCacheLoadOutgoing(id)
	}
	if wantIn {
		inIDs, inHit = b.adjCacheLoadIncoming(id)
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
			pending, err := b.readBufferedEntitiesWithContext(ctx, w, v, bufferedEntityRead{kind: bufferedLiveAdjacency, source: id, outgoing: outgoing})
			live = pending.edges
			if err != nil {
				return nil, false, err
			}
		}
		if len(ids) == 0 && len(live) == 0 {
			return nil, true, nil
		}
		result := make([]*Edge, 0, len(ids)+len(live))
		for _, edgeID := range ids {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if w != nil && w.entityStateLocked(bufferedEdgeEntity, string(edgeID), bufferedReadVisibility{engine: b, visibility: storedRead}).found {
				continue
			}
			edge := b.edgeCache[edgeID]
			if edge == nil {
				return nil, false, nil
			}
			if attachedAt(edge, id, outgoing) && !b.filterEntityByDecay(v, edge, now) {
				result = append(result, edge)
			}
		}
		for _, edge := range live {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			result = append(result, copyEdge(edge))
		}
		return result, true, nil
	}
	var out, in []*Edge
	var ok bool
	var err error
	if wantOut {
		out, ok, err = read(outIDs, true)
		if err != nil || !ok {
			return nil, nil, false, err
		}
	}
	if wantIn {
		in, ok, err = read(inIDs, false)
		if err != nil || !ok {
			return nil, nil, false, err
		}
	}
	return out, in, true, nil
}

// liveAdjacencyLocked enumerates the overlapping adjacency index, not the
// buffer's global tombstones. Cached committed IDs resolve tombstones directly.
func (w *WriteBehindBuffer) liveAdjacencyLockedWithContext(ctx context.Context, id NodeID, outgoing bool) ([]*Edge, error) {
	var result []*Edge
	var seen map[EdgeID]bool
	visit := func(buf *CommitBuffer) error {
		ids := buf.incomingByNode[id]
		if outgoing {
			ids = buf.outgoingByNode[id]
		}
		for _, edgeID := range ids {
			if err := ctx.Err(); err != nil {
				return err
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
		return nil
	}
	if err := visit(w.active); err != nil {
		return nil, err
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := visit(w.draining[i]); err != nil {
			return nil, err
		}
	}
	return result, ctx.Err()
}
