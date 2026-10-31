package storage

// nativeAdjacencyReader is one owned physical read cut. Returning the frame
// by value keeps directional state out of escaping higher-order callbacks.
type nativeAdjacencyReader struct {
	engine              *BadgerEngine
	nodeID              NodeID
	visibility          readVisibility
	adjGen, edgeGen     uint64
	out, in             adjacencyDirection
	cachedOut, cachedIn []edgeImage
	view                *badgerReadView
}
type adjacencyDirection struct {
	ids        []EdgeID
	hit        bool
	live       []edgeImage
	suppressed map[EdgeID]struct{}
	deleted    bool
}

// captureAdjacencyReader selects both native directions at one cache/overlay
// boundary. A nil view means the captured cache result is already complete.
func (b *BadgerEngine) captureAdjacencyReader(nodeID NodeID, wantOut, wantIn bool, v readVisibility) (reader nativeAdjacencyReader, err error) {
	if nodeID == "" {
		return reader, ErrInvalidID
	}
	reader.engine, reader.nodeID, reader.visibility = b, nodeID, v
	reader.adjGen, reader.edgeGen = b.adjCacheGen.current(), b.edgeCacheGen.current()
	reader.view, err = b.captureReadView(func(w *WriteBehindBuffer) {
		if wantOut {
			reader.out.ids, reader.out.hit = b.adjCacheLoad(nodeID, true)
			if w != nil {
				pendingAdjacency := b.readBufferedEntities(w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: true})
				reader.out.live, reader.out.suppressed, reader.out.deleted = pendingAdjacency.edges, pendingAdjacency.suppressed, pendingAdjacency.sourceDeleted
			}
		}
		if wantIn {
			reader.in.ids, reader.in.hit = b.adjCacheLoad(nodeID, false)
			if w != nil {
				pendingAdjacency := b.readBufferedEntities(w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: false})
				reader.in.live, reader.in.suppressed, reader.in.deleted = pendingAdjacency.edges, pendingAdjacency.suppressed, pendingAdjacency.sourceDeleted
			}
		}
	}, readViewOptions{scope: namespaceForNodeID(nodeID), cached: func(w *WriteBehindBuffer) (bool, error) {
		var complete bool
		reader.cachedOut, reader.cachedIn, complete = b.cachedAdjacency(w, nodeID, wantOut, wantIn, v)
		return complete, nil
	}})
	return reader, err
}

// deliver hands out one captured direction: the read selects images, then the
// delivery owner copies exactly the retained ones it returns (NOR-160).
func (r *nativeAdjacencyReader) deliver(outgoing bool) ([]*Edge, error) {
	images, err := r.read(outgoing)
	return deliverEdges(images), err
}

// read hydrates one captured direction as images; it never selects a later
// view or cache, and never copies.
func (r *nativeAdjacencyReader) read(outgoing bool) (edges []edgeImage, err error) {
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
				edges, d.ids, err = b.collectEdgesByIndexPrefix(txn, prefix, DecayScoringTime(), edgeGen, v)
				if err != nil {
					return err
				}
			}
			b.adjCacheStore(adjGen, nodeID, d.ids, txn.ReadTs(), outgoing)
		}
		// Read cached IDs from this transaction, never a later body cache.
		if edges == nil {
			for _, id := range d.ids {
				if _, hidden := d.suppressed[id]; hidden {
					continue
				}
				edge, err := b.readIndexedEdgeInTxn(txn, id, edgeGen)
				if err != nil {
					return err
				}
				if edge.Edge != nil && !b.filterEntityByDecay(v, edge.Edge, DecayScoringTime()) {
					edges = append(edges, edge)
				}
			}
		} else if len(d.suppressed) != 0 {
			visible := edges[:0]
			for _, edge := range edges {
				if _, hidden := d.suppressed[edge.ID]; !hidden {
					visible = append(visible, edge)
				}
			}
			edges = visible
		}
		attached := edges[:0]
		for _, edge := range edges {
			if attachedAt(edge.Edge, nodeID, outgoing) {
				attached = append(attached, edge)
			}
		}
		edges = attached
		edges = append(edges, d.live...)
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

// cachedAdjacency resolves a traversal as one captured operation over borrowed
// images: cached bodies through cacheLoadEdge, pending ones through
// readBufferedEntities. Nothing is copied here, so abandoning an incomplete
// attempt for the physical read drops no copy; the reader delivers once.
func (b *BadgerEngine) cachedAdjacency(w *WriteBehindBuffer, id NodeID, wantOut, wantIn bool, v readVisibility) ([]edgeImage, []edgeImage, bool) {
	var outIDs, inIDs []EdgeID
	var outHit, inHit = !wantOut, !wantIn
	if wantOut {
		outIDs, outHit = b.adjCacheLoad(id, true)
	}
	if wantIn {
		inIDs, inHit = b.adjCacheLoad(id, false)
	}
	if !outHit || !inHit {
		return nil, nil, false
	}
	if w != nil && w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead}).deleted {
		return nil, nil, true
	}
	now := DecayScoringTime()
	read := func(ids []EdgeID, outgoing bool) ([]edgeImage, bool) {
		var live []edgeImage
		if w != nil {
			live = b.readBufferedEntities(w, v, bufferedEntityRead{kind: bufferedLiveAdjacency, source: id, outgoing: outgoing}).edges
		}
		if len(ids) == 0 && len(live) == 0 {
			return nil, true
		}
		result := make([]edgeImage, 0, len(ids)+len(live))
		for _, edgeID := range ids {
			if w != nil && w.entityStateLocked(bufferedEdgeEntity, string(edgeID), bufferedReadVisibility{engine: b, visibility: storedRead}).found {
				continue
			}
			edge, hit := b.cacheLoadEdge(edgeID, edgeCacheRead{bufferExcluded: true, match: func(edge *Edge) bool {
				return attachedAt(edge, id, outgoing) && !b.filterEntityByDecay(v, edge, now)
			}})
			if !hit {
				return nil, false
			}
			if edge.Edge != nil {
				result = append(result, edge)
			}
		}
		result = append(result, live...)
		return result, true
	}
	var out, in []edgeImage
	var ok bool
	if wantOut {
		out, ok = read(outIDs, true)
		if !ok {
			return nil, nil, false
		}
	}
	if wantIn {
		in, ok = read(inIDs, false)
		if !ok {
			return nil, nil, false
		}
	}
	return out, in, true
}

// liveAdjacencyLocked enumerates the overlapping adjacency index, not the
// buffer's global tombstones. Cached committed IDs resolve tombstones directly.
func (w *WriteBehindBuffer) liveAdjacencyLocked(id NodeID, outgoing bool) []*Edge {
	var result []*Edge
	var seen map[EdgeID]bool
	visit := func(buf *CommitBuffer) {
		ids := buf.incomingByNode[id]
		if outgoing {
			ids = buf.outgoingByNode[id]
		}
		for _, edgeID := range ids {
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
	for i := len(w.draining) - 1; i >= 0; i-- {
		visit(w.draining[i])
	}
	return result
}
