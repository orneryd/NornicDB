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
			reader.out.ids, reader.out.hit = b.adjCacheLoadOutgoing(nodeID)
			if w != nil {
				pendingAdjacency := b.readBufferedEntities(w, v, bufferedEntityRead{kind: bufferedAdjacency, source: nodeID, outgoing: true})
				reader.out.live, reader.out.suppressed, reader.out.deleted = pendingAdjacency.edges, pendingAdjacency.suppressed, pendingAdjacency.sourceDeleted
			}
		}
		if wantIn {
			reader.in.ids, reader.in.hit = b.adjCacheLoadIncoming(nodeID)
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
	// The whole selection (buffered replacements, direction, visibility) is
	// decided before a cached body is copied (NOR-160).
	now := DecayScoringTime()
	keep := func(edge *Edge) bool {
		if _, hidden := d.suppressed[edge.ID]; hidden {
			return false
		}
		return attachedAt(edge, nodeID, outgoing) && !b.filterEntityByDecay(v, edge, now)
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
				edges, d.ids, err = b.collectEdgesByIndexPrefix(txn, prefix, now, edgeGen, v, keep)
				if err != nil {
					return err
				}
			}
			if outgoing {
				b.adjCacheStoreOutgoing(adjGen, nodeID, d.ids)
			} else {
				b.adjCacheStoreIncoming(adjGen, nodeID, d.ids)
			}
		} else {
			// Read cached IDs from this transaction, never a later body cache.
			for _, id := range d.ids {
				if _, hidden := d.suppressed[id]; hidden {
					continue
				}
				edge, err := b.readIndexedEdgeInTxn(txn, id, edgeGen, keep)
				if err != nil {
					return err
				}
				if edge.Edge != nil {
					edges = append(edges, edge)
				}
			}
		}
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
		outIDs, outHit = b.adjCacheLoadOutgoing(id)
	}
	if wantIn {
		inIDs, inHit = b.adjCacheLoadIncoming(id)
	}
	if !outHit || !inHit {
		return nil, nil, false
	}
	if w != nil && w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead}).deleted {
		return nil, nil, true
	}
	now := DecayScoringTime()
	// Every cached body of every requested direction is checked before any is
	// copied: a miss anywhere falls back to the physical read having copied
	// nothing (NOR-160).
	buffered := func(edgeID EdgeID) bool {
		return w != nil && w.entityStateLocked(bufferedEdgeEntity, string(edgeID), bufferedReadVisibility{engine: b, visibility: storedRead}).found
	}
	list := func(ids []EdgeID, outgoing bool) edgeCacheList {
		return edgeCacheList{ids: ids, skip: buffered, read: edgeCacheRead{bufferExcluded: true, match: func(edge *Edge) bool {
			return attachedAt(edge, id, outgoing) && !b.filterEntityByDecay(v, edge, now)
		}}}
	}
	var lists []edgeCacheList
	if wantOut {
		lists = append(lists, list(outIDs, true))
	}
	if wantIn {
		lists = append(lists, list(inIDs, false))
	}
	copied, ok := b.cacheLoadEdgeLists(lists)
	if !ok {
		return nil, nil, false
	}
	withLive := func(edges []*Edge, outgoing bool) []edgeImage {
		var live []edgeImage
		if w != nil {
			live = b.readBufferedEntities(w, v, bufferedEntityRead{kind: bufferedLiveAdjacency, source: id, outgoing: outgoing}).edges
		}
		if len(edges) == 0 && len(live) == 0 {
			return nil
		}
		return append(decodedEdges(edges), live...)
	}
	var out, in []edgeImage
	next := 0
	if wantOut {
		out = withLive(copied[next], true)
		next++
	}
	if wantIn {
		in = withLive(copied[next], false)
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
