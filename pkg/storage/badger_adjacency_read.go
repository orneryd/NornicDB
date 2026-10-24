package storage

import "github.com/dgraph-io/badger/v4"

// readAdjacency captures both directions, including cached ID lists, at one
// read point. Cache publication may overlap replay; the captured overlay still
// owns every generation absent from the transaction.
func (b *BadgerEngine) readAdjacency(nodeID NodeID, wantOut, wantIn bool, v readVisibility) ([]*Edge, []*Edge, error) {
	if nodeID == "" {
		return nil, nil, ErrInvalidID
	}
	type direction struct {
		ids        []EdgeID
		hit        bool
		epoch      uint64
		live       []*Edge
		suppressed map[EdgeID]struct{}
		deleted    bool
	}
	var out, in direction
	load := func(w *WriteBehindBuffer) {
		if wantOut {
			out.ids, out.epoch, out.hit = b.adjCacheCapture(nodeID, true)
			if w != nil {
				out.live, out.suppressed, out.deleted = w.edgeAdjacencyOverlayLocked(nodeID, true)
			}
		}
		if wantIn {
			in.ids, in.epoch, in.hit = b.adjCacheCapture(nodeID, false)
			if w != nil {
				in.live, in.suppressed, in.deleted = w.edgeAdjacencyOverlayLocked(nodeID, false)
			}
		}
	}
	adjGen, edgeGen := b.adjCacheGen.current(), b.edgeCacheGen.current()
	var outgoing, incoming []*Edge
	cached := func(w *WriteBehindBuffer) (bool, error) {
		var ok bool
		outgoing, incoming, ok = b.cachedAdjacency(w, nodeID, wantOut, wantIn, v)
		return ok, nil
	}
	err := b.withReadView(load, func(txn *badger.Txn) error {
		read := func(d *direction, outgoing bool) ([]*Edge, error) {
			if d.deleted {
				return nil, nil
			}
			var edges []*Edge
			if !d.hit || d.epoch > txn.ReadTs() {
				prefix := b.incomingIndexPrefixString(nodeID)
				if outgoing {
					prefix = b.outgoingIndexPrefixString(nodeID)
				}
				if prefix != nil {
					var err error
					edges, d.ids, err = b.collectEdgesByIndexPrefix(txn, prefix, DecayScoringTime(), edgeGen, v)
					if err != nil {
						return nil, err
					}
				}
				if outgoing {
					b.adjCacheStoreOutgoing(adjGen, nodeID, d.ids, txn.ReadTs())
				} else {
					b.adjCacheStoreIncoming(adjGen, nodeID, d.ids, txn.ReadTs())
				}
			}
			// Read cached IDs from this transaction, never a later body cache.
			if edges == nil {
				for _, id := range d.ids {
					if _, hidden := d.suppressed[id]; hidden {
						continue
					}
					edge, err := b.readIndexedEdgeInTxn(txn, id, edgeGen)
					if err != nil {
						return nil, err
					}
					if edge != nil && !b.hidesEdge(v, edge, DecayScoringTime()) {
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
			for _, edge := range d.live {
				if !b.hidesEdge(v, edge, DecayScoringTime()) {
					edges = append(edges, copyEdge(edge))
				}
			}
			return edges, nil
		}
		var err error
		if wantOut {
			outgoing, err = read(&out, true)
			if err != nil {
				return err
			}
		}
		if wantIn {
			incoming, err = read(&in, false)
		}
		return err
	}, readViewOptions{scope: namespaceForNodeID(nodeID), cached: cached})
	return outgoing, incoming, err
}

// cachedAdjacency resolves a traversal as one captured operation. The edge cache
// is locked once for both directions; only pending adjacency entries are copied.
func (b *BadgerEngine) cachedAdjacency(w *WriteBehindBuffer, id NodeID, wantOut, wantIn bool, v readVisibility) ([]*Edge, []*Edge, bool) {
	out, in, ok := b.captureCachedAdjacency(w, id, wantOut, wantIn)
	if !ok {
		return nil, nil, false
	}
	// Scoring may read durable access metadata. Resolve visibility only after
	// releasing both cache locks; a cache lock must never enclose storage I/O,
	// schema locks, or a wait that can depend on durable issuance.
	if v == visibleRead && b.visibilityFiltered() {
		now := DecayScoringTime()
		filter := func(edges []*Edge) []*Edge {
			visible := edges[:0]
			for _, edge := range edges {
				if !b.hidesEdge(v, edge, now) {
					visible = append(visible, edge)
				}
			}
			return visible
		}
		out, in = filter(out), filter(in)
	}
	return out, in, true
}

func (b *BadgerEngine) captureCachedAdjacency(w *WriteBehindBuffer, id NodeID, wantOut, wantIn bool) ([]*Edge, []*Edge, bool) {
	// Hold the endpoint versions through body resolution. The durable owner
	// locks in this same order, so a result covers one coherent set of keys.
	b.adjCacheMu.RLock()
	defer b.adjCacheMu.RUnlock()
	var outIDs, inIDs []EdgeID
	var outHit, inHit = !wantOut, !wantIn
	if wantOut {
		entry, ok := b.outgoingAdjCache[id]
		outIDs, outHit = entry.ids, ok && entry.ids != nil && entry.coherence.admits(entry.readTs, 0, b.adjCoherence)
	}
	if wantIn {
		entry, ok := b.incomingAdjCache[id]
		inIDs, inHit = entry.ids, ok && entry.ids != nil && entry.coherence.admits(entry.readTs, 0, b.adjCoherence)
	}
	if !outHit || !inHit {
		return nil, nil, false
	}
	if w != nil && w.entityStateLocked(bufferedNodeEntity, string(id)).deleted {
		return nil, nil, true
	}
	b.edgeCacheMu.RLock()
	defer b.edgeCacheMu.RUnlock()
	read := func(ids []EdgeID, outgoing bool) ([]*Edge, bool) {
		var live []*Edge
		if w != nil {
			live = w.liveAdjacencyLocked(id, outgoing)
		}
		if len(ids) == 0 && len(live) == 0 {
			return nil, true
		}
		result := make([]*Edge, 0, len(ids)+len(live))
		for _, edgeID := range ids {
			if w != nil && w.entityStateLocked(bufferedEdgeEntity, string(edgeID)).found {
				continue
			}
			entry := b.edgeCache[edgeID]
			edge := entry.Edge
			if edge == nil || !entry.coherence.admits(entry.readTs, 0, b.edgeCoherence) {
				return nil, false
			}
			result = append(result, edge)
		}
		for _, edge := range live {
			result = append(result, copyEdge(edge))
		}
		return result, true
	}
	var out, in []*Edge
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

func (b *BadgerEngine) adjCacheCapture(id NodeID, outgoing bool) ([]EdgeID, uint64, bool) {
	b.adjCacheMu.RLock()
	defer b.adjCacheMu.RUnlock()
	entry, ok := b.incomingAdjCache[id]
	if outgoing {
		entry, ok = b.outgoingAdjCache[id]
	}
	return entry.ids, entry.readTs, ok && entry.ids != nil && entry.coherence.admits(entry.readTs, 0, b.adjCoherence)
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
