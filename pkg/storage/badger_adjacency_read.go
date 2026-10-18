package storage

import "github.com/dgraph-io/badger/v4"

// readAdjacency captures both directions, including cached ID lists, at one
// read point. Cache publication may overlap replay; the captured overlay still
// owns every generation absent from the transaction.
func (b *BadgerEngine) readAdjacency(nodeID NodeID, wantOut, wantIn bool) ([]*Edge, []*Edge, error) {
	if nodeID == "" {
		return nil, nil, ErrInvalidID
	}
	type direction struct {
		ids        []EdgeID
		hit        bool
		live       []*Edge
		suppressed map[EdgeID]struct{}
		deleted    bool
	}
	var out, in direction
	load := func(w *WriteBehindBuffer) {
		if wantOut {
			out.ids, out.hit = b.adjCacheLoadOutgoing(nodeID)
			if w != nil {
				out.live, out.suppressed, out.deleted = w.edgeAdjacencyOverlayLocked(nodeID, true)
			}
		}
		if wantIn {
			in.ids, in.hit = b.adjCacheLoadIncoming(nodeID)
			if w != nil {
				in.live, in.suppressed, in.deleted = w.edgeAdjacencyOverlayLocked(nodeID, false)
			}
		}
	}
	if b.writeBehind == nil {
		load(nil)
		if (!wantOut || out.hit) && (!wantIn || in.hit) {
			o, err := b.materializeAdjEdges(out.ids)
			if err != nil {
				return nil, nil, err
			}
			i, err := b.materializeAdjEdges(in.ids)
			return o, i, err
		}
	}
	adjGen, edgeGen := b.adjCacheGen.current(), b.edgeCacheGen.current()
	var outgoing, incoming []*Edge
	err := b.withReadView(load, func(txn *badger.Txn) error {
		read := func(d *direction, outgoing bool) ([]*Edge, error) {
			if d.deleted {
				return nil, nil
			}
			var edges []*Edge
			if !d.hit {
				prefix := b.incomingIndexPrefixString(nodeID)
				if outgoing {
					prefix = b.outgoingIndexPrefixString(nodeID)
				}
				if prefix != nil {
					var err error
					edges, d.ids, err = b.collectEdgesByIndexPrefix(txn, prefix, DecayScoringTime(), edgeGen)
					if err != nil {
						return nil, err
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
					if _, hidden := d.suppressed[id]; hidden {
						continue
					}
					edge, err := b.readIndexedEdgeInTxn(txn, id, edgeGen)
					if err != nil {
						return nil, err
					}
					if edge != nil && !b.filterEdgeByDecay(edge, DecayScoringTime()) {
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
				if !b.filterEdgeByDecay(edge, DecayScoringTime()) {
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
	})
	return outgoing, incoming, err
}
