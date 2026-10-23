package storage

import "github.com/dgraph-io/badger/v4"

// readCurrentAdjacency validates both cached directions against one durable
// boundary. If that boundary moves, use a single authoritative index view;
// resolving old IDs against newer bodies could otherwise miss a new edge.
func (b *BadgerEngine) readCurrentAdjacency(id NodeID, wantOut, wantIn bool) ([]*Edge, []*Edge, error) {
	if id == "" {
		return nil, nil, ErrInvalidID
	}
	epoch := b.nodeCacheCommitTs.Load()
	var outIDs, inIDs []EdgeID
	outHit, inHit := !wantOut, !wantIn
	if wantOut {
		outIDs, outHit = b.adjCacheLoadOutgoing(id)
	}
	if wantIn {
		inIDs, inHit = b.adjCacheLoadIncoming(id)
	}
	var outgoing, incoming []*Edge
	var err error
	if outHit && inHit {
		if wantOut {
			outgoing, err = b.mergeWriteBehindAdjacency(id, true, nil, outIDs)
		}
		if err == nil && wantIn {
			incoming, err = b.mergeWriteBehindAdjacency(id, false, nil, inIDs)
		}
		if err != nil || epoch == b.nodeCacheCommitTs.Load() {
			return outgoing, incoming, err
		}
	}
	adjGen, edgeGen := b.adjCacheGen.current(), b.edgeCacheGen.current()
	err = b.withView(func(txn *badger.Txn) error {
		read := func(out bool) ([]*Edge, []EdgeID, error) {
			prefix := b.incomingIndexPrefixString(id)
			if out {
				prefix = b.outgoingIndexPrefixString(id)
			}
			if prefix == nil {
				return nil, nil, nil
			}
			return b.collectEdgesByIndexPrefix(txn, prefix, DecayScoringTime(), edgeGen)
		}
		if wantOut {
			outgoing, outIDs, err = read(true)
			if err != nil {
				return err
			}
			b.adjCacheStoreOutgoing(adjGen, id, outIDs, txn.ReadTs())
		}
		if wantIn {
			incoming, inIDs, err = read(false)
			if err != nil {
				return err
			}
			b.adjCacheStoreIncoming(adjGen, id, inIDs, txn.ReadTs())
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if wantOut {
		outgoing, err = b.mergeWriteBehindAdjacency(id, true, outgoing, outIDs)
	}
	if err == nil && wantIn {
		incoming, err = b.mergeWriteBehindAdjacency(id, false, incoming, inIDs)
	}
	return outgoing, incoming, err
}
