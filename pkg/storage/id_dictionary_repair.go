package storage

import (
	"context"
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
)

// IDDictionaryReport lists the stored nodes and relationships whose ID has no
// ID-dictionary entry. Their records are found by ID, but every index keyed
// by their numeric ID (adjacency, relationship type, label, MVCC heads)
// points at a number nothing resolves: a relationship isn't traversed and a
// node isn't found by label. A rolled-back transaction could leave them so
// before #907's I29 fix.
type IDDictionaryReport struct {
	Nodes []NodeID
	Edges []EdgeID
	// The stored entities whose dictionary entry holds a number none of their
	// index keys use: a restart that rebuilt the MVCC heads gave such a lost
	// entity a new number, while its label or adjacency keys keep the old one.
	MismatchedNodes []NodeID
	MismatchedEdges []EdgeID
	// After RepairIDDictionary: the entities whose original numeric ID was
	// restored, and those it could not identify unambiguously.
	RestoredNodes   []NodeID
	RestoredEdges   []EdgeID
	UnresolvedNodes []NodeID
	UnresolvedEdges []EdgeID
}

// CheckIDDictionary reports the stored entities with no ID-dictionary entry,
// and those whose entry holds a number none of their index keys use: a
// labelled node in no label index key, a relationship in no outgoing key.
// Read-only.
func (b *BadgerEngine) CheckIDDictionary() (*IDDictionaryReport, error) {
	report := &IDDictionaryReport{}
	var unlabelled []NodeID
	err := b.db.View(func(txn *badger.Txn) error {
		labelled := numIDsInKeys(txn, prefixLabelIndex)
		linked := numIDsInKeys(txn, prefixOutgoingIndex)
		for _, prefix := range []byte{prefixNode, prefixEdge} {
			opts := badgerPrefixIteratorOptions([]byte{prefix})
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				id := string(it.Item().Key()[1:])
				if prefix == prefixNode {
					if num, ok := b.idDict.lookupNodeNumID(NodeID(id)); !ok {
						report.Nodes = append(report.Nodes, NodeID(id))
					} else if !labelled[num] {
						unlabelled = append(unlabelled, NodeID(id))
					}
				} else if num, ok := b.idDict.lookupEdgeNumID(EdgeID(id)); !ok {
					report.Edges = append(report.Edges, EdgeID(id))
				} else if !linked[num] {
					report.MismatchedEdges = append(report.MismatchedEdges, EdgeID(id))
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	// A node in no label index key is mismatched only if it has labels.
	for _, id := range unlabelled {
		if node, err := b.GetStoredNode(id); err == nil && len(node.Labels) > 0 {
			report.MismatchedNodes = append(report.MismatchedNodes, id)
		}
	}
	return report, nil
}

// numIDsInKeys collects the numeric IDs ending the keys under prefix: node
// IDs for the label index, relationship IDs for the adjacency indexes.
func numIDsInKeys(txn *badger.Txn, prefix byte) map[uint64]bool {
	found := make(map[uint64]bool)
	opts := badgerPrefixIteratorOptions([]byte{prefix})
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if key := it.Item().Key(); len(key) >= 1+8 {
			found[binary.BigEndian.Uint64(key[len(key)-8:])] = true
		}
	}
	return found
}

// RepairIDDictionary restores the original numeric ID of each entity
// CheckIDDictionary reports, when the index keys left behind identify it
// unambiguously, so the existing index keys are valid again:
//   - a node: the one unresolved numeric ID in the label index of every one
//     of its labels;
//   - a relationship: the one unresolved numeric ID in its start node's
//     outgoing keys, its end node's incoming keys and its type's index.
//
// A mismatched entity's entry is pointed back at the original number the
// same way, and its MVCC head moved with it.
//
// Nodes are repaired first, so relationships between repaired nodes resolve.
// An entity that isn't identified unambiguously (an unlabelled node, two
// lost relationships alike) is listed as unresolved and left unchanged.
func (b *BadgerEngine) RepairIDDictionary() (*IDDictionaryReport, error) {
	release, err := b.beginNamespaceMutation(context.Background(), nil, nil, false, namespacePublicationFresh)
	if err != nil {
		return nil, err
	}
	defer release()
	report, err := b.CheckIDDictionary()
	if err != nil {
		return nil, err
	}
	claimed := make(map[uint64]bool)
	for _, lost := range [][]NodeID{report.Nodes, report.MismatchedNodes} {
		for _, id := range lost {
			num, ok, err := b.lostNodeNumID(id, claimed)
			if err != nil {
				return report, err
			}
			if !ok {
				report.UnresolvedNodes = append(report.UnresolvedNodes, id)
				continue
			}
			if err := b.restoreIDEntry(id, "", num); err != nil {
				return report, err
			}
			claimed[num] = true
			report.RestoredNodes = append(report.RestoredNodes, id)
		}
	}
	claimed = make(map[uint64]bool)
	for _, lost := range [][]EdgeID{report.Edges, report.MismatchedEdges} {
		for _, id := range lost {
			num, ok, err := b.lostEdgeNumID(id, claimed)
			if err != nil {
				return report, err
			}
			if !ok {
				report.UnresolvedEdges = append(report.UnresolvedEdges, id)
				continue
			}
			if err := b.restoreIDEntry("", id, num); err != nil {
				return report, err
			}
			claimed[num] = true
			report.RestoredEdges = append(report.RestoredEdges, id)
		}
	}
	if len(report.RestoredNodes) > 0 || len(report.RestoredEdges) > 0 {
		// Index keys that resolved to nothing now resolve: drop what the caches
		// hold, as after a restore, and recount what the counts left out (a
		// relationship whose endpoint didn't resolve isn't counted).
		b.invalidateCachesAfterRestore()
		b.labelCountWriteMu.Lock()
		err := b.rebuildAuthoritativeLabelCountsLocked()
		b.labelCountWriteMu.Unlock()
		if err != nil {
			return report, err
		}
		b.edgeTypeCountWriteMu.Lock()
		err = b.rebuildAuthoritativeEdgeTypeCountsLocked()
		b.edgeTypeCountWriteMu.Unlock()
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

// lostNodeNumID finds a lost node's numeric ID: the one unresolved, unclaimed
// ID in the label index of every one of its labels.
func (b *BadgerEngine) lostNodeNumID(id NodeID, claimed map[uint64]bool) (uint64, bool, error) {
	node, err := b.GetStoredNode(id)
	if err != nil || len(node.Labels) == 0 {
		return 0, false, nil
	}
	var candidates map[uint64]bool
	for _, label := range node.Labels {
		prefix := labelIndexPrefix(label)
		found, err := b.unresolvedNumIDs(prefix, func(key []byte) (uint64, bool) {
			return extractNodeNumIDFromLabelIndex(key, len(label))
		}, func(num uint64) bool {
			_, known := b.idDict.lookupNodeIDByNum(num)
			return known || claimed[num]
		})
		if err != nil {
			return 0, false, err
		}
		candidates = intersectNumIDs(candidates, found)
	}
	return soleNumID(candidates)
}

// lostEdgeNumID finds a lost relationship's numeric ID: the one unresolved,
// unclaimed ID in its start node's outgoing keys, its end node's incoming
// keys and its type's index.
func (b *BadgerEngine) lostEdgeNumID(id EdgeID, claimed map[uint64]bool) (uint64, bool, error) {
	edge, err := b.GetStoredEdge(id)
	if err != nil {
		return 0, false, nil
	}
	startNum, startOK := b.idDict.lookupNodeNumID(edge.StartNode)
	endNum, endOK := b.idDict.lookupNodeNumID(edge.EndNode)
	if !startOK || !endOK {
		return 0, false, nil
	}
	unknown := func(num uint64) bool {
		_, known := b.idDict.lookupEdgeIDByNum(num)
		return known || claimed[num]
	}
	var candidates map[uint64]bool
	for _, source := range []struct {
		prefix  []byte
		extract func([]byte) (uint64, bool)
	}{
		{outgoingIndexPrefix(startNum), extractEdgeNumIDFromOutgoingKey},
		{incomingIndexPrefix(endNum), extractEdgeNumIDFromOutgoingKey},
		{edgeTypeIndexPrefix(edge.Type), extractEdgeNumIDFromEdgeTypeKey},
	} {
		found, err := b.unresolvedNumIDs(source.prefix, source.extract, unknown)
		if err != nil {
			return 0, false, err
		}
		candidates = intersectNumIDs(candidates, found)
	}
	return soleNumID(candidates)
}

// unresolvedNumIDs collects the numeric IDs under prefix that skip doesn't
// rule out (resolved or already claimed).
func (b *BadgerEngine) unresolvedNumIDs(prefix []byte, extract func([]byte) (uint64, bool), skip func(uint64) bool) (map[uint64]bool, error) {
	found := make(map[uint64]bool)
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badgerPrefixIteratorOptions(prefix)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if num, ok := extract(it.Item().Key()); ok && !skip(num) {
				found[num] = true
			}
		}
		return nil
	})
	return found, err
}

func intersectNumIDs(current, next map[uint64]bool) map[uint64]bool {
	if current == nil {
		return next
	}
	for num := range current {
		if !next[num] {
			delete(current, num)
		}
	}
	return current
}

func soleNumID(candidates map[uint64]bool) (uint64, bool, error) {
	var sole uint64
	for num := range candidates {
		sole = num
	}
	return sole, len(candidates) == 1, nil
}

// restoreIDEntry writes the dictionary entry of a node (or, when edge is set,
// a relationship) for num, and puts it in memory as committed. An entry the
// entity already has, with another number, is replaced: that number's
// reverse entry is deleted and its MVCC head moved to num.
func (b *BadgerEngine) restoreIDEntry(node NodeID, edge EdgeID, num uint64) error {
	d := b.idDict
	var previous uint64
	var hadPrevious bool
	reversePrefix, headKey := prefixIDDictNodeReverse, mvccNodeHeadKey
	if edge != "" {
		previous, hadPrevious = d.lookupEdgeNumID(edge)
		reversePrefix, headKey = prefixIDDictEdgeReverse, mvccEdgeHeadKey
	} else {
		previous, hadPrevious = d.lookupNodeNumID(node)
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		if hadPrevious && previous != num {
			if err := txn.Delete(append([]byte{reversePrefix}, encodeNumID(previous)...)); err != nil {
				return err
			}
			item, err := txn.Get(headKey(previous))
			if err == nil {
				head, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				if err := txn.Set(headKey(num), head); err != nil {
					return err
				}
				if err := txn.Delete(headKey(previous)); err != nil {
					return err
				}
			} else if err != badger.ErrKeyNotFound {
				return err
			}
		}
		if edge != "" {
			return setEdgeEntryInTxn(txn, edge, num)
		}
		return setNodeEntryInTxn(txn, node, num)
	}); err != nil {
		return err
	}
	// The entry's previous pair (its reverse only when it still names this
	// ID) goes, and the repaired pair takes its place.
	if edge != "" {
		d.edgePairRemoveByID(edge)
		d.edgePairStore(edge, num, nil)
	} else {
		d.nodePairRemoveByID(node)
		d.nodePairStore(node, num, nil)
	}
	return nil
}
