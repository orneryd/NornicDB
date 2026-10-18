package storage

import "github.com/dgraph-io/badger/v4"

// keyConstraintNodes captures the committed equality-index candidates and the
// pending key candidates together, then resolves bodies in the captured view.
// A replay cannot move a holder between those candidate sets during the capture.
func (b *BadgerEngine) keyConstraintNodes(idx *CompositeIndex, label, property string, values []interface{}) ([]*Node, error) {
	var ids []NodeID
	var pending []*Node
	var states map[NodeID]bufferedEntityState
	if w := b.writeBehind; w != nil { w.trackPendingValues([]pendingPropertyKey{{label:label, property:property}}) } else { ids = idx.LookupFull(values...) }
	var nodes []*Node
	err := b.withReadView(func(w *WriteBehindBuffer) {
		ids = idx.LookupFull(values...)
		if valueKey, ok := indexValueKey(values[0]); ok { pending = w.keyCandidatesLocked(label, property, valueKey) } else { pending, _ = w.labelOverlayLocked(label) }
		states = make(map[NodeID]bufferedEntityState, len(ids))
		for _, id := range ids { states[id] = w.entityStateLocked(bufferedNodeEntity, string(id)) }
	}, func(txn *badger.Txn) error {
		seen := make(map[NodeID]bool, len(ids)+len(pending))
		for _, id := range ids {
			state := states[id]
			if state.deleted { continue }
			node := state.node
			if !state.found { var err error; node, err = b.loadNodeForTemporalTxn(txn, id, false); if err != nil { return err } }
			if node != nil && !b.filterNodeByDecay(node, DecayScoringTime()) { nodes = append(nodes, copyNode(node)); seen[id] = true }
		}
		for _, node := range pending { if !seen[node.ID] && !b.filterNodeByDecay(node, DecayScoringTime()) { nodes = append(nodes, copyNode(node)) } }
		return nil
	})
	return nodes, err
}
