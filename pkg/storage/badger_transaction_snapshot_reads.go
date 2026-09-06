package storage

import "github.com/dgraph-io/badger/v4"

func (tx *BadgerTransaction) getAllCommittedNodesLocked() ([]*Node, error) {
	if tx.readTS.IsZero() {
		return tx.engine.AllNodes()
	}
	return tx.engine.getNodesByLabelVisibleAtWithView("", tx.readTS, tx.withSnapshotViewLocked)
}

// EdgeDirection selects which adjacency side an adjacency read resolves. It is
// a typed enum rather than a raw string so the hot bound-relationship-delete
// path compares an integer tag instead of magic strings.
type EdgeDirection uint8

const (
	// Outgoing selects edges whose start node is the queried node.
	Outgoing EdgeDirection = iota
	// Incoming selects edges whose end node is the queried node.
	Incoming
)

// getCommittedAdjacentEdgesLocked returns the committed edges adjacent to
// nodeID in the requested direction. Under an active snapshot (non-zero read
// timestamp) it resolves against the directional visible-at adjacency index so
// the cost is O(deg(nodeID)) rather than O(E): the previous implementation
// scanned every visible edge in the graph and filtered by endpoint in memory,
// which degraded linearly with the total edge count on large graphs. Pending
// transaction writes are merged by the caller.
func (tx *BadgerTransaction) getCommittedAdjacentEdgesLocked(nodeID NodeID, direction EdgeDirection) ([]*Edge, error) {
	if tx.readTS.IsZero() {
		switch direction {
		case Outgoing:
			return tx.engine.GetOutgoingEdges(nodeID)
		case Incoming:
			return tx.engine.GetIncomingEdges(nodeID)
		default:
			return nil, ErrInvalidData
		}
	}
	switch direction {
	case Outgoing:
		return tx.engine.getOutgoingEdgesVisibleAtWithView(nodeID, tx.readTS, tx.withSnapshotViewLocked)
	case Incoming:
		return tx.engine.getIncomingEdgesVisibleAtWithView(nodeID, tx.readTS, tx.withSnapshotViewLocked)
	default:
		return nil, ErrInvalidData
	}
}

// withSnapshotViewLocked keeps every snapshot read on the same physical Badger
// version. The MVCC namespace sequence alone can include an uncommitted peer's
// reservation. Fresh Views would admit that peer halfway through this reader.
// The separate read-only transaction does not enlarge the writer's SSI read set.
func (tx *BadgerTransaction) withSnapshotViewLocked(read func(*badger.Txn) error) error {
	if tx.snapshotTx != nil {
		return read(tx.snapshotTx)
	}
	// Legacy manually constructed transactions lack a lifetime-pinned reader.
	return tx.engine.withView(read)
}

// snapshotHeadConflict also compares physical publication versions: namespace
// MVCC reservations can predate this reader even when their data commits later.
// A separate read transaction preserves the existing consumer conflict shape
// without adding planning reads to the writer's Badger SSI set.
func (tx *BadgerTransaction) snapshotHeadConflict(key []byte, version MVCCVersion) (bool, error) {
	if tx.snapshotIsolationConflict(version) {
		return true, nil
	}
	if tx.snapshotTx == nil || key == nil {
		return false, nil
	}
	var changed bool
	err := tx.engine.withView(func(view *badger.Txn) error {
		item, err := view.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		changed = item.Version() > tx.snapshotTx.ReadTs()
		return nil
	})
	return changed, err
}
