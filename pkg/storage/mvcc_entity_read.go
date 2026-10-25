package storage

import "github.com/dgraph-io/badger/v4"

// mvccEntityRead selects hydration and graph visibility at one logical version.
// Edge bodies have no embedding hydration; vectors is used only by node loads.
type mvccEntityRead struct {
	// nodeNum pins an index candidate to its physical snapshot identity.
	nodeNum    uint64
	projected  bool
	properties []string
	vectors    bool
	visibility readVisibility
}

// readEntityAtWithView is the single point-read admission/reader-registration
// owner. A typed method expression selects the entity decoder; all loads keep
// the caller's physical snapshot and return its exact value/error pair.
func readEntityAtWithView[I ~string, T Node | Edge, P any](b *BadgerEngine, id I, version MVCCVersion, policy P, view func(func(*badger.Txn) error) error, load func(*BadgerEngine, *badger.Txn, I, MVCCVersion, P) (*T, error)) (*T, error) {
	namespace, _, _ := ParseDatabasePrefix(string(id))
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return nil, err
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var entity *T
	err = view(func(txn *badger.Txn) error {
		var loadErr error
		entity, loadErr = load(b, txn, id, version, policy)
		return loadErr
	})
	return entity, err
}

// readAdjacentEdgesAtWithView owns the two directional MVCC traversals. Pinned
// transactions already use their registered cached reader before reaching this
// fallback, while public/fallback requests register for this complete view.
func (b *BadgerEngine) readAdjacentEdgesAtWithView(nodeID NodeID, version MVCCVersion, direction EdgeDirection, view func(func(*badger.Txn) error) error) ([]*Edge, error) {
	if err := b.pendingNamespaceConflict(namespaceForNodeID(nodeID)); err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	var prefix []byte
	switch direction {
	case Outgoing:
		prefix = b.mvccOutgoingAdjacencyPrefixString(nodeID)
	case Incoming:
		prefix = b.mvccIncomingAdjacencyPrefixString(nodeID)
	default:
		return nil, ErrInvalidData
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = view(func(txn *badger.Txn) error {
		ids, err := b.collectVisibleAdjacencyEdgeIDsInTxn(txn, prefix, version)
		if err != nil {
			return err
		}
		for _, id := range ids {
			edge, e := b.getEdgeAtInTxn(txn, id, version, mvccEntityRead{visibility: visibleRead})
			if e == ErrNotFound || e == ErrNotVisibleAtSnapshot {
				continue
			}
			if e != nil {
				return e
			}
			if edge != nil && ((direction == Outgoing && edge.StartNode == nodeID) || (direction == Incoming && edge.EndNode == nodeID)) {
				edges = append(edges, edge)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}
