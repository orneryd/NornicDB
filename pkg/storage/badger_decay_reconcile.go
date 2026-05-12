package storage

import badger "github.com/dgraph-io/badger/v4"

type SuppressionStateChange struct {
	EntityID string
	Tokens   []string
	IsEdge   bool
}

// ReconcileDecaySuppression re-evaluates current suppression state for all
// entities in a namespace after knowledge-policy changes.
func (b *BadgerEngine) ReconcileDecaySuppression(namespace string) error {
	if !b.decayEnabled || namespace == "" {
		return nil
	}

	nodeIDs, err := b.collectNamespaceNodeIDs(namespace)
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		if _, err := b.EnqueueDeindexIfSuppressed(string(nodeID), false); err != nil {
			return err
		}
	}

	edgeIDs, err := b.collectNamespaceEdgeIDs(namespace)
	if err != nil {
		return err
	}
	for _, edgeID := range edgeIDs {
		if _, err := b.EnqueueDeindexIfSuppressed(string(edgeID), true); err != nil {
			return err
		}
	}

	return nil
}

// ReconcileDecaySuppressionWithChanges re-evaluates suppression state and returns
// the entities whose visibility-suppressed state changed.
func (b *BadgerEngine) ReconcileDecaySuppressionWithChanges(namespace string) ([]SuppressionStateChange, error) {
	if !b.decayEnabled || namespace == "" {
		return nil, nil
	}

	changes := make([]SuppressionStateChange, 0)
	nodeIDs, err := b.collectNamespaceNodeIDs(namespace)
	if err != nil {
		return nil, err
	}
	for _, nodeID := range nodeIDs {
		before, labels, err := b.readNodeSuppressionState(nodeID)
		if err != nil {
			return nil, err
		}
		if _, err := b.EnqueueDeindexIfSuppressed(string(nodeID), false); err != nil {
			return nil, err
		}
		after, _, err := b.readNodeSuppressionState(nodeID)
		if err != nil {
			return nil, err
		}
		if before != after {
			changes = append(changes, SuppressionStateChange{EntityID: string(nodeID), Tokens: append([]string(nil), labels...)})
		}
	}

	edgeIDs, err := b.collectNamespaceEdgeIDs(namespace)
	if err != nil {
		return nil, err
	}
	for _, edgeID := range edgeIDs {
		before, edgeType, err := b.readEdgeSuppressionState(edgeID)
		if err != nil {
			return nil, err
		}
		if _, err := b.EnqueueDeindexIfSuppressed(string(edgeID), true); err != nil {
			return nil, err
		}
		after, _, err := b.readEdgeSuppressionState(edgeID)
		if err != nil {
			return nil, err
		}
		if before != after {
			changes = append(changes, SuppressionStateChange{EntityID: string(edgeID), Tokens: []string{edgeType}, IsEdge: true})
		}
	}

	return changes, nil
}

func (b *BadgerEngine) collectNamespaceNodeIDs(namespace string) ([]NodeID, error) {
	prefix := append([]byte{prefixNode}, []byte(namespace+":")...)
	ids := make([]NodeID, 0)
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			ids = append(ids, NodeID(it.Item().Key()[1:]))
		}
		return nil
	})
	return ids, err
}

func (b *BadgerEngine) collectNamespaceEdgeIDs(namespace string) ([]EdgeID, error) {
	prefix := append([]byte{prefixEdge}, []byte(namespace+":")...)
	ids := make([]EdgeID, 0)
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			ids = append(ids, EdgeID(it.Item().Key()[1:]))
		}
		return nil
	})
	return ids, err
}

func (b *BadgerEngine) readNodeSuppressionState(nodeID NodeID) (bool, []string, error) {
	var suppressed bool
	var labels []string
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			node, err := decodeNodeWithEmbeddings(txn, val, nodeID)
			if err != nil {
				return err
			}
			suppressed = node.VisibilitySuppressed
			labels = append([]string(nil), node.Labels...)
			return nil
		})
	})
	return suppressed, labels, err
}

func (b *BadgerEngine) readEdgeSuppressionState(edgeID EdgeID) (bool, string, error) {
	var suppressed bool
	var edgeType string
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			edge, err := b.decodeEdgeBodyWithID(val, edgeID)
			if err != nil {
				return err
			}
			suppressed = edge.VisibilitySuppressed
			edgeType = edge.Type
			return nil
		})
	})
	return suppressed, edgeType, err
}
