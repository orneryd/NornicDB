package storage

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

// IsCurrentTemporalNodeInNamespace evaluates temporal acceptance in this
// transaction's read view, including its pending replacements and deletes.
// It never substitutes the engine's latest committed view for the pinned reader.
func (tx *BadgerTransaction) IsCurrentTemporalNodeInNamespace(namespace string, node *Node, asOf time.Time) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return false, err
	}
	if node == nil {
		return false, nil
	}
	if err := tx.pinNamespaceFromIDLocked(string(qualifyTemporalNodeID(namespace, node.ID))); err != nil {
		return false, err
	}
	schema := tx.schemaRuntime
	if schema == nil {
		schema = tx.engine.GetSchemaForNamespace(namespace)
	}
	return temporalNodeIsCurrent(namespace, node, schema, asOf, func(constraint Constraint, keyValue interface{}) (*Node, error) {
		return tx.currentTemporalNodeForConstraintLocked(namespace, schema, constraint, keyValue, asOf)
	})
}

func (tx *BadgerTransaction) currentTemporalNodeForConstraintLocked(namespace string, schema *SchemaManager, constraint Constraint, keyValue interface{}, asOf time.Time) (*Node, error) {
	choice := temporalNodeChoice{namespace: namespace, constraint: constraint, keyValue: keyValue, asOf: asOf}
	if !tx.readTS.IsZero() && !tx.engine.writeBehindEnabled() && tx.engine.temporalIndexAuthoritative(schema, constraint) {
		// Read the history on the transaction's physical snapshot, excluding
		// committed versions whose replacement/deletion belongs to this view.
		exclude := make(map[NodeID]struct{}, len(tx.pendingNodes)+len(tx.deletedNodes))
		for id := range tx.pendingNodes {
			exclude[id] = struct{}{}
		}
		for id := range tx.deletedNodes {
			exclude[id] = struct{}{}
		}
		target := temporalRefreshTarget{constraint: constraint, desc: makeTemporalDescriptor(namespace, constraint, keyValue), keyValue: keyValue}
		if err := tx.withSnapshotViewLocked(func(snapshot *badger.Txn) error {
			node, err := tx.engine.temporalHistoryNodeAsOfInTxn(snapshot, target, asOf, exclude, false)
			choice.consider(node)
			return err
		}); err != nil {
			return nil, err
		}
	} else {
		// Compatibility and buffered views use the existing transaction read
		// owner, which preserves their snapshot/overlay visibility contract.
		nodes, err := tx.getNodesByLabelLocked(constraint.Label)
		if err != nil {
			return nil, err
		}
		for _, candidate := range nodes {
			if candidate != nil && !tx.committedNodeSupersededLocked(candidate.ID) {
				choice.consider(candidate)
			}
		}
	}
	for _, candidate := range tx.pendingNodes {
		choice.consider(candidate)
	}
	return choice.node, nil
}

// temporalNodeChoice is the common winner rule for an indexed history's
// snapshot candidate, a compatibility scan and a transaction's own writes.
type temporalNodeChoice struct {
	namespace  string
	constraint Constraint
	keyValue   interface{}
	asOf       time.Time
	node       *Node
	start      time.Time
}

func (choice *temporalNodeChoice) consider(candidate *Node) {
	if candidate == nil || !hasLabel(candidate.Labels, choice.constraint.Label) {
		return
	}
	namespace, _, qualified := ParseDatabasePrefix(string(candidate.ID))
	if !qualified || namespace != choice.namespace || !nodeMatchesTemporalLookup(candidate, choice.constraint, choice.keyValue, choice.asOf) {
		return
	}
	_, start, _, _, valid := temporalNodeState(candidate, choice.constraint)
	if valid && (choice.node == nil || start.After(choice.start)) {
		choice.node, choice.start = candidate, start
	}
}
