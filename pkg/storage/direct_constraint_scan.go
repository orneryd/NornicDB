package storage

import (
	"context"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// visitDirectConstraintNodes is a direct mutation's integrity population, before
// its graph image is staged. Without a buffer, the publishing transaction owns
// that population. With a buffer, its older physical timestamp is insufficient:
// a holder can drain after that timestamp and disappear from both that snapshot
// and the current overlay. The existing stored-inclusive stream captures a fresh
// physical snapshot and overlay together, independently of the mutation's target
// snapshot. Value/entity authority already excludes conflicting logical writes.
// Neither branch materializes the complete label or clones each decoded body.
func (b *BadgerEngine) visitDirectConstraintNodes(ctx context.Context, txn *badger.Txn, namespace, label string, properties []string, exclude NodeID, visit func(*Node) error) error {
	scope := namespace + ":"
	emit := func(node *Node) error {
		if node.ID == exclude || !strings.HasPrefix(string(node.ID), scope) {
			return nil
		}
		return visit(node)
	}
	if b.writeBehind != nil {
		return b.streamNodesByLabelProjectedInScope(ctx, scope, label, properties, storedRead, emit)
	}
	prefix := labelIndexPrefix(label)
	iter := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer iter.Close()
	include := propertyProjectionSet(properties)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		number, ok := extractNodeNumIDFromLabelIndex(iter.Item().Key(), len(label))
		if !ok {
			continue
		}
		id, ok := b.idDict.lookupNodeIDByNum(number)
		if !ok || id == "" || id == exclude || !strings.HasPrefix(string(id), scope) {
			continue
		}
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
		node, err := b.readProjectedNodeItemInTxn(txn, item, id, properties, include, nodeBodyDetached)
		if err != nil {
			return err
		}
		if err := visit(node); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// visitDirectConstraintEdges applies the same authority rule to direct
// relationship writes. StoredEntityReader's stream includes suppressed holders,
// merges ACKed type membership and releases read leases on every visitor error.
func (b *BadgerEngine) visitDirectConstraintEdges(ctx context.Context, txn *badger.Txn, namespace, edgeType string, exclude EdgeID, visit func(*Edge) error) error {
	scope := ""
	if namespace != "" {
		scope = namespace + ":"
	}
	emit := func(edge *Edge) error {
		if edge.ID == exclude || !strings.HasPrefix(string(edge.ID), scope) {
			return nil
		}
		return visit(edge)
	}
	if b.writeBehind != nil {
		return b.streamEdgesByTypeInScope(ctx, scope, edgeType, storedRead, emit)
	}
	prefix := edgeTypeIndexPrefix(edgeType)
	iter := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer iter.Close()
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		number, ok := extractEdgeNumIDFromEdgeTypeKey(iter.Item().Key())
		if !ok {
			continue
		}
		id, ok := b.idDict.lookupEdgeIDByNum(number)
		if !ok || id == "" || id == exclude || !strings.HasPrefix(string(id), scope) {
			continue
		}
		edge, err := b.readEdgeInTxn(txn, id)
		if err != nil {
			return err
		}
		if edge == nil {
			continue // A stale type-index entry can outlive its deleted body.
		}
		if err := visit(edge); err != nil {
			return err
		}
	}
	return ctx.Err()
}
