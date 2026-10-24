package storage

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// A temporal constraint's history index (prefixTemporalIndex) is complete
// when it has an entry for every stored node the constraint covers. Every
// write path maintains it (transaction commits through
// bufferTemporalIndexWrites, the engine's direct node writes through
// applyTemporalIndexesForNodeChangeInTxn, bulk writes through transactions),
// and RebuildTemporalIndexes rebuilds it from the stored nodes on start. The
// one gap is a constraint created over nodes that already exist: they have no
// entries until the next rebuild.
//
// While the index is complete, NO OVERLAP validation reads the history of
// the written node's own key instead of scanning every node of the label
// (#1126): with no entries for a key, no stored node has that key. While it
// is not, validation keeps the label scan.

// temporalConstraintScanHook lets storage tests assert that NO OVERLAP
// validation with a complete history index does not fall back to scanning
// the label (the transaction's committed scan and the engine's legacy scan).
var temporalConstraintScanHook atomic.Pointer[func()]

func runTemporalConstraintScanHook() {
	if hook := temporalConstraintScanHook.Load(); hook != nil {
		(*hook)()
	}
}

func setTemporalConstraintScanHook(hook func()) func() {
	previous := temporalConstraintScanHook.Swap(&hook)
	return func() { temporalConstraintScanHook.Store(previous) }
}

// temporalIndexCompletenessKey identifies a temporal constraint by what its
// index entries are keyed on (label and properties), not by name.
func temporalIndexCompletenessKey(c Constraint) string {
	return c.Label + "\x00" + strings.Join(c.Properties, "\x00")
}

// temporalIndexAuthoritative reports whether c's history index covers every
// stored node c applies to: c was created on an empty label (complete from
// the start), or the engine's index was rebuilt and c was not created since
// over existing nodes.
func (b *BadgerEngine) temporalIndexAuthoritative(sm *SchemaManager, c Constraint) bool {
	if b == nil || sm == nil {
		return false
	}
	key := temporalIndexCompletenessKey(c)
	sm.mu.RLock()
	_, complete := sm.temporalIndexCompleteKeys[key]
	_, incomplete := sm.temporalIndexIncompleteKeys[key]
	sm.mu.RUnlock()
	return complete || (b.temporalIndexReady.Load() && !incomplete)
}

// forgetTemporalIndexCompleteLocked drops c's runtime completeness marks: a
// constraint that is (re)registered or dropped starts over.
func (sm *SchemaManager) forgetTemporalIndexCompleteLocked(c Constraint) {
	if c.Type != ConstraintTemporal {
		return
	}
	key := temporalIndexCompletenessKey(c)
	delete(sm.temporalIndexCompleteKeys, key)
	delete(sm.temporalIndexIncompleteKeys, key)
}

func (sm *SchemaManager) markTemporalIndexCreated(c Constraint, complete bool) {
	key := temporalIndexCompletenessKey(c)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	target := &sm.temporalIndexIncompleteKeys
	if complete {
		target = &sm.temporalIndexCompleteKeys
	}
	if *target == nil {
		*target = make(map[string]struct{})
	}
	(*target)[key] = struct{}{}
}

// HasConstraintNamed reports whether a constraint named name is registered.
func (sm *SchemaManager) HasConstraintNamed(name string) bool {
	if sm == nil {
		return false
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.constraints[name]
	return ok
}

// MarkNewTemporalConstraintsForEngine makes the history index of the
// temporal node constraints DDL just created complete, and records it:
//   - on an empty label (the usual schema-first load) there is nothing to
//     index, and every later write maintains the index;
//   - over existing nodes, which were written before the constraint and have
//     no entries, their entries are written now (backfillTemporalIndexForLabel),
//     so the index never stays incomplete across a restart that skips the
//     rebuild (a clean shutdown certifies the derived indexes as current).
//
// Only an engine that doesn't unwrap to a BadgerEngine leaves a constraint over
// existing nodes incomplete; it is validated by label scan until a rebuild.
func MarkNewTemporalConstraintsForEngine(engine Engine, schema *SchemaManager, constraints []Constraint) error {
	if engine == nil || schema == nil {
		return nil
	}
	badgerEngine, _ := UnwrapEngine(engine).(*BadgerEngine)
	for _, c := range constraints {
		if c.Type != ConstraintTemporal || c.EntityType == ConstraintEntityRelationship {
			continue
		}
		if badgerEngine == nil {
			empty, err := labelHasNoNodes(engine, c.Label)
			if err != nil {
				return err
			}
			schema.markTemporalIndexCreated(c, empty)
			continue
		}
		if err := badgerEngine.markNewTemporalConstraint(engine, schema, c); err != nil {
			return err
		}
	}
	return nil
}

// markNewTemporalConstraint decides whether the label has nodes, writes
// their history entries, and records the index complete, as one maintenance
// write over the engine's database (beginMaintenanceWrite): every
// write-behind commit acknowledged before it is applied first, so a node of
// the label still in the buffer is seen and backfilled, and no write runs
// across it.
func (b *BadgerEngine) markNewTemporalConstraint(engine Engine, schema *SchemaManager, c Constraint) error {
	lifecycle, err := b.beginMaintenanceWrite(engineScope(engine))
	if err != nil {
		return err
	}
	defer lifecycle.finish()
	empty, err := labelHasNoNodes(engine, c.Label)
	if err != nil {
		return err
	}
	if !empty {
		if err := b.backfillTemporalIndexForLabel(engine, c.Label); err != nil {
			return err
		}
	}
	schema.markTemporalIndexCreated(c, true)
	return nil
}

// engineScope is the database a scoped wrapper of the engine serves, or nil
// (the whole store) for an unscoped engine.
func engineScope(engine Engine) []string {
	for current := engine; current != nil; {
		if scoped, ok := current.(interface{ Namespace() string }); ok {
			return []string{scoped.Namespace()}
		}
		wrapper, ok := current.(EngineUnwrapper)
		if !ok {
			return nil
		}
		current = wrapper.GetInnerEngine()
	}
	return nil
}

// backfillTemporalIndexForLabel writes the temporal history entries (and
// current pointers) of every stored node of label visible through engine (a
// database-scoped wrapper of b), in chunks: the same per-node work as
// RebuildTemporalIndexes, for one label instead of the whole store.
func (b *BadgerEngine) backfillTemporalIndexForLabel(engine Engine, label string) error {
	var ids []NodeID
	if err := StreamStoredNodesByLabelProjected(engine, label, []string{}, func(node *Node) error {
		if node != nil {
			ids = append(ids, EnsureNodeIDDatabasePrefixForEngine(engine, node.ID))
		}
		return nil
	}); err != nil {
		return err
	}
	asOf := time.Now().UTC()
	const chunk = 500
	for start := 0; start < len(ids); start += chunk {
		part := ids[start:min(start+chunk, len(ids))]
		if err := b.withUpdate(func(txn *badger.Txn) error {
			for _, id := range part {
				node, err := b.loadNodeForTemporalTxn(txn, id, false)
				if err != nil {
					return err
				}
				if err := b.rebuildTemporalNodeInTxn(txn, node, asOf); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

var errStopLabelProbe = errors.New("label has nodes")

func labelHasNoNodes(engine Engine, label string) (bool, error) {
	err := StreamStoredNodesByLabelProjected(engine, label, nil, func(node *Node) error {
		if node == nil {
			return nil
		}
		return errStopLabelProbe
	})
	if errors.Is(err, errStopLabelProbe) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// temporalKeyHistoryNodesLatest returns the stored nodes in the history of
// node's key under c, read at the latest committed state (the state the label
// scan reads). Nodes whose entry outlived them (a rebuild racing a delete)
// are skipped. ok is false when node has no complete key or start.
func (b *BadgerEngine) temporalKeyHistoryNodesLatest(ctx context.Context, namespace string, c Constraint, node *Node) (nodes []*Node, ok bool, err error) {
	target, _, ok := temporalTargetForNode(namespace, node, c)
	if !ok {
		return nil, false, nil
	}
	prefix := temporalHistoryPrefix(target.desc)
	var overlay []*Node
	var touched map[NodeID]bool
	err = b.withReadView(func(w *WriteBehindBuffer) { overlay, touched = b.bufferedLabelNodes(w, storedRead, c.Label) }, func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			id := extractNodeIDFromTemporalHistoryKey(it.Item().KeyCopy(nil), len(prefix))
			if id == "" || touched[id] {
				continue
			}
			stored, err := b.loadNodeForTemporalTxn(txn, id, false)
			if err != nil {
				return err
			}
			if stored != nil {
				nodes = append(nodes, stored)
			}
		}
		return nil
	})
	for _, n := range overlay {
		if namespace == "" || nodeIDInScope(n.ID, namespace+":") {
			nodes = append(nodes, copyNode(n))
		}
	}
	return nodes, true, err
}
