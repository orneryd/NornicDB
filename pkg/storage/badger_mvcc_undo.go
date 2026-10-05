package storage

import (
	"errors"
	"reflect"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/orneryd/nornicdb/pkg/util"
)

// Inverse-diff MVCC history (#859 phase I, #911).
//
// While retention keeps history, an update archives the superseded version
// as an undo record instead of a complete copy: the version's metadata
// (everything but properties) plus the properties that differ from the next
// version, and the properties the next version added. The record is stored
// at the superseded version's key like a complete record and names the
// version it undoes (Next). Reading it rebuilds the next version first, from
// the next record (complete or undo) or from the current record when Next is
// the live head, and applies the undo.
//
// Chains stay intact because retention archives every superseded version and
// pruning only removes a key's oldest versions. A deleted entity's last
// version is archived complete, so no chain crosses a delete. An undo whose
// Next version can't be found (retention turned off and on again, say) reads
// as not found, never as another version's state.
//
// Head-only retention archives only for active snapshot readers, so a later
// update may skip archiving; undo records are then not written and complete
// copies are archived as before.

// mvccUndoRecordTag starts an encoded undo record. 0xC1 is never a msgpack
// type byte, so it can't begin a complete record.
const mvccUndoRecordTag byte = 0xC1

// errMVCCUndoChain reports an undo record whose Next version doesn't follow
// its own: the chain can't be followed safely.
var errMVCCUndoChain = errors.New("mvcc undo record does not point to a later version")

// mvccNodeUndo undoes the change from version Next back to the version it is
// stored at.
type mvccNodeUndo struct {
	Next MVCCVersion
	// Meta is the superseded node without properties or embedding payloads.
	Meta *Node
	// Restore holds the superseded node's properties that are absent from
	// or differ in Next.
	Restore map[string]any
	// Remove lists the properties Next added.
	Remove []string
}

// mvccEdgeUndo is the relationship analogue of mvccNodeUndo.
type mvccEdgeUndo struct {
	Next    MVCCVersion
	Meta    *Edge
	Restore map[string]any
	Remove  []string
}

// diffProperties returns the properties of old that differ in or are absent
// from next, and the sorted keys next added.
func diffProperties(old, next map[string]any) (map[string]any, []string) {
	var restore map[string]any
	for key, value := range old {
		if nextValue, ok := next[key]; ok && reflect.DeepEqual(nextValue, value) {
			continue
		}
		if restore == nil {
			restore = make(map[string]any)
		}
		restore[key] = value
	}
	var remove []string
	for key := range next {
		if _, ok := old[key]; !ok {
			remove = append(remove, key)
		}
	}
	sort.Strings(remove)
	return restore, remove
}

// undoProperties applies restore and remove to a copy of next.
func undoProperties(next, restore map[string]any, remove []string) map[string]any {
	properties := make(map[string]any, len(next)+len(restore))
	for key, value := range next {
		properties[key] = value
	}
	for _, key := range remove {
		delete(properties, key)
	}
	for key, value := range restore {
		properties[key] = value
	}
	return properties
}

func newMVCCNodeUndo(old, next *Node, nextVersion MVCCVersion) *mvccNodeUndo {
	meta := mvccSnapshotNode(old)
	restore, remove := diffProperties(meta.Properties, next.Properties)
	meta.Properties = nil
	return &mvccNodeUndo{Next: nextVersion, Meta: meta, Restore: restore, Remove: remove}
}

func (u *mvccNodeUndo) apply(next *Node) *Node {
	node := copyNode(u.Meta)
	node.Properties = undoProperties(next.Properties, u.Restore, u.Remove)
	return node
}

func newMVCCEdgeUndo(old, next *Edge, nextVersion MVCCVersion) *mvccEdgeUndo {
	meta := copyEdge(old)
	restore, remove := diffProperties(meta.Properties, next.Properties)
	meta.Properties = nil
	return &mvccEdgeUndo{Next: nextVersion, Meta: meta, Restore: restore, Remove: remove}
}

func (u *mvccEdgeUndo) apply(next *Edge) *Edge {
	edge := copyEdge(u.Meta)
	edge.Properties = undoProperties(next.Properties, u.Restore, u.Remove)
	return edge
}

func encodeMVCCUndo(undo any) ([]byte, error) {
	payload, err := msgpack.Marshal(undo)
	if err != nil {
		return nil, err
	}
	return append([]byte{mvccUndoRecordTag}, payload...), nil
}

func encodeMVCCNodeUndoRecord(undo *mvccNodeUndo, _ bool) ([]byte, error) {
	return encodeMVCCUndo(undo)
}

func encodeMVCCEdgeUndoRecord(undo *mvccEdgeUndo, _ bool) ([]byte, error) {
	return encodeMVCCUndo(undo)
}

// isMVCCUndoRecord reports whether an encoded version record is an undo
// record.
func isMVCCUndoRecord(data []byte) bool {
	return len(data) > 0 && data[0] == mvccUndoRecordTag
}

func decodeMVCCNodeUndo(data []byte) (*mvccNodeUndo, error) {
	var undo mvccNodeUndo
	if err := util.DecodeMsgpackBytes(data[1:], &undo); err != nil {
		return nil, err
	}
	return &undo, nil
}

func decodeMVCCEdgeUndo(data []byte) (*mvccEdgeUndo, error) {
	var undo mvccEdgeUndo
	if err := util.DecodeMsgpackBytes(data[1:], &undo); err != nil {
		return nil, err
	}
	return &undo, nil
}

// resolveNodeUndoInTxn rebuilds the node version an undo record stored at
// version at describes: it follows Next links forward to a complete record
// or the live head's current record, then applies the undos from the newest
// back. A link that can't be followed reads as ErrNotFound.
func (b *BadgerEngine) resolveNodeUndoInTxn(txn *badger.Txn, id NodeID, at MVCCVersion, undo *mvccNodeUndo) (mvccNodeRecord, error) {
	chain := []*mvccNodeUndo{undo}
	previous := at
	var base *Node
	for base == nil {
		next := chain[len(chain)-1].Next
		if next.Compare(previous) <= 0 {
			return mvccNodeRecord{}, errMVCCUndoChain
		}
		record, err := loadMVCCRecordExactInTxn[mvccNodeRecord, nodeMVCCVersionKeyer](b, txn, string(id), next, decodeMVCCNodeRecord)
		switch {
		case err == nil && record.undo != nil:
			chain = append(chain, record.undo)
			previous = next
		case err == nil:
			if record.Tombstoned || record.Node == nil {
				return mvccNodeRecord{}, ErrNotFound
			}
			base = record.Node
		case err == ErrNotFound:
			if base, err = b.currentNodeAtHeadInTxn(txn, id, next); err != nil {
				return mvccNodeRecord{}, err
			}
		default:
			return mvccNodeRecord{}, err
		}
	}
	for i := len(chain) - 1; i >= 0; i-- {
		base = chain[i].apply(base)
	}
	return mvccNodeRecord{Node: base}, nil
}

// currentNodeAtHeadInTxn returns the node's current record, without
// embedding payloads, when its live head is at version.
func (b *BadgerEngine) currentNodeAtHeadInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) (*Node, error) {
	head, err := b.loadNodeMVCCHeadInTxn(txn, id)
	if err != nil || head.Tombstoned || head.Version.Compare(version) != 0 {
		return nil, ErrNotFound
	}
	item, err := txn.Get(nodeKey(id))
	if err != nil {
		return nil, ErrNotFound
	}
	var node *Node
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		node, decodeErr = b.decodeNode(namespaceForNodeID(id), val)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	return mvccSnapshotNode(node), nil
}

// resolveEdgeUndoInTxn is the relationship analogue of resolveNodeUndoInTxn.
func (b *BadgerEngine) resolveEdgeUndoInTxn(txn *badger.Txn, id EdgeID, at MVCCVersion, undo *mvccEdgeUndo) (mvccEdgeRecord, error) {
	chain := []*mvccEdgeUndo{undo}
	previous := at
	var base *Edge
	for base == nil {
		next := chain[len(chain)-1].Next
		if next.Compare(previous) <= 0 {
			return mvccEdgeRecord{}, errMVCCUndoChain
		}
		record, err := loadMVCCRecordExactInTxn[mvccEdgeRecord, edgeMVCCVersionKeyer](b, txn, string(id), next, decodeMVCCEdgeRecord)
		switch {
		case err == nil && record.undo != nil:
			chain = append(chain, record.undo)
			previous = next
		case err == nil:
			if record.Tombstoned || record.Edge == nil {
				return mvccEdgeRecord{}, ErrNotFound
			}
			base = record.Edge
		case err == ErrNotFound:
			if base, err = b.currentEdgeAtHeadInTxn(txn, id, next); err != nil {
				return mvccEdgeRecord{}, err
			}
		default:
			return mvccEdgeRecord{}, err
		}
	}
	for i := len(chain) - 1; i >= 0; i-- {
		base = chain[i].apply(base)
	}
	return mvccEdgeRecord{Edge: base}, nil
}

// currentEdgeAtHeadInTxn returns the relationship's current record when its
// live head is at version.
func (b *BadgerEngine) currentEdgeAtHeadInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) (*Edge, error) {
	head, err := b.loadEdgeMVCCHeadInTxn(txn, id)
	if err != nil || head.Tombstoned || head.Version.Compare(version) != 0 {
		return nil, ErrNotFound
	}
	item, err := txn.Get(edgeKey(id))
	if err != nil {
		return nil, ErrNotFound
	}
	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyByID(val, id)
		return decodeErr
	}); err != nil {
		return nil, err
	}
	return edge, nil
}

// archiveNodeUpdateBodyInTxn archives old, the version an update to next at
// nextVersion supersedes, at atVersion: as an undo record while retention
// keeps history, otherwise as a complete record (archiveNodeBodyInTxn).
func (b *BadgerEngine) archiveNodeUpdateBodyInTxn(txn kvWriter, id NodeID, old, next *Node, atVersion, nextVersion MVCCVersion) error {
	if old == nil || next == nil || !b.retentionRetainsHistory() {
		return b.archiveNodeBodyInTxn(txn, id, old, atVersion)
	}
	return archiveMVCCBodyInTxn[*mvccNodeUndo, nodeMVCCVersionKeyer](b, txn, string(id), newMVCCNodeUndo(old, next, nextVersion), atVersion, encodeMVCCNodeUndoRecord)
}

// archiveEdgeUpdateBodyInTxn is the relationship analogue of
// archiveNodeUpdateBodyInTxn.
func (b *BadgerEngine) archiveEdgeUpdateBodyInTxn(txn kvWriter, id EdgeID, old, next *Edge, atVersion, nextVersion MVCCVersion) error {
	if old == nil || next == nil || !b.retentionRetainsHistory() {
		return b.archiveEdgeBodyInTxn(txn, id, old, atVersion)
	}
	return archiveMVCCBodyInTxn[*mvccEdgeUndo, edgeMVCCVersionKeyer](b, txn, string(id), newMVCCEdgeUndo(old, next, nextVersion), atVersion, encodeMVCCEdgeUndoRecord)
}
