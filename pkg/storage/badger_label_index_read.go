package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type labelIndexReader interface {
	Next() bool
	Close()
	Item() *badger.Item
	NodeID() NodeID
	Err() error
	UsesLabelFallback() bool
	IndexKey() []byte
}

// labelFallbackReader owns the larger scan state that is used only while the
// label index is not ready. Ready reads use nodeIndexReader without allocating
// these iterator and context fields.
type labelFallbackReader struct {
	engine          *BadgerEngine
	txn             *badger.Txn
	labelContext    context.Context
	label           string
	visibility      readVisibility
	nowNanos        int64
	after           NodeID
	bodyPrefix      []byte
	forwardPrefix   []byte
	bodyScan        *badger.Iterator
	forwardScan     *badger.Iterator
	bodyFallback    bool
	bodyScanStarted bool
	idScanStarted   bool
	item            *badger.Item
	id              NodeID
	err             error
}

func (r *labelFallbackReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.bodyFallback {
		return r.nextLabelBody()
	}
	return r.nextLabelID()
}

func (r *labelFallbackReader) Close() {
	if r.bodyScan != nil {
		r.bodyScan.Close()
	}
	if r.forwardScan != nil {
		r.forwardScan.Close()
	}
}

func (r *labelFallbackReader) Item() *badger.Item      { return r.item }
func (r *labelFallbackReader) NodeID() NodeID          { return r.id }
func (r *labelFallbackReader) Err() error              { return r.err }
func (r *labelFallbackReader) UsesLabelFallback() bool { return true }
func (r *labelFallbackReader) IndexKey() []byte        { return nil }

// newLabelIndexReader is the owner of label-index candidate reads. Until the
// background rebuild commits its ready marker, it scans the authoritative node
// bodies and checks their labels instead of trusting a partially rebuilt index.
// Snapshot reads stream every known node ID because historical labels can
// differ from the current node body.
func (b *BadgerEngine) newLabelIndexReader(
	txn *badger.Txn,
	scope, label string,
	after NodeID,
	visibility readVisibility,
	snapshot bool,
) labelIndexReader {
	return b.newLabelIndexReaderWithContext(context.Background(), txn, scope, label, after, visibility, snapshot)
}

func (b *BadgerEngine) newLabelIndexReaderWithContext(
	ctx context.Context,
	txn *badger.Txn,
	scope, label string,
	after NodeID,
	visibility readVisibility,
	snapshot bool,
) labelIndexReader {
	if ctx == nil {
		ctx = context.Background()
	}
	ready, _, err := b.labelIndexAdmissionInTxn(txn)
	if err != nil {
		return &nodeIndexReader{err: err}
	}
	if ready {
		// The same MVCC snapshot supplied the ready marker and stays with this
		// stream, binding its index reads to that completed generation.
		return b.newNodeIndexReader(txn, scope, labelIndexPrefix(label), after)
	}

	reader := &labelFallbackReader{
		engine:       b,
		txn:          txn,
		labelContext: ctx,
		label:        label,
		visibility:   visibility,
		nowNanos:     DecayScoringTime(),
		after:        after,
		bodyFallback: !snapshot,
	}
	if snapshot {
		reader.forwardPrefix = nodeIDForwardKey(NodeID(scope))
		reader.bodyPrefix = nodeKey(NodeID(scope))
		return reader
	}
	reader.bodyPrefix = nodeKey(NodeID(scope))
	reader.bodyScan = txn.NewIterator(badgerPrefixIteratorOptions(reader.bodyPrefix))
	return reader
}

func nodeScopeForNamespace(namespace string) string {
	if namespace == "" {
		return ""
	}
	return namespace + ":"
}

// nodeHasLabelInTxn checks one stored node body. HasLabelBatch uses this point
// lookup while the range index is rebuilding, so EXISTS and aggregate plans do
// not treat an absent index entry as a negative label match.
func (b *BadgerEngine) nodeHasLabelInTxn(txn *badger.Txn, id NodeID, label string) (bool, error) {
	item, err := txn.Get(nodeKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var matches bool
	err = item.Value(func(value []byte) error {
		node, err := b.decodeNodeProjected(namespaceForNodeID(id), value, map[string]struct{}{})
		if err != nil {
			return err
		}
		matches = nodeHasLabel(node, label)
		return nil
	})
	return matches, err
}

func nodeHasLabel(node *Node, label string) bool {
	if node == nil {
		return false
	}
	for _, existing := range node.Labels {
		if existing == label {
			return true
		}
	}
	return false
}

// countStoredLabelNodes is the complete count fallback used while the label
// index is rebuilding. It accounts for buffered writes in the same read view.
func (b *BadgerEngine) countStoredLabelNodes(scope, label string) (int64, error) {
	var overlay []*Node
	var touched map[NodeID]bool
	var count int64
	capture := func(w *WriteBehindBuffer) {
		if w != nil {
			overlay, touched = w.labelOverlayLocked(label)
		}
	}
	err := b.withReadView(capture, func(txn *badger.Txn) error {
		reader := b.newLabelIndexReader(txn, scope, label, "", storedRead, false)
		defer reader.Close()
		for reader.Next() {
			if touched[reader.NodeID()] {
				continue
			}
			count++
		}
		return reader.Err()
	})
	if err != nil {
		return 0, err
	}
	for _, node := range overlay {
		if node != nil && nodeIDInScope(node.ID, scope) && nodeHasLabel(node, label) {
			count++
		}
	}
	return count, nil
}
