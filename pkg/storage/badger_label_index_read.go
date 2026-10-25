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
	// NodeRef is the current node with the numeric identity it has in the
	// reader's snapshot. Materializers that read MVCC heads or history use
	// it instead of the live dictionary, which a drop and recreation of the
	// same ID can change under an open reader.
	NodeRef() (nodeRef, bool, error)
}

// labelFallbackReader owns the larger scan state that is used only while the
// label index is not ready. Ready reads use nodeIndexReader without allocating
// these fallback iterator fields.
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
	if err := r.labelContext.Err(); err != nil {
		r.err = err
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

// NodeRef resolves the fallback candidate through the reader's snapshot.
func (r *labelFallbackReader) NodeRef() (nodeRef, bool, error) {
	return r.engine.snapshotNodeRef(r.txn, r.id)
}

// newLabelIndexReader is the owner of label-index candidate reads. Until the
// background rebuild commits its ready marker, it scans the authoritative node
// bodies and checks their labels instead of trusting a partially rebuilt index.
// Snapshot reads stream every known node ID because historical labels can
// differ from the current node body. Context is required at this owner: statement
// streams pass their caller through ready, scoped and rebuilding cursor work.
// Legacy context-free APIs adapt explicitly at their own call boundary.
func (b *BadgerEngine) newLabelIndexReader(
	ctx context.Context,
	txn *badger.Txn,
	scope, label string,
	after NodeID,
	visibility readVisibility,
	snapshot bool,
) labelIndexReader {
	if err := ctx.Err(); err != nil {
		return &nodeIndexReader{err: err}
	}
	ready, _, err := b.labelIndexAdmissionInTxn(txn)
	if err != nil {
		return &nodeIndexReader{err: err}
	}
	if err := ctx.Err(); err != nil {
		return &nodeIndexReader{err: err}
	}
	if ready {
		// The same MVCC snapshot supplied the ready marker and stays with this
		// stream, binding its index reads to that completed generation.
		return b.newNodeIndexReader(ctx, txn, scope, labelIndexPrefix(label), after)
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
		node, err := decodeNodeMetadata(value)
		if err != nil {
			return err
		}
		matches = labelsContain(node.Labels, label)
		return nil
	})
	return matches, err
}

func nodeHasLabel(node *Node, label string) bool {
	if node == nil {
		return false
	}
	return labelsContain(node.Labels, label)
}

func labelsContain(labels []string, label string) bool {
	for _, existing := range labels {
		if existing == label {
			return true
		}
	}
	return false
}

// countStoredLabelNodes is the complete count fallback used while the label
// index is rebuilding. It accounts for buffered writes in the same read view.
func (b *BadgerEngine) countStoredLabelNodes(ctx context.Context, scope, label string) (int64, error) {
	var captureErr error
	var overlay []*Node
	var touched map[NodeID]bool
	var count int64
	capture := func(w *WriteBehindBuffer) {
		if w != nil {
			captured, readErr := b.readBufferedEntitiesWithContext(ctx, w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
			overlay, touched, captureErr = captured.nodes, captured.nodesTouched, readErr
		}
	}
	err := b.withReadView(capture, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr
		}
		reader := b.newLabelIndexReader(ctx, txn, scope, label, "", storedRead, false)
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
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if node != nil && nodeIDInScope(node.ID, scope) && nodeHasLabel(node, label) {
			count++
		}
	}
	return count, nil
}
