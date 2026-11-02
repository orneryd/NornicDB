package storage

import (
	"bytes"
	"context"

	"github.com/dgraph-io/badger/v4"
)

type labelIndexReader interface {
	Next() bool
	Close()
	NodeID() NodeID
	Err() error
	IndexKey() []byte
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

func (r *labelFallbackReader) NodeID() NodeID   { return r.id }
func (r *labelFallbackReader) Err() error       { return r.err }
func (r *labelFallbackReader) IndexKey() []byte { return nil }

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
) (labelIndexReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ready, err := b.labelIndexReadyInTxn(txn)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ready {
		// The same MVCC snapshot supplied the ready marker and stays with this
		// stream, binding its index reads to that completed generation.
		return b.newNodeIndexReader(ctx, txn, scope, labelIndexPrefix(label), after), nil
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
		return reader, nil
	}
	reader.bodyPrefix = nodeKey(NodeID(scope))
	reader.bodyScan = txn.NewIterator(badgerPrefixIteratorOptions(reader.bodyPrefix))
	return reader, nil
}

func (r *labelFallbackReader) nextLabelBody() bool {
	for {
		if err := r.labelContext.Err(); err != nil {
			r.err = err
			return false
		}
		if !r.bodyScanStarted {
			r.bodyScanStarted = true
			if r.after == "" {
				r.bodyScan.Rewind()
			} else {
				start := nodeKey(r.after)
				r.bodyScan.Seek(start)
				if r.bodyScan.Valid() && string(r.bodyScan.Item().Key()) == string(start) {
					r.bodyScan.Next()
				}
			}
		} else {
			r.bodyScan.Next()
		}
		if !r.bodyScan.ValidForPrefix(r.bodyPrefix) {
			return false
		}

		item := r.bodyScan.Item()
		key := item.Key()
		if len(key) <= len(r.bodyPrefix) {
			continue
		}
		id := NodeID(key[1:])

		var labels []string
		hidden := false
		if err := item.Value(func(value []byte) error {
			// Label-only discovery must not walk/resolve the property payload
			// of every rejected body. The canonical metadata decoder skips
			// that payload using its stored length.
			if r.visibility == storedRead || !r.engine.visibilityFiltered() {
				var err error
				labels, err = decodeStoredNodeLabels(value)
				return err
			}
			// Visible decay scoring needs stored properties and timestamps. Stored
			// discovery and non-decay label checks use the payload-free mode instead.
			if r.visibility == visibleRead && r.engine.decayEnabled && !r.engine.revealAll.Load() {
				node, err := r.engine.decodeNodeProjected(r.txn, namespaceForNodeID(id), value, nil)
				if err != nil {
					return err
				}
				labels = node.Labels
				hidden = r.engine.filterEntityByDecay(r.visibility, node, r.nowNanos)
				return nil
			}
			node, err := r.engine.decodeNodeProjected(r.txn, namespaceForNodeID(id), value, map[string]struct{}{})
			if err != nil {
				return err
			}
			labels = node.Labels
			hidden = r.engine.filterEntityByDecay(r.visibility, node, r.nowNanos)
			return nil
		}); err != nil {
			r.err = err
			return false
		}
		if !hasLabel(labels, r.label) || hidden {
			continue
		}

		r.id = id
		return true
	}
}

func (r *labelFallbackReader) nextLabelID() bool {
	if !r.idScanStarted {
		r.idScanStarted = true
		r.forwardScan = r.txn.NewIterator(badgerPrefixIteratorOptions(r.forwardPrefix))
		r.bodyScan = r.txn.NewIterator(badgerPrefixIteratorOptions(r.bodyPrefix))
		seekAfter := func(it *badger.Iterator, prefix, start []byte) {
			if r.after == "" {
				it.Rewind()
				return
			}
			it.Seek(start)
			if it.ValidForPrefix(prefix) && string(it.Item().Key()) == string(start) {
				it.Next()
			}
		}
		seekAfter(r.forwardScan, r.forwardPrefix, nodeIDForwardKey(r.after))
		seekAfter(r.bodyScan, r.bodyPrefix, nodeKey(r.after))
	}

	for {
		if err := r.labelContext.Err(); err != nil {
			r.err = err
			return false
		}
		forwardValid := r.forwardScan.ValidForPrefix(r.forwardPrefix)
		bodyValid := r.bodyScan.ValidForPrefix(r.bodyPrefix)
		if !forwardValid && !bodyValid {
			return false
		}

		var id NodeID
		switch {
		case !bodyValid:
			key := r.forwardScan.Item().Key()
			if len(key) > 1 {
				id = NodeID(key[1:])
			}
			r.forwardScan.Next()
		case !forwardValid:
			key := r.bodyScan.Item().Key()
			if len(key) > 1 {
				id = NodeID(key[1:])
			}
			r.bodyScan.Next()
		default:
			forwardKey := r.forwardScan.Item().Key()
			bodyKey := r.bodyScan.Item().Key()
			forwardID := forwardKey[1:]
			bodyID := bodyKey[1:]
			switch bytes.Compare(forwardID, bodyID) {
			case -1:
				id = NodeID(forwardID)
				r.forwardScan.Next()
			case 1:
				id = NodeID(bodyID)
				r.bodyScan.Next()
			default:
				id = NodeID(forwardID)
				r.forwardScan.Next()
				r.bodyScan.Next()
			}
		}
		if id == "" {
			continue
		}
		r.id = id
		return true
	}
}
