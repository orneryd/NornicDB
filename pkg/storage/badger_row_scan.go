package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// A committed-row scan has four required settings: the ID scope, the rows a
// write-behind overlay replaces, the decode-error policy and the visibility
// rule. Each setting's zero value is invalid, so a caller that leaves one out
// gets errRowScanSettings instead of a silently chosen behaviour.

var errRowScanSettings = errors.New("storage: committed-row scan setting not chosen")

// rowScope limits a scan to IDs with a prefix. Build it with allRows or
// rowsWithPrefix.
type rowScope struct {
	prefix string
	set    bool
}

func allRows() rowScope                     { return rowScope{set: true} }
func rowsWithPrefix(prefix string) rowScope { return rowScope{prefix: prefix, set: true} }

// rowOverlay names the committed rows a write-behind overlay replaces. Build
// it with noOverlay or overlayReplaces.
type rowOverlay[ID comparable] struct {
	replaced map[ID]bool
	set      bool
}

func noOverlay[ID comparable]() rowOverlay[ID] { return rowOverlay[ID]{set: true} }
func overlayReplaces[ID comparable](ids map[ID]bool) rowOverlay[ID] {
	return rowOverlay[ID]{replaced: ids, set: true}
}

func (o rowOverlay[ID]) replacesRow(id ID) bool { return len(o.replaced) > 0 && o.replaced[id] }

// decodePolicy says what a row that fails to decode does to the scan.
type decodePolicy uint8

const (
	decodePolicyUnset decodePolicy = iota
	// decodeErrorsFail returns the decode error as the scan's error.
	decodeErrorsFail
	// decodeErrorsSkip skips the row.
	decodeErrorsSkip
)

// decodePolicyFor maps a caller's skip-invalid contract to its policy.
func decodePolicyFor(skipInvalid bool) decodePolicy {
	if skipInvalid {
		return decodeErrorsSkip
	}
	return decodeErrorsFail
}

// scanVisibility selects the decay/suppression rule applied to each row.
type scanVisibility uint8

const (
	scanVisibilityUnset scanVisibility = iota
	// scanVisible drops rows that visible reads hide.
	scanVisible
	// scanStored keeps every stored row.
	scanStored
)

func visibilityOf(v readVisibility) scanVisibility {
	if v == storedRead {
		return scanStored
	}
	return scanVisible
}

// decayVisibility maps StreamNodesOptions.ApplyDecayFilter to its rule.
func decayVisibility(apply bool) scanVisibility {
	if apply {
		return scanVisible
	}
	return scanStored
}

func (v scanVisibility) read() readVisibility {
	if v == scanStored {
		return storedRead
	}
	return visibleRead
}

// rowHooks are the optional parts of a committed-row scan. A zero value
// changes nothing. A batched scan's resume key is a separate argument of the
// scan (scanEdgeRowsFrom, streamNodeRowsFrom), not a field here: the iterator
// keeps that key, and a settings value that reached the iterator would take
// every caller's hook closures to the heap with it.
type rowHooks[ID comparable] struct {
	// before may skip a row by ID before it is decoded, or end the scan.
	before func(ID) (skip bool, err error)
	// decodeFailure wraps a decode error that ends the scan.
	decodeFailure func(ID, error) error
}

// edgeRowScan is the required settings of scanEdgeRows.
type edgeRowScan struct {
	scope      rowScope
	overlay    rowOverlay[EdgeID]
	decode     decodePolicy
	visibility scanVisibility

	hooks rowHooks[EdgeID]
	// body may consume a row's stored bytes instead of the decoded edge.
	// ErrIterationStopped from body ends the scan.
	body func(EdgeID, []byte) (handled bool, err error)
}

func (s edgeRowScan) validate() error {
	if !s.scope.set || !s.overlay.set || s.decode == decodePolicyUnset || s.visibility == scanVisibilityUnset {
		return errRowScanSettings
	}
	return nil
}

// nodeRowScan is the required settings of streamNodesWithOptionsInTxn, plus
// the optional hooks AllNodes uses for its node cache.
type nodeRowScan struct {
	scope      rowScope
	overlay    rowOverlay[NodeID]
	decode     decodePolicy
	visibility scanVisibility

	hooks rowHooks[NodeID]
	// body may consume a row's stored bytes instead of the decoded node.
	// ErrIterationStopped from body ends the scan.
	body func(NodeID, []byte) (handled bool, err error)
	// item may read a row's stored record through the caller's own versioned
	// reader instead of the kernel's decode.
	item func(NodeID, *badger.Item) (handled bool, err error)
	// cachedNode may supply a node for a row without decoding it.
	cachedNode func(NodeID) *Node
	// fullItem decodes through the versioned body reader with full embeddings.
	fullItem bool
	// decoded sees each node decoded from its row that passed the filters.
	decoded func(*Node)
}

func (s nodeRowScan) validate() error {
	if !s.scope.set || !s.overlay.set || s.decode == decodePolicyUnset || s.visibility == scanVisibilityUnset {
		return errRowScanSettings
	}
	return nil
}

// scanEdgeRows is the one committed relationship-row scan. AllEdges,
// StreamEdges and read snapshots differ only in its settings. visit returning
// ErrIterationStopped ends the scan with stopped set and no error.
func (b *BadgerEngine) scanEdgeRows(ctx context.Context, txn *badger.Txn, scan edgeRowScan, visit func(*Edge) error) (stopped bool, err error) {
	return b.scanEdgeRowsFrom(ctx, txn, nil, scan, visit)
}

// scanEdgeRowsFrom is scanEdgeRows resumed at the full row key start (a
// batched scan); a nil start scans from the first row.
func (b *BadgerEngine) scanEdgeRowsFrom(ctx context.Context, txn *badger.Txn, start []byte, scan edgeRowScan, visit func(*Edge) error) (stopped bool, err error) {
	if err := scan.validate(); err != nil {
		return false, err
	}
	nowNanos := DecayScoringTime()
	prefix := append([]byte{prefixEdge}, scan.scope.prefix...)
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()
	if start != nil {
		it.Seek(start)
	} else {
		it.Rewind()
	}
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		key := it.Item().Key()
		var edgeID EdgeID
		if len(key) > 1 {
			edgeID = EdgeID(key[1:])
		}
		if scan.overlay.replacesRow(edgeID) {
			continue // the write-behind overlay shadows this committed row
		}
		if scan.hooks.before != nil {
			skip, err := scan.hooks.before(edgeID)
			if err != nil {
				return false, err
			}
			if skip {
				continue
			}
		}
		var edge *Edge
		var handled, bodyFailed bool
		if err := it.Item().Value(func(val []byte) error {
			if scan.body != nil {
				var err error
				if handled, err = scan.body(edgeID, val); err != nil || handled {
					bodyFailed = err != nil
					return err
				}
			}
			var decodeErr error
			edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
			return decodeErr
		}); err != nil {
			if bodyFailed {
				if err == ErrIterationStopped {
					return true, nil
				}
				return false, err
			}
			if scan.decode == decodeErrorsFail {
				if scan.hooks.decodeFailure != nil {
					err = scan.hooks.decodeFailure(edgeID, err)
				}
				return false, err
			}
			continue
		}
		if handled {
			continue
		}
		if edge == nil || b.filterEntityByDecay(scan.visibility.read(), edge, nowNanos) {
			continue
		}
		if err := visit(edge); err != nil {
			if err == ErrIterationStopped {
				return true, nil
			}
			return false, err
		}
	}
	return false, nil
}
