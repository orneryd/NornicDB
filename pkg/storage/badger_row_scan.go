package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"

	"github.com/orneryd/nornicdb/pkg/util"
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

// scanVisibility is the read visibility (readVisibility) a scan applies to
// each row, once chosen. Build it with scanVisible, scanStored or
// visibilityOf; options-driven scans pass StreamNodesOptions.readVisibility.
type scanVisibility struct {
	read readVisibility
	set  bool
}

var (
	// scanVisible drops rows that visible reads hide.
	scanVisible = visibilityOf(visibleRead)
	// scanStored is the stored-entity read (storedRead).
	scanStored = visibilityOf(storedRead)
)

func visibilityOf(v readVisibility) scanVisibility { return scanVisibility{read: v, set: true} }

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

// skipEmptyEdgeID is the before hook of scans that skip the bare
// relationship-prefix row, which has no ID.
func skipEmptyEdgeID(id EdgeID) (bool, error) { return id == "", nil }

// skipOutsideNamespace is the before hook of scans that skip rows outside a
// database namespace without decoding them.
func skipOutsideNamespace[ID ~string](id ID) (bool, error) {
	_, _, ok := ParseDatabasePrefix(string(id))
	return !ok, nil
}

// scanPage is one batch of a batched scan: up to limit items, the row key of
// the last one, and whether the scan ended before the page filled.
type scanPage[T any] struct {
	items      []T
	last       []byte
	reachedEnd bool
	limit      int
}

func newScanPage[T any](limit int) *scanPage[T] {
	return &scanPage[T]{items: make([]T, 0, util.SafePreallocCap(limit)), reachedEnd: true, limit: limit}
}

// add keeps item, read from row key, and ends the scan once the page is full.
func (p *scanPage[T]) add(item T, key []byte) error {
	p.items = append(p.items, item)
	p.last = key
	if len(p.items) >= p.limit {
		p.reachedEnd = false
		return ErrIterationStopped
	}
	return nil
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

// skipRow is the one decision before a row is decoded: the write-behind overlay
// shadows it, or the before hook skips it or ends the scan.
func skipRow[ID comparable](overlay rowOverlay[ID], hooks rowHooks[ID], id ID) (bool, error) {
	if overlay.replacesRow(id) {
		return true, nil
	}
	if hooks.before != nil {
		return hooks.before(id)
	}
	return false, nil
}

// rowBodyHook gives a row's stored bytes to a scan's body hook: handled ends the
// row without decoding it, and its error (ErrIterationStopped included) is not a
// decode failure.
func rowBodyHook[ID comparable](item *badger.Item, id ID, body func(ID, []byte) (bool, error)) (handled bool, err error) {
	err = item.Value(func(val []byte) error {
		var hookErr error
		handled, hookErr = body(id, val)
		return hookErr
	})
	return handled, err
}

// rowHasID reports a row key with an ID after its prefix byte; the bare prefix
// row has none.
func rowHasID(key []byte) bool { return len(key) > 1 }

// nodeRowsOfOptions is the node row scan a streaming read's options select,
// over the overlay the caller captured: rows that fail to decode are skipped.
func nodeRowsOfOptions(opts StreamNodesOptions, overlay rowOverlay[NodeID]) nodeRowScan {
	return nodeRowScan{scope: rowsWithPrefix(opts.Prefix), overlay: overlay, decode: decodeErrorsSkip, visibility: visibilityOf(opts.readVisibility())}
}

// rowDecodeFailure is the one decode-error policy: the error that ends the
// scan, or nil to skip the row.
func rowDecodeFailure[ID comparable](decode decodePolicy, hooks rowHooks[ID], id ID, err error) error {
	if decode != decodeErrorsFail {
		return nil
	}
	if hooks.decodeFailure != nil {
		return hooks.decodeFailure(id, err)
	}
	return err
}

// scanRows is the one walk of committed rows under prefix, for either entity
// kind, resumed at the full row key start when it is non-nil. Cancellation is
// checked before each row; ErrIterationStopped from visit ends the walk with
// stopped set and no error.
func scanRows(ctx context.Context, txn *badger.Txn, prefix, start []byte, visit func(*badger.Item) error) (stopped bool, err error) {
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
		if err := visit(it.Item()); err != nil {
			if err == ErrIterationStopped {
				return true, nil
			}
			return false, err
		}
	}
	return false, nil
}

// drivePages is the one driver of a paged scan over several read views: collect
// a page from start, apply a non-empty page, stop when the scan reached its end,
// otherwise resume just after the page's last row key.
func drivePages[T any](start []byte, collect func(start []byte) (items []T, last []byte, reachedEnd bool, err error), apply func([]T) error) error {
	for {
		items, last, reachedEnd, err := collect(start)
		if err != nil {
			return err
		}
		if len(items) > 0 {
			if err := apply(items); err != nil {
				return err
			}
		}
		if reachedEnd {
			return nil
		}
		start = nextScanStart(last)
	}
}

// requireRowSettings is the one admission decision for both entity scans:
// each of the four required settings has been chosen.
func requireRowSettings[ID comparable](scope rowScope, overlay rowOverlay[ID], decode decodePolicy, visibility scanVisibility) error {
	if !scope.set || !overlay.set || decode == decodePolicyUnset || !visibility.set {
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

// choosesByID reports whether a node scan needs each row's ID before decoding.
func (s nodeRowScan) choosesByID() bool {
	return len(s.overlay.replaced) > 0 || s.hooks.before != nil || s.body != nil || s.item != nil || s.cachedNode != nil
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
	if err := requireRowSettings(scan.scope, scan.overlay, scan.decode, scan.visibility); err != nil {
		return false, err
	}
	nowNanos := DecayScoringTime()
	prefix := append([]byte{prefixEdge}, scan.scope.prefix...)
	return scanRows(ctx, txn, prefix, start, func(item *badger.Item) error {
		edgeID := EdgeID(item.Key()[1:])
		if skip, err := skipRow(scan.overlay, scan.hooks, edgeID); err != nil || skip {
			return err
		}
		if scan.body != nil {
			if handled, err := rowBodyHook(item, edgeID, scan.body); err != nil || handled {
				return err
			}
		}
		var edge *Edge
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
			return decodeErr
		}); err != nil {
			return rowDecodeFailure(scan.decode, scan.hooks, edgeID, err)
		}
		if edge == nil || b.filterEntityByDecay(scan.visibility.read, edge, nowNanos) {
			return nil
		}
		return visit(edge)
	})
}
