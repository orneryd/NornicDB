package storage

import (
	"context"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

type readScopeKind uint8

const (
	readNamespaceScope readScopeKind = iota
	readPrefixScope
	readNodeIDsScope
	readNodePairScope // preserve ordered admission, including invalid empty endpoints
)

// readViewOptions is private to the storage owner. cached must have no externally
// visible effects: a concurrent publication can require a retry. current is only
// for materializers with no user callbacks; it permits current body-cache reads,
// validated as one group, with a strict snapshot retry if a cache changes.
type readViewOptions struct {
	scopeKind readScopeKind
	nodeIDs   []NodeID

	longLived bool
	scope     string
	cached    func(*WriteBehindBuffer) (bool, error)
	current   *bool
}

type badgerReadView struct {
	scope  string
	txn    *badger.Txn
	db     *managedBadgerDB
	engine *BadgerEngine
	readTs uint64
	mu     contextRWMutex
	// Keep the small state fields together after aligned admission state.
	scopeKind readScopeKind
	longLived bool
	closed    bool
}

// captureReadView owns every committed/overlay boundary. A nil successful view
// means cached completed the read. The idle case uses two epoch loads, no
// transaction, overlay copy or view allocation. Busy cache reads and transaction
// capture share the retirement lock. Default callers always get a strict view.
//
// Whenever a view is opened, capture always runs under the retirement boundary,
// including when write-behind is disabled or idle. Its argument is the selected
// overlay source: nil means empty, and the overlay selectors accept nil without
// allocating. Committed index/cache reads belong outside any nil-overlay guard.
// Only a complete cached result can return without opening a view or capturing.
// Context is a separate admission argument: placing it beside cached in the
// options makes the callback and its captures escape during context polling,
// allocating even when a completed cache read needs no managed transaction.
func (b *BadgerEngine) captureReadView(ctx context.Context, capture func(*WriteBehindBuffer), options ...readViewOptions) (*badgerReadView, error) {
	// Internal detached reads may pass nil; a supplied caller context owns
	// admission even when no buffer or transaction is needed.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var opts readViewOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if err := b.pendingReadConflict(opts.scopeKind, opts.scope, opts.nodeIDs, ctx); err != nil {
		return nil, err
	}
	w := b.writeBehind
	if opts.cached != nil {
		epoch, idle := w.idleReadEpoch(opts.scope)
		if idle {
			done, err := opts.cached(nil)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if after, stillIdle := w.idleReadEpoch(opts.scope); stillIdle && after == epoch && (done || err != nil) {
				return nil, err
			}
		}
	}
	// Only the slow capture branch acquires a transaction and retirement lease.
	if w != nil {
		if err := w.mu.RLockContext(ctx); err != nil {
			return nil, err
		}
		defer w.mu.RUnlock()
		if opts.cached != nil {
			done, err := opts.cached(w)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if done || err != nil {
				return nil, err
			}
		}
	}
	db, err := b.beginHelperTxn()
	if err != nil {
		return nil, err
	}
	var txn *badger.Txn
	var readTs uint64
	if opts.longLived {
		readTs, err = db.oracle.beginRead()
	} else {
		txn, readTs, err = db.beginTxn(false)
	}
	if err != nil {
		b.work.Leave()
		return nil, err
	}
	view := &badgerReadView{scope: strings.Clone(opts.scope), scopeKind: opts.scopeKind, txn: txn, db: db, engine: b, readTs: readTs, longLived: opts.longLived}
	if capture != nil {
		overlay := w
		if _, idle := w.idleReadEpoch(opts.scope); idle {
			overlay = nil
		}
		// The capture runs inside the view's own unwind boundary. A panic from a
		// capture callback (a defect, not a result) still unwinds, but it must not
		// leave this read's transaction, timestamp or the engine's close barrier
		// owned by a dead stack: Close releases them and the panic continues
		// unchanged, so Engine.Close never waits for a read that cannot return.
		func() {
			defer func() {
				if r := recover(); r != nil {
					view.Close()
					if opts.longLived {
						b.work.Leave()
					}
					panic(r)
				}
			}()
			capture(overlay)
		}()
	}
	if opts.longLived {
		// A long-lived view holds only its oracle timestamp; each scan is
		// admitted on its own (withView).
		b.work.Leave()
	}
	return view, nil
}

func (v *badgerReadView) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return
	}
	v.closed = true
	if v.txn != nil {
		v.txn.Discard()
		v.txn = nil
	}
	v.db.endRead(v.readTs)
	if !v.longLived {
		v.engine.work.Leave() // the short view's admission, returned by Close
	}
}

// Long-lived views hold only the oracle timestamp between scans. Engine Close
// waits for an active scan, never for the entire build lifetime.
func (v *badgerReadView) withView(ctx context.Context, read func(*badger.Txn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !v.longLived {
		return read(v.txn)
	}
	if err := v.mu.LockContext(ctx); err != nil {
		return err
	}
	defer v.mu.Unlock()
	if v.closed {
		return ErrStorageClosed
	}
	return v.readPinned(ctx, read)
}

// readPinned executes against the captured timestamp. Its caller must keep
// that timestamp leased until the callback returns: withView holds v.mu;
// historical streams hold their read owner's active-operation lease so a
// callback can perform another read without recursively acquiring v.mu.
func (v *badgerReadView) readPinned(ctx context.Context, read func(*badger.Txn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := v.engine.pendingReadConflict(v.scopeKind, v.scope, nil, ctx); err != nil {
		return err
	}

	db, err := v.engine.beginHelperTxn()
	if err != nil {
		return err
	}
	defer v.engine.work.Leave()
	if db != v.db {
		return ErrStorageClosed
	}
	return recoverBadgerClosedPanic(func() error {
		txn := db.newTxnAt(v.readTs, false)
		defer txn.Discard()
		return read(txn)
	})
}

func (b *BadgerEngine) withReadView(ctx context.Context, capture func(*WriteBehindBuffer), read func(*badger.Txn) error, options ...readViewOptions) error {
	if len(options) != 0 && options[0].current != nil {
		return b.withCurrentReadView(ctx, capture, read, options[0])
	}
	view, err := b.captureReadView(ctx, capture, options...)
	if err != nil || view == nil {
		return err
	}
	return view.readAndClose(ctx, read)
}

func (v *badgerReadView) readAndClose(ctx context.Context, read func(*badger.Txn) error) error {
	defer v.Close()
	return recoverBadgerClosedPanic(func() error { return v.withView(ctx, read) })
}

// Only materializers opting into current body caches need group validation.
func (b *BadgerEngine) withCurrentReadView(ctx context.Context, capture func(*WriteBehindBuffer), read func(*badger.Txn) error, opts readViewOptions) error {
	nodeGen, edgeGen := b.nodeCacheGen.current(), b.edgeCacheGen.current()
	*opts.current = true
	view, err := b.captureReadView(ctx, capture, opts)
	if err != nil || view == nil {
		return err
	}
	defer view.Close()

	err = recoverBadgerClosedPanic(func() error { return view.withView(ctx, read) })
	if nodeGen != b.nodeCacheGen.current() || edgeGen != b.edgeCacheGen.current() {
		// Re-evaluate only internal materialization, never a user callback.
		*opts.current = false
		return recoverBadgerClosedPanic(func() error { return view.withView(ctx, read) })
	}
	return err
}

// pendingReadConflict admits exactly the graph population the capture reads.
// Bounded ID requests only consult their namespaces when any DROP is pending;
// the healthy batch path retains one atomic admission check rather than N.
func (b *BadgerEngine) pendingReadConflict(kind readScopeKind, scope string, ids []NodeID, ctx context.Context) error {
	switch kind {
	case readPrefixScope:
		return b.pendingPrefixConflict(scope)
	case readNodeIDsScope, readNodePairScope:
		if err := ctx.Err(); err != nil {
			return err
		}
		if b.pendingNamespaceConflict("") == nil {
			return nil
		}
		var cancel bulkReadCancellation
		for _, id := range ids {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			if id != "" || kind == readNodePairScope {
				if err := b.pendingNamespaceConflict(namespaceForNodeID(id)); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return b.pendingNamespaceConflict(scope)
	}
}
