package storage

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// EdgeTypeStreamer is an optional extension interface for visiting the
// relationships of one type one at a time, without first materializing them
// into a slice. It is the edge counterpart of ProjectedLabelNodeReader.
//
// Engines that wrap another engine forward the stream (merging their own
// pending state, e.g. AsyncEngine's unflushed writes, or filtering to their
// database, e.g. NamespacedEngine). The callback must treat the edge as
// read-only; returning an error stops the stream and that error is returned.
//
// Example:
//
//	err := storage.StreamEdgesByType(ctx, engine, "KNOWS", func(e *storage.Edge) error {
//		counts[e.StartNode]++
//		return nil
//	})
type EdgeTypeStreamer interface {
	StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error
}

// ScopedEdgeTypeStreamer is EdgeTypeStreamer within one database. The edge
// type index is shared by every database on the server, so an unscoped stream
// decodes the type's edges of all databases; a scoped stream skips another
// database's index entries before reading their edge records. scope is the
// database's ID prefix ("db:"); "" streams every database.
//
// Like ScopedLabelNodeReader, a wrapper that cannot pass the scope down falls
// back to the unscoped stream, so callers keep filtering by database.
type ScopedEdgeTypeStreamer interface {
	StreamEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error
}

// StreamEdgesByType visits every relationship of edgeType visible through
// engine, one at a time.
//
// It uses EdgeTypeStreamer when engine implements it. Otherwise it falls back
// to engine.GetEdgesByType(edgeType) and visits the returned slice: that
// fallback still reads only the one relationship type through the type index,
// never the whole graph (AllEdges), but it holds the type's edges in memory at
// once. Engines on hot paths (Badger, WAL, Async, Namespaced, Composite and the
// transaction wrappers) implement the streaming interface.
func StreamEdgesByType(ctx context.Context, engine Engine, edgeType string, visit func(*Edge) error) error {
	if engine == nil || visit == nil {
		return ErrInvalidData
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if streamer, ok := engine.(EdgeTypeStreamer); ok {
		return streamer.StreamEdgesByType(ctx, edgeType, visit)
	}
	return visitEdgeSlice(ctx, engine.GetEdgesByType, edgeType, visit)
}

// streamEdgesByTypeInScope is the scoped stream of an inner engine for the
// wrapping engines: the scoped stream when the inner engine has it, the
// unscoped stream (or GetEdgesByType) otherwise.
func streamEdgesByTypeInScope(ctx context.Context, engine Engine, scope, edgeType string, visit func(*Edge) error) error {
	if engine == nil || visit == nil {
		return ErrInvalidData
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if scoped, ok := engine.(ScopedEdgeTypeStreamer); ok {
		return scoped.StreamEdgesByTypeInScope(ctx, scope, edgeType, visit)
	}
	return StreamEdgesByType(ctx, engine, edgeType, visit)
}

func visitEdgeSlice(ctx context.Context, load func(string) ([]*Edge, error), edgeType string, visit func(*Edge) error) error {
	edges, err := load(edgeType)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return err
		}
		if edge == nil {
			continue
		}
		if err := visit(edge); err != nil {
			return err
		}
	}
	return nil
}

// edgeIDInScope reports whether id belongs to the database scope ("" is every
// database).
func edgeIDInScope(id EdgeID, scope string) bool {
	return scope == "" || strings.HasPrefix(string(id), scope)
}

// ============================================================================
// BadgerEngine
// ============================================================================

// StreamEdgesByType iterates the edge type index in one read transaction and
// decodes one edge record at a time (EdgeTypeStreamer).
func (b *BadgerEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return b.StreamEdgesByTypeInScope(ctx, "", edgeType, visit)
}

// StreamEdgesByTypeInScope is StreamEdgesByType within one database
// (ScopedEdgeTypeStreamer). Index entries of edges outside scope are skipped
// after resolving the edge ID from the in-memory ID dictionary, before their
// records are read or decoded. An empty edgeType streams every edge, matching
// GetEdgesByType("").
func (b *BadgerEngine) StreamEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurScan)
	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Write-behind overlay: acknowledged-but-unflushed edges replace or
	// shadow committed rows for their IDs and are visited after the scan.
	// An empty edgeType streams every edge, so it uses the whole-buffer
	// edge overlay.
	var overlay []*Edge
	var touched map[EdgeID]bool
	if b.writeBehind != nil {
		if edgeType != "" {
			overlay, touched = b.writeBehind.TypeOverlay(edgeType)
		} else {
			overlay, touched = b.writeBehind.AllEdgesOverlay()
		}
	}

	nowNanos := DecayScoringTime()
	emit := func(edge *Edge) error {
		if edge == nil || b.filterEdgeByDecay(edge, nowNanos) {
			return nil
		}
		if len(touched) > 0 && touched[edge.ID] {
			return nil // the overlay shadows committed rows for this ID
		}
		return visit(edge)
	}
	err := b.withView(func(txn *badger.Txn) error {
		if edgeType == "" {
			return b.streamAllEdgesInScopeTxn(ctx, txn, scope, emit)
		}
		prefix := edgeTypeIndexPrefix(edgeType)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Item().Key()
			edgeNum, ok := extractEdgeNumIDFromEdgeTypeKey(key)
			if !ok {
				continue
			}
			edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok || edgeID == "" || !edgeIDInScope(edgeID, scope) {
				continue
			}
			if checkTombstones && hasIndexTombstone(txn, it.Item().KeyCopy(nil)) {
				continue
			}
			edge, err := b.readEdgeInTxn(txn, edgeID)
			if err != nil {
				return err
			}
			if err := emit(edge); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, e := range overlay {
		if !edgeIDInScope(e.ID, scope) || b.filterEdgeByDecay(e, nowNanos) {
			continue
		}
		if err := visit(copyEdge(e)); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// readEdgeInTxn decodes one edge record; a missing record is (nil, nil).
func (b *BadgerEngine) readEdgeInTxn(txn *badger.Txn, edgeID EdgeID) (*Edge, error) {
	item, err := txn.Get(edgeKey(edgeID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var edge *Edge
	err = item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
		return decodeErr
	})
	return edge, err
}

func (b *BadgerEngine) streamAllEdgesInScopeTxn(ctx context.Context, txn *badger.Txn, scope string, emit func(*Edge) error) error {
	prefix := []byte{prefixEdge}
	if scope != "" {
		prefix = edgeKey(EdgeID(scope))
	}
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Item().Key()
		if len(key) <= 1 {
			continue
		}
		edgeID := EdgeID(key[1:])
		var edge *Edge
		if err := it.Item().Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = b.decodeEdgeBodyByID(val, edgeID)
			return decodeErr
		}); err != nil {
			return err
		}
		if err := emit(edge); err != nil {
			return err
		}
	}
	return nil
}

// streamEdgesByTypeVisibleAtSnapshotWithView is the streaming form of
// getEdgesByTypeVisibleAtSnapshotWithView: type candidates come from the same
// physical snapshot an explicit transaction reads, one edge at a time.
func (b *BadgerEngine) streamEdgesByTypeVisibleAtSnapshotWithView(ctx context.Context, scope, edgeType string, version MVCCVersion, view func(func(*badger.Txn) error) error, visit func(*Edge) error) error {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return err
	}
	defer deregister()
	if ctx == nil {
		ctx = context.Background()
	}
	return view(func(txn *badger.Txn) error {
		if edgeType == "" {
			return b.iterateEdgesVisibleAtInTxn(txn, version, func(edge *Edge) error {
				if edge == nil || !edgeIDInScope(edge.ID, scope) {
					return nil
				}
				return visit(edge)
			})
		}
		prefix := edgeTypeIndexPrefix(edgeType)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			edgeNum, ok := extractEdgeNumIDFromEdgeTypeKey(it.Item().Key())
			if !ok {
				continue
			}
			edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok || edgeID == "" || !edgeIDInScope(edgeID, scope) {
				continue
			}
			edge, getErr := b.getEdgeVisibleAtInTxn(txn, edgeID, version)
			if getErr == ErrNotFound || getErr == ErrNotVisibleAtSnapshot {
				continue
			}
			if getErr != nil {
				return getErr
			}
			if edge != nil && edge.Type == edgeType {
				if err := visit(edge); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// ============================================================================
// Wrapping engines
// ============================================================================

// StreamEdgesByType forwards the type stream to the underlying engine.
func (w *WALEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return StreamEdgesByType(ctx, w.engine, edgeType, visit)
}

// StreamEdgesByTypeInScope forwards a scoped type stream (ScopedEdgeTypeStreamer).
func (w *WALEngine) StreamEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error {
	return streamEdgesByTypeInScope(ctx, w.engine, scope, edgeType, visit)
}

// StreamEdgesByType forwards the type stream to the traced engine.
func (t *TracedEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return StreamEdgesByType(ctx, t.Engine, edgeType, visit)
}

// StreamEdgesByTypeInScope forwards a scoped type stream (ScopedEdgeTypeStreamer).
func (t *TracedEngine) StreamEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error {
	return streamEdgesByTypeInScope(ctx, t.Engine, scope, edgeType, visit)
}

// StreamEdgesByType streams this namespace's edges of edgeType with
// user-facing (unprefixed) IDs. The inner stream is scoped to the namespace,
// so other databases' edges are skipped before they are decoded.
func (n *NamespacedEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	return streamEdgesByTypeInScope(ctx, n.inner, n.nodeScope(), edgeType, func(edge *Edge) error {
		if edge == nil || !n.hasEdgePrefix(edge.ID) {
			return nil
		}
		return visit(n.toUserEdge(edge))
	})
}

// StreamEdgesByType streams deduplicated edges of edgeType from the readable
// constituents, one constituent at a time.
func (c *CompositeEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	seen := make(map[EdgeID]struct{})
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		var visitErr error
		err = StreamEdgesByType(ctx, engine, edgeType, func(edge *Edge) error {
			if edge == nil {
				return nil
			}
			if _, exists := seen[edge.ID]; exists {
				return nil
			}
			seen[edge.ID] = struct{}{}
			if err := visit(edge); err != nil {
				visitErr = err
				return err
			}
			return nil
		})
		if visitErr != nil {
			return visitErr
		}
		if err != nil {
			return localizedError(localization.StorageCompositeConstituentQueryFailed(alias, err), err)
		}
	}
	return nil
}

var (
	_ ScopedEdgeTypeStreamer = (*BadgerEngine)(nil)
	_ ScopedEdgeTypeStreamer = (*WALEngine)(nil)
	_ ScopedEdgeTypeStreamer = (*TracedEngine)(nil)
	_ EdgeTypeStreamer       = (*NamespacedEngine)(nil)
	_ EdgeTypeStreamer       = (*CompositeEngine)(nil)
	_ EdgeTypeStreamer       = (*BadgerTransaction)(nil)
)
