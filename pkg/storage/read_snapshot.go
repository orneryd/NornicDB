package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// ReadSnapshotter opens one consistent view of storage. Every read through
// the snapshot sees the store as of one point, acknowledged write-behind
// entries included, until Close. A search index build reads its whole
// baseline (nodes and relationships) through one snapshot, so the baseline
// is a state the store actually had.
type ReadSnapshotter interface {
	OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error)
}

// ReadSnapshotView is a consistent view of storage opened by OpenReadSnapshot.
// Its reads may run one after another, each seeing the same point. Close
// releases it; reads after Close fail.
type ReadSnapshotView interface {
	IterateNodes(fn func(*Node) bool) error
	StreamEdges(ctx context.Context, fn func(*Edge) error) error
	Close()
}

// badgerReadSnapshot is BadgerEngine's ReadSnapshotView: a long-lived read
// view of the storage capture owner (captureReadView) and the write-behind
// overlay captured with it. The view holds only its registered read
// timestamp between scans, which keeps compaction from discarding a version
// it can see until Close; each scan opens its own transaction at it.
type badgerReadSnapshot struct {
	b            *BadgerEngine
	view         *badgerReadView
	overlayNodes []*Node
	nodesTouched map[NodeID]bool
	overlayEdges []*Edge
	edgesTouched map[EdgeID]bool
}

// OpenReadSnapshot captures a long-lived read view and, at the same point,
// the whole write-behind overlay (captureReadView).
func (b *BadgerEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	snap := &badgerReadSnapshot{b: b}
	view, err := b.captureReadView(func(w *WriteBehindBuffer) {
		// Nodes are read whatever their visibility, relationships as visible
		// reads see them, as their stored rows below.
		snap.overlayNodes, snap.nodesTouched = b.bufferedAllNodes(w, storedRead)
		snap.overlayEdges, snap.edgesTouched = b.bufferedAllEdges(w, visibleRead)
	}, readViewOptions{longLived: true})
	if err != nil {
		return nil, err
	}
	snap.view = view
	return snap, nil
}

// IterateNodes is BadgerEngine.IterateNodes at the snapshot, with the
// write-behind overlay merged as StreamEdges merges it: a buffered node
// shadows its committed row and is visited after the scan.
func (s *badgerReadSnapshot) IterateNodes(fn func(*Node) bool) error {
	stopped := false
	err := s.view.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			if s.nodesTouched[nodeID] {
				continue // the write-behind overlay shadows this committed row
			}
			var node *Node
			if err := it.Item().Value(func(val []byte) error {
				var decErr error
				node, decErr = s.b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decErr
			}); err != nil {
				continue // Skip invalid nodes, as IterateNodes does
			}
			if !fn(node) {
				stopped = true
				return nil
			}
		}
		return nil
	})
	if err != nil || stopped {
		return err
	}
	for _, node := range s.overlayNodes {
		if !fn(copyNode(node)) {
			return nil
		}
	}
	return nil
}

// StreamEdges is BadgerEngine.StreamEdges at the snapshot, with the
// write-behind overlay captured when the snapshot was opened.
func (s *badgerReadSnapshot) StreamEdges(ctx context.Context, fn func(*Edge) error) error {
	nowNanos := DecayScoringTime()
	err := s.view.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Item().Key()
			var edgeID EdgeID
			if len(key) > 1 {
				edgeID = EdgeID(key[1:])
			}
			if s.edgesTouched[edgeID] {
				continue // the write-behind overlay shadows this committed row
			}
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decErr error
				edge, decErr = s.b.decodeEdgeBodyByID(val, edgeID)
				return decErr
			}); err != nil {
				continue // Skip invalid edges, as StreamEdges does
			}
			if edge == nil || s.b.filterEdgeByDecay(edge, nowNanos) {
				continue
			}
			if err := fn(edge); err != nil {
				if err == ErrIterationStopped {
					return errSnapshotScanStopped
				}
				return err
			}
		}
		return nil
	})
	if err == errSnapshotScanStopped {
		return nil
	}
	if err != nil {
		return err
	}
	for _, edge := range s.overlayEdges {
		if err := fn(copyEdge(edge)); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// errSnapshotScanStopped ends a snapshot scan the caller stopped, before the
// overlay is visited.
var errSnapshotScanStopped = &snapshotScanStopped{}

type snapshotScanStopped struct{}

func (*snapshotScanStopped) Error() string { return "storage: snapshot scan stopped" }

// Close releases the snapshot's read view; later reads fail. It waits for a
// scan in progress and is idempotent (badgerReadView.Close).
func (s *badgerReadSnapshot) Close() { s.view.Close() }

// ErrReadSnapshotUnsupported reports an engine that cannot open a read
// snapshot (CompositeEngine and RemoteEngine span other stores). A search
// build over it does not queue live writes: they wait for the build.
var ErrReadSnapshotUnsupported = errors.New("storage: read snapshots are not supported by this engine")

// OpenReadSnapshot opens a read snapshot of engine, or returns
// ErrReadSnapshotUnsupported when engine has no ReadSnapshotter.
func OpenReadSnapshot(ctx context.Context, engine any) (ReadSnapshotView, error) {
	if snapshotter, ok := engine.(ReadSnapshotter); ok {
		return snapshotter.OpenReadSnapshot(ctx)
	}
	return nil, ErrReadSnapshotUnsupported
}

// OpenReadSnapshot forwards to the engine the WAL writes through: the WAL
// applies every write to it before acknowledging, so it holds no other
// state a read could see.
func (w *WALEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	return OpenReadSnapshot(ctx, w.engine)
}

// OpenReadSnapshot forwards to the traced engine.
func (t *TracedEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	return OpenReadSnapshot(ctx, t.Engine)
}

// OpenReadSnapshot opens the inner engine's snapshot and scopes it to this
// namespace, translating IDs as IterateNodes and StreamEdges do.
func (n *NamespacedEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	inner, err := OpenReadSnapshot(ctx, n.inner)
	if err != nil {
		return nil, err
	}
	return &namespacedReadSnapshot{inner: inner, n: n}, nil
}

type namespacedReadSnapshot struct {
	inner ReadSnapshotView
	n     *NamespacedEngine
}

func (s *namespacedReadSnapshot) IterateNodes(fn func(*Node) bool) error {
	return s.inner.IterateNodes(func(node *Node) bool {
		if node == nil || !s.n.hasNodePrefix(node.ID) {
			return true
		}
		return fn(s.n.toUserNode(node, nodeUserView{}))
	})
}

func (s *namespacedReadSnapshot) StreamEdges(ctx context.Context, fn func(*Edge) error) error {
	return s.inner.StreamEdges(ctx, func(edge *Edge) error {
		if edge == nil || !s.n.hasEdgePrefix(edge.ID) {
			return nil
		}
		return fn(s.n.toUserEdge(edge))
	})
}

func (s *namespacedReadSnapshot) Close() { s.inner.Close() }

// SupportsReadSnapshot reports whether engine can open a read snapshot, by
// opening one and closing it at once (a read-timestamp registration).
func SupportsReadSnapshot(ctx context.Context, engine any) bool {
	snapshot, err := OpenReadSnapshot(ctx, engine)
	if err != nil {
		return false
	}
	snapshot.Close()
	return true
}
