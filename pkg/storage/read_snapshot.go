// Author: Codex implementation lane, 2026-10-27 (NOR-148 visible snapshot adapter).
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

// VisibleReadSnapshot exposes the canonical user-visible node population at a
// captured view. IterateNodes retains its stored-entity build contract; this
// adapter applies the same visibility and decode-error policy as AllNodes.
type VisibleReadSnapshot interface {
	ReadSnapshotView
	IterateVisibleNodes(context.Context, func(*Node) bool) error
}

type readSnapshotPrefixKey struct{}

// badgerReadSnapshot is BadgerEngine's ReadSnapshotView: a long-lived read
// view of the storage capture owner (captureReadView) and the write-behind
// overlay captured with it. The view holds only its registered read
// timestamp between scans, which keeps compaction from discarding a version
// it can see until Close; each scan opens its own transaction at it.
type badgerReadSnapshot struct {
	reader        SnapshotReader
	revision      SnapshotReadRevision
	revisionValid bool
	prefix        string
	b             *BadgerEngine
	view          *badgerReadView
	overlayNodes  []*Node
	nodesTouched  map[NodeID]bool
	overlayEdges  []*Edge
	edgesTouched  map[EdgeID]bool
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
	prefix, _ := ctx.Value(readSnapshotPrefixKey{}).(string)
	snap := &badgerReadSnapshot{b: b, prefix: prefix, reader: requestedSnapshotReader(ctx)}
	var before SnapshotReadRevision
	var beforeOK bool
	if snap.reader != SnapshotStored {
		before, beforeOK = b.snapshotReadRevision(snap.reader)
	}
	view, err := b.captureReadView(func(w *WriteBehindBuffer) {
		// Nodes are read whatever their visibility, relationships as visible
		// reads see them, as their stored rows below.
		if snap.reader != SnapshotAllEdges && snap.reader != SnapshotStreamEdges {
			pendingNodes := b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedNodes, prefix: prefix})
			snap.overlayNodes, snap.nodesTouched = pendingNodes.nodes, pendingNodes.nodesTouched
		}
		if snap.reader != SnapshotAllNodes {
			pendingEdges := b.readBufferedEntities(w, visibleRead, bufferedEntityRead{kind: bufferedEdges, prefix: prefix})
			snap.overlayEdges, snap.edgesTouched = pendingEdges.edges, pendingEdges.edgesTouched
		}
	}, readViewOptions{longLived: true, scope: prefix, scopeKind: readPrefixScope})
	if err != nil {
		return nil, err
	}
	snap.view = view
	if snap.reader != SnapshotStored {
		after, afterOK := b.snapshotReadRevision(snap.reader)
		snap.revision, snap.revisionValid = after, beforeOK && afterOK && before == after && after.timestamp == view.readTs
		snap.revision.timestamp = view.readTs
	}
	return snap, nil
}

// IterateNodes is BadgerEngine.IterateNodes at the snapshot, with the
// write-behind overlay merged as StreamEdges merges it: a buffered node
// shadows its committed row and is visited after the scan.
func (s *badgerReadSnapshot) IterateNodes(fn func(*Node) bool) error {
	return s.iterateNodes(fn, true)
}

// IterateVisibleNodes uses the same captured transaction/overlay iterator and
// canonical visibility kernel as ordinary user reads. Cancellation stops scans.
func (s *badgerReadSnapshot) IterateVisibleNodes(ctx context.Context, fn func(*Node) bool) error {
	now := DecayScoringTime()
	err := s.iterateNodes(func(node *Node) bool {
		if ctx.Err() != nil {
			return false
		}
		if s.b.filterEntityByDecay(visibleRead, node, now) {
			return true
		}
		return fn(node)
	}, false)
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (s *badgerReadSnapshot) iterateNodes(fn func(*Node) bool, skipInvalid bool) error {
	stopped := false
	err := s.view.withView(func(txn *badger.Txn) error {
		// The engine's one node-row scan, over the captured transaction.
		// Captured scans decode once into the retained population; the
		// canonical decoder/hydrator needs no additional body-cache copy.
		// Visibility is applied by IterateVisibleNodes to committed and overlay
		// nodes alike, so the row scan itself keeps every stored row.
		rows := nodeRowScan{
			scope:      rowsWithPrefix(s.prefix),
			overlay:    overlayReplaces(s.nodesTouched),
			decode:     decodePolicyFor(skipInvalid), // skipping is the stored build iterator's contract
			visibility: scanStored,
		}
		return s.b.streamNodesWithOptionsInTxn(context.Background(), txn, rows, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
			if !fn(node) {
				stopped = true
				return ErrIterationStopped
			}
			return nil
		})
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
	return s.iterateEdges(ctx, fn, true)
}

// IterateAllEdges is AllEdges at the captured point, including decode errors.
func (s *badgerReadSnapshot) IterateAllEdges(ctx context.Context, fn func(*Edge) error) error {
	return s.iterateEdges(ctx, fn, false)
}

func (s *badgerReadSnapshot) iterateEdges(ctx context.Context, fn func(*Edge) error, skipInvalid bool) error {
	stopped := false
	err := s.view.withView(func(txn *badger.Txn) error {
		var err error
		stopped, err = s.b.scanEdgeRows(ctx, txn, edgeRowScan{scope: rowsWithPrefix(s.prefix), overlay: overlayReplaces(s.edgesTouched), decode: decodePolicyFor(skipInvalid), visibility: scanVisible}, fn)
		return err
	})
	if err != nil {
		return err
	}
	if stopped {
		return nil
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
	if requestedSnapshotReader(ctx) != SnapshotStored {
		issued, ok := engine.(Engine)
		owner, owns := engine.(SnapshotReaderOwner)
		if !ok || !owns || !owner.OwnsSnapshotReader(issued) {
			return nil, ErrReadSnapshotUnsupported
		}
	}
	if snapshotter, ok := engine.(ReadSnapshotter); ok {
		return snapshotter.OpenReadSnapshot(ctx)
	}
	return nil, ErrReadSnapshotUnsupported
}

// OpenReadSnapshot forwards to the engine the WAL writes through: the WAL
// applies every write to it before acknowledging, so it holds no other
// state a read could see.
func (w *WALEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	return OpenReadSnapshot(SnapshotStreamFallbackContext(ctx, w.engine), w.engine)
}

// OpenReadSnapshot forwards to the traced engine.
func (t *TracedEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	return OpenReadSnapshot(ctx, t.Engine)
}

// OpenReadSnapshot opens the inner engine's snapshot and scopes it to this
// namespace, translating IDs as IterateNodes and StreamEdges do.
func (n *NamespacedEngine) OpenReadSnapshot(ctx context.Context) (ReadSnapshotView, error) {
	ctx = SnapshotStreamFallbackContext(ctx, n.inner)
	if requestedSnapshotReader(ctx) == SnapshotStored {
		prefix, _ := ctx.Value(readSnapshotPrefixKey{}).(string)
		ctx = context.WithValue(ctx, readSnapshotPrefixKey{}, string(n.PrefixNodeID(NodeID(prefix))))
	}

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

// IterateVisibleNodes forwards visibility to the captured owner before applying
// this namespace's ID view, exactly as the ordinary namespaced node population.
func (s *namespacedReadSnapshot) IterateVisibleNodes(ctx context.Context, fn func(*Node) bool) error {
	inner, ok := s.inner.(VisibleReadSnapshot)
	if !ok {
		return ErrReadSnapshotUnsupported
	}
	return inner.IterateVisibleNodes(ctx, func(node *Node) bool {
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
