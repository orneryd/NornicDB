package storage

import (
	"context"
)

// storedSnapshotSource is an internal pinned, stored-inclusive read point of one
// native store: the node and relationship population a snapshot exports, read
// through the scan kernels the engine's own scans use. It captures one
// long-lived read view (captureReadView) together with the write-behind overlay
// read at that same point (readBufferedEntities), so every scan it serves sees
// storage as it stood at the pin: the entities the visibility layer hides and
// the vectors kept beside their bodies included.
//
// It is the read side of a snapshot. The barrier and the WAL cut that decide
// when a snapshot may be taken belong to its caller.
type storedSnapshotSource struct {
	engine Engine
	target storedReadResolution
	view   *badgerReadView
	nodes  capturedNodes
	edges  capturedEdges
}

// openStoredSnapshotSource pins engine's stored-inclusive population. It
// resolves the stored read exactly as the snapshot write does
// (resolveStoredRead), so an engine whose stored read is an override or a
// non-native database has no native pin and reports
// errStoredSnapshotUnsupported: its caller keeps the acquisition it already
// uses for that engine.
func openStoredSnapshotSource(ctx context.Context, engine Engine) (*storedSnapshotSource, error) {
	target := resolveStoredRead(engine, "", nil)
	if target.native == nil {
		return nil, errStoredSnapshotUnsupported
	}
	b := target.native
	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	source := &storedSnapshotSource{engine: engine, target: target}
	var captureErr error
	view, err := b.captureReadView(ctx, func(w *WriteBehindBuffer) {
		// A snapshot holds what storage holds: the hidden entities are stored
		// data too, so both overlays are read with storedRead.
		if err := source.nodes.capture(ctx, b, w, storedRead, target.scope); err != nil {
			captureErr = err
			return
		}
		captureErr = source.edges.capture(ctx, b, w, storedRead, target.scope, "")
	}, readViewOptions{longLived: true, scope: target.scope, scopeKind: readPrefixScope})
	if err != nil {
		return nil, err
	}
	if captureErr != nil {
		view.Close()
		return nil, captureErr
	}
	source.view = view
	return source, nil
}

// StreamNodes visits every stored node of the pin, the ones the visibility layer
// hides included, hydrated as the engine's own scan hydrates them (stored
// vectors from their sidecars). A namespaced pin delivers this database's nodes
// with user-facing IDs, as NamespacedEngine.StreamNodesWithOptions does. The
// caller applies its record encoding, as it does to the live stream.
func (s *storedSnapshotSource) StreamNodes(ctx context.Context, visit func(*Node) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	opts := StreamNodesOptions{Prefix: s.target.scope, WithEmbeddings: true}
	fn := visit
	if ids := s.target.ids; ids != nil {
		fn = func(node *Node) error { return visit(ids.toUserNode(node, nodeUserView{})) }
	}
	return s.target.native.streamNodesWithOptionsAtView(ctx, s.view.withView, &s.nodes, opts, fn)
}

// StreamEdges writes every stored relationship of the pin through the snapshot
// record writer: the same enumeration, namespace translation and stored-body
// transcoder the live snapshot write uses, at this pin.
func (s *storedSnapshotSource) StreamEdges(ctx context.Context, write func(byte, any) error) error {
	if write == nil {
		return ErrInvalidData
	}
	writer := snapshotStoredEdgeWriter{write: write, pin: s}
	return writer.stream(ctx, s.engine, "")
}

// streamSnapshotEdgeBodies is the pinned counterpart of
// BadgerEngine.streamSnapshotEdgeBodies: the same snapshot body transcoder over
// the resolved stored edges of this pin.
func (s *storedSnapshotSource) streamSnapshotEdgeBodies(ctx context.Context, resolved storedReadResolution, writer *snapshotStoredEdgeWriter) error {
	if resolved.native != s.target.native || resolved.scope != s.target.scope {
		return errStoredSnapshotUnsupported
	}
	var transcoder snapshotEdgeTranscoder
	body := snapshotEdgeBodyWriter(s.target.native, &transcoder, writer.ids, writer.write)
	return s.target.native.streamEdgesByTypeInScopeAtView(ctx, s.view.withView, &s.edges, s.target.scope, "", storedRead, writer.visit, body)
}

// Close releases the pin's read view; later scans fail. It waits for a scan in
// progress and is idempotent (badgerReadView.Close).
func (s *storedSnapshotSource) Close() { s.view.Close() }
