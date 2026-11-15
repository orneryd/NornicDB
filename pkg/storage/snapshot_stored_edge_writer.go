package storage

import (
	"context"
	"errors"
)

// errStoredSnapshotUnsupported reports a stored read that has no native read
// point to pin: the engine's stored read is an override or a non-native
// database (composite, remote). The caller keeps the acquisition it already
// uses for that engine.
var errStoredSnapshotUnsupported = errors.New("storage: pinned stored snapshot is unsupported by this engine")

type snapshotStoredEdgeWriter struct {
	write func(byte, any) error
	ids   *NamespacedEngine
	// pin, when set, reads the resolved native store at one pinned read view
	// instead of its current point (storedSnapshotSource).
	pin *storedSnapshotSource
}

func writeSnapshotEdges(ctx context.Context, engine Engine, write func(byte, any) error) error {
	writer := snapshotStoredEdgeWriter{write: write}
	return writer.stream(ctx, engine, "")
}

// stream dispatches through the stored-reader owner. Only canonical physical
// readers expose bodies: arbitrary stored readers (including decorators that
// override a stored stream) retain their enumeration semantics via the general
// interface. A pinned source reads the same resolution at its own read point.
func (w *snapshotStoredEdgeWriter) stream(ctx context.Context, engine Engine, scope string) error {
	resolved := resolveStoredRead(engine, scope, w.ids)
	w.ids = resolved.ids
	if resolved.native != nil {
		if w.pin != nil {
			return w.pin.streamSnapshotEdgeBodies(ctx, resolved, w)
		}
		return resolved.native.streamSnapshotEdgeBodies(ctx, resolved.scope, w)
	}
	if w.pin != nil {
		// A pin cannot serve an override's own enumeration: its caller keeps the
		// acquisition that enumerates.
		return errStoredSnapshotUnsupported
	}
	if resolved.scope == "" {
		// Prefer the reader's public method here: an arbitrary decorator may
		// override it while inheriting a scoped method from its embedded engine.
		return StreamStoredEdgesByType(ctx, resolved.engine, "", w.visit)
	}
	return streamStoredEdgesByTypeInScope(ctx, resolved.engine, resolved.scope, "", func(edge *Edge) error {
		if edge == nil || !edgeIDInScope(edge.ID, resolved.scope) {
			return nil
		}
		return w.visit(edge)
	})
}

func (w *snapshotStoredEdgeWriter) visit(edge *Edge) error {
	if w.ids != nil {
		edge = w.ids.toUserEdge(edge)
	}
	return w.write(streamSnapshotEdge, edge)
}

func (b *BadgerEngine) streamSnapshotEdgeBodies(ctx context.Context, scope string, writer *snapshotStoredEdgeWriter) error {
	var transcoder snapshotEdgeTranscoder
	body := snapshotEdgeBodyWriter(b, &transcoder, writer.ids, writer.write)
	return b.streamEdgesByTypeInScopeWithBodies(ctx, scope, "", storedRead, writer.visit, body)
}
