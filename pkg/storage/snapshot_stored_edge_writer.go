package storage

import "context"

type snapshotStoredEdgeWriter struct {
	frames *snapshotFrameWriter
	limit int64
	count uint64
	ids *NamespacedEngine
}

func writeSnapshotEdges(ctx context.Context, engine Engine, frames *snapshotFrameWriter, limit int64) (uint64, error) {
	writer := snapshotStoredEdgeWriter{frames: frames, limit: limit}
	err := writer.stream(ctx, engine, "")
	return writer.count, err
}

// stream dispatches through the stored-reader owner. Only canonical physical
// readers expose bodies: arbitrary stored readers (including decorators that
// override a stored stream) retain their enumeration semantics via the general
// interface. Transparent wrappers are resolved by StoredReader itself.
func (w *snapshotStoredEdgeWriter) stream(ctx context.Context, engine Engine, scope string) error {
	if reader, ok := StoredReader(engine); ok {
		switch source := reader.(type) {
		case *BadgerEngine:
			return source.streamSnapshotEdgeBodies(ctx, scope, w)
		case *MemoryEngine:
			return source.BadgerEngine.streamSnapshotEdgeBodies(ctx, scope, w)
		case *WALEngine:
			return w.stream(ctx, source.engine, scope)
		case *NamespacedEngine:
			if scope == "" && w.ids == nil {
				w.ids = source
				return w.stream(ctx, source.inner, source.nodeScope())
			}
		}
	}
	return streamStoredEdgesByTypeInScope(ctx, engine, scope, "", func(edge *Edge) error {
		if edge == nil || !edgeIDInScope(edge.ID, scope) { return nil }
		return w.visit(edge)
	})
}

func (w *snapshotStoredEdgeWriter) visit(edge *Edge) error {
	if w.ids != nil { edge = w.ids.toUserEdge(edge) }
	if err := w.frames.write(streamSnapshotEdge, edge, w.limit); err != nil { return err }
	w.count++
	return nil
}

func (b *BadgerEngine) streamSnapshotEdgeBodies(ctx context.Context, scope string, writer *snapshotStoredEdgeWriter) error {
	var transcoder snapshotEdgeTranscoder
	body := func(id EdgeID, data []byte) (bool, error) {
		fast, err := transcoder.writeBody(b, id, data, writer.ids, writer.frames, writer.limit)
		if fast && err == nil { writer.count++ }
		return fast, err
	}
	return b.streamEdgesByTypeInScopeWithBodies(ctx, scope, "", storedRead, writer.visit, body)
}
