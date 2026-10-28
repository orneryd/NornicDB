package storage

import "context"

type snapshotStoredEdgeWriter struct {
	write func(byte, any) error
	ids   *NamespacedEngine
}

func writeSnapshotEdges(ctx context.Context, engine Engine, write func(byte, any) error) error {
	writer := snapshotStoredEdgeWriter{write: write}
	return writer.stream(ctx, engine, "")
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
	if scope == "" {
		// Prefer the reader's public method here: an arbitrary decorator may
		// override it while inheriting a scoped method from its embedded engine.
		return StreamStoredEdgesByType(ctx, engine, "", w.visit)
	}
	return streamStoredEdgesByTypeInScope(ctx, engine, scope, "", func(edge *Edge) error {
		if edge == nil || !edgeIDInScope(edge.ID, scope) {
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
	body := func(id EdgeID, data []byte) (bool, error) {
		return transcoder.writeBody(b, id, data, writer.ids, writer.write)
	}
	return b.streamEdgesByTypeInScopeWithBodies(ctx, scope, "", storedRead, writer.visit, body)
}
