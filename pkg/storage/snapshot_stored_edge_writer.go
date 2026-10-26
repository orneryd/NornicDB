package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
)

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
	// Select through the shared cursor, then keep the selected provider and
	// namespace adapter for this stream. Frames cross the common delivery
	// boundary, so refusal after a write can never replay the snapshot.
	return VisitWithCapability(engine, func(reader StoredEntityReader, deliver func(any) error) error {
		selected := *w
		selected.write = func(_ byte, frame any) error { return deliver(frame) }
		switch source := reader.(type) {
		case *BadgerEngine:
			return source.streamSnapshotEdgeBodies(ctx, scope, &selected)
		case *MemoryEngine:
			return source.BadgerEngine.streamSnapshotEdgeBodies(ctx, scope, &selected)
		case *NamespacedEngine:
			if scope == "" && selected.ids == nil {
				selected.ids = source
				return selected.stream(ctx, source.inner, source.nodeScope())
			}
		}
		if scope == "" {
			return reader.StreamStoredEdgesByType(ctx, "", selected.visit)
		}
		return streamStoredEdgesByTypeInScope(ctx, engine, scope, "", func(edge *Edge) error {
			if edge == nil || !edgeIDInScope(edge.ID, scope) {
				return nil
			}
			return selected.visit(edge)
		})
	}, func(deliver func(any) error) error {
		return StreamEdgesByType(ctx, engine, "", func(edge *Edge) error {
			if edge == nil || !edgeIDInScope(edge.ID, scope) {
				return nil
			}
			if w.ids != nil {
				edge = w.ids.toUserEdge(edge)
			}
			return deliver(edge)
		})
	}, func(frame any) error { return w.write(streamSnapshotEdge, frame) })
}

func (w *snapshotStoredEdgeWriter) visit(edge *Edge) error {
	if w.ids != nil {
		edge = w.ids.toUserEdge(edge)
	}
	return w.write(streamSnapshotEdge, edge)
}

func (b *BadgerEngine) streamSnapshotEdgeBodies(ctx context.Context, scope string, writer *snapshotStoredEdgeWriter) error {
	var transcoder snapshotEdgeTranscoder
	body := func(txn *badger.Txn, id EdgeID, data []byte) (bool, error) {
		return transcoder.writeBody(b, embeddingRecordReader{txn: txn, ctx: ctx}, id, data, writer.ids, writer.write)
	}
	return b.streamEdgesByTypeInScopeWithBodies(ctx, scope, "", storedRead, writer.visit, body)
}
