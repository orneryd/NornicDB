package storage

import "context"

// Method-sized contracts prevent support for node streaming from depending on
// an unrelated edge or chunk forward. Compound consumer requests still require
// all of their methods through the normal capability cursor.
type nodeStreamer interface {
	StreamNodes(context.Context, func(*Node) error) error
}
type edgeStreamer interface {
	StreamEdges(context.Context, func(*Edge) error) error
}
type nodeChunkStreamer interface {
	StreamNodeChunks(context.Context, int, func([]*Node) error) error
}

// nodeReadOperation is the ordinary node fallback's semantic key. The engine
// is the original view (including its namespace, constituent, snapshot and
// pending state); options retain projection, embeddings and visible/stored
// semantics. This current-read operation is never a historical fallback.
// Label and prefix are checked before delivery even if a kernel treats them
// as hints. Selection happens outside the row loop.
type nodeReadOperation struct {
	view    Engine
	options StreamNodesOptions
}

func (op nodeReadOperation) visit(ctx context.Context, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if visit == nil {
		return ErrInvalidData
	}
	return op.view.StreamNodesWithOptions(ctx, op.options, func(node *Node) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if node == nil || !nodeIDInScope(node.ID, op.options.Prefix) {
			return nil
		}
		if op.options.Label != "" {
			found := false
			for _, label := range node.Labels {
				if label == op.options.Label {
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}
		return visit(node)
	})
}

// streamNodeChunksCore bounds working storage by the requested chunk. Rows may
// be borrowed by a kernel, so retain owned copies until the chunk is delivered.
func streamNodeChunksCore(ctx context.Context, engine Engine, options StreamNodesOptions, size int, visit func([]*Node) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if size <= 0 {
		size = 1
	}
	chunk := make([]*Node, 0, size)
	err := (nodeReadOperation{engine, options}).visit(ctx, func(node *Node) error {
		chunk = append(chunk, CopyNode(node))
		if len(chunk) < size {
			return nil
		}
		err := visit(chunk)
		chunk = chunk[:0]
		return err
	})
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		return visit(chunk)
	}
	return nil
}

// streamEdgesCore is the ordinary edge contract. There is no mandatory edge
// kernel: only this exact incapable view may need its materialized edge API.
func streamEdgesCore(ctx context.Context, engine Engine, visit func(*Edge) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	values, err := engine.AllEdges()
	if err != nil {
		return err
	}
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(value); err != nil {
			return err
		}
	}
	return nil
}

// The type and callback protocol are part of this ordinary edge operation's
// semantic key. A native type index remains the terminal fallback; a child
// with an ordinary edge stream retains streaming and early termination.
func streamOrdinaryEdgesByType(ctx context.Context, engine Engine, kind string, visit func(*Edge) error) error {
	return VisitWithCapability(engine, func(reader edgeStreamer, deliver func(*Edge) error) error {
		return reader.StreamEdges(ctx, func(edge *Edge) error {
			if edge != nil && (kind == "" || edge.Type == kind) {
				return deliver(edge)
			}
			return nil
		})
	}, func(deliver func(*Edge) error) error {
		return visitEdgeSlice(ctx, engine.GetEdgesByType, kind, deliver)
	}, visit)
}
