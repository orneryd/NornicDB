package storage

import (
	"context"
	"sync"
)

// A scan can re-enter the transaction while its callback runs without the
// transaction mutex. Borrow an independent visitor for each active scan;
// its bound callback is created once rather than for every options read.
type queryNodeScanVisitor struct {
	ctx      context.Context
	prefix   string
	fn       func(*Node) error
	callback func(*Node) error
}

var queryNodeScanVisitors = sync.Pool{New: func() any {
	visitor := &queryNodeScanVisitor{}
	visitor.callback = visitor.visit
	return visitor
}}

func borrowQueryNodeScanVisitor(ctx context.Context, opts StreamNodesOptions, fn func(*Node) error) *queryNodeScanVisitor {
	visitor := queryNodeScanVisitors.Get().(*queryNodeScanVisitor)
	visitor.ctx, visitor.prefix, visitor.fn = ctx, opts.Prefix, fn
	return visitor
}

func (visitor *queryNodeScanVisitor) visit(node *Node) error {
	if err := visitor.ctx.Err(); err != nil {
		return err
	}
	if !nodeIDInScope(node.ID, visitor.prefix) {
		return nil
	}
	// The transaction label stream delivers caller-owned nodes, already
	// stripped in their delivery shape.
	return visitor.fn(node)
}

func (visitor *queryNodeScanVisitor) release() {
	visitor.ctx, visitor.prefix, visitor.fn = nil, "", nil
	queryNodeScanVisitors.Put(visitor)
}

// nodeScanOptions preserves the full, visible read used by existing private
// scan callers; options-driven query scans pass their complete contract.
func nodeScanOptions(options []StreamNodesOptions) StreamNodesOptions {
	if len(options) > 0 {
		return options[0]
	}
	return StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}
}

func (opts StreamNodesOptions) readVisibility() readVisibility {
	if opts.ApplyDecayFilter {
		return visibleRead
	}
	return storedRead
}

func (opts StreamNodesOptions) copyResidentNode(node *Node) *Node {
	return copyShaped(node, opts.Projection, opts.StripEmbeddings)
}
