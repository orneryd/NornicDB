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
	strip    bool
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
	visitor.ctx, visitor.prefix, visitor.strip, visitor.fn = ctx, opts.Prefix, opts.StripEmbeddings, fn
	return visitor
}

func (visitor *queryNodeScanVisitor) visit(node *Node) error {
	if err := visitor.ctx.Err(); err != nil {
		return err
	}
	if !nodeIDInScope(node.ID, visitor.prefix) {
		return nil
	}
	if visitor.strip {
		node = copyNodeWithoutEmbeddings(node)
	}
	return visitor.fn(node)
}

func (visitor *queryNodeScanVisitor) release() {
	visitor.ctx, visitor.prefix, visitor.strip, visitor.fn = nil, "", false, nil
	queryNodeScanVisitors.Put(visitor)
}

// nodeScanOptions preserves the full, visible read used by existing private
// scan callers; options-driven query scans pass their complete contract.
func nodeScanOptions(options []StreamNodesOptions) StreamNodesOptions {
	if len(options) > 0 {
		return options[0]
	}
	return fullNodeStreamOptions("")
}

func (opts StreamNodesOptions) readVisibility() readVisibility {
	if opts.ApplyDecayFilter {
		return visibleRead
	}
	return storedRead
}

// scanDecodesFilter reports whether the scan's options carry a property filter,
// so a whole-node metadata fast path must yield to the scan decoder that
// applies it before the node is materialized.
func (opts StreamNodesOptions) scanDecodesFilter() bool {
	return opts.PropertyFilter != nil || len(opts.PropertyStringEquals) > 0
}

// keepsResidentNode settles the scan's property filter on a decoded node, as
// the scan decoder settles it on a stored body: a property that must equal a
// string is rejected when it is stored with any other value, and
// PropertyFilter reads the node's properties. It is given all of them, where
// the scan decoder gives it the ones the scan names: a filter that tests the
// properties it named, as every filter Cypher builds does, decides the same
// on both, and one that looks at which properties the map holds may not. The
// filter reads node and keeps nothing of it.
func (opts StreamNodesOptions) keepsResidentNode(node *Node) bool {
	for name, required := range opts.PropertyStringEquals {
		if value, stored := node.Properties[name]; stored {
			if text, isString := value.(string); !isString || text != required {
				return false
			}
		}
	}
	return opts.PropertyFilter == nil || opts.PropertyFilter(node.Properties)
}

func (opts StreamNodesOptions) copyResidentNode(node *Node) *Node {
	return projectCachedNodeForRead(node, opts.Projection, opts.StripEmbeddings)
}
