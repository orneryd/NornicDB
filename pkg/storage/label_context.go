package storage

import "context"

// GetNodesByLabelWithContext dispatches the existing operation with caller context.
func GetNodesByLabelWithContext(ctx context.Context, engine interface {
	GetNodesByLabel(label string) ([]*Node, error)
}, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error)
	}); ok {
		return reader.GetNodesByLabelWithContext(ctx, label)
	}
	return engine.GetNodesByLabel(label)
}

// GetStoredNodesByLabelWithContext dispatches the existing operation with caller context.
func GetStoredNodesByLabelWithContext(ctx context.Context, engine Engine, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetStoredNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error)
	}); ok {
		return reader.GetStoredNodesByLabelWithContext(ctx, label)
	}
	if reader, ok := StoredReader(engine); ok {
		if contextual, ok := reader.(interface {
			GetStoredNodesByLabelWithContext(context.Context, string) ([]*Node, error)
		}); ok {
			return contextual.GetStoredNodesByLabelWithContext(ctx, label)
		}
		return reader.GetStoredNodesByLabel(label)
	}
	return GetNodesByLabelWithContext(ctx, engine, label)
}

// GetFirstNodeByLabelWithContext dispatches the existing operation with caller context.
func GetFirstNodeByLabelWithContext(ctx context.Context, engine interface {
	GetFirstNodeByLabel(label string) (*Node, error)
}, label string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetFirstNodeByLabelWithContext(ctx context.Context, label string) (*Node, error)
	}); ok {
		return reader.GetFirstNodeByLabelWithContext(ctx, label)
	}
	return engine.GetFirstNodeByLabel(label)
}

// StreamNodesByLabelProjectedWithContext dispatches the existing operation with caller context.
func StreamNodesByLabelProjectedWithContext(ctx context.Context, engine interface {
	StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
}, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader, ok := engine.(interface {
		StreamNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error
	}); ok {
		return reader.StreamNodesByLabelProjectedWithContext(ctx, label, properties, visit)
	}
	return engine.StreamNodesByLabelProjected(label, properties, visit)
}

// StreamStoredNodesByLabelProjectedWithContext dispatches the existing operation with caller context.
func StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, engine interface {
	StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
}, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader, ok := engine.(interface {
		StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error
	}); ok {
		return reader.StreamStoredNodesByLabelProjectedWithContext(ctx, label, properties, visit)
	}
	return engine.StreamStoredNodesByLabelProjected(label, properties, visit)
}

// ForEachNodeIDByLabelWithContext dispatches the existing operation with caller context.
func ForEachNodeIDByLabelWithContext(ctx context.Context, engine interface {
	ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error
}, label string, visit func(NodeID) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader, ok := engine.(interface {
		ForEachNodeIDByLabelWithContext(ctx context.Context, label string, visit func(NodeID) bool) error
	}); ok {
		return reader.ForEachNodeIDByLabelWithContext(ctx, label, visit)
	}
	return engine.ForEachNodeIDByLabel(label, visit)
}

// GetNodesByLabelInScopeWithContext dispatches the existing operation with caller context.
func GetNodesByLabelInScopeWithContext(ctx context.Context, engine interface {
	GetNodesByLabelInScope(scope, label string) ([]*Node, error)
}, scope, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetNodesByLabelInScopeWithContext(ctx context.Context, scope, label string) ([]*Node, error)
	}); ok {
		return reader.GetNodesByLabelInScopeWithContext(ctx, scope, label)
	}
	return engine.GetNodesByLabelInScope(scope, label)
}

// GetFirstNodeByLabelInScopeWithContext dispatches the existing operation with caller context.
func GetFirstNodeByLabelInScopeWithContext(ctx context.Context, engine interface {
	GetFirstNodeByLabelInScope(scope, label string) (*Node, error)
}, scope, label string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetFirstNodeByLabelInScopeWithContext(ctx context.Context, scope, label string) (*Node, error)
	}); ok {
		return reader.GetFirstNodeByLabelInScopeWithContext(ctx, scope, label)
	}
	return engine.GetFirstNodeByLabelInScope(scope, label)
}

// StreamNodesByLabelProjectedInScopeWithContext dispatches the existing operation with caller context.
func StreamNodesByLabelProjectedInScopeWithContext(ctx context.Context, engine interface {
	StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
}, scope, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader, ok := engine.(interface {
		StreamNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error
	}); ok {
		return reader.StreamNodesByLabelProjectedInScopeWithContext(ctx, scope, label, properties, visit)
	}
	return engine.StreamNodesByLabelProjectedInScope(scope, label, properties, visit)
}

// StreamStoredNodesByLabelProjectedInScopeWithContext dispatches the existing operation with caller context.
func StreamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, engine interface {
	StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
}, scope, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader, ok := engine.(interface {
		StreamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error
	}); ok {
		return reader.StreamStoredNodesByLabelProjectedInScopeWithContext(ctx, scope, label, properties, visit)
	}
	return engine.StreamStoredNodesByLabelProjectedInScope(scope, label, properties, visit)
}

// ForEachNodeIDByLabelInScopeWithContext dispatches the existing operation with caller context.
func ForEachNodeIDByLabelInScopeWithContext(ctx context.Context, engine interface {
	ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error
}, scope, label string, visit func(NodeID) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader, ok := engine.(interface {
		ForEachNodeIDByLabelInScopeWithContext(ctx context.Context, scope, label string, visit func(NodeID) bool) error
	}); ok {
		return reader.ForEachNodeIDByLabelInScopeWithContext(ctx, scope, label, visit)
	}
	return engine.ForEachNodeIDByLabelInScope(scope, label, visit)
}

// GetNodesByLabelVisibleAtWithContext dispatches the existing operation with caller context.
func GetNodesByLabelVisibleAtWithContext(ctx context.Context, engine interface {
	GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error)
}, label string, version MVCCVersion) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetNodesByLabelVisibleAtWithContext(ctx context.Context, label string, version MVCCVersion) ([]*Node, error)
	}); ok {
		return reader.GetNodesByLabelVisibleAtWithContext(ctx, label, version)
	}
	return engine.GetNodesByLabelVisibleAt(label, version)
}

// GetNodesByLabelVisibleAtInScopeWithContext dispatches the existing operation with caller context.
func GetNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, engine interface {
	GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error)
}, scope, label string, version MVCCVersion) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		GetNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error)
	}); ok {
		return reader.GetNodesByLabelVisibleAtInScopeWithContext(ctx, scope, label, version)
	}
	return engine.GetNodesByLabelVisibleAtInScope(scope, label, version)
}

// NodeCountByLabelWithContext dispatches the existing operation with caller context.
func NodeCountByLabelWithContext(ctx context.Context, engine interface {
	NodeCountByLabel(label string) (int64, error)
}, label string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if reader, ok := engine.(interface {
		NodeCountByLabelWithContext(ctx context.Context, label string) (int64, error)
	}); ok {
		return reader.NodeCountByLabelWithContext(ctx, label)
	}
	return engine.NodeCountByLabel(label)
}

// NodeCountByLabelInNamespaceWithContext dispatches the existing operation with caller context.
func NodeCountByLabelInNamespaceWithContext(ctx context.Context, engine interface {
	NodeCountByLabelInNamespace(namespace, label string) (int64, error)
}, namespace, label string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if reader, ok := engine.(interface {
		NodeCountByLabelInNamespaceWithContext(ctx context.Context, namespace, label string) (int64, error)
	}); ok {
		return reader.NodeCountByLabelInNamespaceWithContext(ctx, namespace, label)
	}
	return engine.NodeCountByLabelInNamespace(namespace, label)
}

// GetStoredNodesByLabelInScopeWithContext forwards a stored scoped read.
func GetStoredNodesByLabelInScopeWithContext(ctx context.Context, engine interface {
	GetStoredNodesByLabelInScope(string, string) ([]*Node, error)
}, scope, label string) ([]*Node, error) {
	if r, ok := engine.(interface {
		GetStoredNodesByLabelInScopeWithContext(context.Context, string, string) ([]*Node, error)
	}); ok {
		return r.GetStoredNodesByLabelInScopeWithContext(ctx, scope, label)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return engine.GetStoredNodesByLabelInScope(scope, label)
}

// StreamStoredLabelNodesWithContext retains StoredReader's established view selection.
func StreamStoredLabelNodesWithContext(ctx context.Context, engine Engine, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScopeWithContext(ctx, engine, "", label, properties, visit)
}

// CreateNodeWithContext carries the statement context into constraint candidate reads.
func CreateNodeWithContext(ctx context.Context, engine interface {
	CreateNode(node *Node) (NodeID, error)
}, node *Node) (NodeID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r, ok := engine.(interface {
		CreateNodeWithContext(ctx context.Context, node *Node) (NodeID, error)
	}); ok {
		return r.CreateNodeWithContext(ctx, node)
	}
	return engine.CreateNode(node)
}

// CreateMintedNodeWithContext carries the statement context into constraint candidate reads.
func CreateMintedNodeWithContext(ctx context.Context, engine interface {
	CreateMintedNode(node *Node) (NodeID, error)
}, node *Node) (NodeID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r, ok := engine.(interface {
		CreateMintedNodeWithContext(ctx context.Context, node *Node) (NodeID, error)
	}); ok {
		return r.CreateMintedNodeWithContext(ctx, node)
	}
	return engine.CreateMintedNode(node)
}

// UpdateNodeWithContext carries the statement context into constraint candidate reads.
func UpdateNodeWithContext(ctx context.Context, engine interface{ UpdateNode(node *Node) error }, node *Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r, ok := engine.(interface {
		UpdateNodeWithContext(ctx context.Context, node *Node) error
	}); ok {
		return r.UpdateNodeWithContext(ctx, node)
	}
	return engine.UpdateNode(node)
}

// BulkCreateNodesWithContext carries the statement context into constraint candidate reads.
func BulkCreateNodesWithContext(ctx context.Context, engine interface{ BulkCreateNodes(nodes []*Node) error }, nodes []*Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r, ok := engine.(interface {
		BulkCreateNodesWithContext(ctx context.Context, nodes []*Node) error
	}); ok {
		return r.BulkCreateNodesWithContext(ctx, nodes)
	}
	return engine.BulkCreateNodes(nodes)
}

// BulkCreateMintedNodesWithContext carries the statement context into constraint candidate reads.
func BulkCreateMintedNodesWithContext(ctx context.Context, engine interface{ BulkCreateMintedNodes(nodes []*Node) error }, nodes []*Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r, ok := engine.(interface {
		BulkCreateMintedNodesWithContext(ctx context.Context, nodes []*Node) error
	}); ok {
		return r.BulkCreateMintedNodesWithContext(ctx, nodes)
	}
	return engine.BulkCreateMintedNodes(nodes)
}

// AllNodesWithContext dispatches the existing full-node materializer.
func AllNodesWithContext(ctx context.Context, engine interface{ AllNodes() ([]*Node, error) }) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader, ok := engine.(interface {
		AllNodesWithContext(context.Context) ([]*Node, error)
	}); ok {
		return reader.AllNodesWithContext(ctx)
	}
	return engine.AllNodes()
}
