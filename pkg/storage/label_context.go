package storage

import "context"

// GetNodesByLabelWithContext dispatches the existing operation with caller context.
func GetNodesByLabelWithContext(ctx context.Context, engine interface {
	GetNodesByLabel(label string) ([]*Node, error)
}, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.GetNodesByLabelWithContext(ctx, label)
	}, func() ([]*Node, error) { return engine.GetNodesByLabel(label) })
}

// GetStoredNodesByLabelWithContext dispatches the existing operation with caller context.
func GetStoredNodesByLabelWithContext(ctx context.Context, engine any, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetStoredNodesByLabelWithContext(context.Context, string) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.GetStoredNodesByLabelWithContext(ctx, label)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader StoredEntityReader) ([]*Node, error) { return reader.GetStoredNodesByLabel(label) }, func() ([]*Node, error) {
			if ordinary, ok := OwnedCapability[Engine](engine); ok {
				return GetNodesByLabelWithContext(ctx, ordinary, label)
			}
			return nil, ErrNotImplemented
		})
	})
}

// GetFirstNodeByLabelWithContext dispatches the existing operation with caller context.
func GetFirstNodeByLabelWithContext(ctx context.Context, engine interface {
	GetFirstNodeByLabel(label string) (*Node, error)
}, label string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetFirstNodeByLabelWithContext(ctx context.Context, label string) (*Node, error)
	}) (*Node, error) {
		return reader.GetFirstNodeByLabelWithContext(ctx, label)
	}, func() (*Node, error) { return engine.GetFirstNodeByLabel(label) })
}

// StreamNodesByLabelProjectedWithContext dispatches the existing operation with caller context.
func StreamNodesByLabelProjectedWithContext(ctx context.Context, engine interface {
	StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
}, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return VisitWithCapability(engine, func(reader interface {
		StreamNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error
	}, deliver func(*Node) error) error {
		return reader.StreamNodesByLabelProjectedWithContext(ctx, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return VisitWithCapability(engine, func(reader interface {
			StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
		}, deliver func(*Node) error) error {
			return reader.StreamNodesByLabelProjected(label, properties, deliver)
		}, nil, deliver)
	}, visit)
}

// StreamStoredNodesByLabelProjectedWithContext dispatches the existing operation with caller context.
func StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, engine interface {
	StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
}, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return VisitWithCapability(engine, func(reader interface {
		StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error
	}, deliver func(*Node) error) error {
		return reader.StreamStoredNodesByLabelProjectedWithContext(ctx, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return VisitWithCapability(engine, func(reader interface {
			StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
		}, deliver func(*Node) error) error {
			return reader.StreamStoredNodesByLabelProjected(label, properties, deliver)
		}, nil, deliver)
	}, visit)
}

// ForEachNodeIDByLabelWithContext dispatches the existing operation with caller context.
func ForEachNodeIDByLabelWithContext(ctx context.Context, engine interface {
	ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error
}, label string, visit func(NodeID) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return VisitWhileWithCapability(engine, func(reader interface {
		ForEachNodeIDByLabelWithContext(ctx context.Context, label string, visit func(NodeID) bool) error
	}, deliver func(NodeID) bool) error {
		return reader.ForEachNodeIDByLabelWithContext(ctx, label, deliver)
	}, func(deliver func(NodeID) bool) error {
		return VisitWhileWithCapability(engine, func(reader interface {
			ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error
		}, deliver func(NodeID) bool) error {
			return reader.ForEachNodeIDByLabel(label, deliver)
		}, nil, deliver)
	}, visit)
}

// GetNodesByLabelInScopeWithContext dispatches the existing operation with caller context.
func GetNodesByLabelInScopeWithContext(ctx context.Context, engine interface {
	GetNodesByLabelInScope(scope, label string) ([]*Node, error)
}, scope, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetNodesByLabelInScopeWithContext(ctx context.Context, scope, label string) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.GetNodesByLabelInScopeWithContext(ctx, scope, label)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader interface {
			GetNodesByLabelInScope(scope, label string) ([]*Node, error)
		}) ([]*Node, error) {
			return reader.GetNodesByLabelInScope(scope, label)
		}, nil)
	})
}

// GetFirstNodeByLabelInScopeWithContext dispatches the existing operation with caller context.
func GetFirstNodeByLabelInScopeWithContext(ctx context.Context, engine interface {
	GetFirstNodeByLabelInScope(scope, label string) (*Node, error)
}, scope, label string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetFirstNodeByLabelInScopeWithContext(ctx context.Context, scope, label string) (*Node, error)
	}) (*Node, error) {
		return reader.GetFirstNodeByLabelInScopeWithContext(ctx, scope, label)
	}, func() (*Node, error) {
		return ReadWithCapability(engine, func(reader interface {
			GetFirstNodeByLabelInScope(scope, label string) (*Node, error)
		}) (*Node, error) {
			return reader.GetFirstNodeByLabelInScope(scope, label)
		}, nil)
	})
}

// StreamNodesByLabelProjectedInScopeWithContext dispatches the existing operation with caller context.
func StreamNodesByLabelProjectedInScopeWithContext(ctx context.Context, engine interface {
	StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
}, scope, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return VisitWithCapability(engine, func(reader interface {
		StreamNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error
	}, deliver func(*Node) error) error {
		return reader.StreamNodesByLabelProjectedInScopeWithContext(ctx, scope, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return VisitWithCapability(engine, func(reader interface {
			StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
		}, deliver func(*Node) error) error {
			return reader.StreamNodesByLabelProjectedInScope(scope, label, properties, deliver)
		}, nil, deliver)
	}, visit)
}

// StreamStoredNodesByLabelProjectedInScopeWithContext dispatches the existing operation with caller context.
func StreamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, engine interface {
	StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
}, scope, label string, properties []string, visit func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return VisitWithCapability(engine, func(reader interface {
		StreamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error
	}, deliver func(*Node) error) error {
		return reader.StreamStoredNodesByLabelProjectedInScopeWithContext(ctx, scope, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return VisitWithCapability(engine, func(reader interface {
			StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
		}, deliver func(*Node) error) error {
			return reader.StreamStoredNodesByLabelProjectedInScope(scope, label, properties, deliver)
		}, nil, deliver)
	}, visit)
}

// ForEachNodeIDByLabelInScopeWithContext dispatches the existing operation with caller context.
func ForEachNodeIDByLabelInScopeWithContext(ctx context.Context, engine interface {
	ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error
}, scope, label string, visit func(NodeID) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return VisitWhileWithCapability(engine, func(reader interface {
		ForEachNodeIDByLabelInScopeWithContext(ctx context.Context, scope, label string, visit func(NodeID) bool) error
	}, deliver func(NodeID) bool) error {
		return reader.ForEachNodeIDByLabelInScopeWithContext(ctx, scope, label, deliver)
	}, func(deliver func(NodeID) bool) error {
		return VisitWhileWithCapability(engine, func(reader interface {
			ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error
		}, deliver func(NodeID) bool) error {
			return reader.ForEachNodeIDByLabelInScope(scope, label, deliver)
		}, nil, deliver)
	}, visit)
}

// GetNodesByLabelVisibleAtWithContext dispatches the existing operation with caller context.
func GetNodesByLabelVisibleAtWithContext(ctx context.Context, engine interface {
	GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error)
}, label string, version MVCCVersion) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetNodesByLabelVisibleAtWithContext(ctx context.Context, label string, version MVCCVersion) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.GetNodesByLabelVisibleAtWithContext(ctx, label, version)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader interface {
			GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error)
		}) ([]*Node, error) {
			return reader.GetNodesByLabelVisibleAt(label, version)
		}, nil)
	})
}

// GetNodesByLabelVisibleAtInScopeWithContext dispatches the existing operation with caller context.
func GetNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, engine interface {
	GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error)
}, scope, label string, version MVCCVersion) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.GetNodesByLabelVisibleAtInScopeWithContext(ctx, scope, label, version)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader interface {
			GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error)
		}) ([]*Node, error) {
			return reader.GetNodesByLabelVisibleAtInScope(scope, label, version)
		}, nil)
	})
}

// NodeCountByLabelWithContext dispatches the existing operation with caller context.
func NodeCountByLabelWithContext(ctx context.Context, engine interface {
	NodeCountByLabel(label string) (int64, error)
}, label string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return ReadWithCapability(engine, func(reader interface {
		NodeCountByLabelWithContext(ctx context.Context, label string) (int64, error)
	}) (int64, error) {
		return reader.NodeCountByLabelWithContext(ctx, label)
	}, func() (int64, error) {
		return ReadWithCapability(engine, func(reader interface {
			NodeCountByLabel(label string) (int64, error)
		}) (int64, error) {
			return reader.NodeCountByLabel(label)
		}, nil)
	})
}

// NodeCountByLabelInNamespaceWithContext dispatches the existing operation with caller context.
func NodeCountByLabelInNamespaceWithContext(ctx context.Context, engine interface {
	NodeCountByLabelInNamespace(namespace, label string) (int64, error)
}, namespace, label string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return ReadWithCapability(engine, func(reader interface {
		NodeCountByLabelInNamespaceWithContext(ctx context.Context, namespace, label string) (int64, error)
	}) (int64, error) {
		return reader.NodeCountByLabelInNamespaceWithContext(ctx, namespace, label)
	}, func() (int64, error) {
		return ReadWithCapability(engine, func(reader interface {
			NodeCountByLabelInNamespace(namespace, label string) (int64, error)
		}) (int64, error) {
			return reader.NodeCountByLabelInNamespace(namespace, label)
		}, nil)
	})
}

// GetStoredNodesByLabelInScopeWithContext forwards a stored scoped read.
func GetStoredNodesByLabelInScopeWithContext(ctx context.Context, engine interface {
	GetStoredNodesByLabelInScope(string, string) ([]*Node, error)
}, scope, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		GetStoredNodesByLabelInScopeWithContext(context.Context, string, string) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.GetStoredNodesByLabelInScopeWithContext(ctx, scope, label)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader interface {
			GetStoredNodesByLabelInScope(string, string) ([]*Node, error)
		}) ([]*Node, error) {
			return reader.GetStoredNodesByLabelInScope(scope, label)
		}, nil)
	})
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
	if r, ok := OwnedCapability[interface {
		CreateNodeWithContext(ctx context.Context, node *Node) (NodeID, error)
	}](engine); ok {
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
	if r, ok := OwnedCapability[interface {
		CreateMintedNodeWithContext(ctx context.Context, node *Node) (NodeID, error)
	}](engine); ok {
		return r.CreateMintedNodeWithContext(ctx, node)
	}
	return engine.CreateMintedNode(node)
}

// UpdateNodeWithContext carries the statement context into constraint candidate reads.
func UpdateNodeWithContext(ctx context.Context, engine interface{ UpdateNode(node *Node) error }, node *Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r, ok := OwnedCapability[interface {
		UpdateNodeWithContext(ctx context.Context, node *Node) error
	}](engine); ok {
		return r.UpdateNodeWithContext(ctx, node)
	}
	return engine.UpdateNode(node)
}

// BulkCreateNodesWithContext carries the statement context into constraint candidate reads.
func BulkCreateNodesWithContext(ctx context.Context, engine interface{ BulkCreateNodes(nodes []*Node) error }, nodes []*Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r, ok := OwnedCapability[interface {
		BulkCreateNodesWithContext(ctx context.Context, nodes []*Node) error
	}](engine); ok {
		return r.BulkCreateNodesWithContext(ctx, nodes)
	}
	return engine.BulkCreateNodes(nodes)
}

// BulkCreateMintedNodesWithContext carries the statement context into constraint candidate reads.
func BulkCreateMintedNodesWithContext(ctx context.Context, engine interface{ BulkCreateMintedNodes(nodes []*Node) error }, nodes []*Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r, ok := OwnedCapability[interface {
		BulkCreateMintedNodesWithContext(ctx context.Context, nodes []*Node) error
	}](engine); ok {
		return r.BulkCreateMintedNodesWithContext(ctx, nodes)
	}
	return engine.BulkCreateMintedNodes(nodes)
}

// AllNodesWithContext dispatches the existing full-node materializer.
func AllNodesWithContext(ctx context.Context, engine interface{ AllNodes() ([]*Node, error) }) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader interface {
		AllNodesWithContext(context.Context) ([]*Node, error)
	}) ([]*Node, error) {
		return reader.AllNodesWithContext(ctx)
	}, func() ([]*Node, error) { return engine.AllNodes() })
}

// StreamLabelNodesWithContext executes the scoped projected-label operation
// while retaining cancellation through its selected provider and ordinary kernel.
func StreamLabelNodesWithContext(ctx context.Context, engine Engine, label string, properties []string, visit func(*Node) error) error {
	return VisitWithCapability(engine, func(reader ProjectedLabelNodeReader, deliver func(*Node) error) error {
		return StreamNodesByLabelProjectedWithContext(ctx, reader, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return (nodeReadOperation{engine, StreamNodesOptions{Label: label, Projection: properties, WithEmbeddings: properties == nil, ApplyDecayFilter: true}}).visit(ctx, deliver)
	}, visit)
}
