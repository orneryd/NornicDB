package storage

import "context"

var (
	_ NodeWithoutEmbeddingsReader          = (*WALEngine)(nil)
	_ BatchNodeWithoutEmbeddingsReader     = (*WALEngine)(nil)
	_ BatchNodeWithoutEmbeddingsCapability = (*WALEngine)(nil)
	_ NodeWithoutEmbeddingsReader          = (*NamespacedEngine)(nil)
	_ BatchNodeWithoutEmbeddingsReader     = (*NamespacedEngine)(nil)
	_ BatchNodeWithoutEmbeddingsCapability = (*NamespacedEngine)(nil)
	_ QueryNodeReader                      = (*BadgerEngine)(nil)
	_ QueryNodeReader                      = (*BadgerTransaction)(nil)
	_ QueryNodeReader                      = (*WALEngine)(nil)
	_ QueryNodeReader                      = (*NamespacedEngine)(nil)
)

// batchNodeWithoutEmbeddingsSupported reports whether engine can perform a
// real batched embedding-free read. Wrappers use this shared check so merely
// implementing the forwarding method does not advertise a broken capability.
func batchNodeWithoutEmbeddingsSupported(engine Engine) bool {
	available := HasReadCapability[BatchNodeWithoutEmbeddingsReader](engine)
	return available
}

// namespaceLabelCount is the shared namespace-aware capability dispatch.
// Wrappers preserve their public method while sharing the provider and scan owner.
func namespaceLabelCount(engine Engine, namespace, label string) (int64, error) {
	return namespaceLabelCountWithContext(context.Background(), engine, namespace, label)
}

// namespaceLabelCountWithContext carries the caller through validation and label materialization.
func namespaceLabelCountWithContext(ctx context.Context, engine Engine, namespace, label string) (int64, error) {
	prefix := namespace + ":"
	ordinary := func() (int64, error) {
		var count int64
		err := (nodeReadOperation{engine, StreamNodesOptions{Prefix: prefix, Label: label, Projection: []string{}, ApplyDecayFilter: true}}).visit(ctx, func(*Node) error { count++; return nil })
		return count, err
	}
	if scoped, ok := ViewMetadataForEngine[interface{ Namespace() string }](engine); ok && scoped.Namespace() != "" {
		if scoped.Namespace() != namespace {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			member := MembershipForEngine(engine)
			if member.Status == MembershipEnded {
				return 0, ErrTransactionClosed
			}
			if member.Status == MembershipUnbound {
				return 0, ErrNotImplemented
			}
			return 0, nil
		}
		// Rows of a namespace view already have logical IDs. Applying its physical
		// prefix again excludes every row, including through a transparent wrapper.
		prefix = ""
		return ordinary()
	}
	return ReadWithCapability(engine, func(reader NamespaceLabelStatsProvider) (int64, error) {
		return NodeCountByLabelInNamespaceWithContext(ctx, reader, namespace, label)
	}, ordinary)
}
