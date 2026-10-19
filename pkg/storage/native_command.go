package storage

import "context"

type nativePublication struct {
	Publish, After func()
	Token          [16]byte
}

func nativePublicationToken(publication []nativePublication) [16]byte {
	if len(publication) == 0 {
		return [16]byte{}
	}
	return publication[0].Token
}

// NativeCommandEngine preserves an atomic direct execution boundary and its
// ordered publication through transparent and protocol wrappers.
type NativeCommandEngine interface {
	NativeCommand(context.Context, Operation, func()) error
	SupportsNativeCommand() bool
}

func SupportsNativeCommand(engine Engine) bool {
	capability, ok := engine.(NativeCommandEngine)
	return ok && capability.SupportsNativeCommand()
}

func ExecuteNativeCommand(ctx context.Context, engine Engine, op Operation, published func()) error {
	if capability, ok := engine.(NativeCommandEngine); ok {
		return capability.NativeCommand(ctx, op, published)
	}
	return ErrNotImplemented
}

func (b *BadgerEngine) SupportsNativeCommand() bool { return true }

func (b *BadgerEngine) NativeCommand(ctx context.Context, op Operation, published func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := string(op.NodeID)
	if op.Node != nil {
		id = string(op.Node.ID)
	}
	if op.Edge != nil {
		id = string(op.Edge.ID)
	}
	if id == "" {
		id = string(op.EdgeID)
	}
	namespace, _, _ := ParseDatabasePrefix(id)
	write := func() error {
		after := CommandAfterPublicationFromContext(ctx)
		completion := nativePublication{Publish: published, After: after, Token: CommandVersionFromContext(ctx)}
		switch op.Type {
		case OpCreateNode:
			_, err := b.createNode(op.Node, completion)
			return err
		case OpUpdateNode:
			return b.updateNode(op.Node, completion)
		case OpDeleteNode:
			return b.deleteNode(op.NodeID, completion)
		case OpCreateEdge:
			return b.createEdge(op.Edge, completion)
		case OpUpdateEdge:
			return b.updateEdge(op.Edge, completion)
		case OpDeleteEdge:
			return b.deleteEdge(op.EdgeID, completion)
		default:
			return ErrNotImplemented
		}
	}
	if b.writeBehind == nil {
		return write()
	}
	return b.orderBehindBufferedWrites(namespace, write)
}

func (n *NamespacedEngine) SupportsNativeCommand() bool { return SupportsNativeCommand(n.inner) }

func (n *NamespacedEngine) NativeCommand(ctx context.Context, op Operation, published func()) error {
	op = namespacedManagedOperation(n, op)
	if op.Node != nil {
		if err := n.GetSchema().RegisterTokens(op.Node.Labels, nil); err != nil {
			return err
		}
	}
	if op.Edge != nil {
		if err := n.GetSchema().RegisterTokens(nil, []string{op.Edge.Type}); err != nil {
			return err
		}
	}
	err := ExecuteNativeCommand(ctx, n.inner, op, published)
	if err == nil && op.Type == OpDeleteNode {
		if pending, ok := n.inner.(interface{ MarkNodeEmbedded(NodeID) }); ok {
			pending.MarkNodeEmbedded(op.NodeID)
		}
	}
	return err
}

func (tx *BadgerTransaction) setCommandPublication(publication []nativePublication) {
	if len(publication) == 0 {
		return
	}
	tx.mu.Lock()
	tx.commandPublication = publication[0].Publish
	tx.commandAfterPublication = publication[0].After
	tx.commandVersion = publication[0].Token
	tx.mu.Unlock()
}
