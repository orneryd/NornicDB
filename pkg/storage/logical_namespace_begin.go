package storage

import (
	"context"
	"fmt"
)

// NamespaceLifetimeSnapshot is an immutable logical-BEGIN token per physical
// owner. It does not copy or scan the owner's database registry.
type namespaceLifetimeBegin struct {
	revision  uint64
	namespace string
}
type NamespaceLifetimeSnapshot map[*BadgerEngine]namespaceLifetimeBegin

func (c *CompositeEngine) SetNamespaceLifetimeSource(engine Engine) {
	c.namespaceLifetimeSource = engine
}

func CaptureNamespaceLifetimes(engine Engine) NamespaceLifetimeSnapshot {
	snapshot := make(NamespaceLifetimeSnapshot)
	seen := make(map[Engine]bool)
	namespace := ""
	for cursor := engine; cursor != nil; {
		if n, ok := cursor.(*NamespacedEngine); ok {
			namespace = n.Namespace()
			break
		}
		if c, ok := cursor.(*CompositeEngine); ok {
			namespace = c.CompositeName()
			break
		}
		wrapper, ok := cursor.(EngineUnwrapper)
		if !ok {
			break
		}
		cursor = wrapper.GetInnerEngine()
	}
	var visit func(Engine)
	visit = func(engine Engine) {
		if engine == nil || seen[engine] {
			return
		}
		seen[engine] = true
		if owner, ok := engine.(interface{ namespaceLifetimeOwner() *BadgerEngine }); ok {
			b := owner.namespaceLifetimeOwner()
			snapshot[b] = namespaceLifetimeBegin{b.namespaceLifetimeRevision(), namespace}
			return
		}
		if c, ok := engine.(*CompositeEngine); ok {
			c.mu.RLock()
			defer c.mu.RUnlock()
			visit(c.namespaceLifetimeSource)
			for _, constituent := range c.constituents {
				visit(constituent)
			}
			return
		}
		if wrapper, ok := engine.(EngineUnwrapper); ok {
			visit(wrapper.GetInnerEngine())
		}
	}
	visit(engine)
	return snapshot
}

func (b *BadgerEngine) namespaceLifetimeOwner() *BadgerEngine { return b }

// BindLogicalBegin is used before a lazy participant executes any statement.
// The commit owner performs the definitive check under its publication lease.
func (tx *BadgerTransaction) BindLogicalBegin(snapshot NamespaceLifetimeSnapshot) error {
	if len(snapshot) == 0 {
		return nil
	}
	begin, ok := snapshot[tx.engine]
	if !ok {
		return fmt.Errorf("%w: storage owner did not exist at logical BEGIN", ErrConflict)
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if begin.revision < tx.beginNamespaceRevision {
		tx.beginNamespaceRevision = begin.revision
	}
	tx.logicalNamespace = begin.namespace
	return nil
}

func (snapshot NamespaceLifetimeSnapshot) Validate(namespace string) error {
	for engine, begin := range snapshot {
		if err := engine.checkNamespaceLifetimeRevision(namespace, begin.revision); err != nil {
			return fmt.Errorf("%w: namespace %q was dropped after logical BEGIN", ErrConflict, namespace)
		}
	}
	return nil
}

type logicalNamespaceBeginKey struct{}

func WithLogicalNamespaceBegin(ctx context.Context, snapshot NamespaceLifetimeSnapshot) context.Context {
	return context.WithValue(ctx, logicalNamespaceBeginKey{}, snapshot)
}
func LogicalNamespaceBegin(ctx context.Context) NamespaceLifetimeSnapshot {
	snapshot, _ := ctx.Value(logicalNamespaceBeginKey{}).(NamespaceLifetimeSnapshot)
	return snapshot
}
