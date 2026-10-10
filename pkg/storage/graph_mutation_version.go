package storage

import (
	"strings"
	"sync"
	"sync/atomic"
)

// GraphMutationVersionProvider reports a process-local revision of visible
// graph data. It is a cache-invalidation token, not a transaction timestamp or a
// durable snapshot version.
type GraphMutationVersionProvider interface {
	GraphMutationVersion() (version uint64, supported bool)
}

// NamespaceGraphMutationVersionProvider reports graph revisions by database.
type NamespaceGraphMutationVersionProvider interface {
	GraphMutationVersionInNamespace(namespace string) (version uint64, supported bool)
}

type graphMutationVersions struct {
	all         atomic.Uint64
	byNamespace sync.Map // map[string]*atomic.Uint64
}

func (v *graphMutationVersions) changed(namespace string) {
	v.all.Add(1)
	if namespace != "" {
		v.namespaceCounter(namespace).Add(1)
	}
}

func (v *graphMutationVersions) read(namespace string) uint64 {
	if namespace == "" {
		return v.all.Load()
	}
	return v.namespaceCounter(namespace).Load()
}

func (v *graphMutationVersions) changedPrefix(prefix string) {
	v.all.Add(1)
	v.byNamespace.Range(func(key, value any) bool {
		namespace := key.(string)
		if strings.HasPrefix(namespace+":", prefix) || strings.HasPrefix(prefix, namespace+":") {
			value.(*atomic.Uint64).Add(1)
		}
		return true
	})
}

func (v *graphMutationVersions) namespaceCounter(namespace string) *atomic.Uint64 {
	if counter, ok := v.byNamespace.Load(namespace); ok {
		return counter.(*atomic.Uint64)
	}
	counter, _ := v.byNamespace.LoadOrStore(namespace, &atomic.Uint64{})
	return counter.(*atomic.Uint64)
}

func graphMutationVersion(engine Engine, namespace string) (uint64, bool) {
	if namespace != "" {
		provider, ok := engine.(NamespaceGraphMutationVersionProvider)
		if !ok {
			return 0, false
		}
		return provider.GraphMutationVersionInNamespace(namespace)
	}
	provider, ok := engine.(GraphMutationVersionProvider)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersion()
}

// GraphMutationVersion reports all graph mutations published by this engine.
func (b *BadgerEngine) GraphMutationVersion() (uint64, bool) {
	return b.graphMutationVersions.read(""), true
}

// GraphMutationVersionInNamespace reports graph mutations for one database.
func (b *BadgerEngine) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	return b.graphMutationVersions.read(namespace), true
}

// GraphMutationVersion reports mutations visible through this namespace.
func (n *NamespacedEngine) GraphMutationVersion() (uint64, bool) {
	return graphMutationVersion(n.inner, n.namespace)
}

// GraphMutationVersionInNamespace keeps nested views scoped to their database.
func (n *NamespacedEngine) GraphMutationVersionInNamespace(string) (uint64, bool) {
	return n.GraphMutationVersion()
}

// GraphMutationVersion forwards revision reporting through the WAL wrapper.
func (w *WALEngine) GraphMutationVersion() (uint64, bool) {
	return graphMutationVersion(w.engine, "")
}

// GraphMutationVersionInNamespace forwards database-scoped revision reporting.
func (w *WALEngine) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	return graphMutationVersion(w.engine, namespace)
}

// GraphMutationVersion forwards revision reporting through tracing.
func (t *TracedEngine) GraphMutationVersion() (uint64, bool) {
	return graphMutationVersion(t.Engine, "")
}

// GraphMutationVersionInNamespace forwards database-scoped revision reporting.
func (t *TracedEngine) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	return graphMutationVersion(t.Engine, namespace)
}
