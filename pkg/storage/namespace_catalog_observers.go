package storage

import "errors"

type namespaceCatalogObserver struct {
	id      uint64
	refresh func() error
}
type namespaceCatalogObservers map[NodeID][]namespaceCatalogObserver

func namespaceLifecycleOwner(engine Engine) *BadgerEngine {
	for engine != nil {
		if owner, ok := engine.(interface{ namespaceLifetimeOwner() *BadgerEngine }); ok {
			return owner.namespaceLifetimeOwner()
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil
		}
		engine = wrapper.GetInnerEngine()
	}
	return nil
}

// NamespaceLifetimeRevision is the portable catalog revision. CREATE captures
// it under lifecycle admission. Transaction BEGIN uses a separate local revision,
// so receiving a remote revision cannot revive an older local transaction.
func NamespaceLifetimeRevision(engine Engine) uint64 {
	if owner := namespaceLifecycleOwner(engine); owner != nil {
		return owner.catalogRevision.Load()
	}
	return 0
}

// Metadata snapshot/bootstrap includes catalog births even when it does not
// include the physical owner's private counters. New leaders must advance past
// those births before publishing their next retirement.
func ObserveNamespaceCatalogLifetimes(engine Engine, births map[string]uint64) {
	if owner := namespaceLifecycleOwner(engine); owner != nil {
		for _, revision := range births {
			owner.observeCatalogRevision(revision)
		}
	}
}

func (b *BadgerEngine) observeCatalogRevision(revision uint64) {
	for current := b.catalogRevision.Load(); current < revision; current = b.catalogRevision.Load() {
		if b.catalogRevision.CompareAndSwap(current, revision) {
			return
		}
	}
}

// NamespaceCatalogRetired rejects an older full-catalog snapshot even after a
// replica or engine restart. Native restore loads its backup's catalog history;
// already-open transactions retain the runtime retirement history separately.
func NamespaceCatalogRetired(engine Engine, namespace string, birth uint64) bool {
	if owner := namespaceLifecycleOwner(engine); owner != nil {
		if state := owner.namespaceLifetimes.Load(); state != nil {
			return state.catalogRetired[namespace] > birth
		}
	}
	return false
}

// RegisterNamespaceCatalogObserver publishes metadata-node changes through the
// same physical notification owner for native, WAL and replication writes.
// Subscribers refresh by point read, serialized with their initial load, so a
// late event cannot publish the old event payload over a newer catalog.
func RegisterNamespaceCatalogObserver(engine Engine, nodeID NodeID, refresh func() error) func() {
	b := namespaceLifecycleOwner(engine)
	if b == nil {
		return func() {}
	}
	b.catalogObserversMu.Lock()
	b.catalogObserverID++
	id := b.catalogObserverID
	next := b.copyNamespaceCatalogObservers()
	next[nodeID] = append(append([]namespaceCatalogObserver(nil), next[nodeID]...), namespaceCatalogObserver{id, refresh})
	b.catalogObservers.Store(&next)
	b.catalogObserversMu.Unlock()
	return func() {
		b.catalogObserversMu.Lock()
		defer b.catalogObserversMu.Unlock()
		next := b.copyNamespaceCatalogObservers()
		var keep []namespaceCatalogObserver
		for _, observer := range next[nodeID] {
			if observer.id != id {
				keep = append(keep, observer)
			}
		}
		if len(keep) == 0 {
			delete(next, nodeID)
		} else {
			next[nodeID] = keep
		}
		b.catalogObservers.Store(&next)
	}
}

func (b *BadgerEngine) copyNamespaceCatalogObservers() namespaceCatalogObservers {
	next := make(namespaceCatalogObservers)
	if previous := b.catalogObservers.Load(); previous != nil {
		for key, observers := range *previous {
			next[key] = observers
		}
	}
	return next
}

func (b *BadgerEngine) publishNamespaceCatalog(node *Node) {
	if node == nil {
		return
	}
	if observers := b.catalogObservers.Load(); observers != nil {
		for _, observer := range (*observers)[node.ID] {
			if err := observer.refresh(); err != nil {
				b.log.Error("database catalog publication failed", "node_id", node.ID, "error", err)
			}
		}
	}
}

func (b *BadgerEngine) hasNamespaceCatalogUpdates(ops []Operation) bool {
	observers := b.catalogObservers.Load()
	if observers == nil {
		return false
	}
	for _, op := range ops {
		if op.Node != nil && len((*observers)[op.Node.ID]) != 0 {
			return true
		}
	}
	return false
}

// Catalog publication is intrinsic completion, not an external event listener:
// a busy search listener cannot delay the standby's committed catalog.
func (b *BadgerEngine) publishNamespaceCatalogOperations(ops []Operation) {
	if b.catalogObservers.Load() == nil {
		return
	}
	for _, op := range ops {
		if op.Type == OpCreateNode || op.Type == OpUpdateNode {
			b.publishNamespaceCatalog(op.Node)
		}
	}
}

func (b *BadgerEngine) refreshNamespaceCatalogs() error {
	var err error
	if observers := b.catalogObservers.Load(); observers != nil {
		for _, group := range *observers {
			for _, observer := range group {
				err = errors.Join(err, observer.refresh())
			}
		}
	}
	return err
}
