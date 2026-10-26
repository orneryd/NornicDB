package storage

import (
	"context"
	"sort"
	"sync"
)

// entityCommitKeys projects the typed identity domain for the canonical
// commit-lock registry. Endpoint and relationship identities never become labels.
func entityCommitKeys(nodes []NodeID, edges []EdgeID, endpoints []NodeID) []uniqueConstraintLockKey {
	keys := make([]uniqueConstraintLockKey, 0, len(nodes)+len(edges)+len(endpoints))
	for _, id := range nodes {
		keys = append(keys, uniqueConstraintLockKey{entityKind: 1, entityID: string(id)})
	}
	for _, id := range edges {
		keys = append(keys, uniqueConstraintLockKey{entityKind: 2, entityID: string(id)})
	}
	for _, id := range endpoints {
		keys = append(keys, uniqueConstraintLockKey{entityKind: 1, entityID: string(id), shared: true})
	}
	return keys
}

func (tx *BadgerTransaction) pendingEntityAuthorityKeysLocked() ([]NodeID, []EdgeID, []NodeID) {
	nodes := make([]NodeID, 0, len(tx.pendingNodes)+len(tx.deletedNodes)+len(tx.pendingAdjacency))
	edges := make([]EdgeID, 0, len(tx.pendingEdges)+len(tx.deletedEdges))
	endpoints := make([]NodeID, 0, len(tx.pendingAdjacency)+2*len(tx.pendingEdges))
	for id := range tx.pendingNodes {
		nodes = append(nodes, id)
	}
	for id := range tx.deletedNodes {
		nodes = append(nodes, id)
	}
	for id := range tx.pendingAdjacency {
		endpoints = append(endpoints, id)
	}
	for id, edge := range tx.pendingEdges {
		edges = append(edges, id)
		endpoints = append(endpoints, edge.StartNode, edge.EndNode)
	}
	for id := range tx.deletedEdges {
		edges = append(edges, id)
	}
	for _, op := range tx.operations {
		if op.OldEdge != nil {
			endpoints = append(endpoints, op.OldEdge.StartNode, op.OldEdge.EndNode)
		}
		if op.Edge != nil {
			endpoints = append(endpoints, op.Edge.StartNode, op.Edge.EndNode)
		}
	}
	return nodes, edges, endpoints
}

// entityWriteContext transfers one outer entity write's authority and lifecycle
// admission through native helper calls without recursive barrier acquisition.
type entityWriteContext struct {
	ctx    context.Context
	engine *BadgerEngine
	owner  string
	writeRelease[entityAuthorityResources]
}

type entityAuthorityResources struct {
	keysOnce         sync.Once
	ordered          orderedWrite
	releaseAuthority func()
	once             sync.Once
}

// entityWritePlan supplies a complete typed footprint before any key is held.
// Readers execute only after the existing lifecycle lease is owned. Static
// batches are for suppression maintenance and may span ordered namespaces.
type entityWritePlan struct {
	known   writtenEntities
	read    func() (writtenEntities, error)
	batches map[string]writtenEntities
}

func (b *BadgerEngine) beginEntityWrite(ctx context.Context, id string, isEdge bool, inherited *entityWriteContext, plans ...entityWritePlan) (*entityWriteContext, func(), error) {
	// Key admission belongs to this lifecycle owner; inherited and new writes
	// use the same footprint acquisition without a separately callable entry.
	acquireKeys := func(admission context.Context, owner string) (func(), error) {
		if len(plans) > 0 && len(plans[0].batches) > 0 {
			batches := plans[0].batches
			namespaces := make([]string, 0, len(batches))
			for ns := range batches {
				namespaces = append(namespaces, ns)
			}
			sort.Strings(namespaces)
			releases := make([]func(), 0, len(namespaces))
			releaseAll := func() {
				for i := len(releases) - 1; i >= 0; i-- {
					releases[i]()
				}
			}
			for _, ns := range namespaces {
				entities := batches[ns]
				release, err := b.GetSchemaForNamespace(ns).acquireUniqueConstraintCommitLocks(admission, owner, entityCommitKeys(entities.nodes, entities.edges, nil))
				if err != nil {
					releaseAll()
					return nil, err
				}
				releases = append(releases, release)
			}
			return releaseAll, nil
		}
		namespace, _, valid := ParseDatabasePrefix(id)
		if !valid {
			return func() {}, nil
		}
		var footprint writtenEntities
		if len(plans) > 0 {
			footprint = plans[0].known
			if plans[0].read != nil {
				extra, err := plans[0].read()
				if err != nil {
					return nil, err
				}
				if len(footprint.nodes) == 0 {
					footprint.nodes = extra.nodes
				} else {
					footprint.nodes = append(footprint.nodes, extra.nodes...)
				}
				if len(footprint.edges) == 0 {
					footprint.edges = extra.edges
				} else {
					footprint.edges = append(footprint.edges, extra.edges...)
				}
			}
		}
		nodes := append([]NodeID(nil), footprint.nodes...)
		edges := append([]EdgeID(nil), footprint.edges...)
		if isEdge {
			edges = append(edges, EdgeID(id))
		} else {
			nodes = append(nodes, NodeID(id))
		}
		return b.GetSchemaForNamespace(namespace).acquireUniqueConstraintCommitLocks(admission, owner, entityCommitKeys(nodes, edges, nil))
	}
	if inherited != nil {
		if inherited.engine != b {
			return nil, nil, ErrInvalidData
		}
		if inherited.resources.releaseAuthority != nil {
			return inherited, func() {}, nil
		}
		// Close lends its existing lifecycle lease. Each batch still claims its
		// complete footprint once, without normal admission or recursive RLock.
		release, err := acquireKeys(inherited.ctx, inherited.owner)
		if err != nil {
			return nil, nil, err
		}
		child := &entityWriteContext{ctx: inherited.ctx, engine: b, owner: inherited.owner, writeRelease: writeRelease[entityAuthorityResources]{resources: entityAuthorityResources{releaseAuthority: release}}}
		return child, child.resources.releaseKeys, nil
	}
	if err := b.ensureOpen(false); err != nil {
		return nil, nil, err
	}
	if len(plans) > 0 && len(plans[0].batches) > 0 {
		namespaces := make([]string, 0, len(plans[0].batches))
		for ns := range plans[0].batches {
			namespaces = append(namespaces, ns)
		}
		sort.Strings(namespaces)
		for _, ns := range namespaces {
			if err := b.initializeManagedEntityNamespace(ctx, ns); err != nil {
				return nil, nil, err
			}
		}
	} else if ns, _, valid := ParseDatabasePrefix(id); valid {
		if err := b.initializeManagedEntityNamespace(ctx, ns); err != nil {
			return nil, nil, err
		}
	}
	ordered, err := b.beginOrderedWrite(ctx)
	if err != nil {
		return nil, nil, err
	}
	if ns, _, valid := ParseDatabasePrefix(id); valid {
		if err := b.pendingNamespaceConflict(ns); err != nil {
			ordered.finish()
			return nil, nil, err
		}
	}
	if len(plans) > 0 {
		for ns := range plans[0].batches {
			if err := b.pendingNamespaceConflict(ns); err != nil {
				ordered.finish()
				return nil, nil, err
			}
		}
	}
	owner := newEngineWriteLockOwner()
	release, err := acquireKeys(ctx, owner)
	if err != nil {
		ordered.finish()
		return nil, nil, err
	}
	write := &entityWriteContext{ctx: ctx, engine: b, owner: owner, writeRelease: writeRelease[entityAuthorityResources]{resources: entityAuthorityResources{ordered: ordered, releaseAuthority: release}}}
	return write, write.release, nil
}

// releaseKeys drops entity/endpoint authority in pure publication; native
// constraint values retain their deferred completion lease. Lifecycle remains through
// metadata work. Outer failure cleanup invokes the same idempotent owner.
func (w *entityAuthorityResources) releaseKeys() {
	w.keysOnce.Do(func() {
		if w.releaseAuthority != nil {
			w.releaseAuthority()
		}
	})
}
func (w *entityWriteContext) stagePublication(cw *commitWriter) error {
	publish := cw.publish
	cw.publish = func(images *commitCacheImages) {
		if publish != nil {
			publish(images)
		}
		w.resources.releaseKeys()
	}
	return nil
}
