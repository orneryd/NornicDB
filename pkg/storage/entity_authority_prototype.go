package storage

import (
	"context"
	"sort"
	"sync"
)

// entityAuthorityMode distinguishes entity body mutation from independently
// live metadata. Both retain identity authority during publication; metadata
// does not take the transaction-lifetime body key held by Cypher writers.
type entityAuthorityMode uint8

const (
	entityBodyAuthority entityAuthorityMode = iota
	entityMetadataAuthority
)

// entityAuthorityFootprint separates identities protected during publication
// from the bodies actually mutated. Incident endpoints and independently live
// metadata retain identity authority without taking a body writer's lifetime key.
type entityAuthorityFootprint struct {
	nodes      []NodeID
	edges      []EdgeID
	endpoints  []NodeID
	nodeBodies []NodeID
	edgeBodies []EdgeID
}

// entityCommitKeys projects the typed identity domain for the canonical
// commit-lock registry. Endpoint and relationship identities never become labels.
func entityCommitKeys(ctx context.Context, footprint entityAuthorityFootprint) ([]uniqueConstraintLockKey, error) {
	keys := make([]uniqueConstraintLockKey, 0, len(footprint.nodes)+len(footprint.edges)+len(footprint.endpoints)+len(footprint.nodeBodies)+len(footprint.edgeBodies))
	for _, id := range footprint.nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, uniqueConstraintLockKey{entityKind: 1, entityID: string(id)})
	}
	for _, id := range footprint.edges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, uniqueConstraintLockKey{entityKind: 2, entityID: string(id)})
	}
	for _, id := range footprint.endpoints {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, uniqueConstraintLockKey{entityKind: 1, entityID: string(id), shared: true})
	}
	for _, id := range footprint.nodeBodies {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, entityWriteLockKey(bufferedNodeEntity, string(id)))
	}
	for _, id := range footprint.edgeBodies {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, entityWriteLockKey(bufferedEdgeEntity, string(id)))
	}
	return keys, nil
}

func (tx *BadgerTransaction) pendingEntityAuthorityKeysLocked(ctx context.Context) ([]NodeID, []EdgeID, []NodeID, error) {
	nodes := make([]NodeID, 0, len(tx.pendingNodes)+len(tx.deletedNodes)+len(tx.pendingAdjacency))
	edges := make([]EdgeID, 0, len(tx.pendingEdges)+len(tx.deletedEdges))
	endpoints := make([]NodeID, 0, len(tx.pendingAdjacency)+2*len(tx.pendingEdges))
	for id := range tx.pendingNodes {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		nodes = append(nodes, id)
	}
	for id := range tx.deletedNodes {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		nodes = append(nodes, id)
	}
	for id := range tx.pendingAdjacency {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		endpoints = append(endpoints, id)
	}
	for id, edge := range tx.pendingEdges {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		edges = append(edges, id)
		endpoints = append(endpoints, edge.StartNode, edge.EndNode)
	}
	for id := range tx.deletedEdges {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		edges = append(edges, id)
	}
	for _, op := range tx.operations {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		if op.OldEdge != nil {
			endpoints = append(endpoints, op.OldEdge.StartNode, op.OldEdge.EndNode)
		}
		if op.Edge != nil {
			endpoints = append(endpoints, op.Edge.StartNode, op.Edge.EndNode)
		}
		// A node delete records the relationships it removes only as facts:
		// their surviving other ends hold the positional counts the commit
		// derives (derivePositionalCountDeltasLocked).
		for _, fact := range op.DeletedEdgeFacts {
			endpoints = append(endpoints, fact.StartNode, fact.EndNode)
		}
	}
	return nodes, edges, endpoints, nil
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
	ordered          writeBarrierLease
	releaseAuthority func()
	once             sync.Once
}

// entityWritePlan supplies a complete typed footprint before any key is held.
// Readers execute only after the existing lifecycle lease is owned. Static
// batches are for suppression maintenance and may span ordered namespaces.
type entityWritePlan struct {
	authority entityAuthorityMode
	known     writtenEntities
	read      func() (writtenEntities, error)
	batches   map[string]writtenEntities
}

func (b *BadgerEngine) beginEntityWrite(ctx context.Context, id string, isEdge bool, inherited *entityWriteContext, plans ...entityWritePlan) (*entityWriteContext, func(), error) {
	// Key admission belongs to this lifecycle owner; inherited and new writes
	// use the same footprint acquisition without a separately callable entry.
	acquireKeys := func(admission context.Context, owner string) (func(), error) {
		authority := entityBodyAuthority
		if len(plans) > 0 {
			authority = plans[0].authority
		}
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
				footprint := entityAuthorityFootprint{nodes: entities.nodes, edges: entities.edges}
				if authority == entityBodyAuthority {
					footprint.nodeBodies, footprint.edgeBodies = entities.nodes, entities.edges
				}
				keys, err := entityCommitKeys(admission, footprint)
				if err != nil {
					releaseAll()
					return nil, err
				}
				release, err := b.GetSchemaForNamespace(ns).acquireUniqueConstraintCommitLocks(admission, owner, keys)
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
		var bodyNodes []NodeID
		var bodyEdges []EdgeID
		if authority == entityBodyAuthority {
			if !isEdge {
				bodyNodes = []NodeID{NodeID(id)}
			}
			bodyEdges = edges
		}
		keys, err := entityCommitKeys(admission, entityAuthorityFootprint{nodes: nodes, edges: edges, nodeBodies: bodyNodes, edgeBodies: bodyEdges})
		if err != nil {
			return nil, err
		}
		return b.GetSchemaForNamespace(namespace).acquireUniqueConstraintCommitLocks(admission, owner, keys)
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
	ordered, err := b.beginBarrierWrite(ctx, false, true)
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
