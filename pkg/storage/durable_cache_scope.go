package storage

import (
	"context"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// cacheWriteScope projects immutable affected facts and physical-key aliases.
// An unknown raw action revokes all representations; metadata explicitly names
// an empty scope. Normal graph writes never require a scan of cache populations.
type cacheWriteScope struct {
	known bool
	facts AffectedFacts
}

func (scope cacheWriteScope) complete() bool {
	if projection, ok := scope.facts.(*physicalCacheProjection); ok && projection.unknown {
		return false
	}
	return scope.known
}

type durableCacheState struct {
	mu        sync.Mutex
	all       uint64
	latest    uint64
	nodes     map[NodeID]uint64
	edges     map[EdgeID]uint64
	adjacency map[NodeID]uint64
	types     map[string]uint64
	labels    map[string]uint64
}

func (state *durableCacheState) nodeFloor(id NodeID) uint64 {
	return max(state.all, state.nodes[id])
}
func (state *durableCacheState) edgeFloor(id EdgeID) uint64 {
	return max(state.all, state.edges[id])
}

// advanceFloorLocked forgets individual floors only after a common source cut
// covers them. Existing unrelated cache bodies stay warm; older in-flight fills
// and delayed writer images remain unable to reinstall a superseded body.
func (state *durableCacheState) advanceFloorLocked() {
	state.all = max(state.all, state.latest)
	state.nodes, state.edges, state.adjacency, state.types, state.labels = nil, nil, nil, nil, nil
}

// commitDurableWrite is the retained managed writer's physical action. Cache
// revocation belongs inside the action, after successful durability and before
// oracle completion. Preparation/replay pass a context with no semantic owner.
func (m *managedBadgerDB) commitDurableWrite(txn *badger.Txn, ts uint64, scope cacheWriteScope, ctx context.Context) error {
	return m.durableAction(ts, scope, ctx, func() error { return txn.CommitAt(ts, nil) })
}

// durableAction also encloses Badger's raw restore/reset actions. Those APIs
// have different physical calls, but share the same decision and revocation.
func (m *managedBadgerDB) durableAction(ts uint64, scope cacheWriteScope, ctx context.Context, action func() error) error {
	return DecidePublication(ctx, func() error {
		if err := action(); err != nil {
			return err
		}
		if m.cacheOwner != nil {
			m.cacheOwner.issueDurableCacheScope(ts, scope)
		}
		runDurableCacheHook(ts)
		return nil
	})
}

func (b *BadgerEngine) issueDurableCacheScope(ts uint64, scope cacheWriteScope) {
	state := &b.durableCaches
	state.mu.Lock()
	defer state.mu.Unlock()
	state.latest = max(state.latest, ts)
	if !scope.complete() {
		state.advanceFloorLocked()
		b.invalidateCachesAfterRestore()
		return
	}
	if scope.facts == nil {
		return
	}
	scope.facts.VisitAffectedFacts(func(fact AffectedFact) {
		if fact.NodeID != "" {
			if state.nodes == nil {
				state.nodes = make(map[NodeID]uint64)
			}
			state.nodes[fact.NodeID] = max(state.nodes[fact.NodeID], ts)
			b.cacheDeleteNode(fact.NodeID, nodeCacheAllForms)
			if fact.Label != "" {
				if state.labels == nil {
					state.labels = make(map[string]uint64)
				}
				state.labels[fact.Label] = max(state.labels[fact.Label], ts)
				b.labelCacheInvalidateForNodeLabels([]string{fact.Label}, fact.NodeID)
			}
		}
		if fact.EdgeID != "" {
			if state.edges == nil {
				state.edges = make(map[EdgeID]uint64)
			}
			state.edges[fact.EdgeID] = max(state.edges[fact.EdgeID], ts)
			b.cacheDeleteEdge(fact.EdgeID)
			if fact.StartNode != "" || fact.EndNode != "" {
				if state.adjacency == nil {
					state.adjacency = make(map[NodeID]uint64)
				}
				state.adjacency[fact.StartNode] = max(state.adjacency[fact.StartNode], ts)
				state.adjacency[fact.EndNode] = max(state.adjacency[fact.EndNode], ts)
				b.adjCacheInvalidate(fact.StartNode, fact.EndNode)
			}
			if fact.Type != "" {
				if state.types == nil {
					state.types = make(map[string]uint64)
				}
				state.types[fact.Type] = max(state.types[fact.Type], ts)
				b.InvalidateEdgeTypeCache(fact.Type)
			}
		}
	})
	// Floor retention follows the engine's existing cache capacities, rather
	// than the number of entities ever written. Coarsening scans no payloads
	// or cache populations and does not clear unrelated cached entries.
	limit := max(1, b.nodeCacheMaxEntries, b.edgeCacheMaxItems, b.adjCacheMaxNodes, b.edgeTypeCacheMaxTypes, b.labelFirstCacheMax)
	if len(state.nodes)+len(state.edges)+len(state.adjacency)+len(state.types)+len(state.labels) > limit {
		state.advanceFloorLocked()
	}
}

var durableCacheHook struct {
	sync.RWMutex
	run func(uint64)
}

func runDurableCacheHook(ts uint64) {
	durableCacheHook.RLock()
	run := durableCacheHook.run
	durableCacheHook.RUnlock()
	if run != nil {
		run(ts)
	}
}

func setDurableCacheHook(run func(uint64)) func() {
	durableCacheHook.Lock()
	previous := durableCacheHook.run
	durableCacheHook.run = run
	durableCacheHook.Unlock()
	return func() {
		durableCacheHook.Lock()
		durableCacheHook.run = previous
		durableCacheHook.Unlock()
	}
}
