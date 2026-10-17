package storage

import "sync"

// Ordered post-commit publication (F-PUBLISH).
//
// After a commit, the structures derived from committed entities are updated
// in memory: the node, query-node and edge caches, the label, edge-type and
// adjacency caches, the property and composite indexes, the unique-value
// registries, the relationship-uniqueness index, the cached counts and the
// pending-embedding candidates. The contract, per entity and per unique
// value: what those structures hold for it is the result of the commits that
// touch it applied in commit-timestamp order up to some timestamp, and a
// write returns only once that covers its own commit.
//
// Each commit's update is its own delta (old to new, from the commit's own
// operations), which is correct only right after the update of the commit
// before it that touched the same entity or value. So every commit hands its
// delta, with the keys it touches, to the commit writer
// (commitWriter.publish, publishKeys) instead of applying it, and the commit
// oracle runs them in one stage (publicationStage): each once, after Badger
// has published it and after every earlier commit's publication that shares
// one of its keys. Publications with disjoint keys run in parallel. The
// committer waits for its own publication before it returns. Engine writes,
// transaction commits, write-behind replay, the decay suppression rewrites
// and the embedding sidecar all commit through the oracle.
//
// A key is an entity (NodeID, EdgeID) or a unique value the commit frees or
// takes (uniqueConstraintLockKey, as the key locks name them). The consumers
// whose updates commute need no key: counts, label, edge-type and adjacency
// cache invalidations, pending-embedding candidates.
//
// A publication only updates in-memory structures: it takes their own locks
// and never commits, so the stage never waits on itself
// (TestPublicationStageBoundary). Listeners (commit notifications) and
// Badger-side follow-ups (positional counts, ID counters, MVCC sequence)
// stay outside it.

// publicationKey is an entity (node or edge) or a unique value a commit's
// publication touches. It is a plain comparable value, so building and
// indexing keys does not box them.
type publicationKey struct {
	node   NodeID
	edge   EdgeID
	unique uniqueConstraintLockKey
}

// commitPublication is a commit's derived-state publication, the keys it is
// ordered on, and its place in the stage once registered.
type commitPublication struct {
	keys []publicationKey
	// publish is the writer's publication; pendingEmbeddingIDs are the
	// pending-embedding candidates the commit added (commutative, no key).
	publish             func()
	pendingEmbeddingIDs map[NodeID]struct{}
	engine              *BadgerEngine

	ts      uint64
	running bool
}

func (p *commitPublication) run() {
	if len(p.pendingEmbeddingIDs) > 0 {
		p.engine.publishPendingEmbeddingAdds(p.pendingEmbeddingIDs)
	}
	if p.publish != nil {
		p.publish()
	}
}

// keyQueue is the pending timestamps of one key: the first inline, so the
// common case of one pending publication per key allocates nothing.
type keyQueue struct {
	first uint64
	rest  []uint64
}

// publicationStage orders the publications of written commits by commit
// timestamp, per key.
type publicationStage struct {
	mu      sync.Mutex
	changed sync.Cond
	pending map[uint64]*commitPublication
	byKey   map[publicationKey]keyQueue
}

// register adds the publication of the commit written at ts. The committer
// registers it after its write and before ts is finished, so every
// publication of a lower timestamp is registered once ts is published.
func (s *publicationStage) register(ts uint64, pub *commitPublication) {
	if pub == nil {
		return
	}
	pub.ts = ts
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		s.pending = make(map[uint64]*commitPublication)
		s.byKey = make(map[publicationKey]keyQueue)
		s.changed.L = &s.mu
	}
	s.pending[ts] = pub
	for _, key := range pub.keys {
		queue, queued := s.byKey[key]
		if !queued {
			s.byKey[key] = keyQueue{first: ts}
			continue
		}
		queue.rest = append(queue.rest, ts)
		s.byKey[key] = queue
	}
}

// apply returns once the publication registered at ts has run, which the
// caller has seen published. It runs it itself when no earlier pending
// publication shares a key with it; otherwise it runs the earliest such
// publication that can run, or waits for the one running elsewhere. So no
// publication depends on its own committer's goroutine.
func (s *publicationStage) apply(ts uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		p := s.pending[ts]
		if p == nil {
			return
		}
		if next := s.runnableLocked(p); next != nil {
			s.runLocked(next)
			continue
		}
		s.changed.Wait()
	}
}

// runnableLocked returns the publication to run to make progress toward p: p
// itself when no earlier pending publication shares a key with it, else the
// one its earliest such publication waits on. Nil when that one is running.
func (s *publicationStage) runnableLocked(p *commitPublication) *commitPublication {
	for {
		earlier := s.earliestDependencyLocked(p)
		if earlier == nil {
			if p.running {
				return nil
			}
			return p
		}
		p = earlier
	}
}

func (s *publicationStage) earliestDependencyLocked(p *commitPublication) *commitPublication {
	var earliest uint64
	consider := func(at uint64) {
		if at < p.ts && (earliest == 0 || at < earliest) {
			earliest = at
		}
	}
	for _, key := range p.keys {
		queue := s.byKey[key]
		consider(queue.first)
		for _, at := range queue.rest {
			consider(at)
		}
	}
	if earliest == 0 {
		return nil
	}
	return s.pending[earliest]
}

// runLocked runs p without holding the stage's lock and retires it, also
// when it panics: the panic surfaces in this caller and the stage goes on.
func (s *publicationStage) runLocked(p *commitPublication) {
	p.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.retireLocked(p)
	}()
	runPublicationStageHook(p.ts)
	p.run()
}

func (s *publicationStage) retireLocked(p *commitPublication) {
	delete(s.pending, p.ts)
	for _, key := range p.keys {
		queue, queued := s.byKey[key]
		if !queued {
			continue
		}
		switch {
		case queue.first == p.ts && len(queue.rest) == 0:
			delete(s.byKey, key)
			continue
		case queue.first == p.ts:
			queue.first, queue.rest = queue.rest[0], queue.rest[1:]
		default:
			for i, at := range queue.rest {
				if at == p.ts {
					queue.rest = append(queue.rest[:i], queue.rest[i+1:]...)
					break
				}
			}
		}
		if len(queue.rest) == 0 {
			queue.rest = nil
		}
		s.byKey[key] = queue
	}
	s.changed.Broadcast()
}

// pendingCount is the number of registered publications not yet run.
func (s *publicationStage) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// appendNodePublicationKeys appends the keys a publication of a node write
// is ordered on: the node, and the unique values old (the pre-write node)
// and written hold, the ones the write frees and takes.
func appendNodePublicationKeys(keys []publicationKey, schema *SchemaManager, id NodeID, old, written *Node) []publicationKey {
	keys = append(keys, publicationKey{node: id})
	if schema == nil {
		return keys
	}
	return schema.appendUniqueValueKeys(schema.appendUniqueValueKeys(keys, old), written)
}

// appendUniqueValueKeys appends the keys of node's values under the unique
// constraints the unique-value registry tracks for its labels.
func (sm *SchemaManager) appendUniqueValueKeys(keys []publicationKey, node *Node) []publicationKey {
	if node == nil {
		return keys
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, uc := range sm.uniqueConstraints {
		if !hasLabel(node.Labels, uc.Label) {
			continue
		}
		value, ok := node.Properties[uc.Property]
		if !ok {
			continue
		}
		if key, keyed := uniqueConstraintValueKey(value); keyed {
			keys = append(keys, publicationKey{unique: uniqueConstraintLockKey{label: uc.Label, property: uc.Property, value: key}})
		}
	}
	return keys
}

// appendEdgePublicationKeys appends the keys a publication of an edge write
// is ordered on: the edge, and the relationship-uniqueness values old and
// written hold.
func appendEdgePublicationKeys(keys []publicationKey, schema *SchemaManager, id EdgeID, old, written *Edge) []publicationKey {
	keys = append(keys, publicationKey{edge: id})
	if schema == nil {
		return keys
	}
	return schema.appendRelationshipUniqueKeys(schema.appendRelationshipUniqueKeys(keys, old), written)
}

// appendRelationshipUniqueKeys appends the keys of edge's values under the
// relationship uniqueness constraints of its type, as the key locks name
// them (lockRelationshipConstraintKeysOf).
func (sm *SchemaManager) appendRelationshipUniqueKeys(keys []publicationKey, edge *Edge) []publicationKey {
	if edge == nil || len(edge.Properties) == 0 {
		return keys
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, c := range sm.constraints {
		if c.Label != edge.Type || c.EffectiveEntityType() != ConstraintEntityRelationship || (c.Type != ConstraintUnique && c.Type != ConstraintRelationshipKey) {
			continue
		}
		values, ok := relationshipConstraintValueKeys(c.Properties, edge.Properties, true)
		if !ok || len(values) == 0 {
			continue
		}
		var key interface{} = values[0]
		if len(values) > 1 {
			key = NewCompositeKey(values...).Hash
		}
		keys = append(keys, publicationKey{unique: uniqueConstraintLockKey{label: "\x00relationship:" + edge.Type, property: relationshipPropertyKey(c.Properties), value: key}})
	}
	return keys
}

// appendOperationPublicationKeys appends the keys of a commit's operations:
// every node and edge they write, edges a node delete removed, and the unique
// values they free and take.
func appendOperationPublicationKeys(keys []publicationKey, schema *SchemaManager, ops []Operation) []publicationKey {
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil {
				keys = appendNodePublicationKeys(keys, schema, op.Node.ID, op.OldNode, op.Node)
			}
		case OpDeleteNode:
			keys = appendNodePublicationKeys(keys, schema, op.NodeID, op.OldNode, nil)
			for _, edgeID := range op.DeletedEdgeIDs {
				keys = append(keys, publicationKey{edge: edgeID})
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge != nil {
				keys = appendEdgePublicationKeys(keys, schema, op.Edge.ID, op.OldEdge, op.Edge)
			}
		case OpDeleteEdge:
			keys = appendEdgePublicationKeys(keys, schema, op.EdgeID, op.OldEdge, nil)
		}
	}
	return keys
}

var (
	publicationStageHookMu sync.RWMutex
	publicationStageHook   func(ts uint64)
)

// setPublicationStageHook installs a test seam the stage calls before each
// publication it runs, with the commit's timestamp, and returns the restore.
func setPublicationStageHook(hook func(ts uint64)) func() {
	publicationStageHookMu.Lock()
	previous := publicationStageHook
	publicationStageHook = hook
	publicationStageHookMu.Unlock()
	return func() {
		publicationStageHookMu.Lock()
		publicationStageHook = previous
		publicationStageHookMu.Unlock()
	}
}

func runPublicationStageHook(ts uint64) {
	publicationStageHookMu.RLock()
	hook := publicationStageHook
	publicationStageHookMu.RUnlock()
	if hook != nil {
		hook(ts)
	}
}
