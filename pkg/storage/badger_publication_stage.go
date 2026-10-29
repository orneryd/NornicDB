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
// A publication updates in-memory structures and may read immutable commit-
// version images for accounting. It never commits or takes managed admission,
// so the stage never waits on itself
// (TestPublicationStageBoundary). Listeners (commit notifications) and
// Badger-side follow-ups (positional counts, ID counters, MVCC sequence)
// stay outside it.

// commitPublication is a commit's derived-state publication and the keys it
// is ordered on.
type commitPublication struct {
	keys  []any
	owner *commitWriter // prepares images and reserves notifications at the physical owner
	run   func()
	ts    uint64 // assigned by register, before any publication can run
}

type stagedPublication struct {
	ts      uint64
	keys    []any
	run     func()
	running bool
}

// publicationKeyQueue keeps the first timestamp inline: most entity keys
// have only one pending writer and need no separately allocated slice.
// Further timestamps retain registration order, which may differ from commit
// order; dependency selection still examines every pending timestamp.
type publicationKeyQueue struct {
	first uint64
	rest  []uint64
}

// publicationStage orders the publications of written commits by commit
// timestamp, per key.
type publicationStage struct {
	mu      sync.Mutex
	changed sync.Cond
	pending map[uint64]*stagedPublication
	byKey   map[any]publicationKeyQueue // the pending timestamps of each key
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
		s.pending = make(map[uint64]*stagedPublication)
		s.byKey = make(map[any]publicationKeyQueue)
		s.changed.L = &s.mu
	}
	s.pending[ts] = &stagedPublication{ts: ts, keys: pub.keys, run: pub.run}
	for _, key := range pub.keys {
		queue, exists := s.byKey[key]
		// Registration holds mu for the entire commit, so another occurrence
		// of its timestamp is either first or the last appended timestamp.
		// Entity operations and embedding hints may name the same key.
		if exists && (queue.first == ts || (len(queue.rest) > 0 && queue.rest[len(queue.rest)-1] == ts)) {
			continue
		}
		if !exists {
			queue.first = ts
		} else {
			queue.rest = append(queue.rest, ts)
		}
		s.byKey[key] = queue
	}
}

// apply returns once the publication registered at ts has run, which the
// caller has seen published. It runs it itself when no earlier pending
// publication shares a key with it; otherwise it runs the earliest such
// publication that can run, or waits for the one running elsewhere. So no
// publication depends on its own committer's goroutine. A zero timestamp drains
// all registered publications under the caller's exclusive commit gate.
func (s *publicationStage) apply(ts uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		target := ts
		if ts == 0 {
			// Zero selects the complete physical cut. Its caller holds the
			// commit gate exclusively, so no registration can extend it.
			for pending := range s.pending {
				if target == 0 || pending < target {
					target = pending
				}
			}
		}
		p := s.pending[target]
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
func (s *publicationStage) runnableLocked(p *stagedPublication) *stagedPublication {
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

func (s *publicationStage) earliestDependencyLocked(p *stagedPublication) *stagedPublication {
	var earliest *stagedPublication
	for _, key := range p.keys {
		queue := s.byKey[key]
		if queue.first < p.ts && (earliest == nil || queue.first < earliest.ts) {
			earliest = s.pending[queue.first]
		}
		for _, at := range queue.rest {
			if at < p.ts && (earliest == nil || at < earliest.ts) {
				earliest = s.pending[at]
			}
		}
	}
	return earliest
}

// runLocked runs p without holding the stage's lock and retires it, also
// when it panics: the panic surfaces in this caller and the stage goes on.
func (s *publicationStage) runLocked(p *stagedPublication) {
	p.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.retireLocked(p)
	}()
	runPublicationStageHook(p.ts)
	p.run()
}

func (s *publicationStage) retireLocked(p *stagedPublication) {
	delete(s.pending, p.ts)
	for _, key := range p.keys {
		queue, exists := s.byKey[key]
		if !exists {
			continue // a repeated dependency was already retired
		}
		if queue.first == p.ts {
			if len(queue.rest) == 0 {
				delete(s.byKey, key)
				continue
			}
			queue.first, queue.rest = queue.rest[0], queue.rest[1:]
		} else {
			for i, at := range queue.rest {
				if at == p.ts {
					queue.rest = append(queue.rest[:i], queue.rest[i+1:]...)
					break
				}
			}
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

// publicationUniqueKey scopes uniqueness publication to its database.
type publicationUniqueKey struct {
	namespace string
	value     uniqueConstraintLockKey
}

// appendNodePublicationKeys appends the keys a publication of a node write
// is ordered on: the node, and the unique values old (the pre-write node)
// and written hold, the ones the write frees and takes.
//
// view is the commit's captured constraint metadata. It supplies every rule
// this reads and takes no lock, so late derivation from a restored operation
// never waits on the schema while the caller may hold the commit gate.
func appendNodePublicationKeys(keys []any, view *publicationConstraintView, id NodeID, old, written *Node) []any {
	keys = append(keys, id)
	if view == nil || view.nodeRules == 0 {
		return keys
	}
	namespace := namespaceForNodeID(id)
	// The rules are walked here, not in a callback: a rule is then addressed
	// through its index and its strings are never copied into a closure.
	for i := -1; i < len(view.rest); i++ {
		rule := &view.first
		if i >= 0 {
			rule = &view.rest[i]
		}
		if rule.empty() || rule.kind != ConstraintEntityNode {
			continue
		}
		// An unchanged UNIQUE value is one dependency, not two queue entries.
		var previous uniqueConstraintLockKey
		var havePrevious bool
		for _, node := range [2]*Node{old, written} {
			if node == nil || !hasLabel(node.Labels, rule.label) {
				continue
			}
			value, ok := node.Properties[rule.property]
			if !ok {
				continue
			}
			key, keyed := indexValueKey(value)
			if !keyed {
				continue
			}
			dependency := uniqueConstraintLockKey{label: rule.label, property: rule.property, value: key}
			if !havePrevious || dependency != previous {
				keys = append(keys, publicationUniqueKey{namespace, dependency})
			}
			previous, havePrevious = dependency, true
		}
	}
	return keys
}

// appendEdgePublicationKeys appends the keys a publication of an edge write
// is ordered on: the edge, and the relationship-uniqueness values old and
// written hold. view supplies every rule, as for nodes.
func appendEdgePublicationKeys(keys []any, view *publicationConstraintView, id EdgeID, old, written *Edge) []any {
	keys = append(keys, id)
	if view == nil {
		return keys
	}
	namespace := namespaceForEdgeID(id)
	for i := -1; i < len(view.rest); i++ {
		rule := &view.first
		if i >= 0 {
			rule = &view.rest[i]
		}
		if rule.empty() || rule.kind != ConstraintEntityRelationship {
			continue
		}
		label := "\x00relationship:" + rule.label
		for _, edge := range [2]*Edge{old, written} {
			if edge == nil || len(edge.Properties) == 0 || edge.Type != rule.label {
				continue
			}
			properties := rule.properties
			if len(properties) == 0 {
				single := [1]string{rule.property}
				properties = single[:]
			}
			values, ok := relationshipConstraintValueKeys(properties, edge.Properties, true)
			if !ok || len(values) == 0 {
				continue
			}
			var key interface{} = values[0]
			if len(values) > 1 {
				key = NewCompositeKey(values...).Hash
			}
			keys = append(keys, publicationUniqueKey{namespace, uniqueConstraintLockKey{label: label, property: rule.propertyKey, value: key}})
		}
	}
	return keys
}

// appendOperationPublicationKeys appends the keys of a commit's operations:
// every node and edge they write, edges a node delete removed, and the unique
// values they free and take.
func appendOperationPublicationKeys(keys []any, view *publicationConstraintView, ops []Operation) []any {
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil {
				keys = appendNodePublicationKeys(keys, view, op.Node.ID, op.OldNode, op.Node)
			}
		case OpDeleteNode:
			keys = appendNodePublicationKeys(keys, view, op.NodeID, op.OldNode, nil)
			for _, edgeID := range op.DeletedEdgeIDs {
				keys = append(keys, edgeID)
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge != nil {
				keys = appendEdgePublicationKeys(keys, view, op.Edge.ID, op.OldEdge, op.Edge)
			}
		case OpDeleteEdge:
			keys = appendEdgePublicationKeys(keys, view, op.EdgeID, op.OldEdge, nil)
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
