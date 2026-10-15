package storage

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// CheckUniqueConstraint checks if a value violates a unique constraint.
// Returns error if constraint is violated.
func (sm *SchemaManager) CheckUniqueConstraint(label, property string, value interface{}, excludeNode NodeID) error {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return nil // No constraint
	}

	constraint.mu.RLock()
	defer constraint.mu.RUnlock()

	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return nil
	}

	if existingNode, found := constraint.values[valueKey]; found {
		if existingNode != excludeNode {
			return localizedError(localization.StorageSchemaUniqueConstraintViolation(label, property, value), nil)
		}
	}

	return nil
}

// LookupUniqueConstraintValue returns the node currently registered for a
// single-property uniqueness constraint value. The second return value reports
// whether the value is present, and the third reports whether the unique
// constraint exists.
func (sm *SchemaManager) LookupUniqueConstraintValue(label, property string, value interface{}) (NodeID, bool, bool) {
	nodeID, found, exists, _ := sm.LookupUniqueConstraintValueForPlanning(label, property, value)
	return nodeID, found, exists
}

// LookupUniqueConstraintValueForPlanning returns the node currently registered
// for a single-property uniqueness constraint value, plus whether the values
// cache has been rebuilt from storage and can be trusted for misses. Planners
// may trust absence only when cacheComplete is true; otherwise they must retain
// a scan fallback because the cache may not have been rebuilt from storage yet.
func (sm *SchemaManager) LookupUniqueConstraintValueForPlanning(label, property string, value interface{}) (nodeID NodeID, valueFound bool, constraintExists bool, cacheComplete bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, exists, false
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return "", false, true, false
	}
	nodeID, valueFound, cacheComplete = sm.lookupUniqueValue(constraint, label, property, value, valueKey)
	return nodeID, valueFound, true, cacheComplete
}

func (sm *SchemaManager) lookupUniqueConstraintValueForValidation(label, property string, value interface{}) (nodeID NodeID, valueFound bool, cacheComplete bool, constraintExists bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, false, exists
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return "", false, false, true
	}

	nodeID, valueFound, cacheComplete = sm.lookupUniqueValue(constraint, label, property, value, valueKey)
	return nodeID, valueFound, cacheComplete, true
}

// uniqueValueHolders returns the nodes holding value under label's unique
// constraint on property, merged with the pending writes: the registered
// holder unless a pending write supersedes it, and every pending node with
// the value. cacheComplete reports whether the registered values cover all
// stored nodes, so an empty result can be trusted.
func (sm *SchemaManager) uniqueValueHolders(label, property string, value interface{}) (holders []NodeID, cacheComplete bool, constraintExists bool) {
	sm.mu.RLock()
	constraint, exists := sm.uniqueConstraints[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()
	if !exists {
		return nil, false, false
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return nil, false, true
	}
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	holder, found := constraint.values[valueKey]
	cacheComplete = constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && view.keep(holder) {
		holders = append(holders, holder)
	}
	holders = view.uniqueValueMatches(holders, label, property, value, valueKey)
	return holders, cacheComplete, true
}

// lookupUniqueValue returns the node holding valueKey under constraint,
// merged with the pending writes: a registered holder with a pending update
// or delete no longer counts, and a pending node with the value does.
func (sm *SchemaManager) lookupUniqueValue(constraint *UniqueConstraint, label, property string, value, valueKey interface{}) (nodeID NodeID, found bool, cacheComplete bool) {
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	nodeID, found = constraint.values[valueKey]
	cacheComplete = constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && !view.keep(nodeID) {
		nodeID, found = "", false
	}
	if pending := view.uniqueValueMatches(nil, label, property, value, valueKey); len(pending) > 0 {
		sort.Slice(pending, func(i, j int) bool { return string(pending[i]) < string(pending[j]) })
		nodeID, found = pending[0], true
	}
	return nodeID, found, cacheComplete
}

// uniqueConstraintLockKey identifies one (label, property, value) triple for
// the purpose of acquiring a commit-time mutex. Two transactions whose
// pending nodes touch the same (label, property, value) serialize at commit;
// transactions touching disjoint values commit in parallel. The granularity
// is per constrained value, not per constraint.
//
// The value is stored in its canonical comparable form returned by
// uniqueConstraintValueKey so semantically equal but type-distinct values
// (e.g. int and int64) use the same lock and serialize correctly. Values
// that are not comparable cannot acquire a lock; their constraint is still
// validated at commit but without commit-window serialization. (In practice
// every UNIQUE-constrained property in Eshu and Neo4j-compatible workloads
// uses comparable scalar types — strings, ints, floats, bools.)
//
// Lock granularity history: an earlier per-(label, property) design
// effectively serialized every writer touching any value of a constrained
// property. Under bootstrap-index Pass 2 fan-out (8 projector workers + the
// collector + the ingester all writing TerraformResource nodes with
// disjoint uids), this collapsed throughput to single-writer levels — a
// "serialization workaround" in disguise. Per-value locking lets disjoint
// writers commit concurrently while still preventing the silent-overwrite
// race that motivated the lock.
type uniqueConstraintLockKey struct {
	label    string
	property string
	value    interface{}
}

// uniqueConstraintCommitLock is one constraint key's lock. It belongs to one
// owner at a time (a transaction ID, or a direct engine write's own ID); the
// owner may take it again (holds counts its acquisitions), and released is
// closed whenever it becomes free, waking its waiters (#961).
type uniqueConstraintCommitLock struct {
	owner    string
	holds    int
	refs     int
	order    uint64
	released chan struct{}
}

type uniqueConstraintLockRequest struct {
	key  uniqueConstraintLockKey
	lock *uniqueConstraintCommitLock
}

// lockConstraintKeysOf acquires the commit locks of the constraint keys the
// nodes' values fall under, and returns their release function: the exact
// value of each single-property UNIQUE constraint, the composite value of each
// NODE KEY, and the key value of each TEMPORAL (no-overlap) constraint. Every
// writer of a constrained node holds them across its constraint check, its
// write and the publication of the value to the constraint cache: a
// transaction at commit, and the engine's own CreateNode / UpdateNode /
// BulkCreateNodes. So two writers of the same key (two transactions, or a
// transaction and a direct engine write such as the AsyncEngine's
// write-through of a constrained node) serialize, and the second one's check
// sees the first one's write (#700).
//
// A key that can't be a map key (a list or map value) takes no lock; the
// constraint check still runs, and serialization is best-effort for such
// values, which constrained workloads don't use in practice. A NODE KEY with a
// missing property takes no lock: the check rejects the node anyway.
//
// owner holds the locks (acquireUniqueConstraintCommitLocks); keys it already
// holds, such as a MERGE key a transaction locked, are taken again without
// waiting.
func (sm *SchemaManager) lockConstraintKeysOf(ctx context.Context, owner string, nodes ...*Node) (func(), error) {
	if sm == nil || len(nodes) == 0 {
		return func() {}, nil
	}
	// Allocated on the first key: an unconstrained write allocates nothing.
	var keys []uniqueConstraintLockKey
	for _, node := range nodes {
		if node == nil || len(node.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels(node.Labels) {
			if c.EffectiveEntityType() != ConstraintEntityNode {
				continue
			}
			var properties []string
			switch {
			case c.Type == ConstraintUnique && len(c.Properties) > 0:
				properties = c.Properties
			case c.Type == ConstraintNodeKey:
				properties = c.Properties
			case c.Type == ConstraintTemporal && len(c.Properties) >= 3:
				// The grouping key: every property before (valid_from, valid_to).
				properties = c.Properties[:len(c.Properties)-2]
			default:
				continue
			}
			if len(properties) == 1 {
				// One key property (UNIQUE, a one-property NODE KEY, the
				// temporal key): no intermediate slice.
				value, ok := constraintLockValue(node, properties[0])
				if ok {
					if keys == nil {
						keys = make([]uniqueConstraintLockKey, 0, len(nodes))
					}
					keys = append(keys, uniqueConstraintLockKey{label: c.Label, property: properties[0], value: value})
				}
				continue
			}
			values := make([]interface{}, len(properties))
			complete := true
			for i, prop := range properties {
				value, ok := constraintLockValue(node, prop)
				if !ok {
					complete = false
					break
				}
				values[i] = value
			}
			if complete {
				keys = append(keys, uniqueConstraintLockKey{
					label:    c.Label,
					property: string(c.Type) + ":" + strings.Join(properties, ","),
					value:    constraintCompositeKey(values),
				})
			}
		}
	}
	return sm.acquireUniqueConstraintCommitLocks(ctx, owner, keys)
}

// constraintLockValue is node's canonical value of prop for a constraint key
// lock; ok is false when the property is missing, null or not comparable (no
// key to lock).
func constraintLockValue(node *Node, prop string) (interface{}, bool) {
	rawValue, has := node.Properties[prop]
	if !has || rawValue == nil {
		return nil, false
	}
	return uniqueConstraintValueKey(rawValue)
}

// acquireUniqueConstraintCommitLocks acquires exact UNIQUE value locks for
// owner in a deterministic order and returns a release function.
// Deterministic ordering eliminates the AB-BA deadlock risk when two
// batches both touch overlapping sets of constrained values. A lock owner
// already holds (a MERGE key its transaction locked, #961) is taken again
// without waiting, and released with this release.
//
// Duplicate keys in the input are deduplicated. An empty input returns a
// no-op release function so callers can safely defer the result regardless
// of whether locks were acquired.
//
// Each lock guards the entire commit window for its specific value —
// validateAllConstraints, badgerTx.Commit, and the RegisterUniqueValue call
// that publishes the committed value to the constraint cache — so a
// subsequent transaction touching the same value always observes a coherent
// cache. Transactions touching disjoint values always acquire disjoint locks.
// Registry entries count both holders and waiters and are evicted when that
// count reaches zero, bounding memory by active commit demand rather than the
// historical graph cardinality.
//
// A wait that would close a cycle of owners waiting for each other fails with
// ErrDeadlock, and a wait ends with ctx's error when ctx ends. The locks
// taken so far are released either way.
func (sm *SchemaManager) acquireUniqueConstraintCommitLocks(ctx context.Context, owner string, keys []uniqueConstraintLockKey) (func(), error) {
	if len(keys) == 0 {
		return func() {}, nil
	}
	requests := make([]uniqueConstraintLockRequest, 0, len(keys))
	seen := make(map[uniqueConstraintLockKey]struct{}, len(keys))
	for _, k := range keys {
		valueType := reflect.TypeOf(k.value)
		if valueType != nil && !valueType.Comparable() {
			continue
		}
		if k.value != nil && k.value != k.value {
			// Non-reflexive comparable values such as NaN never conflict
			// under equality and cannot be safely used as registry map keys:
			// a lookup or delete with the same value would never find them.
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		requests = append(requests, uniqueConstraintLockRequest{key: k})
	}
	if len(requests) == 0 {
		return func() {}, nil
	}

	sm.uniqueConstraintCommitLocksMu.Lock()
	if sm.uniqueConstraintCommitLocks == nil {
		sm.uniqueConstraintCommitLocks = make(map[uniqueConstraintLockKey]*uniqueConstraintCommitLock, len(requests))
	}
	if len(sm.uniqueConstraintCommitLocks) == 0 {
		sm.uniqueConstraintCommitOrder = 0
	}
	for i := range requests {
		lock := sm.uniqueConstraintCommitLocks[requests[i].key]
		if lock == nil {
			sm.uniqueConstraintCommitOrder++
			if sm.uniqueConstraintCommitOrder == 0 {
				panic("UNIQUE commit lock order overflow")
			}
			lock = &uniqueConstraintCommitLock{order: sm.uniqueConstraintCommitOrder, released: make(chan struct{})}
			sm.uniqueConstraintCommitLocks[requests[i].key] = lock
		}
		lock.refs++
		requests[i].lock = lock
	}
	sm.uniqueConstraintCommitLocksMu.Unlock()
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].lock.order < requests[j].lock.order
	})

	for i := range requests {
		if err := sm.waitForUniqueConstraintLock(ctx, owner, requests[i]); err != nil {
			sm.releaseUniqueConstraintLocks(owner, requests[:i], requests[i:])
			return nil, err
		}
	}
	return func() {
		sm.releaseUniqueConstraintLocks(owner, requests, nil)
	}, nil
}

// waitForUniqueConstraintLock takes request's lock for owner, waiting while
// another owner holds it.
func (sm *SchemaManager) waitForUniqueConstraintLock(ctx context.Context, owner string, request uniqueConstraintLockRequest) error {
	lock := request.lock
	sm.uniqueConstraintCommitLocksMu.Lock()
	for {
		if lock.owner == "" || lock.owner == owner {
			lock.owner = owner
			lock.holds++
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return nil
		}
		if sm.uniqueConstraintLockWaitClosesCycleLocked(owner, lock) {
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return localizedError(localization.StorageTransactionDeadlockDetected(request.key.label, request.key.property), ErrDeadlock)
		}
		if sm.uniqueConstraintLockWaits == nil {
			sm.uniqueConstraintLockWaits = make(map[string]*uniqueConstraintCommitLock)
		}
		sm.uniqueConstraintLockWaits[owner] = lock
		released := lock.released
		sm.uniqueConstraintCommitLocksMu.Unlock()
		select {
		case <-released:
		case <-ctx.Done():
			sm.uniqueConstraintCommitLocksMu.Lock()
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return ctx.Err()
		}
		sm.uniqueConstraintCommitLocksMu.Lock()
	}
}

// uniqueConstraintLockWaitClosesCycleLocked reports whether owner waiting for
// lock would close a cycle: lock's owner waits, directly or through other
// owners, for a lock owner holds.
func (sm *SchemaManager) uniqueConstraintLockWaitClosesCycleLocked(owner string, lock *uniqueConstraintCommitLock) bool {
	holder := lock.owner
	for steps := 0; holder != "" && steps <= len(sm.uniqueConstraintLockWaits); steps++ {
		if holder == owner {
			return true
		}
		next, waiting := sm.uniqueConstraintLockWaits[holder]
		if !waiting {
			return false
		}
		holder = next.owner
	}
	return false
}

// releaseUniqueConstraintLocks releases owner's hold on each of held and drops
// the registry references of held and unacquired.
func (sm *SchemaManager) releaseUniqueConstraintLocks(owner string, held, unacquired []uniqueConstraintLockRequest) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for i := len(held) - 1; i >= 0; i-- {
		lock := held[i].lock
		if lock.owner != owner || lock.holds == 0 {
			continue
		}
		lock.holds--
		if lock.holds == 0 {
			lock.owner = ""
			close(lock.released)
			lock.released = make(chan struct{})
		}
	}
	for _, requests := range [2][]uniqueConstraintLockRequest{held, unacquired} {
		for _, request := range requests {
			request.lock.refs--
			if request.lock.refs == 0 && sm.uniqueConstraintCommitLocks[request.key] == request.lock {
				delete(sm.uniqueConstraintCommitLocks, request.key)
			}
		}
	}
}

// engineWriteLockOwners numbers the lock owners of direct engine writes,
// which aren't transactions.
var engineWriteLockOwners atomic.Uint64

// newEngineWriteLockOwner returns a lock owner for one direct engine write.
func newEngineWriteLockOwner() string {
	return "engine-write-" + strconv.FormatUint(engineWriteLockOwners.Add(1), 10)
}

// uniqueMergeKey returns the lock key of value under label's single-property
// UNIQUE constraint (or one-property NODE KEY) on property, as
// lockConstraintKeysOf builds it; ok is false when there's no such
// constraint or value is null.
func (sm *SchemaManager) uniqueMergeKey(label, property string, value interface{}) (uniqueConstraintLockKey, bool) {
	if sm == nil || value == nil {
		return uniqueConstraintLockKey{}, false
	}
	for _, c := range sm.GetConstraintsForLabels([]string{label}) {
		if c.EffectiveEntityType() != ConstraintEntityNode || c.Label != label || len(c.Properties) != 1 || c.Properties[0] != property {
			continue
		}
		if c.Type != ConstraintUnique && c.Type != ConstraintNodeKey {
			continue
		}
		key, ok := uniqueConstraintValueKey(value)
		if !ok {
			return uniqueConstraintLockKey{}, false
		}
		return uniqueConstraintLockKey{label: label, property: property, value: key}, true
	}
	return uniqueConstraintLockKey{}, false
}

// HasUniqueValueTracking reports whether any unique constraint exists whose
// values are tracked in the unique-value cache. When false, callers can skip
// per-value RegisterUniqueValue/UnregisterUniqueValue work entirely.
func (sm *SchemaManager) HasUniqueValueTracking() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.uniqueConstraints) > 0
}

// RegisterUniqueValue registers a value for a unique constraint.
func (sm *SchemaManager) RegisterUniqueValue(label, property string, value interface{}, nodeID NodeID) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	constraint.values[valueKey] = nodeID
	constraint.mu.Unlock()
}

// UnregisterUniqueValue removes a value from a unique constraint.
func (sm *SchemaManager) UnregisterUniqueValue(label, property string, value interface{}) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	delete(constraint.values, valueKey)
	constraint.mu.Unlock()
}
